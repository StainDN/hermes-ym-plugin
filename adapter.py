"""
Yandex Messenger (Яндекс Мессенджер) platform adapter using the Bot API.

Connects to the Yandex Messenger Bot API for inbound updates (polling) and
uses the same API for outbound messages.

The Bot API is OAuth-token based and lives at botapi.messenger.yandex.net.
Updates are fetched with ``messages/getUpdates`` (offset-based cursor), and
outbound text is sent with ``messages/sendText``.

Note on chat addressing:
  * Group chats and channels have a ``chat.id`` (e.g. ``0/0/<guid>``) which is
    passed as the ``chat_id`` parameter of send methods.
  * Private chats have NO meaningful id. The peer is identified by the user's
    ``login``, so for DMs we target the ``login`` parameter instead.
  * Messages inside a thread carry a ``thread_id`` (the timestamp of the
    thread's root message); replies are routed back into that same thread by
    echoing the ``thread_id`` to the send methods.

Configuration in config.yaml:
    gateway:
      platforms:
        ym:
          enabled: true
          extra:
            token: "At..."                 # or YANDEX_BOT_TOKEN env var
            dm_policy: "open"              # open | allowlist | disabled
            allow_from: ["ivan_ivanov"]
            group_policy: "open"           # open | allowlist | disabled
            group_allow_from: ["0/0/<guid>"]

Reference: https://yandex.ru/dev/messenger/doc/ru/
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

try:
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

YANDEX_API_BASE = "https://botapi.messenger.yandex.net/bot/v1/"
POLL_INTERVAL = 1.0  # seconds between getUpdates calls (no long-poll timeout)
POLL_LIMIT = 100  # updates per getUpdates request
RECONNECT_DELAY = 3  # seconds before retrying after an error
MAX_MESSAGE_LENGTH = 6000  # Yandex Messenger text limit
TYPING_TEXT = "печатаю"  # custom processing indicator text (private chats only)

# Quick-command buttons attached to every text reply (suggest_buttons). The
# names are canonical Hermes slash commands; /commands opens Hermes' own
# paginated browser over the full registry. Order mirrors Hermes'
# _TELEGRAM_MENU_PRIORITY (most-typed everyday commands first).
QUICK_COMMANDS: tuple = (
    "help", "new", "stop", "status",
    "resume", "sessions", "model", "commands",
)

PLATFORM_NAME = "ym"

# Endpoints that the Yandex Bot API serves over GET only; everything else is
# POST. Sending POST to these answers 405 "HTTP method POST not allowed".
GET_METHODS = frozenset({"self/get", "chats/getChat"})


def _is_group_chat_id(chat_id: str) -> bool:
    """True when *chat_id* addresses a group chat / channel rather than a user login.

    Group and channel ids look like ``0/0/<guid>`` (they contain a slash); user
    logins never do. This lets ``send()`` pick the right send parameter.
    """
    return "/" in str(chat_id)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class YandexAdapter(BasePlatformAdapter):
    """Yandex Messenger adapter using the Bot API (polling)."""

    supports_code_blocks: bool = False
    typed_command_prefix: str = "/"

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform(PLATFORM_NAME))

        extra = config.extra or {}
        if config.extra is None:
            config.extra = extra
        # Each message thread is an independent Hermes conversation by default
        # (the base build_session_key keeps threads merged unless this is set).
        extra.setdefault("thread_sessions_per_user", True)

        # Auto-thread replies: on a message from the main chat the bot opens a
        # new thread under that message. "group" — group chats only, "all" —
        # also private chats, "off" — reply inline as before.
        # Default to "group": on a message from the main chat the bot opens a
        # thread under it, and replies inside the thread stay routed by the
        # thread anchor (thread_id = the main-chat message id that rooted the
        # thread). The send() safety (drop reply_message_id when thread_id is
        # present) keeps the Bot API happy — it rejects a thread_id paired with
        # reply_message_id, and rejects an in-thread message id used as the
        # thread anchor. "all" — also auto-thread private chats; "off" — reply
        # inline as before (no threads).
        self.thread_replies = str(extra.get("thread_replies", "group")).lower()
        if self.thread_replies not in ("all", "group", "off"):
            self.thread_replies = "group"

        # Quick-command buttons (suggest_buttons) under every text reply.
        # ``quick_commands: false`` turns them off; ``quick_commands_persist``
        # keeps the buttons visible even after newer messages arrive. The
        # button set is built from the live Hermes command registry, falling
        # back to QUICK_COMMANDS if it can't be imported.
        self.quick_commands = bool(extra.get("quick_commands", True))
        self.quick_commands_persist = bool(extra.get("quick_commands_persist", True))
        self._quick_buttons_cache: Optional[dict] = None

        # Auth
        self.token = os.getenv("YANDEX_BOT_TOKEN") or extra.get("token", "")

        # Access policy
        self.dm_policy = extra.get("dm_policy", "open")
        self.group_policy = extra.get("group_policy", "open")
        self.allow_from: List[str] = extra.get("allow_from", [])
        self.group_allow_from: List[str] = extra.get("group_allow_from", [])

        # Env-based allowlist
        env_allowed = os.getenv("YANDEX_ALLOWED_USERS", "").strip()
        if env_allowed:
            self.allow_from = [uid.strip() for uid in env_allowed.split(",") if uid.strip()]
        self.allow_all = os.getenv("YANDEX_ALLOW_ALL_USERS", "").strip().lower() == "true"

        # Polling state
        self._offset: int = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._bot_id: Optional[str] = None
        self._bot_login: Optional[str] = None

        # Rate limiting
        self._last_api_call: float = 0
        self._api_call_delay: float = 0.34  # ~3 calls per second

        # Learned chat addressing: chat_id -> "dm" | "group"
        self._chat_types: Dict[str, str] = {}
        # Raw Yandex chat kind: chat_id -> "private" | "group" | "channel"
        # (channels are collapsed to "group" in _chat_types; quick-command
        # buttons are suppressed for channels, so we keep the raw kind too).
        self._chat_kinds: Dict[str, str] = {}
        # Cache of known users: user id (GUID) -> display name
        self._user_cache: Dict[str, str] = {}

    # ── Access policy ────────────────────────────────────────────────────

    @property
    def enforces_own_access_policy(self) -> bool:
        return self.dm_policy == "allowlist" or self.group_policy == "allowlist"

    def _is_user_allowed(self, user_id: str, chat_type: str) -> bool:
        """Check if a user is allowed to interact with the bot."""
        if self.allow_all:
            return True

        if chat_type == "dm":
            if self.dm_policy == "disabled":
                return False
            if self.dm_policy == "allowlist":
                return user_id in self.allow_from
            return True  # "open"

        if chat_type == "group":
            if self.group_policy == "disabled":
                return False
            if self.group_policy == "allowlist":
                return user_id in self.group_allow_from
            return True  # "open"

        return True

    # ── Yandex Bot API helpers ──────────────────────────────────────────

    async def _api_request(
        self, method: str, params: Optional[Dict[str, Any]] = None,
        http: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Make a Yandex Messenger Bot API request with rate limiting.

        `http` overrides the verb; by default GET is used for the endpoints in
        ``GET_METHODS`` and POST for everything else. GET parameters are sent as
        a query string, POST parameters as a JSON body.

        Returns the parsed JSON body (or a synthetic ``{"ok": False, ...}`` on
        transport failure).
        """
        if not AIOHTTP_AVAILABLE:
            logger.error("aiohttp is not installed — cannot make Yandex API requests")
            return {"ok": False, "description": "aiohttp not installed"}

        # Rate limiting
        now = time.monotonic()
        since_last = now - self._last_api_call
        if since_last < self._api_call_delay:
            await asyncio.sleep(self._api_call_delay - since_last)
        self._last_api_call = time.monotonic()

        url = f"{YANDEX_API_BASE}{method}/"
        headers = {
            "Authorization": f"OAuth {self.token}",
            "Content-Type": "application/json",
        }
        verb = (http or ("GET" if method in GET_METHODS else "POST")).upper()
        timeout = aiohttp.ClientTimeout(total=30)

        try:
            if verb == "GET":
                if params:
                    url = f"{url}?{urlencode(params)}"
                request = self._session.get(url, headers=headers, timeout=timeout)
            else:
                request = self._session.post(
                    url,
                    json=params or {},
                    headers=headers,
                    timeout=timeout,
                )
            async with request as resp:
                data = await resp.json(content_type=None)
        except asyncio.TimeoutError:
            logger.warning("Yandex API request timed out: %s", method)
            return {"ok": False, "description": "timeout"}
        except aiohttp.ClientError as e:
            logger.warning("Yandex API request failed: %s — %s", method, e)
            return {"ok": False, "description": str(e)}
        except Exception as e:
            logger.warning("Yandex API request error: %s — %s", method, e)
            return {"ok": False, "description": str(e)}

        if not data.get("ok", False):
            logger.warning(
                "Yandex API error [%s] params=%s: %s", method, params, data.get("description", "unknown")
            )

        return data

    async def _get_bot_info(self) -> bool:
        """Fetch bot identity via ``self/get`` (also validates the token)."""
        data = await self._api_request("self/get")
        if not data.get("ok"):
            logger.error("Failed to get bot info: %s", data.get("description"))
            return False
        self._bot_id = data.get("id")
        self._bot_login = data.get("login")
        logger.info(
            "Connected as Yandex Messenger bot login=%s id=%s",
            self._bot_login, self._bot_id,
        )
        return True

    async def _get_chat_title(self, chat_id: str) -> str:
        """Get the title of a group chat or channel via ``chats/getChat``."""
        data = await self._api_request("chats/getChat", {"chat_id": chat_id})
        if data.get("ok"):
            chat = data.get("data", {})
            title = chat.get("name") or chat.get("title")
            if title:
                return title
        return chat_id

    @staticmethod
    def _thread_param(metadata) -> Optional[int]:
        """Extract a valid integer ``thread_id`` from send metadata.

        Hermes carries ``event.source.thread_id`` in ``metadata["thread_id"]``
        as a string; the Yandex Bot API wants an integer. Returns ``None`` for
        missing/zero/non-numeric values so the request stays thread-agnostic.
        """
        if not metadata:
            return None
        raw = metadata.get("thread_id")
        if raw in (None, "", 0):
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            logger.debug("Ignoring non-numeric thread_id: %s", raw)
            return None

    def _quick_command_buttons(self) -> dict:
        """Build the ``suggest_buttons`` keyboard with Hermes slash commands.

        Command names come from the live Hermes registry (``hermes_cli.commands``)
        so the menu tracks the installed version; if that import is unavailable
        we fall back to :data:`QUICK_COMMANDS`. Buttons carry a ``send_message``
        directive, so pressing one is equivalent to the user typing the command
        (``/help`` etc.) — the gateway then routes it to the normal
        slash-command handler. Cached after first build.
        """
        if self._quick_buttons_cache is not None:
            return self._quick_buttons_cache

        available: set = set()
        try:
            from hermes_cli.commands import COMMAND_REGISTRY, _is_gateway_available
            available = {
                cmd.name for cmd in COMMAND_REGISTRY
                if _is_gateway_available(cmd)
            }
        except Exception as exc:  # registry moved/unavailable — keep the menu
            logger.debug("Quick commands: Hermes registry unavailable (%s)", exc)

        names = [n for n in QUICK_COMMANDS if not available or n in available]
        if not names:
            names = list(QUICK_COMMANDS)

        buttons = [
            {
                "id": name,
                "title": f"/{name}",
                "directives": [{"type": "send_message", "text": f"/{name}"}],
            }
            for name in names
        ]
        self._quick_buttons_cache = {
            "layout": "true",
            "persist": self.quick_commands_persist,
            "buttons": [buttons[i:i + 4] for i in range(0, len(buttons), 4)],
        }
        return self._quick_buttons_cache

    # ── Polling loop ───────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Main polling loop — receives updates via ``messages/getUpdates``."""
        while self._running:
            data = await self._api_request(
                "messages/getUpdates",
                {"limit": POLL_LIMIT, "offset": self._offset},
            )

            if not data.get("ok", False):
                await asyncio.sleep(RECONNECT_DELAY)
                continue

            updates = data.get("updates", []) or []
            if not updates:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            logger.info("Received %d Yandex Messenger updates", len(updates))

            max_update_id = self._offset - 1
            for update in updates:
                try:
                    await self._process_update(update)
                except Exception:
                    logger.exception("Failed to process Yandex update: %s", update)
                update_id = update.get("update_id")
                if isinstance(update_id, int) and update_id > max_update_id:
                    max_update_id = update_id

            # Advance the cursor past every received update so the server can
            # forget them (getUpdates drops updates with update_id < offset).
            self._offset = max_update_id + 1

    async def _process_update(self, update: dict) -> None:
        """Process a single inbound update.

        Only text messages are handled. Stickers, files, images, reactions and
        membership events are ignored.
        """
        text = (update.get("text") or "").strip()
        if not text:
            return

        sender = update.get("from") or {}
        chat = update.get("chat") or {}

        # Ignore messages sent by bots (including our own) to avoid echo loops.
        if sender.get("robot"):
            return
        if self._bot_id and sender.get("id") == self._bot_id:
            return

        chat_type_raw = chat.get("type")
        from_id = str(sender.get("id", ""))
        display_name = sender.get("display_name") or sender.get("login") or from_id
        message_id = update.get("message_id", 0)

        # Messages inside a thread carry a ``thread_id`` in the ``chat`` object.
        # It is an integer — the timestamp of the thread's root message (anchor).
        thread_id: Optional[str] = None
        thread_raw = chat.get("thread_id")
        if thread_raw in (None, "", 0):
            thread_raw = update.get("thread_id")
        if thread_raw not in (None, "", 0):
            try:
                thread_id = str(int(thread_raw))
            except (TypeError, ValueError):
                logger.debug("Ignoring non-numeric thread_id: %r", thread_raw)

        if chat_type_raw == "private":
            # Private chat has no meaningful id — address the peer by login.
            login = sender.get("login")
            if not login:
                logger.debug("Skipping private message without sender login")
                return
            chat_type = "dm"
            chat_id = login
            user_id = from_id
            user_name = display_name
            chat_name = display_name
            display_text = text
        elif chat_type_raw in ("group", "channel"):
            chat_id = str(chat.get("id", ""))
            if not chat_id:
                logger.debug("Skipping %s message without chat id", chat_type_raw)
                return
            chat_type = "group"
            user_id = from_id
            user_name = display_name
            chat_name = chat_id
            # In group chats, prefix with the user's name (as VK adapter does).
            display_text = f"[{display_name}] {text}" if display_name else text
        else:
            logger.debug("Ignored Yandex chat type: %s", chat_type_raw)
            return

        # Auto-thread: a message from the main chat (no thread_id yet) opens a
        # new thread anchored under that message, so the reply starts the
        # thread and the dialogue continues inside it. Scope is controlled by
        # ``thread_replies``; channels stay inline (threads don't fit them).
        if thread_id is None:
            auto_thread = (
                self.thread_replies == "all"
                or (self.thread_replies == "group" and chat_type_raw == "group")
            )
            if auto_thread and message_id:
                thread_id = str(int(message_id))

        # Access control
        if not self._is_user_allowed(user_id, chat_type):
            logger.info("User %s not allowed (chat_type=%s)", user_id, chat_type)
            return

        # Remember how to address this chat on the way out.
        self._chat_types[chat_id] = chat_type
        self._chat_kinds[chat_id] = chat_type_raw
        self._user_cache[user_id] = display_name

        event = MessageEvent(
            text=display_text,
            message_type=MessageType.TEXT,
            message_id=str(message_id),
            raw_message=update,
        )

        # When auto-threading is off, don't propagate the inbound thread_id:
        # replying with it would route the send into a thread (and the Bot API
        # rejects an in-thread message id used as a thread anchor). Respond
        # inline in the main chat instead. With threads on (group/all) the
        # inbound thread_id is the thread's main-chat anchor and is exactly
        # what we need to keep replying inside the thread.
        if self.thread_replies == "off":
            thread_id = None

        event.source = SessionSource(
            platform=Platform(PLATFORM_NAME),
            chat_id=chat_id,
            user_id=user_id,
            user_name=user_name,
            chat_name=chat_name,
            chat_type=chat_type,
            thread_id=thread_id,
            message_id=str(message_id),
        )

        # Forward to Hermes
        await self.handle_message(event)

    # ── BasePlatformAdapter interface ───────────────────────────────────

    async def connect(self, is_reconnect: bool = False) -> bool:
        """Connect to the Yandex Messenger Bot API and start polling."""
        if not AIOHTTP_AVAILABLE:
            logger.error(
                "aiohttp is required for Yandex Messenger adapter. "
                "Install: pip install aiohttp"
            )
            return False

        if not self.token:
            logger.error(
                "YANDEX_BOT_TOKEN is not set. "
                "Set it in .env or config.yaml (gateway.platforms.ym.extra.token)"
            )
            return False

        self._session = aiohttp.ClientSession()

        # Validate the token and learn our own identity.
        if not await self._get_bot_info():
            await self._session.close()
            self._session = None
            return False

        self._mark_connected()

        # Start polling in background
        self._poll_task = asyncio.create_task(self._poll_loop())

        logger.info(
            "Yandex Messenger adapter connected (dm_policy=%s, group_policy=%s)",
            self.dm_policy, self.group_policy,
        )
        return True

    async def disconnect(self) -> None:
        """Disconnect from the Yandex Messenger Bot API."""
        self._running = False

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        if self._session:
            await self._session.close()
            self._session = None

        self._mark_disconnected()
        logger.info("Yandex Messenger adapter disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to a Yandex Messenger chat."""
        if not content:
            return SendResult(success=True, message_id=None)

        content = content[:MAX_MESSAGE_LENGTH]

        params: Dict[str, Any] = {
            "text": content,
            "payload_id": f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
        }

        # Group/channel → chat_id param; private chat → login param.
        if _is_group_chat_id(chat_id) or self._chat_types.get(chat_id) == "group":
            params["chat_id"] = chat_id
        else:
            params["login"] = chat_id

        reply_pid: Optional[int] = None
        if reply_to:
            try:
                reply_pid = int(reply_to)
            except (TypeError, ValueError):
                logger.debug("Ignoring non-numeric reply_to: %s", reply_to)

        # Reply inside the same thread the user wrote in, if any.
        thread_id = self._thread_param(metadata)
        # When opening a NEW thread anchored on the incoming message
        # (thread_id == reply target), skip the redundant quote — the message
        # is already the thread root.
        if reply_pid is not None and not (thread_id is not None and thread_id == reply_pid):
            params["reply_message_id"] = reply_pid
        if thread_id is not None:
            params["thread_id"] = thread_id
        # Safety: the Bot API rejects sendText that carries BOTH thread_id and
        # reply_message_id (and rejects reply_message_id pointing at an
        # in-thread message). If we are routing into a thread, drop the quote —
        # the thread_id already anchors the conversation context.
        if thread_id is not None and "reply_message_id" in params:
            params.pop("reply_message_id", None)

        # Quick-command buttons under the reply. Skipped in channels (menus
        # don't fit there); attached to text replies only.
        if self.quick_commands and self._chat_kinds.get(chat_id) != "channel":
            params["suggest_buttons"] = self._quick_command_buttons()

        logger.debug(
            "ym sendText target=%s thread_id=%s reply=%s content_len=%d",
            chat_id, params.get("thread_id"), params.get("reply_message_id"), len(content),
        )
        data = await self._api_request("messages/sendText", params)
        if not data.get("ok", False):
            logger.warning(
                "Failed to send message to %s: %s", chat_id, data.get("description")
            )
            return SendResult(
                success=False,
                error=f"Yandex API error: {data.get('description', 'unknown')}",
            )

        msg_id = data.get("message_id")
        return SendResult(success=True, message_id=str(msg_id) if msg_id else None)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Show a typing/processing indicator while the bot is generating a reply.

        The base-class ``_keep_typing`` heartbeat calls this every ~2s with
        ``metadata=`` and stops it once the reply is delivered. ``metadata``
        must be accepted here or the heartbeat fails the call and the
        indicator silently never appears.

        Private chats use ``type=processing`` with a custom text (the only way
        to show arbitrary status text); group chats and channels fall back to
        the standard ``type=text`` indicator because ``processing`` is not
        available there.
        """
        if _is_group_chat_id(chat_id) or self._chat_types.get(chat_id) == "group":
            params: Dict[str, Any] = {"chat_id": chat_id, "type": "text"}
        else:
            params = {
                "login": chat_id,
                "type": "processing",
                "processing_content": {"display": "text", "text": TYPING_TEXT},
            }
        # Longer than the 2s heartbeat interval so a slow tick never lets the
        # indicator lapse between refreshes.
        params["timeout"] = 6
        thread_id = self._thread_param(metadata)
        if thread_id is not None:
            params["thread_id"] = thread_id
        await self._api_request("messages/sendTyping", params)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Get information about a Yandex Messenger chat."""
        if _is_group_chat_id(chat_id) or self._chat_types.get(chat_id) == "group":
            title = await self._get_chat_title(chat_id)
            return {"name": title, "type": "group", "chat_id": chat_id}

        # DM — we only know the login; reuse a cached display name if present.
        for uid, name in self._user_cache.items():
            if name == chat_id:
                return {"name": name, "type": "dm", "chat_id": chat_id}
        return {"name": chat_id, "type": "dm", "chat_id": chat_id}

    def format_message(self, content: str) -> str:
        """Format message for Yandex Messenger. Plain text only."""
        return content


# ── Plugin entry point ─────────────────────────────────────────────────


def check_requirements() -> bool:
    """Check if Yandex Messenger adapter requirements are met."""
    if not AIOHTTP_AVAILABLE:
        return False
    return bool(os.getenv("YANDEX_BOT_TOKEN"))


def validate_config(config) -> bool:
    """Validate Yandex Messenger adapter configuration."""
    extra = getattr(config, "extra", {}) or {}
    token = os.getenv("YANDEX_BOT_TOKEN") or extra.get("token", "")
    return bool(token)


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Auto-enable from environment variables."""
    token = os.getenv("YANDEX_BOT_TOKEN", "").strip()
    if not token:
        return None

    seed: Dict[str, Any] = {"token": token}

    # Home channel for cron delivery
    home = os.getenv("YANDEX_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": "Yandex Messenger Home"}

    return seed


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Yandex Messenger",
        adapter_factory=lambda cfg: YandexAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["YANDEX_BOT_TOKEN"],
        install_hint="pip install aiohttp",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="YANDEX_HOME_CHANNEL",
        allowed_users_env="YANDEX_ALLOWED_USERS",
        allow_all_env="YANDEX_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=(
            "You are chatting via Yandex Messenger (Яндекс Мессенджер). "
            "It supports plain text messages. "
            "Use /commands for Hermes controls."
        ),
        emoji="💬",
    )
