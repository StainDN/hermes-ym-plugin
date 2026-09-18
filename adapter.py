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
import re
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

PLATFORM_NAME = "ym"

# Endpoints that the Yandex Bot API serves over GET only; everything else is
# POST. Sending POST to these answers 405 "HTTP method POST not allowed".
GET_METHODS = frozenset({"self/get", "chats/getChat"})

# Authorization approval command typed by the admin: "/auth <code>". The code
# is the 8-char pairing code produced by the Hermes core PairingStore.
AUTH_APPROVE_RE = re.compile(r"^/?auth[ \t]+([A-Z0-9]{8})[ \t]*$", re.IGNORECASE)
# Must match the core's PairingStore TTL (gateway/pairing.py CODE_TTL_SECONDS).
AUTH_CODE_TTL = 3600


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

        # Feed channels (e.g. an error stream) are written by OTHER bots; accept
        # their posts in groups/channels when enabled. DMs from robots stay
        # subject to the DM policy, so this cannot open a private back door.
        self.allow_robot_senders = bool(extra.get("allow_robot_senders", False))

        # Analysis-only ("silent") group chats: the bot READS them but never
        # replies in the group — the analysis is routed to the owner's DM and
        # the agent answers with a silent marker when nothing is worth
        # reporting (the gateway drops such turns).
        self.silent_chats = [str(c) for c in (extra.get("silent_chats") or [])]
        self.silent_dm_target = str(extra.get("silent_dm_target") or "")
        self.silent_dm_user_id = str(extra.get("silent_dm_user_id") or "")
        self.silent_dm_user_name = str(extra.get("silent_dm_user_name") or "")

        # Admin-gated authorization. When enabled, only the admin, users
        # already approved in the core PairingStore, and the explicit
        # allowlists above may talk to the bot. New private-chat users get a
        # pairing code, which is also forwarded to the admin; the admin
        # approves with "/auth <code>" and the user is paired permanently.
        self.auth_enabled = bool(extra.get("auth_enabled", False))
        self.auth_admin_login = str(extra.get("auth_admin_login", "") or "").strip().lower()

        # Back-reference injected by the gateway runner after creation so
        # plugin adapters can reach core services (here: the PairingStore).
        # See run.py _create_adapter: "gateway_runner" attribute injection.
        self.gateway_runner = None
        # Cached admin user id (GUID) — learned from the first admin message
        # so the admin is recognized even if their login ever differs.
        self._auth_admin_user_id: Optional[str] = None
        # Codes handed out by this adapter: code -> request context. Kept in
        # memory so "/auth" only ever hits codes we actually issued (a random
        # string must not count as a failed approval against the core store).
        self._issued_codes: Dict[str, Dict[str, Any]] = {}

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
        # Cache of known users: user id (GUID) -> display name
        self._user_cache: Dict[str, str] = {}
        # Thread ids created by auto-threading (not yet "activated" by a
        # successful sendText). Typing indicator for these goes to the main
        # chat, because the thread doesn't exist on Yandex's side yet.
        self._pending_threads: set = set()

    # ── Access policy ────────────────────────────────────────────────────

    @property
    def enforces_own_access_policy(self) -> bool:
        return (
            self.auth_enabled
            or self.dm_policy == "allowlist"
            or self.group_policy == "allowlist"
        )

    def _is_user_allowed(self, user_id: str, chat_type: str, chat_id: str = "") -> bool:
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
                # Accept either the sender's id or the chat's own id
                # (``0/0/<guid>``) — the latter is how a feed channel that is
                # written by other bots is allowlisted.
                return user_id in self.group_allow_from or chat_id in self.group_allow_from
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

    # ── Admin-gated authorization ─────────────────────────────────────

    def _pairing_store(self):
        """Return the core PairingStore, or None when unavailable.

        The store is reached through ``gateway_runner``, which the gateway
        injects on plugin adapters that declare the attribute. Without it
        (tests, unusual runtimes) authorization degrades to the plain
        allowlist behaviour.
        """
        runner = getattr(self, "gateway_runner", None)
        if runner is None:
            return None
        store = getattr(runner, "pairing_store", None)
        if store is None:
            return None
        return store

    def _prune_issued_codes(self) -> None:
        """Drop handed-out codes older than the core's code TTL."""
        if not self._issued_codes:
            return
        now = time.time()
        stale = [
            code for code, ctx in self._issued_codes.items()
            if now - ctx.get("issued_at", 0) > AUTH_CODE_TTL
        ]
        for code in stale:
            self._issued_codes.pop(code, None)

    def _is_auth_admin_user(self, from_id: str, login: str) -> bool:
        """True when the sender is the configured authorization admin."""
        if not self.auth_admin_login and self._auth_admin_user_id is None:
            return False
        if self._auth_admin_user_id and from_id == self._auth_admin_user_id:
            return True
        if login and self.auth_admin_login and login.strip().lower() == self.auth_admin_login:
            return True
        return False

    def _approve_admin_in_core(self, from_id: str, display_name: str) -> None:
        """Put the admin into the core approved list so forwarded messages pass.

        The admin is trusted by login, not by a code, so instead of a pairing
        exchange we add them to the PairingStore approved list directly
        (``_approve_user`` must run under the store lock). No-op when the
        store is unavailable.
        """
        store = self._pairing_store()
        if store is None:
            return
        try:
            with store._lock:
                store._approve_user("ym", from_id, display_name)
        except Exception:
            logger.exception("Failed to auto-approve admin %s in pairing store", from_id)

    async def _send_to_admin(self, text: str) -> None:
        """Deliver a message to the admin's private chat by login."""
        if not self.auth_admin_login:
            return
        try:
            await self.send(self.auth_admin_login, text)
        except Exception:
            logger.exception("Failed to notify authorization admin")

    def _parse_auth_code(self, text: str) -> Optional[str]:
        """Extract the pairing code from an admin ``/auth <code>`` message."""
        match = AUTH_APPROVE_RE.match(text.strip())
        if not match:
            return None
        return match.group(1).upper()

    async def _handle_admin_message(self, chat_id: str, from_id: str,
                                    display_name: str, text: str) -> bool:
        """Process an admin message that carries an approval command.

        Returns True when the message was consumed as an authorization act
        (the caller must NOT forward it to the core) and False when it is a
        normal admin message that should flow through as usual.
        """
        if self._auth_admin_user_id is None:
            self._auth_admin_user_id = from_id
            self._approve_admin_in_core(from_id, display_name)

        code = self._parse_auth_code(text)
        if code is None:
            return False

        self._prune_issued_codes()
        ctx = self._issued_codes.get(code)
        store = self._pairing_store()
        if store is None or ctx is None:
            # Never call approve_code for codes we have not issued — a random
            # string must not increment the core's failed-approval counter.
            await self.send(chat_id, "Код не найден или устарел. Запросите новый код.")
            return True

        approved = store.approve_code("ym", code)
        if not approved:
            await self.send(chat_id, "Не удалось одобрить код. Попробуйте ещё раз.")
            return True

        self._issued_codes.pop(code, None)
        user_id = approved.get("user_id") or ctx.get("user_id") or ""
        user_name = approved.get("user_name") or ctx.get("user_name") or ""
        logger.info(
            "Authorization admin approved code for user %s (%s) on ym",
            user_name, user_id,
        )
        await self.send(
            chat_id, f"Доступ выдан. Пользователь: {user_name} ({ctx.get('login', '')}).",
        )
        user_login = ctx.get("login")
        if user_login:
            await self.send(user_login, "Вы авторизованы. Теперь можете общаться с ботом.")
        return True

    async def _handle_unauthorized_user(self, chat_id: str, from_id: str, login: str,
                                        display_name: str, chat_type: str) -> None:
        """Answer an unauthorized user with a pairing code (DMs only).

        In group chats / channels unauthorized members get nothing at all.
        """
        if chat_type != "dm" or not login:
            logger.info("Ignoring unauthorized user %s in %s", from_id, chat_type)
            return

        self._prune_issued_codes()
        store = self._pairing_store()
        if store is None:
            logger.info("Pairing store unavailable — ignoring unauthorized user %s", from_id)
            return

        code = store.generate_code("ym", from_id, display_name)
        if not code:
            logger.info(
                "Pairing code not issued for %s (rate-limited/lockout/limit)", from_id
            )
            return

        # Keep the code in memory so /auth only approves codes we know about.
        self._issued_codes[code.upper()] = {
            "login": login,
            "user_id": from_id,
            "user_name": display_name,
            "issued_at": time.time(),
        }
        logger.info("Issued authorization code for new user %s on ym", from_id)

        await self.send(
            chat_id,
            f"Доступ к боту ограничен.\n\n"
            f"Ваш код авторизации: {code}\n\n"
            f"Передайте его администратору — после подтверждения вы сможете пользоваться ботом.",
        )
        await self._send_to_admin(
            f"Запрос на авторизацию:\n"
            f"Имя: {display_name}\n"
            f"Логин: {login}\n"
            f"ID: {from_id}\n"
            f"Код: {code}\n\n"
            f"Для одобрения отправьте: /auth {code}",
        )

    def _is_user_authorized_local(self, from_id: str) -> bool:
        """Whether a non-admin sender may interact with the bot.

        True for core-approved (paired) users and the explicit allowlist /
        allow-all admittance. Otherwise False — a new user must be paired.
        """
        if self.allow_all:
            return True
        if from_id in self.allow_from:
            return True
        store = self._pairing_store()
        if store is not None:
            try:
                return bool(store.is_approved("ym", from_id))
            except Exception:
                logger.exception("Failed to check pairing store for %s", from_id)
        return False

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

        chat_type_raw = chat.get("type")

        # Ignore messages sent by OUR OWN bot (echo loop). Posts by OTHER bots
        # are dropped too, unless allow_robot_senders is on and they land in a
        # group/channel (reading a bot-written feed channel); robot DMs still
        # fall through to the DM policy below.
        if self._bot_id and sender.get("id") == self._bot_id:
            return
        if sender.get("robot") and not (
            self.allow_robot_senders and chat_type_raw in ("group", "channel")
        ):
            return

        from_id = str(sender.get("id", ""))
        display_name = sender.get("display_name") or sender.get("login") or from_id
        login = sender.get("login") or ""
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
                self._pending_threads.add(thread_id)

        # A bot-written feed channel is admitted by the chat-id group allowlist
        # below and has no pairing code, so the auth gate must not swallow it.
        robot_feed = bool(
            self.allow_robot_senders and sender.get("robot") and chat_type == "group"
        )

        # Admin-gated authorization. When enabled, the access gate is: admin
        # (recognized by login/user id) may always talk and may approve codes
        # with "/auth <code>"; approved-and-allowlisted users may talk; anyone
        # else is denied — a new private-chat user gets a pairing code (also
        # forwarded to the admin) and is otherwise not answered at all.
        if self.auth_enabled and not robot_feed:
            if self._is_auth_admin_user(from_id, login):
                consumed = await self._handle_admin_message(
                    chat_id, from_id, display_name, text,
                )
                if consumed:
                    return
            elif not self._is_user_authorized_local(user_id):
                await self._handle_unauthorized_user(
                    chat_id, from_id, login, display_name, chat_type,
                )
                return

        # Access control
        if not self._is_user_allowed(user_id, chat_type, chat_id):
            logger.info(
                "User %s not allowed (chat_type=%s chat_id=%s)", user_id, chat_type, chat_id
            )
            return

        # Analysis-only ("silent") group chats: never answer in the group. The
        # message is re-addressed to the owner's DM, so the agent's analysis
        # lands in the DM; when nothing is worth reporting the agent answers
        # NO_REPLY and the gateway suppresses delivery entirely.
        if chat_type == "group" and chat_id in self.silent_chats and self.silent_dm_target:
            title = await self._get_chat_title(chat_id)
            origin = f"«{title}»" if title and title != chat_id else chat_id
            author = display_name or from_id
            display_text = (
                f"[Пересылка из группового чата {origin}. Автор — {author}. "
                "Это НЕ личное сообщение, и отвечать в группу нельзя: твой ответ "
                "уйдёт Дмитрию в личку. Разбери сообщение по сути — если в нём "
                "проблема, ошибка, вопрос или что-то, требующее действий Дмитрия, "
                "дай краткий разбор (и следующий шаг, если он очевиден). Если "
                "ничего существенного (рабочий трёп, обсуждение не по нашей части, "
                "информационный шум) — ответь ровно NO_REPLY.]\n\n"
                f"{text}"
            )
            chat_id = self.silent_dm_target
            chat_type = "dm"
            user_id = self.silent_dm_user_id or user_id
            display_name = self.silent_dm_user_name or display_name
            user_name = display_name
            chat_name = display_name
            thread_id = None

        # Remember how to address this chat on the way out.
        self._chat_types[chat_id] = chat_type
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
        if self.auth_enabled and not self.auth_admin_login:
            logger.error(
                "auth_enabled is true but auth_admin_login is not set — "
                "no one can approve pairing codes; set extra.auth_admin_login"
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

        # Analysis-only ("silent") chats must never receive a public message.
        # The inbound path already re-addresses such messages to the owner's DM,
        # but a turn can still target the group directly: one accepted before
        # the silent routing existed (queued over a gateway restart and
        # auto-resumed afterwards), or a synthetic notice (shutdown, watch).
        # Catch it at delivery time too and re-address it to the same DM.
        if (
            self.silent_dm_target
            and chat_id != self.silent_dm_target
            and chat_id in self.silent_chats
        ):
            logger.info(
                "ym silent chat %s: re-addressing outgoing message to %s (len=%d)",
                chat_id, self.silent_dm_target, len(content),
            )
            chat_id = self.silent_dm_target
            reply_to = None
            metadata = None
            self._chat_types[chat_id] = "dm"

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
        # Thread is now "activated" on Yandex's side — typing indicator can
        # target it from now on.
        tid = params.get("thread_id")
        if tid is not None:
            self._pending_threads.discard(str(tid))
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
        # If the thread was just created by auto-threading and hasn't been
        # "activated" by a successful sendText yet, show typing in the main
        # chat — the thread doesn't exist on Yandex's side for sendTyping.
        if thread_id is not None and thread_id in self._pending_threads:
            thread_id = None
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


# ── Out-of-process delivery (``hermes send`` / cron detached from the gateway) ──

async def _standalone_send(
    pconfig, chat_id: str, message: str, *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Send one text message through the Bot API without a running gateway.

    A process that does not own a gateway adapter (``hermes send --to ym:...``,
    a cron job spawned outside the gateway) reaches the platform through this
    function instead of the live-adapter path. Media has no upload endpoint
    here, so attachments are reported in the text rather than silently dropped.
    """
    extra = getattr(pconfig, "extra", {}) or {}
    token = os.getenv("YANDEX_BOT_TOKEN") or extra.get("token", "")
    if not token:
        return {"error": "Yandex Messenger standalone send: no token (set YANDEX_BOT_TOKEN)"}
    if not chat_id:
        return {"error": "Yandex Messenger standalone send: no chat_id"}

    text = message or ""
    if media_files:
        note = f"[{len(media_files)} attachment(s) generated but not deliverable out-of-process]"
        text = f"{text}\n{note}".strip()
    if not text.strip():
        return {"error": "Yandex Messenger standalone send: empty message"}
    text = text[:MAX_MESSAGE_LENGTH]

    # The core target parser only splits the platform prefix, so a thread
    # arrives glued to the chat id — both as ``--to ym:<group>:<thread>`` and
    # as the channel-directory entry ``0/0/<guid>:<thread>``. Split it back out.
    if not thread_id and ":" in str(chat_id):
        head, _, tail = str(chat_id).rpartition(":")
        if head and tail.isdigit():
            chat_id, thread_id = head, tail

    params: Dict[str, Any] = {
        "text": text,
        "payload_id": f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
    }
    # Group/channel → chat_id param; private chat → login param.
    if _is_group_chat_id(chat_id):
        params["chat_id"] = chat_id
    else:
        params["login"] = chat_id
    if thread_id:
        try:
            params["thread_id"] = int(thread_id)
        except (TypeError, ValueError):
            return {"error": f"Yandex Messenger standalone send: bad thread_id {thread_id!r}"}

    if aiohttp is None:  # AIOHTTP_AVAILABLE guard, narrowed for type checkers
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{YANDEX_API_BASE}messages/sendText/",
                json=params,
                headers={
                    "Authorization": f"OAuth {token}",
                    "Content-Type": "application/json",
                },
            ) as resp:
                data = await resp.json(content_type=None)
    except Exception as exc:  # aiohttp errors, JSON errors, DNS, timeouts
        logger.warning("Standalone sendText to %s failed: %s", chat_id, exc)
        return {"error": f"Yandex Messenger standalone send failed: {exc}"}

    if not data.get("ok", False):
        return {"error": f"Yandex API error: {data.get('description', 'unknown')}"}
    return {"success": True, "message_id": data.get("message_id")}


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
        # Out-of-process delivery (hermes send / cron outside the gateway).
        standalone_sender_fn=_standalone_send,
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
