# Hermes Yandex Messenger Plugin

Плагин для интеграции [Hermes Agent](https://github.com/NousResearch/hermes-agent) с Яндекс Мессенджером через Bot API.

**Лицензия:** MIT

## Возможности

- Подключение к Bot API Яндекс Мессенджера в реальном времени (polling)
- Приём и обработка входящих сообщений (приватные + групповые чаты)
- Отправка ответов от имени бота
- Индикатор «печатаю» в личных чатах и «печатает…» в группах во время ответа (`sendTyping`)
- Ответы возвращаются в тот же тред (ветку обсуждения), откуда пришло сообщение
- Новое сообщение из основного чата группы открывает новый тред, и диалог продолжается в нём (`thread_replies`)
- Авто-регистрация платформы `ym` (не требует правок core Hermes)
- Политики доступа: `open`, `allowlist`, `disabled`
- Отправка в групповой чат по `chat_id`, в личку — по логину пользователя

## Установка

```bash
# Вариант 1: установка из репозитория
hermes plugins install https://github.com/StainDN/hermes-ym-plugin
hermes plugins enable ym

# Вариант 2: вручную
git clone https://github.com/StainDN/hermes-ym-plugin ~/.hermes/plugins/hermes-ym-plugin
hermes plugins enable ym

# aiohttp обязателен (обычно уже есть в venv Hermes)
python3 -c "import aiohttp" || pip install aiohttp
```

После включения плагина: заполнить `.env`, прописать `gateway.platforms.ym.enabled: true`,
затем `hermes -p <профиль> gateway restart` и проверить в логе `✓ ym connected`.

## Конфигурация

### .env (обязательно)

```env
YANDEX_BOT_TOKEN=At.YourBotTokenHere
```

### config.yaml

```yaml
gateway:
  platforms:
    ym:
      enabled: true
      extra:
        token: "At.YourBotTokenHere"   # можно не указывать, если задан YANDEX_BOT_TOKEN
        dm_policy: open                # open | allowlist | disabled
        group_policy: open             # open | allowlist | disabled
        thread_replies: group          # group | all | off — авто-новый тред при ответе
```

### Опционально

```env
YANDEX_ALLOWED_USERS=0f361203-fe57-9e7f-41fd-e0e9afdf4b0a   # UUID пользователей, НЕ логины
YANDEX_ALLOW_ALL_USERS=true                                  # Разрешить всех
YANDEX_HOME_CHANNEL=0/0/<guid>                               # Чат для cron-уведомлений (chat_id или логин)
```

> **`YANDEX_ALLOWED_USERS` принимает UUID, а не логины.** Ядро Hermes авторизует
> входящее сообщение по `SessionSource.user_id`, а для этой платформы там лежит
> `from.id` из `getUpdates` — UUID вида `0f361203-fe57-9e7f-41fd-e0e9afdf4b0a`.
> Логин (`ivan_ivanov`) не совпадёт никогда, и гейтвей молча выбросит сообщение,
> написав в лог `Unauthorized user: <uuid> (<ФИО>) on ym`. UUID можно вытащить из
> лога первого отклонённого сообщения либо из `getUpdates`; он стабилен для
> пользователя. Альтернатива — `YANDEX_ALLOW_ALL_USERS=true` (бот и так виден
> только сотрудникам организации).

## Создание бота

1. Откройте [Боты в Мессенджере](https://admin.yandex.ru/bot-platform) (Яндекс 360 для бизнеса).
2. Создайте бота и скопируйте его OAuth-токен.
3. Укажите токен в `YANDEX_BOT_TOKEN`.

## Запуск

```bash
# Убить старые процессы (если есть)
pkill -f 'hermes gateway'

# Запустить gateway с Яндекс Мессенджером
hermes gateway run --verbose
```

## Как это работает

1. Плагин регистрирует платформу `ym` через `platform_registry`
2. Gateway при старте создаёт `YandexAdapter` и вызывает `connect()`
3. Адаптер проверяет токен через `self/get`
4. Запускается `_poll_loop()` — цикл опроса `messages/getUpdates`
5. При получении сообщения создаётся `MessageEvent` и передаётся в Hermes
6. Hermes обрабатывает сообщение и отправляет ответ через `messages/sendText`

## Структура файлов

```
hermes-ym-plugin/
├── plugin.yaml    # Метаданные плагина
├── __init__.py    # Точка входа, регистрация платформы
├── adapter.py     # YandexAdapter — polling + Bot API
└── README.md      # Этот файл
```

## Требования

- Hermes Agent (совместимо с версией, где есть `Platform._missing_()`)
- Python 3.11+
- aiohttp
- Бот Яндекс Мессенджера с OAuth-токеном

## Известные особенности

- **Адресация чатов:** у приватного чата в Яндекс Мессенджере нет значимого `chat_id`, поэтому ответы в личку отправляются по `login` собеседника. Групповые чаты и каналы адресуются по `chat_id` (формат `0/0/<guid>`).
- **Bot API метод-строгий.** Эндпоинты чтения (`self/get`, `chats/getChat`) принимают **только GET** (параметры — в query string), `messages/*` — **только POST** (JSON-body). POST на `self/get` отвечает `405 http_method_not_allowed`, GET на `messages/sendText` — тем же. Адаптер выбирает метод по эндпоинту (`GET_METHODS` в `adapter.py`); при добавлении новых методов проверяйте оба варианта.
- **`.env` парсится строго:** только `KEY=value`, без пробелов вокруг `=`. Строка `YANDEX_BOT_TOKEN = At...` не подхватится — платформа просто не запустится.
- **Проверка токена:** `self/get` возвращает карточку бота (`login`, `id`, `organizations`). Не `ok: true` — значит проблема с токеном, смотреть лог `hermes_plugins.ym.adapter`.
- **`YANDEX_ALLOWED_USERS` — UUID, не логин** (см. выше).
- **Polling без long-poll:** метод `getUpdates` не поддерживает удержание соединения, поэтому адаптер опрашивает сервер с интервалом ~1 секунда и продвигает курсор `offset = max(update_id) + 1`.
- **Лимит сообщения:** 6000 символов.
- **`payload_id`:** каждому исходящему сообщению присваивается уникальный `payload_id` — повторные запросы с тем же ID трактуются Яндексом как дубликаты.
- **Фильтр роботов:** сообщения от ботов (`from.robot`) игнорируются, чтобы избежать эхо-циклов.
- **Индикатор «печатаю»:** базовый heartbeat Hermes (`_keep_typing`) вызывает `send_typing(chat_id, metadata=...)` каждые ~2 секунды автоматически. Сигнатура адаптера обязана принимать `metadata=None` — иначе вызов падает с `TypeError` и индикатор молча не отправляется. Кастомный текст «печатаю» (`type=processing`) доступен только в личных чатах; в группах/каналах используется стандартный `type=text`.
- **Треды:** обновление сообщения из треда несёт `thread_id` на верхнем уровне (integer — timestamp корневого сообщения, в официальной таблице типа `Update` поле не описано). Адаптер проставляет его в `event.source.thread_id`, и база сама прокидывает его в `metadata`; `send()` и `send_typing()` передают `thread_id` в соответствующие методы Bot API (как int). Каждый тред — независимая сессия Hermes (`thread_sessions_per_user`, включено по умолчанию; отключить — `extra.thread_sessions_per_user: false`).
- **Авто-новый тред (`thread_replies`):** по умолчанию `group` — сообщение из основного чата группового чата заставляет бота открыть **новый** тред, взяв в качестве якоря `thread_id = message_id` этого сообщения (первое сообщение треда — ответ бота, без дублирующей цитаты). Диалог продолжается внутри треда: последующие сообщения из треда несут тот же `thread_id` и ответы идут в него же. `all` — то же и в личных чатах; `off` — ответ inline, как раньше. Каналы (`channel`) всегда отвечают inline — треды в них не используются.

## Лицензия

MIT
