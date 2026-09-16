# Hermes Yandex Messenger Plugin

Плагин для интеграции [Hermes Agent](https://github.com/NousResearch/hermes-agent) с Яндекс Мессенджером через Bot API.

**Лицензия:** MIT

## Возможности

- Подключение к Bot API Яндекс Мессенджера в реальном времени (polling)
- Приём и обработка входящих сообщений (приватные + групповые чаты)
- Отправка ответов от имени бота
- Индикатор набора текста (`sendTyping`)
- Авто-регистрация платформы `ym` (не требует правок core Hermes)
- Политики доступа: `open`, `allowlist`, `disabled`
- Отправка в групповой чат по `chat_id`, в личку — по логину пользователя

## Установка

```bash
# 1. Скопировать плагин в директорию Hermes
cp -r hermes-ym-plugin ~/.hermes/plugins/ym

# 2. Убедиться что aiohttp установлен
~/.hermes/hermes-agent/venv/bin/pip install aiohttp

# 3. Включить плагин
hermes plugins enable ym
```

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
```

### Опционально

```env
YANDEX_ALLOWED_USERS=ivan_ivanov,petr_petrov   # Список разрешённых логинов
YANDEX_ALLOW_ALL_USERS=true                    # Разрешить всех
YANDEX_HOME_CHANNEL=0/0/<guid>                 # Чат для cron-уведомлений (chat_id или логин)
```

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
- **Polling без long-poll:** метод `getUpdates` не поддерживает удержание соединения, поэтому адаптер опрашивает сервер с интервалом ~1 секунда и продвигает курсор `offset = max(update_id) + 1`.
- **Лимит сообщения:** 6000 символов.
- **`payload_id`:** каждому исходящему сообщению присваивается уникальный `payload_id` — повторные запросы с тем же ID трактуются Яндексом как дубликаты.
- **Фильтр роботов:** сообщения от ботов (`from.robot`) игнорируются, чтобы избежать эхо-циклов.

## Лицензия

MIT
