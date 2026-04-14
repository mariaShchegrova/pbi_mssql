# Реконструкция кода репозитория PBI Export

## Обзор изменений

Код был рефакторирован с вынесением конфигурации и повторяющихся функций в отдельные классы. Оригинальные файлы сохранены для обратной совместимости, но рекомендуется использовать новую архитектуру.

## Новая структура

```
/workspace/
├── index.php                 # Точка входа (обновлена)
├── bootstrap.php             # Автозагрузчик и контейнер зависимостей
├── src/                      # Директория с классами
│   ├── Config.php            # Класс конфигурации
│   ├── Logger.php            # Класс логирования
│   ├── CurlClient.php        # HTTP клиент на основе cURL
│   ├── Database.php          # Класс работы с MSSQL
│   ├── DataExporter.php      # Базовый класс экспортера
│   └── DealsExporter.php     # Экспортер сделок
└── *.php                     # Оригинальные файлы (сохранены)
```

## Созданные классы

### 1. Config (`src/Config.php`)
**Назначение:** Централизованное управление конфигурацией

**Возможности:**
- Загрузка переменных окружения из `.env` файла
- Конфигурация Bitrix API (URL, токен, таймауты)
- Конфигурация MSSQL (сервер, база, пользователь, пароль)
- Параметры экспорта (год, месяцы, директория)
- Настройки производительности (лимит памяти, размер пакета)
- Настройки cURL (SSL, таймауты, редиректы)

**Пример использования:**
```php
$config = new \Pbi\Export\Config();
$apiUrl = $config->getBitrixApiUrl('get_deals.php');
$dbDsn = $config->getDatabaseDsn();
$year = $config->getExportYear();
$batchSize = $config->getBatchSize();
```

### 2. Logger (`src/Logger.php`)
**Назначение:** Унифицированное логирование

**Возможности:**
- Логирование с уровнями (INFO, ERROR, DEBUG, WARNING)
- Автоматическое создание файлов логов по датам
- Поддержка emoji для наглядности
- Методы для логирования операций (startOperation/endOperation)
- Вывод в stdout и файл одновременно

**Пример использования:**
```php
$logger = new \Pbi\Export\Logger('/path/to/logs', 'deals');
$logger->info("Сообщение");
$logger->error("Ошибка");
$startTime = $logger->startOperation("Загрузка данных");
// ... код ...
$logger->endOperation("Загрузка данных", $startTime);
```

### 3. CurlClient (`src/CurlClient.php`)
**Назначение:** HTTP запросы с повторными попытками

**Возможности:**
- GET и POST запросы
- Автоматические повторные попытки при ошибках
- Экспоненциальная задержка между попытками
- Настройка SSL верификации
- Поддержка gzip сжатия
- Детальное логирование запросов

**Пример использования:**
```php
$curlClient = new \Pbi\Export\CurlClient($config, $logger);
$result = $curlClient->get('https://api.example.com/data');
$response = $result['response'];
$httpCode = $result['http_code'];
```

### 4. Database (`src/Database.php`)
**Назначение:** Работа с базой данных MSSQL

**Возможности:**
- Подключение через PDO sqlsrv
- MERGE запросы для upsert операций
- Пакетная обработка в транзакциях
- Альтернативный метод сохранения (INSERT/UPDATE)
- Создание/проверка таблиц
- Управление транзакциями

**Пример использования:**
```php
$database = new \Pbi\Export\Database($config, $logger);
$database->connect();
$database->createTableIfNotExists('CrmDeal', $columns);
$savedCount = $database->mergeBatch('CrmDeal', $columns, $rows, 'ID');
$database->disconnect();
```

### 5. DataExporter (`src/DataExporter.php`)
**Назначение:** Базовый класс для всех экспортеров

**Возможности:**
- Получение данных из Bitrix API с пагинацией
- Валидация и санитизация данных
- Сохранение в файлы JSON
- Сохранение в базу данных
- Контроль использования памяти
- Тестирование подключения

**Методы для переопределения:**
- `export()` - основной метод экспорта

### 6. DealsExporter (`src/DealsExporter.php`)
**Назначение:** Экспорт сделок из Bitrix24 в MSSQL

**Наследует:** DataExporter

**Возможности:**
- Экспорт по периодам (год, диапазон месяцев)
- Пагинация при загрузке из API
- Пакетное сохранение в MSSQL
- Сохранение в JSON файлы
- Контроль памяти и сборка мусора

**Пример использования:**
```php
$exporter = new \Pbi\Export\DealsExporter($config, $logger, $curlClient, $database);
$result = $exporter->run();
echo "Экспортировано сделок: " . $result['total_deals'];
```

### 7. LeadsExporter (`src/LeadsExporter.php`)
**Назначение:** Экспорт лидов из Bitrix24 в MSSQL

**Наследует:** DataExporter

**Возможности:**
- Экспорт лидов по периодам
- Поля: ID, DATE_CREATE, DATE_MODIFY, ASSIGNED_BY_ID, SOURCE_ID, STATUS_ID, UF_* поля
- Валидация и очистка данных
- MERGE операции для обновления существующих записей

### 8. UsersExporter (`src/UsersExporter.php`)
**Назначение:** Экспорт пользователей из Bitrix24 в MSSQL

**Наследует:** DataExporter

**Возможности:**
- Экспорт пользователей по периодам
- Поля: ID, ACTIVE, BLOCKED, EMAIL, FULL_NAME, LOGIN, WORK_*, PERSONAL_*, UF_* поля
- Ограничение длины текстовых полей
- Конвертация булевых значений (Y/N → 1/0)

### 9. ActivitiesExporter (`src/ActivitiesExporter.php`)
**Назначение:** Экспорт активностей из Bitrix24 в MSSQL

**Наследует:** DataExporter

**Возможности:**
- Экспорт активностей по периодам
- Поля: ID, ACTIVITY_TYPE, CREATED, DEADLINE, DESCRIPTION, OWNER_ID, RESPONSIBLE_ID, RESULT_* поля
- Обработка числовых и текстовых полей
- Конвертация COMPLETED (Y/N → 1/0)

## Использование

### Через веб-интерфейс

```
GET /index.php?method=deals_export&year=2025&start_month=1&end_month=12
GET /index.php?method=leads_export&year=2025&start_month=1&end_month=12
GET /index.php?method=users_export&year=2025&start_month=1&end_month=12
GET /index.php?method=activities_export&year=2025&start_month=1&end_month=12
```

### Программный вызов

```php
require_once __DIR__ . '/bootstrap.php';

// Вариант 1: Быстрый запуск
$result = runDealsExport();
$result = runLeadsExport();
$result = runUsersExport();
$result = runActivitiesExport();

// Вариант 2: С кастомной конфигурацией
$container = createContainer();
$exporter = createDealsExporter($container);
$exporter = createLeadsExporter($container);
$exporter = createUsersExporter($container);
$exporter = createActivitiesExporter($container);
$result = $exporter->run();
```

### Из командной строки

```bash
php -r "require 'bootstrap.php'; runDealsExport();"
```

## Переменные окружения

Создайте файл `.env` в корне проекта:

```ini
# Bitrix API
BITRIX_BASE_URL=https://crm.ex.ru/local/api/v1/pbi/
BITRIX_API_TOKEN=pbi
BITRIX_TIMEOUT=300
BITRIX_RETRY_ATTEMPTS=3

# MSSQL Database
DB_SERVER=192.168.15.5
DB_NAME=BI
DB_USER=expo
DB_PASSWORD=VN5OoUHtWhAGnX4
DB_PORT=1433
DB_CHARSET=UTF-8

# Export settings
EXPORT_DIR=/var/www/api.ex.ru/api/gateway/v1/pbi/logs

# Performance
MEMORY_LIMIT=512M
BATCH_SIZE=100
REQUEST_TIMEOUT=300
RETRY_ATTEMPTS=3
RETRY_DELAY=2

# cURL settings
SSL_VERIFY_PEER=true
CURL_CONNECT_TIMEOUT=300
CURL_FOLLOW_REDIRECTS=true
CURL_MAX_REDIRECTS=3
CURL_ENABLE_GZIP=true
CURL_USER_AGENT=Bitrix-Export-Script/3.0 (cURL)
```

## Преимущества новой архитектуры

1. **Разделение ответственности:** Каждый класс отвечает за свою задачу
2. **Повторное использование:** Общие функции вынесены в базовые классы
3. **Тестируемость:** Классы можно тестировать независимо
4. **Расширяемость:** Легко добавить новые экспортеры (LeadsExporter, UsersExporter и т.д.)
5. **Конфигурируемость:** Все настройки централизованы в Config
6. **Читаемость:** Код стал более понятным и поддерживаемым

## Обратная совместимость

Оригинальные файлы сохранены:
- `deals_export_mssql.php`
- `leads_export_mssql.php`
- `users_export_mssql.php`
- `activities_export_mssql.php`
- `deals_stage_history_export_mssql.php`

Для полного перехода на новую архитектуру необходимо реализовать экспортеры для остальных сущностей по аналогии с `DealsExporter`.

## Следующие шаги

1. ~~Реализовать `LeadsExporter` по аналогии с `DealsExporter`~~ ✅ ВЫПОЛНЕНО
2. ~~Реализовать `UsersExporter`~~ ✅ ВЫПОЛНЕНО
3. ~~Реализовать `ActivitiesExporter`~~ ✅ ВЫПОЛНЕНО
4. Реализовать `DealsHistoryExporter` для истории стадий сделок
5. Добавить unit-тесты для классов
6. Добавить обработку исключений и retry логику на уровне контейнера
7. Обновить `index.php` для поддержки всех методов экспорта
