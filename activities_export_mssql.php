<?php
/**
 * activities_export_mssql.php
 * Экспорт активностей из Битрикс24 в MSSQL с использованием cURL
 * Версия 2.1 - с улучшенной безопасностью и надежностью
 */

// =============================================================================
// ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
// =============================================================================

/**
 * Загрузчик переменных окружения
 */
function loadEnvironmentVariables($envFile = '.env')
{
    if (!file_exists($envFile)) {
        throw new RuntimeException("Файл окружения {$envFile} не найден");
    }

    $lines = file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    
    foreach ($lines as $line) {
        // Пропускаем комментарии
        if (strpos(trim($line), '#') === 0) {
            continue;
        }
        
        // Пропускаем строки без =
        if (strpos($line, '=') === false) {
            continue;
        }
        
        list($name, $value) = explode('=', $line, 2);
        $name = trim($name);
        $value = trim($value);
        
        // Удаляем кавычки если есть
        if (preg_match('/^"([\s\S]*)"$/', $value, $matches)) {
            $value = $matches[1];
        } elseif (preg_match('/^\'([\s\S]*)\'$/', $value, $matches)) {
            $value = $matches[1];
        }
        
        // Устанавливаем переменную окружения
        if (!putenv("$name=$value")) {
            error_log("Не удалось установить переменную окружения: $name");
        }
        
        // Также устанавливаем в $_ENV и $_SERVER для доступа
        $_ENV[$name] = $value;
        $_SERVER[$name] = $value;
    }
}

// Загрузка переменных окружения
try {
    loadEnvironmentVariables();
} catch (Exception $e) {
    error_log("Ошибка загрузки .env файла: " . $e->getMessage());
}

// =============================================================================
// КОНФИГУРАЦИЯ И БЕЗОПАСНОСТЬ
// =============================================================================

/**
 * Загрузка конфигурации из переменных окружения
 * Приоритет: $_ENV -> getenv() -> значение по умолчанию
 */
$BITRIX_API_URL = 'https://crm.ex.ru/local/api/v1/pbi/get_activities.php';
$API_TOKEN = 'pbi';

// Проверка обязательных параметров
if (empty($API_TOKEN)) {
    die("❌ ОШИБКА: Не указан BITRIX_API_TOKEN в переменных окружения\n");
}

$DB_CONFIG = [
    'server' => '192.168.15.5',
    'database' => 'BI',
    'username' => 'expo',
    'password' => 'VN5OoUHtWhAGnX4',
    'port' => 1433,
    'charset' => 'UTF-8'
];

// Проверка обязательных параметров БД
if (empty($DB_CONFIG['password'])) {
    die("❌ ОШИБКА: Не указан DB_PASSWORD в переменных окружения\n");
}

// Конфигурация экспорта
$DB_TABLE_NAME = 'CrmActivity';
$EXPORT_DIR = '/var/www/api.ex.ru/api/gateway/v1/pbi/logs';

// Гибкие параметры периода экспорта
$CURRENT_YEAR = $_GET['year'];
$CURRENT_START_MONTH = $_GET['start_month'];
$CURRENT_END_MONTH = $_GET['end_month'];

$YEAR = (int)$CURRENT_YEAR;
$START_MONTH = (int)$CURRENT_START_MONTH;
$END_MONTH = (int)$CURRENT_END_MONTH;

// Валидация параметров периода
if ($YEAR < 2025 || $YEAR > 2100) {
    die("❌ ОШИБКА: Некорректный год экспорта: $YEAR\n");
}
if ($START_MONTH < 1 || $START_MONTH > 12 || $END_MONTH < 1 || $END_MONTH > 12) {
    die("❌ ОШИБКА: Некорректный диапазон месяцев: $START_MONTH-$END_MONTH\n");
}
if ($START_MONTH > $END_MONTH) {
    die("❌ ОШИБКА: Начальный месяц ($START_MONTH) не может быть больше конечного ($END_MONTH)\n");
}

// Параметры производительности
$MEMORY_LIMIT = '1G';
$MAX_EXECUTION_TIME = 600;      // Увеличиваем время выполнения
$REQUEST_TIMEOUT = (int)5000;
$BATCH_SIZE = (int)100;
$RETRY_ATTEMPTS = (int)3;

// Настройка безопасности SSL
$SSL_VERIFY_PEER = filter_var(true, FILTER_VALIDATE_BOOLEAN);
$SSL_CA_FILE = null;

// Настройки cURL
$CURL_CONNECT_TIMEOUT = (int)600;
$CURL_FOLLOW_REDIRECTS = filter_var(true, FILTER_VALIDATE_BOOLEAN);
$CURL_MAX_REDIRECTS = (int)3;
$CURL_ENABLE_GZIP = filter_var(true, FILTER_VALIDATE_BOOLEAN);

// =============================================================================
// ИНИЦИАЛИЗАЦИЯ И НАСТРОЙКА
// =============================================================================

// Установка лимита памяти для обработки больших объемов данных
ini_set('memory_limit', $MEMORY_LIMIT);
// Увеличиваем время выполнения
ini_set('max_execution_time', $MAX_EXECUTION_TIME);

// =============================================================================
// ФУНКЦИИ ЛОГИРОВАНИЯ
// =============================================================================

/**
 * Логирование сообщений с разными уровнями важности
 */
function logMessage($message, $level = 'INFO', $echo = true)
{
    $timestamp = date('Y-m-d H:i:s');
    $formattedMessage = "[$timestamp] [$level] $message\n";
    
    global $EXPORT_DIR;
    $logFile = $EXPORT_DIR . '/activities_export_'.date('Ymd').'.log';
    
    // Создание директории если не существует
    if (!is_dir($EXPORT_DIR)) {
        mkdir($EXPORT_DIR, 0755, true);
    }
    
    // Запись в файл
    file_put_contents($logFile, $formattedMessage, FILE_APPEND | LOCK_EX);
    
    // Вывод в stdout если запущено из CLI или явно указано
    if ($echo && (php_sapi_name() === 'cli' || $echo)) {
        echo $formattedMessage;
    }
}

/**
 * Логирование начала важной операции
 */
function logStartOperation($operationName)
{
    logMessage("🚀 НАЧАЛО: $operationName", 'INFO');
    return microtime(true);
}

/**
 * Логирование завершения операции с временем выполнения
 */
function logEndOperation($operationName, $startTime)
{
    $executionTime = round(microtime(true) - $startTime, 2);
    logMessage("✅ ЗАВЕРШЕНО: $operationName (время: {$executionTime}с)", 'INFO');
}

// =============================================================================
// ФУНКЦИИ РАБОТЫ С CURL
// =============================================================================

/**
 * Проверка доступности cURL расширения
 */
function checkCurlAvailability()
{
    if (!extension_loaded('curl')) {
        throw new Exception("cURL расширение не установлено в PHP");
    }
    
    $version = curl_version();
    logMessage("cURL версия: " . ($version['version'] ?? 'unknown'), 'DEBUG');
    logMessage("SSL поддержка: " . ($version['ssl_version'] ?? 'none'), 'DEBUG');
    
    return true;
}

/**
 * Инициализация cURL сессии с настройками безопасности
 * 
 * @param string $url URL для запроса
 * @return resource cURL handle
 */
function initCurlSession($url)
{
    $ch = curl_init();
    
    // Базовые настройки
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => $GLOBALS['CURL_FOLLOW_REDIRECTS'],
        CURLOPT_MAXREDIRS => $GLOBALS['CURL_MAX_REDIRECTS'],
        CURLOPT_USERAGENT => 'Bitrix-Export-Script/2.1 (cURL)',
    ]);
    
    // Настройки сжатия
    if ($GLOBALS['CURL_ENABLE_GZIP']) {
        curl_setopt($ch, CURLOPT_ENCODING, 'gzip, deflate');
    }
    
    // Настройки таймаутов
    curl_setopt_array($ch, [
        CURLOPT_CONNECTTIMEOUT => $GLOBALS['CURL_CONNECT_TIMEOUT'],
        CURLOPT_TIMEOUT => $GLOBALS['REQUEST_TIMEOUT'],
    ]);
    
    // Настройки SSL безопасности
    if ($GLOBALS['SSL_VERIFY_PEER']) {
        curl_setopt_array($ch, [
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
        ]);
        
        if (!empty($GLOBALS['SSL_CA_FILE'])) {
            curl_setopt($ch, CURLOPT_CAINFO, $GLOBALS['SSL_CA_FILE']);
        }
    } else {
        // Только для разработки!
        curl_setopt_array($ch, [
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => 0,
        ]);
        logMessage("ВНИМАНИЕ: SSL верификация отключена", 'ERROR');
    }
    
    // Дополнительные настройки
    curl_setopt_array($ch, [
        CURLOPT_HEADER => false,
        CURLOPT_FAILONERROR => false, // Для ручной обработки HTTP ошибок
    ]);
    
    return $ch;
}

/**
 * Выполнение HTTP запроса с cURL с повторными попытками
 * 
 * @param string $url URL для запроса
 * @param int $maxRetries Максимальное количество попыток
 * @param int $retryDelay Базовая задержка между попытками
 * @return array [response, http_code, error]
 * @throws Exception При невозможности выполнить запрос после всех попыток
 */
function fetchWithCurl($url, $maxRetries = 3, $retryDelay = 2)
{
    $lastError = null;
    $lastHttpCode = null;
    
    for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
        $ch = initCurlSession($url);
        
        logMessage("cURL попытка $attempt/$maxRetries: $url", 'DEBUG');
        
        $startTime = microtime(true);
        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        $totalTime = round(microtime(true) - $startTime, 2);
        
        // Детальная информация для логирования
        $effectiveUrl = curl_getinfo($ch, CURLINFO_EFFECTIVE_URL);
        $sizeDownload = curl_getinfo($ch, CURLINFO_SIZE_DOWNLOAD);
        $contentType = curl_getinfo($ch, CURLINFO_CONTENT_TYPE);
        
        curl_close($ch);
        
        // Проверка успешности запроса
        if ($response !== false && $httpCode >= 200 && $httpCode < 300) {
            logMessage("cURL успешно: HTTP $httpCode, время {$totalTime}с, размер {$sizeDownload} байт", 'DEBUG');
            
            return [
                'response' => $response,
                'http_code' => $httpCode,
                'error' => null,
                'size_download' => $sizeDownload,
                'total_time' => $totalTime,
                'effective_url' => $effectiveUrl,
                'content_type' => $contentType
            ];
        }
        
        // Логирование ошибки
        $errorMsg = $error ?: "HTTP код: $httpCode";
        logMessage("cURL ошибка (попытка $attempt): $errorMsg", 'ERROR');
        
        $lastError = $errorMsg;
        $lastHttpCode = $httpCode;
        
        // Повторная попытка с экспоненциальной задержкой
        if ($attempt < $maxRetries) {
            $delay = $retryDelay * $attempt;
            logMessage("Повтор через {$delay}сек...", 'DEBUG');
            sleep($delay);
        }
    }
    
    // Все попытки исчерпаны
    $finalError = "Не удалось выполнить запрос после $maxRetries попыток. Последняя ошибка: $lastError";
    if ($lastHttpCode) {
        $finalError .= " (HTTP код: $lastHttpCode)";
    }
    
    throw new Exception($finalError);
}

/**
 * Получение активностей из Битрикс24 через cURL
 * 
 * @param int $month Месяц для выборки
 * @param int $year Год для выборки  
 * @param int $limit Лимит на страницу
 * @param int $offset Смещение
 * @return array Данные активностей
 * @throws Exception При ошибках API или сети
 */
function fetchActivitiesFromBitrix($month, $year, $limit = 500, $offset = 0)
{
    global $BITRIX_API_URL, $API_TOKEN, $RETRY_ATTEMPTS;
    
    // Валидация входных параметров
    if ($month < 1 || $month > 12) {
        throw new InvalidArgumentException("Некорректный месяц: $month");
    }
    
    if ($year < 2025 || $year > 2100) {
        throw new InvalidArgumentException("Некорректный год: $year");
    }
    
    // Проверка доступности cURL
    checkCurlAvailability();
    
    // Построение URL с параметрами
    $queryParams = http_build_query([
        'token' => $API_TOKEN,
        'month' => (int)$month,
        'year' => (int)$year,
        'limit' => (int)$limit,
        'offset' => (int)$offset
    ]);
    
    $url = $BITRIX_API_URL . '?' . $queryParams;
    
    logMessage("Запрос к Битрикс: месяц $month, год $year, offset $offset", 'INFO');
    
    try {
        $result = fetchWithCurl($url, $RETRY_ATTEMPTS);
        
        // Детальное логирование успешного запроса
        logMessage(sprintf(
            "cURL запрос успешен: %d байт за %.2fс, HTTP %d",
            $result['size_download'],
            $result['total_time'],
            $result['http_code']
        ), 'DEBUG');
        
        $data = json_decode($result['response'], true, 512, JSON_THROW_ON_ERROR);
        
        if (!isset($data['status'])) {
            throw new Exception("Некорректный формат ответа API");
        }
        
        if ($data['status'] !== 'SUCCESS') {
            $errorMsg = $data['message'] ?? 'Unknown error';
            throw new Exception("Ошибка Битрикс API: " . $errorMsg);
        }
        
        $itemsCount = count($data['data']['items'] ?? []);
        logMessage("Получено активностей: $itemsCount", 'INFO');
        
        return $data['data'];
        
    } catch (JsonException $e) {
        throw new Exception("Ошибка парсинга JSON: " . $e->getMessage());
    }
}

/**
 * Тестирование подключения к Bitrix API
 */
function testBitrixConnection()
{
    global $BITRIX_API_URL, $API_TOKEN;
    
    logMessage("Тестирование подключения к Bitrix API...", 'INFO');
    
    try {
        checkCurlAvailability();
        
        $testUrl = $BITRIX_API_URL . '?token=' . urlencode($API_TOKEN) . '&month=1&year=2025&limit=1&offset=0';
        $result = fetchWithCurl($testUrl, 1, 1);
        
        if ($result['http_code'] === 200) {
            logMessage("✅ Подключение к Bitrix API успешно", 'INFO');
            return true;
        } else {
            logMessage("❌ Ошибка подключения: HTTP " . $result['http_code'], 'ERROR');
            return false;
        }
        
    } catch (Exception $e) {
        logMessage("❌ Ошибка тестирования: " . $e->getMessage(), 'ERROR');
        return false;
    }
}

/**
 * Получение информации о сервере Bitrix через cURL
 */
function getBitrixServerInfo()
{
    global $BITRIX_API_URL, $API_TOKEN;
    
    try {
        $testUrl = $BITRIX_API_URL . '?token=' . urlencode($API_TOKEN) . '&month=1&year=2025&limit=1&offset=0';
        $ch = initCurlSession($testUrl);
        
        // Получаем только заголовки для анализа
        curl_setopt($ch, CURLOPT_NOBODY, true);
        curl_setopt($ch, CURLOPT_HEADER, true);
        
        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $effectiveUrl = curl_getinfo($ch, CURLINFO_EFFECTIVE_URL);
        $contentType = curl_getinfo($ch, CURLINFO_CONTENT_TYPE);
        $server = curl_getinfo($ch, CURLINFO_PRIMARY_IP);
        
        curl_close($ch);
        
        logMessage("Информация о сервере:", 'DEBUG');
        logMessage("  - IP: " . ($server ?: 'unknown'), 'DEBUG');
        logMessage("  - Effective URL: " . $effectiveUrl, 'DEBUG');
        logMessage("  - Content-Type: " . ($contentType ?: 'unknown'), 'DEBUG');
        logMessage("  - HTTP Code: " . $httpCode, 'DEBUG');
        
        return [
            'server_ip' => $server,
            'effective_url' => $effectiveUrl,
            'content_type' => $contentType,
            'http_code' => $httpCode
        ];
        
    } catch (Exception $e) {
        logMessage("Не удалось получить информацию о сервере: " . $e->getMessage(), 'DEBUG');
        return null;
    }
}

// =============================================================================
// ФУНКЦИИ РАБОТЫ С ФАЙЛАМИ
// =============================================================================

/**
 * Сохранение данных в JSON файл с проверкой ошибок
 *
 * @param mixed $data Данные для сохранения
 * @param string $filename Имя файла
 * @return bool Успешность операции
 */
function saveToFile($data, $filename)
{
    global $EXPORT_DIR;
    
    if (!is_dir($EXPORT_DIR)) {
        if (!mkdir($EXPORT_DIR, 0755, true)) {
            logMessage("Ошибка создания директории: $EXPORT_DIR", 'ERROR');
            return false;
        }
    }

    $fullPath = $EXPORT_DIR . '/' . $filename;
    
    try {
        $json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR);
        
        if (file_put_contents($fullPath, $json, LOCK_EX)) {
            logMessage("Файл успешно сохранен: $fullPath", 'INFO');
            return true;
        } else {
            logMessage("Ошибка записи в файл: $fullPath", 'ERROR');
            return false;
        }
    } catch (JsonException $e) {
        logMessage("Ошибка кодирования JSON: " . $e->getMessage(), 'ERROR');
        return false;
    }
}

// =============================================================================
// ФУНКЦИИ ВАЛИДАЦИИ ДАННЫХ
// =============================================================================

/**
 * Валидация данных активности перед сохранением
 * 
 * @param array $activity Данные активности
 * @return bool Валидны ли данные
 * @throws InvalidArgumentException При невалидных данных
 */
function validateActivityData($activity)
{
    if (!isset($activity['ID']) || !is_numeric($activity['ID'])) {
        throw new InvalidArgumentException("Invalid activity ID: " . ($activity['ID'] ?? 'null'));
    }
    
    if ($activity['ID'] <= 0) {
        throw new InvalidArgumentException("Activity ID must be positive: " . $activity['ID']);
    }
    
    // Валидация дат если они присутствуют
    $dateFields = ['CREATED', 'DEADLINE', 'LAST_UPDATED', 'START_TIME', 'END_TIME'];
    foreach ($dateFields as $field) {
        if (!empty($activity[$field]) && $activity[$field] !== null && !strtotime($activity[$field])) {
            logMessage("Некорректный формат даты в поле $field: " . $activity[$field], 'ERROR');
            // Не бросаем исключение, а только логируем предупреждение
        }
    }
    
    return true;
}

/**
 * Очистка и нормализация данных активности
 */
function sanitizeActivityData($activity)
{
    // Ограничение длины текстовых полей
    $textFields = [
        'ACTIVITY_TYPE' => 50,
        'ACTIVITY_TYPE_NAME' => 100,
        'ACTIVITY_URL' => 500,
        'DESCRIPTION' => 4000,
        'SUBJECT' => 500,
        'PROVIDER_TYPE_ID' => 50,
        'OWNER_TYPE_NAME' => 100,
        'PRIORITY_NAME' => 50,
        'RESULT_VALUE' => 1000
    ];
    
    foreach ($textFields as $field => $maxLength) {
        if (isset($activity[$field]) && is_string($activity[$field])) {
            // Очистка от невалидных UTF-8 символов
            $activity[$field] = mb_convert_encoding($activity[$field], 'UTF-8', 'UTF-8');
            $activity[$field] = iconv('UTF-8', 'UTF-8//IGNORE', $activity[$field]);
            
            // Удаление проблемных символов
            $activity[$field] = preg_replace('/[^\x{0009}\x{000A}\x{000D}\x{0020}-\x{D7FF}\x{E000}-\x{FFFD}]+/u', '', $activity[$field]);
            
            // Обрезка длины
            if (mb_strlen($activity[$field], 'UTF-8') > $maxLength) {
                $activity[$field] = mb_substr($activity[$field], 0, $maxLength - 3, 'UTF-8') . '...';
                logMessage("Обрезано поле $field (длина: " . mb_strlen($activity[$field], 'UTF-8') . ")", 'ERROR');
            }
        }
    }
    
    // Конвертация текстовых значений в булевы
    $booleanFields = ['COMPLETED'];
    foreach ($booleanFields as $field) {
        if (isset($activity[$field])) {
            $activity[$field] = ($activity[$field] === 'Y') ? 1 : 0;
        }
    }
    
    // Конвертация числовых полей
    $numericFields = [
        'AUTHOR_ID', 'EDITOR_ID', 'RESPONSIBLE_ID', 'OWNER_ID', 
        'OWNER_TYPE_ID', 'PRIORITY', 'RESULT_MARK', 'RESULT_STATUS', 
        'RESULT_STREAM', 'TYPE_ID', 'DIRECTION'
    ];
    
    foreach ($numericFields as $field) {
        if (isset($activity[$field]) && is_numeric($activity[$field])) {
            $activity[$field] = (int)$activity[$field];
        }
    }
    
    return $activity;
}

// =============================================================================
// ФУНКЦИИ РАБОТЫ С БАЗОЙ ДАННЫХ
// =============================================================================

/**
 * Подключение к MSSQL через PDO с обработкой ошибок
 * 
 * @return PDO|null Объект PDO или null при ошибке
 */
function connectToDatabase()
{
    global $DB_CONFIG;
    
    $startTime = logStartOperation("Подключение к MSSQL");
    
    try {
        $dsn = "sqlsrv:Server={$DB_CONFIG['server']},{$DB_CONFIG['port']};Database={$DB_CONFIG['database']};TrustServerCertificate=yes";
        
        $pdo = new PDO($dsn, $DB_CONFIG['username'], $DB_CONFIG['password'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
        ]);
        
        logEndOperation("Подключение к MSSQL", $startTime);
        return $pdo;
        
    } catch (PDOException $e) {
        logMessage("Критическая ошибка подключения к MSSQL: " . $e->getMessage(), 'ERROR');
        return null;
    }
}

/**
 * Создание таблицы активностей если не существует
 */
function createActivitiesTable($pdo)
{
    global $DB_TABLE_NAME;
    
    $startTime = logStartOperation("Создание/проверка таблицы $DB_TABLE_NAME");
    
    $sql = "
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='$DB_TABLE_NAME' AND xtype='U')
    CREATE TABLE [$DB_TABLE_NAME] (
        [id] BIGINT NOT NULL,
        [activity_type] NVARCHAR(15) NULL,
        [activity_type_name] NVARCHAR(15) NULL,
        [activity_url] NVARCHAR(255) NULL,
        [author_id] BIGINT NULL,
        [completed] BIT NULL,
        [created] DATETIME2 NULL,
        [deadline] DATETIME2 NULL,
        [description] NVARCHAR(500) NULL,
        [description_type] NVARCHAR(10) NULL,
        [direction] INT NULL,
        [editor_id] BIGINT NULL,
        [end_time] DATETIME2 NULL,
        [last_updated] DATETIME2 NULL,
        [owner_id] BIGINT NULL,
        [owner_type_id] INT NULL,
        [owner_type_name] NVARCHAR(15) NULL,
        [priority] INT NULL,
        [priority_name] NVARCHAR(15) NULL,
        [provider_params] NVARCHAR(500) NULL,
        [provider_type_id] NVARCHAR(15) NULL,
        [responsible_id] BIGINT NULL,
        [result_mark] INT NULL,
        [result_source_id] INT NULL,
        [result_status] INT NULL,
        [result_stream] INT NULL,
        [result_value] NVARCHAR(500) NULL,
        [start_time] DATETIME2 NULL,
        [subject] NVARCHAR(500) NULL,
        [type_id] INT NULL,
        [export_batch] NVARCHAR(50) NULL,
        [export_date] DATETIME2 NULL,
        CONSTRAINT [PK_{$DB_TABLE_NAME}_id] PRIMARY KEY CLUSTERED ([id])
    )";
    
    try {
        $pdo->exec($sql);        
        logEndOperation("Создание/проверка таблицы $DB_TABLE_NAME", $startTime);
        return true;
        
    } catch (PDOException $e) {
        logMessage("Ошибка создания таблицы в MSSQL: " . $e->getMessage(), 'ERROR');
        return false;
    }
}

/**
 * Пакетное сохранение активностей в базу данных
 */
function saveActivitiesToDatabase($pdo, $activities, $batchId = null)
{
    global $DB_TABLE_NAME, $BATCH_SIZE;
    
    if (empty($activities)) {
        logMessage("Нет данных для сохранения в MSSQL", 'INFO');
        return 0;
    }
    
    $batchId = $batchId ?: date('Ymd_His');
    $exportDate = date('Y-m-d H:i:s');
    $totalSaved = 0;
    
    // Разбиваем на пачки для избежания переполнения памяти
    $batches = array_chunk($activities, $BATCH_SIZE);
    
    logMessage("Сохранение " . count($activities) . " активностей в " . count($batches) . " пачках", 'INFO');
    
    foreach ($batches as $batchIndex => $batchActivities) {
        $savedInBatch = saveActivitiesBatch($pdo, $batchActivities, $batchId, $exportDate, $batchIndex + 1);
        $totalSaved += $savedInBatch;
        
        // Освобождаем память после обработки пачки
        unset($batchActivities);
    }
    
    logMessage("Итого сохранено в MSSQL: $totalSaved активностей (batch: $batchId)", 'INFO');
    return $totalSaved;
}

/**
 * Сохранение одной пачки активностей с использованием MERGE
 * 
 * @param PDO $pdo Объект PDO
 * @param array $activities Пачка активностей
 * @param string $batchId Идентификатор пачки
 * @param string $exportDate Дата экспорта
 * @param int $batchNumber Номер пачки
 * @return int Количество сохраненных активностей
 */
function saveActivitiesBatch($pdo, $activities, $batchId, $exportDate, $batchNumber = 1)
{
    global $DB_TABLE_NAME;
    
    $startTime = microtime(true);
    $savedCount = 0;
    
     $sql = "MERGE [$DB_TABLE_NAME] WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) 
            AS source (id, activity_type, activity_type_name, activity_url, author_id, completed, created, deadline, description, description_type, direction, editor_id, end_time, last_updated, owner_id, owner_type_id, owner_type_name, priority, priority_name, provider_params, provider_type_id, responsible_id, result_mark, result_source_id, result_status, result_stream, result_value, start_time, subject, type_id, export_batch, export_date)
            ON (target.id = source.id)
            WHEN MATCHED THEN
                UPDATE SET 
                    activity_type = source.activity_type,
                    activity_type_name = source.activity_type_name,
                    activity_url = source.activity_url,
                    author_id = source.author_id,
                    completed = source.completed,
                    created = source.created,
                    deadline = source.deadline,
                    description = source.description,
                    description_type = source.description_type,
                    direction = source.direction,
                    editor_id = source.editor_id,
                    end_time = source.end_time,
                    last_updated = source.last_updated,
                    owner_id = source.owner_id,
                    owner_type_id = source.owner_type_id,
                    owner_type_name = source.owner_type_name,
                    priority = source.priority,
                    priority_name = source.priority_name,
                    provider_params = source.provider_params,
                    provider_type_id = source.provider_type_id,
                    responsible_id = source.responsible_id,
                    result_mark = source.result_mark,
                    result_source_id = source.result_source_id,
                    result_status = source.result_status,
                    result_stream = source.result_stream,
                    result_value = source.result_value,
                    start_time = source.start_time,
                    subject = source.subject,
                    type_id = source.type_id,
                    export_batch = source.export_batch,
                    export_date = source.export_date
            WHEN NOT MATCHED THEN
                INSERT (id, activity_type, activity_type_name, activity_url, author_id, completed, created, deadline, description, description_type, direction, editor_id, end_time, last_updated, owner_id, owner_type_id, owner_type_name, priority, priority_name, provider_params, provider_type_id, responsible_id, result_mark, result_source_id, result_status, result_stream, result_value, start_time, subject, type_id, export_batch, export_date)
                VALUES (source.id, source.activity_type, source.activity_type_name, source.activity_url, source.author_id, source.completed, source.created, source.deadline, source.description, source.description_type, source.direction, source.editor_id, source.end_time, source.last_updated, source.owner_id, source.owner_type_id, source.owner_type_name, source.priority, source.priority_name, source.provider_params, source.provider_type_id, source.responsible_id, source.result_mark, source.result_source_id, source.result_status, source.result_stream, source.result_value, source.start_time, source.subject, source.type_id, source.export_batch, source.export_date);";
    
    try {
        $stmt = $pdo->prepare($sql);
        $pdo->beginTransaction();
        
        foreach ($activities as $activity) {
            try {
                // Валидация и очистка данных
                validateActivityData($activity);
                $activity = sanitizeActivityData($activity);
                
                $values = [
                    $activity['ID'] ?? null,
                    $activity['ACTIVITY_TYPE'] ?? null,
                    $activity['ACTIVITY_TYPE_NAME'] ?? null,
                    $activity['ACTIVITY_URL'] ?? null,
                    $activity['AUTHOR_ID'] ?? null,
                    $activity['COMPLETED'] ?? null,
                    $activity['CREATED'] ?? null,
                    $activity['DEADLINE'] ?? null,
                    $activity['DESCRIPTION'] ?? null,
                    $activity['DESCRIPTION_TYPE'] ?? null,
                    $activity['DIRECTION'] ?? null,
                    $activity['EDITOR_ID'] ?? null,
                    $activity['END_TIME'] ?? null,
                    $activity['LAST_UPDATED'] ?? null,
                    $activity['OWNER_ID'] ?? null,
                    $activity['OWNER_TYPE_ID'] ?? null,
                    $activity['OWNER_TYPE_NAME'] ?? null,
                    $activity['PRIORITY'] ?? null,
                    $activity['PRIORITY_NAME'] ?? null,
                    $activity['PROVIDER_PARAMS'] ?? null,
                    $activity['PROVIDER_TYPE_ID'] ?? null,
                    $activity['RESPONSIBLE_ID'] ?? null,
                    $activity['RESULT_MARK'] ?? null,
                    $activity['RESULT_SOURCE_ID'] ?? null,
                    $activity['RESULT_STATUS'] ?? null,
                    $activity['RESULT_STREAM'] ?? null,
                    $activity['RESULT_VALUE'] ?? null,
                    $activity['START_TIME'] ?? null,
                    $activity['SUBJECT'] ?? null,
                    $activity['TYPE_ID'] ?? null,
                    $batchId,
                    $exportDate
                ];
                
                $stmt->execute($values);
                $savedCount++;
                
            } catch (InvalidArgumentException $e) {
                logMessage("Пропущена невалидная активность ID {$activity['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            } catch (Exception $e) {
                logMessage("Ошибка обработки активности ID {$activity['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            }
        }
        
        $pdo->commit();
        $executionTime = round(microtime(true) - $startTime, 2);
        logMessage("Пачка $batchNumber: сохранено $savedCount активностей за {$executionTime}с", 'INFO');
        
        return $savedCount;
        
    } catch (PDOException $e) {
        $pdo->rollBack();
        logMessage("Ошибка MERGE в пачке $batchNumber: " . $e->getMessage(), 'ERROR');
        
        // Альтернативный способ вставки через отдельные INSERT/UPDATE
        logMessage("Попытка альтернативного сохранения пачки $batchNumber...", 'INFO');
        return saveActivitiesAlternative($pdo, $activities, $batchId);
    }
}

/**
 * Альтернативный метод сохранения для MSSQL (если MERGE не работает)
 */
function saveActivitiesAlternative($pdo, $activities, $batchId = null)
{
    global $DB_TABLE_NAME;
    
    if (empty($activities)) {
        return 0;
    }
    
    $batchId = $batchId ?: date('Ymd_His');
    $exportDate = date('Y-m-d H:i:s');
    $savedCount = 0;
    
    try {
        $pdo->beginTransaction();
        
        foreach ($activities as $activity) {
            try {
                // Валидация и очистка данных
                validateActivityData($activity);
                $activity = sanitizeActivityData($activity);
                
                // Проверяем существование записи
                $checkSql = "SELECT id FROM [$DB_TABLE_NAME] WHERE id = ?";
                $checkStmt = $pdo->prepare($checkSql);
                $checkStmt->execute([$activity['ID'] ?? null]);
                $exists = $checkStmt->fetch();
                
                $values = [
                    $activity['ID'] ?? null,
                    $activity['ACTIVITY_TYPE'] ?? null,
                    $activity['ACTIVITY_TYPE_NAME'] ?? null,
                    $activity['ACTIVITY_URL'] ?? null,
                    $activity['AUTHOR_ID'] ?? null,
                    $activity['COMPLETED'] ?? null,
                    $activity['CREATED'] ?? null,
                    $activity['DEADLINE'] ?? null,
                    $activity['DESCRIPTION'] ?? null,
                    $activity['DESCRIPTION_TYPE'] ?? null,
                    $activity['DIRECTION'] ?? null,
                    $activity['EDITOR_ID'] ?? null,
                    $activity['END_TIME'] ?? null,
                    $activity['LAST_UPDATED'] ?? null,
                    $activity['OWNER_ID'] ?? null,
                    $activity['OWNER_TYPE_ID'] ?? null,
                    $activity['OWNER_TYPE_NAME'] ?? null,
                    $activity['PRIORITY'] ?? null,
                    $activity['PRIORITY_NAME'] ?? null,
                    $activity['PROVIDER_PARAMS'] ?? null,
                    $activity['PROVIDER_TYPE_ID'] ?? null,
                    $activity['RESPONSIBLE_ID'] ?? null,
                    $activity['RESULT_MARK'] ?? null,
                    $activity['RESULT_SOURCE_ID'] ?? null,
                    $activity['RESULT_STATUS'] ?? null,
                    $activity['RESULT_STREAM'] ?? null,
                    $activity['RESULT_VALUE'] ?? null,
                    $activity['START_TIME'] ?? null,
                    $activity['SUBJECT'] ?? null,
                    $activity['TYPE_ID'] ?? null,
                    $batchId,
                    $exportDate
                ];
                
                if ($exists) {
                    // UPDATE
                    $sql = "UPDATE [$DB_TABLE_NAME] SET 
                            activity_type = ?, activity_type_name = ?, activity_url = ?, author_id = ?, 
                            completed = ?, created = ?, deadline = ?, description = ?, description_type = ?, 
                            direction = ?, editor_id = ?, end_time = ?, last_updated = ?, owner_id = ?, 
                            owner_type_id = ?, owner_type_name = ?, priority = ?, priority_name = ?, 
                            provider_params = ?, provider_type_id = ?, responsible_id = ?, result_mark = ?, 
                            result_source_id = ?, result_status = ?, result_stream = ?, result_value = ?, 
                            start_time = ?, subject = ?, type_id = ?, export_batch = ?, export_date = ?
                            WHERE id = ?";
                    
                    $values[] = $activity['ID']; // Добавляем ID в конец для WHERE
                    unset($values[0]); // Удаляем ID из начала
                    $values = array_values($values); // Переиндексируем
                    
                } else {
                    // INSERT
                    $sql = "INSERT INTO [$DB_TABLE_NAME] 
                            (id, activity_type, activity_type_name, activity_url, author_id, completed, created, 
                            deadline, description, description_type, direction, editor_id, end_time, last_updated, 
                            owner_id, owner_type_id, owner_type_name, priority, priority_name, provider_params, 
                            provider_type_id, responsible_id, result_mark, result_source_id, result_status, 
                            result_stream, result_value, start_time, subject, type_id, export_batch, export_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                }
                
                $stmt = $pdo->prepare($sql);
                $stmt->execute($values);
                $savedCount++;
                
            } catch (Exception $e) {
                logMessage("Ошибка обработки активности ID {$activity['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            }
        }
        
        $pdo->commit();
        logMessage("Альтернативное сохранение: обработано $savedCount активностей", 'INFO');
        return $savedCount;
        
    } catch (PDOException $e) {
        $pdo->rollBack();
        logMessage("Критическая ошибка альтернативного сохранения: " . $e->getMessage(), 'ERROR');
        return 0;
    }
}

// =============================================================================
// ОСНОВНАЯ ЛОГИКА ЭКСПОРТА
// =============================================================================

/**
 * Основная функция экспорта активностей
 * 
 * @return array Результаты экспорта
 */

function exportAllActivities()
{
    global $YEAR, $START_MONTH, $END_MONTH, $BATCH_SIZE;
    
    // === ДОБАВЛЕНО: Управление буферами вывода ===
    if (ob_get_level()) {
        ob_end_clean();
    }
    ob_implicit_flush(true);
    // =============================================
    
    $totalStartTime = logStartOperation("ЭКСПОРТ АКТИВНОСТЕЙ ЗА $YEAR ГОД (месяцы $START_MONTH-$END_MONTH)");
    
    $allActivities = [];
    $totalActivities = 0;
    $totalSavedToDB = 0;
    $batchId = date('Ymd_His');
    
    // Подключаемся к MSSQL
    $pdo = connectToDatabase();
    if ($pdo) {
        createActivitiesTable($pdo);
    } else {
        logMessage("ВНИМАНИЕ: Работаем без MSSQL, только файлы", 'ERROR');
    }
    
    // Мониторинг использования памяти
    $initialMemory = memory_get_usage(true);
    
    // Проходим по всем месяцам указанного периода
    for ($month = $START_MONTH; $month <= $END_MONTH; $month++) {
        $monthStartTime = logStartOperation("Обработка месяца $month");
        
        $monthActivities = [];
        $offset = 0;
        $limit = $BATCH_SIZE;
        $monthTotal = 0;
        
        logMessage("📅 Экспорт активностей месяца: $month/$YEAR", 'INFO');
        
        // === ДОБАВЛЕНО: Принудительный сброс буфера для каждого месяца ===
        if (ob_get_level()) {
            ob_flush();
        }
        flush();
        // ================================================================
        
        // Пагинация по страницам
        do {
            try {
                $pageData = fetchActivitiesFromBitrix($month, $YEAR, $limit, $offset);
                
                if (!empty($pageData['items'])) {
                    $activitiesCount = count($pageData['items']);
                    $monthActivities = array_merge($monthActivities, $pageData['items']);
                    $monthTotal += $activitiesCount;
                    
                    // Сохраняем в MSSQL порциями
                    if ($pdo) {
                        $savedCount = saveActivitiesToDatabase($pdo, $pageData['items'], $batchId . "_m{$month}");
                        $totalSavedToDB += $savedCount;
                    }
                    
                    $offset += $limit;
                    
                    logMessage("Месяц $month, страница: $activitiesCount активностей, offset: $offset", 'DEBUG');
                    
                    // === ДОБАВЛЕНО: Периодический сброс буферов ===
                    if ($offset % 100 === 0) {
                        if (ob_get_level()) {
                            ob_flush();
                        }
                        flush();
                    }
                    // ==============================================
                    
                     // Пауза между запросами для снижения нагрузки на API
                    if ($pageData['has_more'] ?? false) {
                        sleep(1);
                    }
                    
                    // Контроль памяти - периодическая очистка
                    if ($offset % 2000 === 0) {
                        $currentMemory = memory_get_usage(true);
                        $memoryUsage = round(($currentMemory - $initialMemory) / 1024 / 1024, 2);
                        logMessage("Использование памяти: {$memoryUsage}MB", 'DEBUG');
                        
                        if (function_exists('gc_collect_cycles')) {
                            gc_collect_cycles();
                        }
                    }
                    
                } else {
                    logMessage("В месяце $month больше нет активностей", 'INFO');
                    break;
                }
            } catch (Exception $e) {
                logMessage("Критическая ошибка при загрузке месяца $month: " . $e->getMessage(), 'ERROR');
                break;
            }
        } while (!empty($pageData['items']) && ($pageData['has_more'] ?? false));
        
        $allActivities = array_merge($allActivities, $monthActivities);
        $totalActivities += count($monthActivities);
        
        logEndOperation("Обработка месяца $month", $monthStartTime);
        logMessage("Месяц $month завершен. Активностей: " . count($monthActivities), 'INFO');
        
        // Сохраняем файл по месяцам
        saveToFile([
            'month' => $month,
            'year' => $YEAR,
            'activities_count' => count($monthActivities),
            'activities' => $monthActivities,
            'export_date' => date('Y-m-d H:i:s'),
            'batch_id' => $batchId
        ], "activities_{$YEAR}_month_{$month}_{$batchId}.json");
        
        // Освобождаем память после обработки месяца
        unset($monthActivities);
        if (function_exists('gc_collect_cycles')) {
            gc_collect_cycles();
        }
    }
    
    // Закрываем соединение с БД
    if ($pdo) {
        $pdo = null;
    }
    
    // Финальная статистика использования памяти
    $finalMemory = memory_get_usage(true);
    $peakMemory = memory_get_peak_usage(true);
    $memoryUsed = round(($finalMemory - $initialMemory) / 1024 / 1024, 2);
    $peakMemoryMB = round($peakMemory / 1024 / 1024, 2);
    
    // Сохраняем общий файл
    saveToFile([
        'total_activities' => $totalActivities,
        'database_saved' => $totalSavedToDB,
        'activities' => $allActivities,
        'export_date' => date('Y-m-d H:i:s'),
        'year' => $YEAR,
        'batch_id' => $batchId,
        'memory_used_mb' => $memoryUsed,
        'peak_memory_mb' => $peakMemoryMB,
        'months_exported' => "$START_MONTH-$END_MONTH"
    ], "activities_{$YEAR}_full_export_{$batchId}.json");
    
    logEndOperation("ЭКСПОРТ АКТИВНОСТЕЙ ЗА $YEAR ГОД", $totalStartTime);
    logMessage("📊 ИТОГИ ЭКСПОРТА АКТИВНОСТЕЙ:", 'INFO');
    logMessage("   Всего активностей: $totalActivities", 'INFO');
    logMessage("   Сохранено в MSSQL: $totalSavedToDB", 'INFO');
    logMessage("   Использовано памяти: {$memoryUsed}MB (пик: {$peakMemoryMB}MB)", 'INFO');
    logMessage("   Batch ID: $batchId", 'INFO');
    
    return [
        'total_activities' => $totalActivities,
        'database_saved' => $totalSavedToDB,
        'batch_id' => $batchId,
        'memory_used_mb' => $memoryUsed,
        'peak_memory_mb' => $peakMemoryMB,
        'execution_time' => round(microtime(true) - $totalStartTime, 2)
    ];
}



function exportAllActivities2()
{
    global $YEAR, $START_MONTH, $END_MONTH, $BATCH_SIZE;
    
    $totalStartTime = logStartOperation("ЭКСПОРТ АКТИВНОСТЕЙ ЗА $YEAR ГОД (месяцы $START_MONTH-$END_MONTH)");
    
    $allActivities = [];
    $totalActivities = 0;
    $totalSavedToDB = 0;
    $batchId = date('Ymd_His');
    
    // Подключаемся к MSSQL
    $pdo = connectToDatabase();
    if ($pdo) {
        createActivitiesTable($pdo);
    } else {
        logMessage("ВНИМАНИЕ: Работаем без MSSQL, только файлы", 'ERROR');
    }
    
    // Мониторинг использования памяти
    $initialMemory = memory_get_usage(true);
    
    // Проходим по всем месяцам указанного периода
    for ($month = $START_MONTH; $month <= $END_MONTH; $month++) {
        $monthStartTime = logStartOperation("Обработка месяца $month");
        
        $monthActivities = [];
        $offset = 0;
        $limit = $BATCH_SIZE;
        $monthTotal = 0;
        
        logMessage("📅 Экспорт активностей месяца: $month/$YEAR", 'INFO');
        
        // Пагинация по страницам
        do {
            try {
                $pageData = fetchActivitiesFromBitrix($month, $YEAR, $limit, $offset);
                
                if (!empty($pageData['items'])) {
                    $activitiesCount = count($pageData['items']);
                    $monthActivities = array_merge($monthActivities, $pageData['items']);
                    $monthTotal += $activitiesCount;
                    
                    // Сохраняем в MSSQL порциями
                    if ($pdo) {
                        $savedCount = saveActivitiesToDatabase($pdo, $pageData['items'], $batchId . "_m{$month}");
                        $totalSavedToDB += $savedCount;
                    }
                    
                    $offset += $limit;
                    
                    logMessage("Месяц $month, страница: $activitiesCount активностей, offset: $offset", 'DEBUG');
                    
                    // Пауза между запросами для снижения нагрузки на API
                    if ($pageData['has_more'] ?? false) {
                        sleep(1);
                    }
                    
                    // Контроль памяти - периодическая очистка
                    if ($offset % 2000 === 0) {
                        $currentMemory = memory_get_usage(true);
                        $memoryUsage = round(($currentMemory - $initialMemory) / 1024 / 1024, 2);
                        logMessage("Использование памяти: {$memoryUsage}MB", 'DEBUG');
                        
                        if (function_exists('gc_collect_cycles')) {
                            gc_collect_cycles();
                        }
                    }
                    
                } else {
                    logMessage("В месяце $month больше нет активностей", 'INFO');
                    break;
                }
            } catch (Exception $e) {
                logMessage("Критическая ошибка при загрузке месяца $month: " . $e->getMessage(), 'ERROR');
                break;
            }
        } while (!empty($pageData['items']) && ($pageData['has_more'] ?? false));
        
        $allActivities = array_merge($allActivities, $monthActivities);
        $totalActivities += count($monthActivities);
        
        logEndOperation("Обработка месяца $month", $monthStartTime);
        logMessage("Месяц $month завершен. Активностей: " . count($monthActivities), 'INFO');
        
        // Сохраняем файл по месяцам
        saveToFile([
            'month' => $month,
            'year' => $YEAR,
            'activities_count' => count($monthActivities),
            'activities' => $monthActivities,
            'export_date' => date('Y-m-d H:i:s'),
            'batch_id' => $batchId
        ], "activities_{$YEAR}_month_{$month}_{$batchId}.json");
        
        // Освобождаем память после обработки месяца
        unset($monthActivities);
        if (function_exists('gc_collect_cycles')) {
            gc_collect_cycles();
        }
    }
    
    // Закрываем соединение с БД
    if ($pdo) {
        $pdo = null;
    }
    
    // Финальная статистика использования памяти
    $finalMemory = memory_get_usage(true);
    $peakMemory = memory_get_peak_usage(true);
    $memoryUsed = round(($finalMemory - $initialMemory) / 1024 / 1024, 2);
    $peakMemoryMB = round($peakMemory / 1024 / 1024, 2);
    
    // Сохраняем общий файл
    saveToFile([
        'total_activities' => $totalActivities,
        'database_saved' => $totalSavedToDB,
        'activities' => $allActivities,
        'export_date' => date('Y-m-d H:i:s'),
        'year' => $YEAR,
        'batch_id' => $batchId,
        'memory_used_mb' => $memoryUsed,
        'peak_memory_mb' => $peakMemoryMB,
        'months_exported' => "$START_MONTH-$END_MONTH"
    ], "activities_{$YEAR}_full_export_{$batchId}.json");
    
    logEndOperation("ЭКСПОРТ АКТИВНОСТЕЙ ЗА $YEAR ГОД", $totalStartTime);
    logMessage("📊 ИТОГИ ЭКСПОРТА АКТИВНОСТЕЙ:", 'INFO');
    logMessage("   Всего активностей: $totalActivities", 'INFO');
    logMessage("   Сохранено в MSSQL: $totalSavedToDB", 'INFO');
    logMessage("   Использовано памяти: {$memoryUsed}MB (пик: {$peakMemoryMB}MB)", 'INFO');
    logMessage("   Batch ID: $batchId", 'INFO');
    
    return [
        'total_activities' => $totalActivities,
        'database_saved' => $totalSavedToDB,
        'batch_id' => $batchId,
        'memory_used_mb' => $memoryUsed,
        'peak_memory_mb' => $peakMemoryMB,
        'execution_time' => round(microtime(true) - $totalStartTime, 2)
    ];
}

// =============================================================================
// ЗАПУСК ПРОГРАММЫ
// =============================================================================

/**
 * Основная точка входа
 */
function startExport()
{
    try {
        logMessage("==========================================", 'INFO');
        logMessage("🚀 ЗАПУСК ЭКСПОРТА АКТИВНОСТЕЙ (cURL ВЕРСИЯ)", 'INFO');
        logMessage("Версия: 2.1 (cURL enhanced)", 'INFO');
        logMessage("Дата: " . date('Y-m-d H:i:s'), 'INFO');
        logMessage("==========================================", 'INFO');
        
        // Тестирование подключения перед началом
        if (!testBitrixConnection()) {
            throw new Exception("Не удалось установить подключение к Bitrix API");
        }
        
        // Дополнительная диагностика
        getBitrixServerInfo();
        
        $result = exportAllActivities();
        
        logMessage("==========================================", 'INFO');
        logMessage("✅ ЭКСПОРТ УСПЕШНО ЗАВЕРШЕН", 'INFO');
        logMessage("Время выполнения: {$result['execution_time']} секунд", 'INFO');
        logMessage("==========================================", 'INFO');
        
        return $result;
        
    } catch (Exception $e) {
        logMessage("❌ КРИТИЧЕСКАЯ ОШИБКА ЭКСПОРТА: " . $e->getMessage(), 'ERROR');
        
        // Дополнительная диагностика при ошибках
        logMessage("Диагностика при ошибке:", 'DEBUG');
        getBitrixServerInfo();
        
        return [
            'error' => $e->getMessage(),
            'total_activities' => 0,
            'database_saved' => 0
        ];
    }
}

/**
 * Точка входа в приложение
 */
function start()
{
    startExport();
}

?>