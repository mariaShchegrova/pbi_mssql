<?php
/**
 * deals_export_mssql.php
 * Экспорт сделок из Битрикс24 в MSSQL с использованием cURL
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
$BITRIX_API_URL = 'https://crm.ex.ru/local/api/v1/pbi/get_deals.php';
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
$DB_TABLE_NAME = 'CrmDeal';
$EXPORT_DIR = '/var/www/api.ex.ru/api/gateway/v1/pbi/logs';

// Гибкие параметры периода экспорта
//$CURRENT_YEAR = date('Y');
//$CURRENT_START_MONTH = date('n');
//$CURRENT_END_MONTH = date('n');
//$CURRENT_YEAR = 2025;
//$CURRENT_START_MONTH = 9;
//$CURRENT_END_MONTH = 10;

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
$MEMORY_LIMIT = '512M';
$REQUEST_TIMEOUT = (int)300;
$BATCH_SIZE = (int)100;
$RETRY_ATTEMPTS = (int)3;

// Настройка безопасности SSL
$SSL_VERIFY_PEER = filter_var(true, FILTER_VALIDATE_BOOLEAN);
$SSL_CA_FILE = null;

// Настройки cURL
$CURL_CONNECT_TIMEOUT = (int)300;
$CURL_FOLLOW_REDIRECTS = filter_var(true, FILTER_VALIDATE_BOOLEAN);
$CURL_MAX_REDIRECTS = (int)3;
$CURL_ENABLE_GZIP = filter_var(true, FILTER_VALIDATE_BOOLEAN);

// =============================================================================
// ИНИЦИАЛИЗАЦИЯ И НАСТРОЙКА
// =============================================================================

// Установка лимита памяти для обработки больших объемов данных
ini_set('memory_limit', $MEMORY_LIMIT);

// =============================================================================
// ФУНКЦИИ ЛОГИРОВАНИЯ
// =============================================================================

/**
 * Логирование сообщений с разными уровнями важности
 * 
 * @param string $message Сообщение для логирования
 * @param string $level Уровень важности (INFO, ERROR, ERROR, DEBUG)
 * @param bool $echo Выводить ли сообщение в stdout
 */
function logMessage($message, $level = 'INFO', $echo = true)
{
    $timestamp = date('Y-m-d H:i:s');
    $formattedMessage = "[$timestamp] [$level] $message\n";
    
    global $EXPORT_DIR;
    $logFile = $EXPORT_DIR . '/deals_export_'.date('Ymd').'.log';
    
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
 * Получение сделок из Битрикс24 через cURL
 * 
 * @param int $month Месяц для выборки
 * @param int $year Год для выборки  
 * @param int $limit Лимит на страницу
 * @param int $offset Смещение
 * @return array Данные сделок
 * @throws Exception При ошибках API или сети
 */
function fetchDealsFromBitrix($month, $year, $limit = 500, $offset = 0)
{
    global $BITRIX_API_URL, $API_TOKEN, $RETRY_ATTEMPTS;
    
    // Валидация входных параметров
    if ($month < 1 || $month > 12) {
        throw new InvalidArgumentException("Некорректный месяц: $month");
    }
    
    if ($year < 2000 || $year > 2100) {
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
        logMessage("Получено сделок: $itemsCount", 'INFO');
        
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
    
    // Создание директории если не существует
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
 * Валидация данных сделки перед сохранением
 * 
 * @param array $deal Данные сделки
 * @return bool Валидны ли данные
 * @throws InvalidArgumentException При невалидных данных
 */
function validateDealData($deal)
{
    if (!isset($deal['ID']) || !is_numeric($deal['ID'])) {
        throw new InvalidArgumentException("Invalid deal ID: " . ($deal['ID'] ?? 'null'));
    }
    
    if ($deal['ID'] <= 0) {
        throw new InvalidArgumentException("Deal ID must be positive: " . $deal['ID']);
    }
    
    // Валидация дат если они присутствуют
    $dateFields = ['DATE_CREATE', 'DATE_MODIFY', 'CLOSEDATE', 'UF_VPZ_DATE', 'UF_TESTDRIVE_DATE'];
    foreach ($dateFields as $field) {
        if (!empty($deal[$field]) && !strtotime($deal[$field])) {
            logMessage("Некорректный формат даты в поле $field: " . $deal[$field], 'ERROR');
            // Не бросаем исключение, а только логируем предупреждение
        }
    }
    
    return true;
}

/**
 * Очистка и нормализация данных сделки
 * 
 * @param array $deal Данные сделки
 * @return array Очищенные данные
 */
function sanitizeDealData($deal)
{
    // Ограничение длины текстовых полей для предотвращения переполнения БД
   /* $textFields = [
        'SOURCE' => 4000,
        'UF_CREDIT_DEAL_URL' => 2000,
        'UF_TRADE_DEAL_URL' => 2000,
        'UF_OBSERVER_NAME' => 1000,
        'UF_DEAL_FAIL_REASONS_NAME' => 2000,
        'UF_DEAL_URL' => 2000
    ];
    
    foreach ($textFields as $field => $maxLength) {
        if (isset($deal[$field]) && is_string($deal[$field]) && strlen($deal[$field]) > $maxLength) {
            $deal[$field] = substr($deal[$field], 0, $maxLength - 3) . '...';
            logMessage("Обрезано поле $field (длина: " . strlen($deal[$field]) . ")", 'DEBUG');
        }
    }*/
    
    // Конвертация пустых строк в null для числовых полей
    $numericFields = ['ASSIGNED_BY_ID', 'LEAD_ID', 'CATEGORY_ID', 'UF_DEPARTMENT_ID', 'UF_VPZ', 'UF_TESTDRIVE'];
    foreach ($numericFields as $field) {
        if (isset($deal[$field]) && $deal[$field] === '') {
            $deal[$field] = null;
        }
    }
    
    return $deal;
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
 * Создание таблицы сделок если не существует
 * 
 * @param PDO $pdo Объект PDO
 * @return bool Успешность операции
 */
function createDealsTable($pdo)
{
    global $DB_TABLE_NAME;
    
    $startTime = logStartOperation("Создание/проверка таблицы $DB_TABLE_NAME");
    
    $sql = "
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='$DB_TABLE_NAME' AND xtype='U')
    CREATE TABLE [$DB_TABLE_NAME] (
        [id] BIGINT NOT NULL,
        [date_create] DATETIME2 NULL,
        [date_modify] DATETIME2 NULL,
        [closedate] DATETIME2 NULL,
        [assigned_by_id] INT NULL,
        [lead_id] INT NULL,
        [source_id] NVARCHAR(50) NULL,
        [source_description] NVARCHAR(MAX) NULL,
        [category_id] INT NULL,
        [stage_id] NVARCHAR(50) NULL,
        [source] NVARCHAR(100) NULL,
        [category] NVARCHAR(100) NULL,
        [stage] NVARCHAR(100) NULL,
        [uf_department_id] INT NULL,
        [uf_vpz] INT NULL,
        [uf_vpz_date_old] NVARCHAR(100) NULL,
        [uf_vpz_date] DATETIME2 NULL,
        [uf_testdrive] INT NULL,
        [uf_testdrive_date] DATETIME2 NULL,
        [uf_credit_deal_id] INT NULL,
        [uf_credit_deal_url] NVARCHAR(100) NULL,
        [uf_trade_deal_id] INT NULL,
        [uf_trade_deal_url] NVARCHAR(100) NULL,
        [uf_issue_date] DATETIME2 NULL,
        [uf_contract_date] DATETIME2 NULL,
        [uf_car_brand] NVARCHAR(100) NULL,
        [uf_car_model] NVARCHAR(100) NULL,
        [uf_observer_name] NVARCHAR(100) NULL,
        [uf_external_system] NVARCHAR(100) NULL,
        [uf_external_system_name] NVARCHAR(100) NULL,
        [uf_deal_fail_reasons] NVARCHAR(100) NULL,
        [uf_deal_fail_reasons_name] NVARCHAR(100) NULL,
        [uf_source_handcrafted] NVARCHAR(100) NULL,
        [uf_source_handcrafted_name] NVARCHAR(100) NULL,
        [uf_department] NVARCHAR(100) NULL,
        [uf_deal_url] NVARCHAR(100) NULL,
        [export_batch] NVARCHAR(50) NULL,
        [export_date] DATETIME2 NULL,
        CONSTRAINT [PK_{$DB_TABLE_NAME}_id] PRIMARY KEY CLUSTERED ([id])
    )";
    
    try {
        $pdo->exec($sql);
        
        // Создание индексов для улучшения производительности
        $indexes = [
            /*"CREATE INDEX [idx_date_create] ON [$DB_TABLE_NAME] ([date_create])",
            "CREATE INDEX [idx_date_modify] ON [$DB_TABLE_NAME] ([date_modify])",
            "CREATE INDEX [idx_category_id] ON [$DB_TABLE_NAME] ([category_id])", 
            "CREATE INDEX [idx_stage_id] ON [$DB_TABLE_NAME] ([stage_id])",
            "CREATE INDEX [idx_assigned_by_id] ON [$DB_TABLE_NAME] ([assigned_by_id])"*/
        ];
        
        foreach ($indexes as $indexSql) {
            try {
                $pdo->exec($indexSql);
                logMessage("Создан индекс: " . substr($indexSql, 0, 50) . "...", 'DEBUG');
            } catch (PDOException $e) {
                // Игнорируем ошибки если индекс уже существует
                logMessage("Индекс уже существует или ошибка создания: " . $e->getMessage(), 'DEBUG');
            }
        }
        
        logEndOperation("Создание/проверка таблицы $DB_TABLE_NAME", $startTime);
        return true;
        
    } catch (PDOException $e) {
        logMessage("Ошибка создания таблицы в MSSQL: " . $e->getMessage(), 'ERROR');
        return false;
    }
}

/**
 * Пакетное сохранение сделок в базу данных
 * 
 * @param PDO $pdo Объект PDO
 * @param array $deals Массив сделок
 * @param string|null $batchId Идентификатор пачки
 * @return int Количество сохраненных сделок
 */
function saveDealsToDatabase($pdo, $deals, $batchId = null)
{
    global $DB_TABLE_NAME, $BATCH_SIZE;
    
    //$deals = [];//оснановка записи в БД
    if (empty($deals)) {
        logMessage("Нет данных для сохранения в MSSQL", 'INFO');
        return 0;
    }
    
    $batchId = $batchId ?: date('Ymd_His');
    $exportDate = date('Y-m-d H:i:s');
    $totalSaved = 0;
    
    // Разбиваем на пачки для избежания переполнения памяти
    $batches = array_chunk($deals, $BATCH_SIZE);
    
    logMessage("Сохранение " . count($deals) . " сделок в " . count($batches) . " пачках", 'INFO');
    //$i=0;
    foreach ($batches as $batchIndex => $batchDeals) {
       // if($i==0){
        $savedInBatch = saveDealsBatch($pdo, $batchDeals, $batchId, $exportDate, $batchIndex + 1);
        $totalSaved += $savedInBatch;
       // }
        //$i++;
        // Освобождаем память после обработки пачки
        unset($batchDeals);
    }
    
    logMessage("Итого сохранено в MSSQL: $totalSaved сделок (batch: $batchId)", 'INFO');
    return $totalSaved;
}

/**
 * Сохранение одной пачки сделок с использованием MERGE
 * 
 * @param PDO $pdo Объект PDO
 * @param array $deals Пачка сделок
 * @param string $batchId Идентификатор пачки
 * @param string $exportDate Дата экспорта
 * @param int $batchNumber Номер пачки
 * @return int Количество сохраненных сделок
 */
function saveDealsBatch($pdo, $deals, $batchId, $exportDate, $batchNumber = 1)
{
    global $DB_TABLE_NAME;
    
    $startTime = microtime(true);
    $savedCount = 0;
    
    $sql = "MERGE [$DB_TABLE_NAME] WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) 
            AS source (id, date_create, date_modify, closedate, assigned_by_id, lead_id, source_id, source_description, category_id, stage_id, source, category, stage, uf_department_id, uf_vpz, uf_vpz_date_old, uf_vpz_date, uf_testdrive, uf_testdrive_date, uf_credit_deal_id, uf_credit_deal_url, uf_trade_deal_id, uf_trade_deal_url, uf_issue_date, uf_contract_date, uf_car_brand, uf_car_model, uf_observer_name, uf_external_system, uf_external_system_name, uf_deal_fail_reasons, uf_deal_fail_reasons_name, uf_source_handcrafted, uf_source_handcrafted_name, uf_department, uf_deal_url, export_batch, export_date)
            ON (target.id = source.id)
            WHEN MATCHED THEN
                UPDATE SET 
                    date_create = source.date_create,
                    date_modify = source.date_modify,
                    closedate = source.closedate,
                    assigned_by_id = source.assigned_by_id,
                    source_id = source.source_id,
                    source_description = source.source_description,
                    category_id = source.category_id,
                    stage_id = source.stage_id,
                    source = source.source,
                    category = source.category,
                    stage = source.stage,
                    uf_department_id = source.uf_department_id,
                    uf_vpz = source.uf_vpz,
                    uf_vpz_date_old = source.uf_vpz_date_old,
                    uf_vpz_date = source.uf_vpz_date,
                    uf_testdrive = source.uf_testdrive,
                    uf_testdrive_date = source.uf_testdrive_date,
                    uf_credit_deal_id = source.uf_credit_deal_id,
                    uf_credit_deal_url = source.uf_credit_deal_url,
                    uf_trade_deal_id = source.uf_trade_deal_id,
                    uf_trade_deal_url = source.uf_trade_deal_url,
                    uf_issue_date = source.uf_issue_date,
                    uf_contract_date = source.uf_contract_date,
                    uf_car_brand = source.uf_car_brand,
                    uf_car_model = source.uf_car_model,
                    uf_observer_name = source.uf_observer_name,
                    uf_external_system = source.uf_external_system,
                    uf_external_system_name = source.uf_external_system_name,
                    uf_deal_fail_reasons = source.uf_deal_fail_reasons,
                    uf_deal_fail_reasons_name = source.uf_deal_fail_reasons_name,
                    uf_source_handcrafted = source.uf_source_handcrafted,
                    uf_source_handcrafted_name = source.uf_source_handcrafted_name,
                    uf_department = source.uf_department,
                    uf_deal_url = source.uf_deal_url,
                    export_batch = source.export_batch,
                    export_date = source.export_date
            WHEN NOT MATCHED THEN
                INSERT (id, date_create, date_modify, closedate, assigned_by_id, lead_id, source_id, source_description, category_id, stage_id, source, category, stage, uf_department_id, uf_vpz, uf_vpz_date_old, uf_vpz_date, uf_testdrive, uf_testdrive_date, uf_credit_deal_id, uf_credit_deal_url, uf_trade_deal_id, uf_trade_deal_url, uf_issue_date, uf_contract_date, uf_car_brand, uf_car_model, uf_observer_name, uf_external_system, uf_external_system_name, uf_deal_fail_reasons, uf_deal_fail_reasons_name, uf_source_handcrafted, uf_source_handcrafted_name, uf_department, uf_deal_url, export_batch, export_date)
                VALUES (source.id, source.date_create, source.date_modify, source.closedate, source.assigned_by_id, source.lead_id, source.source_id, source.source_description, source.category_id, source.stage_id, source.source, source.category, source.stage, source.uf_department_id, source.uf_vpz, source.uf_vpz_date_old, source.uf_vpz_date, source.uf_testdrive, source.uf_testdrive_date, source.uf_credit_deal_id, source.uf_credit_deal_url, source.uf_trade_deal_id, source.uf_trade_deal_url, source.uf_issue_date, source.uf_contract_date, source.uf_car_brand, source.uf_car_model, source.uf_observer_name, source.uf_external_system, source.uf_external_system_name, source.uf_deal_fail_reasons, source.uf_deal_fail_reasons_name, source.uf_source_handcrafted, source.uf_source_handcrafted_name, source.uf_department, source.uf_deal_url, source.export_batch, source.export_date);";
    
    try {
        $stmt = $pdo->prepare($sql);
        $pdo->beginTransaction();
        
        foreach ($deals as $deal) {
            try {
                // Валидация и очистка данных
                validateDealData($deal);
                $deal = sanitizeDealData($deal);
                
                $values = [
                    $deal['ID'] ?? null,
                    $deal['DATE_CREATE'] ?? null,
                    $deal['DATE_MODIFY'] ?? null,
                    $deal['CLOSEDATE'] ?? null,
                    $deal['ASSIGNED_BY_ID'] ?? null,
                    $deal['LEAD_ID'] ?? null,
                    $deal['SOURCE_ID'] ?? null,
                    $deal['SOURCE_DESCRIPTION'] ?? null,
                    $deal['CATEGORY_ID'] ?? null,
                    $deal['STAGE_ID'] ?? null,
                    $deal['SOURCE'] ?? null,
                    $deal['CATEGORY'] ?? null,
                    $deal['STAGE'] ?? null,
                    $deal['UF_DEPARTMENT_ID'] ?? null,
                    $deal['UF_VPZ'] ?? null,
                    $deal['UF_VPZ_DATE_OLD'] ?? null,
                    $deal['UF_VPZ_DATE'] ?? null,
                    $deal['UF_TESTDRIVE'] ?? null,
                    $deal['UF_TESTDRIVE_DATE'] ?? null,
                    $deal['UF_CREDIT_DEAL_ID'] ?? null,
                    $deal['UF_CREDIT_DEAL_URL'] ?? null,
                    $deal['UF_TRADE_DEAL_ID'] ?? null,
                    $deal['UF_TRADE_DEAL_URL'] ?? null,
                    $deal['UF_ISSUE_DATE'] ?? null,
                    $deal['UF_CONTRACT_DATE'] ?? null,
                    $deal['UF_CAR_BRAND'] ?? null,
                    $deal['UF_CAR_MODEL'] ?? null,
                    $deal['UF_OBSERVER_NAME'] ?? null,
                    $deal['UF_EXTERNAL_SYSTEM'] ?? null,
                    $deal['UF_EXTERNAL_SYSTEM_NAME'] ?? null,
                    $deal['UF_DEAL_FAIL_REASONS'] ?? null,
                    $deal['UF_DEAL_FAIL_REASONS_NAME'] ?? null,
                    $deal['UF_SOURCE_HANDCRAFTED'] ?? null,
                    $deal['UF_SOURCE_HANDCRAFTED_NAME'] ?? null,
                    $deal['UF_DEPARTMENT'] ?? null,
                    $deal['UF_DEAL_URL'] ?? null,
                    $batchId,
                    $exportDate
                ];
                
                $stmt->execute($values);
                $savedCount++;
                
            } catch (InvalidArgumentException $e) {
                logMessage("Пропущена невалидная сделка ID {$deal['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            } catch (Exception $e) {
                logMessage("Ошибка обработки сделки ID {$deal['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            }
        }
        
        $pdo->commit();
        $executionTime = round(microtime(true) - $startTime, 2);
        logMessage("Пачка $batchNumber: сохранено $savedCount сделок за {$executionTime}с", 'INFO');
        
        return $savedCount;
        
    } catch (PDOException $e) {
        $pdo->rollBack();
        logMessage("Ошибка MERGE в пачке $batchNumber: " . $e->getMessage(), 'ERROR');
        
        // Альтернативный способ вставки через отдельные INSERT/UPDATE
        logMessage("Попытка альтернативного сохранения пачки $batchNumber...", 'INFO');
        return saveDealsAlternative($pdo, $deals, $batchId);
    }
}

/**
 * Альтернативный метод сохранения для MSSQL (если MERGE не работает)
 * 
 * @param PDO $pdo Объект PDO
 * @param array $deals Массив сделок
 * @param string|null $batchId Идентификатор пачки
 * @return int Количество сохраненных сделок
 */
function saveDealsAlternative($pdo, $deals, $batchId = null)
{
    global $DB_TABLE_NAME;
    
    if (empty($deals)) {
        return 0;
    }
    
    $batchId = $batchId ?: date('Ymd_His');
    $exportDate = date('Y-m-d H:i:s');
    $savedCount = 0;
    
    try {
        $pdo->beginTransaction();
        
        foreach ($deals as $deal) {
            try {
                // Валидация и очистка данных
                validateDealData($deal);
                $deal = sanitizeDealData($deal);
                
                // Проверяем существование записи
                $checkSql = "SELECT id FROM [$DB_TABLE_NAME] WHERE id = ?";
                $checkStmt = $pdo->prepare($checkSql);
                $checkStmt->execute([$deal['ID'] ?? null]);
                $exists = $checkStmt->fetch();
                
                $values = [
                    $deal['ID'] ?? null,
                    $deal['DATE_CREATE'] ?? null,
                    $deal['DATE_MODIFY'] ?? null,
                    $deal['CLOSEDATE'] ?? null,
                    $deal['ASSIGNED_BY_ID'] ?? null,
                    $deal['LEAD_ID'] ?? null,
                    $deal['SOURCE_ID'] ?? null,
                    $deal['SOURCE_DESCRIPTION'] ?? null,
                    $deal['CATEGORY_ID'] ?? null,
                    $deal['STAGE_ID'] ?? null,
                    $deal['SOURCE'] ?? null,
                    $deal['CATEGORY'] ?? null,
                    $deal['STAGE'] ?? null,
                    $deal['UF_DEPARTMENT_ID'] ?? null,
                    $deal['UF_VPZ'] ?? null,
                    $deal['UF_VPZ_DATE_OLD'] ?? null,
                    $deal['UF_VPZ_DATE'] ?? null,
                    $deal['UF_TESTDRIVE'] ?? null,
                    $deal['UF_TESTDRIVE_DATE'] ?? null,
                    $deal['UF_CREDIT_DEAL_ID'] ?? null,
                    $deal['UF_CREDIT_DEAL_URL'] ?? null,
                    $deal['UF_TRADE_DEAL_ID'] ?? null,
                    $deal['UF_TRADE_DEAL_URL'] ?? null,
                    $deal['UF_ISSUE_DATE'] ?? null,
                    $deal['UF_CONTRACT_DATE'] ?? null,
                    $deal['UF_CAR_BRAND'] ?? null,
                    $deal['UF_CAR_MODEL'] ?? null,
                    $deal['UF_OBSERVER_NAME'] ?? null,
                    $deal['UF_EXTERNAL_SYSTEM'] ?? null,
                    $deal['UF_EXTERNAL_SYSTEM_NAME'] ?? null,
                    $deal['UF_DEAL_FAIL_REASONS'] ?? null,
                    $deal['UF_DEAL_FAIL_REASONS_NAME'] ?? null,
                    $deal['UF_SOURCE_HANDCRAFTED'] ?? null,
                    $deal['UF_SOURCE_HANDCRAFTED_NAME'] ?? null,
                    $deal['UF_DEPARTMENT'] ?? null,
                    $deal['UF_DEAL_URL'] ?? null,
                    $batchId,
                    $exportDate
                ];
                
                if ($exists) {
                    // UPDATE существующей записи
                    $updateSql = "UPDATE [$DB_TABLE_NAME] SET 
                        date_create = ?, date_modify = ?, closedate = ?, assigned_by_id = ?, 
                        source_id = ?, source_description = ?, category_id = ?, stage_id = ?, 
                        source = ?, category = ?, stage = ?, uf_department_id = ?, 
                        uf_vpz = ?, uf_vpz_date_old = ?, uf_vpz_date = ?, uf_testdrive = ?, 
                        uf_testdrive_date = ?, uf_credit_deal_id = ?, uf_credit_deal_url = ?, 
                        uf_trade_deal_id = ?, uf_trade_deal_url = ?, uf_issue_date = ?, 
                        uf_contract_date = ?, uf_car_brand = ?, uf_car_model = ?, 
                        uf_observer_name = ?, uf_external_system = ?, uf_external_system_name = ?, 
                        uf_deal_fail_reasons = ?, uf_deal_fail_reasons_name = ?, 
                        uf_source_handcrafted = ?, uf_source_handcrafted_name = ?, 
                        uf_department = ?, uf_deal_url = ?, export_batch = ?, export_date = ?
                        WHERE id = ?";
                    
                    // Добавляем ID в конец для WHERE условия
                    $values[] = $deal['ID'] ?? null;
                    $stmt = $pdo->prepare($updateSql);
                } else {
                    // INSERT новой записи
                    $insertSql = "INSERT INTO [$DB_TABLE_NAME] (
                        id, date_create, date_modify, closedate, assigned_by_id, lead_id,
                        source_id, source_description, category_id, stage_id, source, category, stage, uf_department_id,
                        uf_vpz, uf_vpz_date_old, uf_vpz_date, uf_testdrive, uf_testdrive_date,
                        uf_credit_deal_id, uf_credit_deal_url, uf_trade_deal_id, uf_trade_deal_url,
                        uf_issue_date, uf_contract_date, uf_car_brand, uf_car_model, uf_observer_name,
                        uf_external_system, uf_external_system_name, uf_deal_fail_reasons, uf_deal_fail_reasons_name,
                        uf_source_handcrafted, uf_source_handcrafted_name, uf_department, uf_deal_url, export_batch, export_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    
                    $stmt = $pdo->prepare($insertSql);
                }
                
                $stmt->execute($values);
                $savedCount++;
                
            } catch (Exception $e) {
                logMessage("Ошибка альтернативного сохранения сделки ID {$deal['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            }
        }
        
        $pdo->commit();
        logMessage("Альтернативное сохранение завершено: $savedCount сделок (batch: $batchId)", 'INFO');
        return $savedCount;
        
    } catch (PDOException $e) {
        $pdo->rollBack();
        logMessage("Критическая ошибка альтернативного сохранения в MSSQL: " . $e->getMessage(), 'ERROR');
        return 0;
    }
}

// =============================================================================
// ОСНОВНАЯ ЛОГИКА ЭКСПОРТА
// =============================================================================

/**
 * Основная функция экспорта сделок
 * 
 * @return array Результаты экспорта
 */
function exportAllDeals()
{
    global $YEAR, $START_MONTH, $END_MONTH;
    
    $totalStartTime = logStartOperation("ЭКСПОРТ ЗА $YEAR ГОД (месяцы $START_MONTH-$END_MONTH)");
    
    $allDeals = [];
    $totalDeals = 0;
    $totalSavedToDB = 0;
    $batchId = date('Ymd_His');
    
    // Подключаемся к MSSQL
    $pdo = connectToDatabase();
    if ($pdo) {
        createDealsTable($pdo);
    } else {
        logMessage("ВНИМАНИЕ: Работаем без MSSQL, только файлы", 'ERROR');
    }
    
    // Мониторинг использования памяти
    $initialMemory = memory_get_usage(true);
    
    // Проходим по всем месяцам указанного периода
    for ($month = $START_MONTH; $month <= $END_MONTH; $month++) {
        $monthStartTime = logStartOperation("Обработка месяца $month");
        
        $monthDeals = [];
        $offset = 0;
        $limit = 500;
        $monthTotal = 0;
        
        logMessage("📅 Экспорт месяца: $month/$YEAR", 'INFO');
        
        // Пагинация по страницам
        do {
            try {
                $pageData = fetchDealsFromBitrix($month, $YEAR, $limit, $offset);
                
                if (!empty($pageData['items'])) {
                    $dealsCount = count($pageData['items']);
                    $monthDeals = array_merge($monthDeals, $pageData['items']);
                    $monthTotal += $dealsCount;
                    
                    // Сохраняем в MSSQL порциями
                    if ($pdo) {
                        $savedCount = saveDealsToDatabase($pdo, $pageData['items'], $batchId . "_m{$month}");
                        $totalSavedToDB += $savedCount;
                    }
                    
                    $offset += $limit;
                    
                    logMessage("Месяц $month, страница: $dealsCount сделок, offset: $offset", 'DEBUG');
                    
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
                    logMessage("В месяце $month больше нет сделок", 'INFO');
                    break;
                }
            } catch (Exception $e) {
                logMessage("Критическая ошибка при загрузке месяца $month: " . $e->getMessage(), 'ERROR');
                break;
            }
        } while (!empty($pageData['items']) && ($pageData['has_more'] ?? false));
        
        $allDeals = array_merge($allDeals, $monthDeals);
        $totalDeals += count($monthDeals);
        
        logEndOperation("Обработка месяца $month", $monthStartTime);
        logMessage("Месяц $month завершен. Сделок: " . count($monthDeals), 'INFO');
        
        // Сохраняем файл по месяцам
        saveToFile([
            'month' => $month,
            'year' => $YEAR,
            'deals_count' => count($monthDeals),
            'deals' => $monthDeals,
            'export_date' => date('Y-m-d H:i:s'),
            'batch_id' => $batchId
        ], "deals_{$YEAR}_month_{$month}_{$batchId}.json");
        
        // Освобождаем память после обработки месяца
        unset($monthDeals);
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
        'total_deals' => $totalDeals,
        'database_saved' => $totalSavedToDB,
        'deals' => $allDeals,
        'export_date' => date('Y-m-d H:i:s'),
        'year' => $YEAR,
        'batch_id' => $batchId,
        'memory_used_mb' => $memoryUsed,
        'peak_memory_mb' => $peakMemoryMB,
        'months_exported' => "$START_MONTH-$END_MONTH"
    ], "deals_{$YEAR}_full_export_{$batchId}.json");
    
    logEndOperation("ЭКСПОРТ ЗА $YEAR ГОД", $totalStartTime);
    logMessage("📊 ИТОГИ ЭКСПОРТА:", 'INFO');
    logMessage("   Всего сделок: $totalDeals", 'INFO');
    logMessage("   Сохранено в MSSQL: $totalSavedToDB", 'INFO');
    logMessage("   Использовано памяти: {$memoryUsed}MB (пик: {$peakMemoryMB}MB)", 'INFO');
    logMessage("   Batch ID: $batchId", 'INFO');
    
    return [
        'total_deals' => $totalDeals,
        'database_saved' => $totalSavedToDB,
        'batch_id' => $batchId,
        'memory_used_mb' => $memoryUsed,
        'peak_memory_mb' => $peakMemoryMB,
        'execution_time' => round(microtime(true) - $totalStartTime, 2)
    ];
}

/**
 * Функция запуска экспорта с улучшенной диагностикой
 */
function startExport()
{
    try {
        logMessage("==========================================", 'INFO');
        logMessage("🚀 ЗАПУСК ЭКСПОРТА СДЕЛОК (cURL ВЕРСИЯ)", 'INFO');
        logMessage("Версия: 2.1 (cURL enhanced)", 'INFO');
        logMessage("Дата: " . date('Y-m-d H:i:s'), 'INFO');
        logMessage("==========================================", 'INFO');
        
        // Тестирование подключения перед началом
        if (!testBitrixConnection()) {
            throw new Exception("Не удалось установить подключение к Bitrix API");
        }
        
        // Дополнительная диагностика
        getBitrixServerInfo();
        
        $result = exportAllDeals();
        
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
            'total_deals' => 0,
            'database_saved' => 0
        ];
    }
}

// =============================================================================
// ЗАПУСК ПРИЛОЖЕНИЯ
// =============================================================================

/**
 * Точка входа в приложение
 */
function start()
{

    // Проверка что запуск из командной строки
   /* if (php_sapi_name() !== 'cli') {
        header('Content-Type: application/json');
        
        try {
            $result = startExport();
            echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        } catch (Exception $e) {
            http_response_code(500);
            echo json_encode([
                'error' => $e->getMessage(),
                'status' => 'error'
            ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        }
        
        exit;
    }*/
    
    // Запуск из командной строки
    startExport();
}

// Автоматический запуск при прямом вызове файла
/*if (isset($_SERVER['argv'][0]) && basename($_SERVER['argv'][0]) === basename(__FILE__)) {
    start();
}*/

// Ручной запуск при необходимости
// start();