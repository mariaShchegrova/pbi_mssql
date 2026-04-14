<?php
/**
 * Экспорт истории сделок из Битрикс24 в MSSQL с использованием cURL
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
$BITRIX_API_URL = 'https://crm.ex.ru/local/api/v1/pbi/get_deals_history.php';
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
$DB_TABLE_NAME = 'CrmDealStageHistory';
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
 * @param string $level Уровень важности (INFO, ERROR, DEBUG)
 * @param bool $echo Выводить ли сообщение в stdout
 */
function logMessage($message, $level = 'INFO', $echo = true)
{
    $timestamp = date('Y-m-d H:i:s');
    $formattedMessage = "[$timestamp] [$level] $message\n";
    
    global $EXPORT_DIR;
    $logFile = $EXPORT_DIR . '/deals_history_export_'.date('Ymd').'.log';
    
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
 * Получение истории сделок из Битрикс24 через cURL
 * 
 * @param int $month Месяц для выборки
 * @param int $year Год для выборки  
 * @param int $limit Лимит на страницу
 * @param int $offset Смещение
 * @return array Данные истории сделок
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
        logMessage("Получено записей истории: $itemsCount", 'INFO');
        
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
 * Валидация данных истории сделки перед сохранением
 * 
 * @param array $dealHistory Данные истории сделки
 * @return bool Валидны ли данные
 * @throws InvalidArgumentException При невалидных данных
 */
function validateDealData($dealHistory)
{
    if (!isset($dealHistory['ID']) || !is_numeric($dealHistory['ID'])) {
        throw new InvalidArgumentException("Invalid deal history ID: " . ($dealHistory['ID'] ?? 'null'));
    }
    
    if ($dealHistory['ID'] <= 0) {
        throw new InvalidArgumentException("Deal history ID must be positive: " . $dealHistory['ID']);
    }
    
    if (!isset($dealHistory['OWNER_ID']) || !is_numeric($dealHistory['OWNER_ID'])) {
        throw new InvalidArgumentException("Invalid OWNER_ID: " . ($dealHistory['OWNER_ID'] ?? 'null'));
    }
    
    // Валидация дат если они присутствуют
    $dateFields = ['CREATED_DATE', 'CREATED_TIME', 'EFFECTIVE_DATE', 'END_DATE', 'START_DATE'];
    foreach ($dateFields as $field) {
        if (!empty($dealHistory[$field]) && !strtotime($dealHistory[$field])) {
            logMessage("Некорректный формат даты в поле $field: " . $dealHistory[$field], 'ERROR');
            // Не бросаем исключение, а только логируем предупреждение
        }
    }
    
    return true;
}

/**
 * Очистка и нормализация данных истории сделки
 * 
 * @param array $dealHistory Данные истории сделки
 * @return array Очищенные данные
 */
function sanitizeDealData($dealHistory)
{
    // Конвертация пустых строк в null для числовых полей
    $numericFields = ['CATEGORY_ID', 'OWNER_ID', 'RESPONSIBLE_ID', 'TYPE_ID'];
    foreach ($numericFields as $field) {
        if (isset($dealHistory[$field]) && $dealHistory[$field] === '') {
            $dealHistory[$field] = null;
        }
    }
    
    // Конвертация текстовых значений в булевы для IS_LOST
    if (isset($dealHistory['IS_LOST'])) {
        $dealHistory['IS_LOST'] = ($dealHistory['IS_LOST'] === 'Y') ? 1 : 0;
    }
    
    return $dealHistory;
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
 * Создание таблицы истории сделок если не существует
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
        [category_id] INT NULL,
        [created_date] DATE  NULL,
        [created_time] DATETIME2 NULL,
        [effective_date] DATE  NULL,
        [end_date] DATE  NULL,
        [end_period_month] INT NULL,
        [end_period_quarter] INT NULL,
        [end_period_year] INT NULL,
        [owner_id] BIGINT NULL,
        [is_lost] BIT NULL,
        [period_month] INT NULL,
        [period_quarter] INT NULL,
        [period_year] INT NULL,
        [responsible_id] INT NULL,
        [stage] NVARCHAR(100) NULL,
        [stage_id] NVARCHAR(50) NULL,
        [stage_semantic] NVARCHAR(50) NULL,
        [stage_semantic_id] NVARCHAR(1) NULL,
        [start_date] DATE NULL,
        [start_period_month] INT NULL,
        [start_period_quarter] INT NULL,
        [start_period_year] INT NULL,
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
 * Пакетное сохранение истории сделок в базу данных
 * 
 * @param PDO $pdo Объект PDO
 * @param array $deals Массив записей истории сделок
 * @param string|null $batchId Идентификатор пачки
 * @return int Количество сохраненных записей
 */
function saveDealsToDatabase($pdo, $deals, $batchId = null)
{
    global $DB_TABLE_NAME, $BATCH_SIZE;
    
    if (empty($deals)) {
        logMessage("Нет данных для сохранения в MSSQL", 'INFO');
        return 0;
    }
    
    $batchId = $batchId ?: date('Ymd_His');
    $exportDate = date('Y-m-d H:i:s');
    $totalSaved = 0;
    
    // Разбиваем на пачки для избежания переполнения памяти
    $batches = array_chunk($deals, $BATCH_SIZE);
    
    logMessage("Сохранение " . count($deals) . " записей истории в " . count($batches) . " пачках", 'INFO');
    
    foreach ($batches as $batchIndex => $batchDeals) {
        $savedInBatch = saveDealsBatch($pdo, $batchDeals, $batchId, $exportDate, $batchIndex + 1);
        $totalSaved += $savedInBatch;
        
        // Освобождаем память после обработки пачки
        unset($batchDeals);
    }
    
    logMessage("Итого сохранено в MSSQL: $totalSaved записей истории (batch: $batchId)", 'INFO');
    return $totalSaved;
}

/**
 * Сохранение одной пачки записей истории с использованием MERGE
 * 
 * @param PDO $pdo Объект PDO
 * @param array $deals Пачка записей истории
 * @param string $batchId Идентификатор пачки
 * @param string $exportDate Дата экспорта
 * @param int $batchNumber Номер пачки
 * @return int Количество сохраненных записей
 */
function saveDealsBatch($pdo, $deals, $batchId, $exportDate, $batchNumber = 1)
{
    global $DB_TABLE_NAME;
    
    $startTime = microtime(true);
    $savedCount = 0;
    
    $sql = "MERGE [$DB_TABLE_NAME] WITH (HOLDLOCK) AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) 
            AS source (id, category_id, created_date, created_time, effective_date, end_date, end_period_month, end_period_quarter, end_period_year, owner_id, is_lost, period_month, period_quarter, period_year, responsible_id, stage, stage_id, stage_semantic, stage_semantic_id, start_date, start_period_month, start_period_quarter, start_period_year, type_id, export_batch, export_date)
            ON (target.id = source.id)
            WHEN MATCHED THEN
                UPDATE SET 
                    category_id = source.category_id,
                    created_date = source.created_date,
                    created_time = source.created_time,
                    effective_date = source.effective_date,
                    end_date = source.end_date,
                    end_period_month = source.end_period_month,
                    end_period_quarter = source.end_period_quarter,
                    end_period_year = source.end_period_year,
                    owner_id = source.owner_id,
                    is_lost = source.is_lost,
                    period_month = source.period_month,
                    period_quarter = source.period_quarter,
                    period_year = source.period_year,
                    responsible_id = source.responsible_id,
                    stage = source.stage,
                    stage_id = source.stage_id,
                    stage_semantic = source.stage_semantic,
                    stage_semantic_id = source.stage_semantic_id,
                    start_date = source.start_date,
                    start_period_month = source.start_period_month,
                    start_period_quarter = source.start_period_quarter,
                    start_period_year = source.start_period_year,
                    type_id = source.type_id,
                    export_batch = source.export_batch,
                    export_date = source.export_date
            WHEN NOT MATCHED THEN
                INSERT (id, category_id, created_date, created_time, effective_date, end_date, end_period_month, end_period_quarter, end_period_year, owner_id, is_lost, period_month, period_quarter, period_year, responsible_id, stage, stage_id, stage_semantic, stage_semantic_id, start_date, start_period_month, start_period_quarter, start_period_year, type_id, export_batch, export_date)
                VALUES (source.id, source.category_id, source.created_date, source.created_time, source.effective_date, source.end_date, source.end_period_month, source.end_period_quarter, source.end_period_year, source.owner_id, source.is_lost, source.period_month, source.period_quarter, source.period_year, source.responsible_id, source.stage, source.stage_id, source.stage_semantic, source.stage_semantic_id, source.start_date, source.start_period_month, source.start_period_quarter, source.start_period_year, source.type_id, source.export_batch, source.export_date);";
    
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
                    $deal['CATEGORY_ID'] ?? null,
                    $deal['CREATED_DATE'] ?? null,
                    $deal['CREATED_TIME'] ?? null,
                    $deal['EFFECTIVE_DATE'] ?? null,
                    $deal['END_DATE'] ?? null,
                    $deal['END_PERIOD_MONTH'] ?? null,
                    $deal['END_PERIOD_QUARTER'] ?? null,
                    $deal['END_PERIOD_YEAR'] ?? null,
                    $deal['OWNER_ID'] ?? null,
                    $deal['IS_LOST'] ?? null,
                    $deal['PERIOD_MONTH'] ?? null,
                    $deal['PERIOD_QUARTER'] ?? null,
                    $deal['PERIOD_YEAR'] ?? null,
                    $deal['RESPONSIBLE_ID'] ?? null,
                    $deal['STAGE'] ?? null,
                    $deal['STAGE_ID'] ?? null,
                    $deal['STAGE_SEMANTIC'] ?? null,
                    $deal['STAGE_SEMANTIC_ID'] ?? null,
                    $deal['START_DATE'] ?? null,
                    $deal['START_PERIOD_MONTH'] ?? null,
                    $deal['START_PERIOD_QUARTER'] ?? null,
                    $deal['START_PERIOD_YEAR'] ?? null,
                    $deal['TYPE_ID'] ?? null,
                    $batchId,
                    $exportDate
                ];
                
                $stmt->execute($values);
                $savedCount++;
                
            } catch (InvalidArgumentException $e) {
                logMessage("Пропущена невалидная запись истории ID {$deal['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            } catch (Exception $e) {
                logMessage("Ошибка обработки записи истории ID {$deal['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            }
        }
        
        $pdo->commit();
        $executionTime = round(microtime(true) - $startTime, 2);
        logMessage("Пачка $batchNumber: сохранено $savedCount записей за {$executionTime}с", 'INFO');
        
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
 * @param array $deals Массив записей истории
 * @param string|null $batchId Идентификатор пачки
 * @return int Количество сохраненных записей
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
                    $deal['CATEGORY_ID'] ?? null,
                    $deal['CREATED_DATE'] ?? null,
                    $deal['CREATED_TIME'] ?? null,
                    $deal['EFFECTIVE_DATE'] ?? null,
                    $deal['END_DATE'] ?? null,
                    $deal['END_PERIOD_MONTH'] ?? null,
                    $deal['END_PERIOD_QUARTER'] ?? null,
                    $deal['END_PERIOD_YEAR'] ?? null,
                    $deal['OWNER_ID'] ?? null,
                    $deal['IS_LOST'] ?? null,
                    $deal['PERIOD_MONTH'] ?? null,
                    $deal['PERIOD_QUARTER'] ?? null,
                    $deal['PERIOD_YEAR'] ?? null,
                    $deal['RESPONSIBLE_ID'] ?? null,
                    $deal['STAGE'] ?? null,
                    $deal['STAGE_ID'] ?? null,
                    $deal['STAGE_SEMANTIC'] ?? null,
                    $deal['STAGE_SEMANTIC_ID'] ?? null,
                    $deal['START_DATE'] ?? null,
                    $deal['START_PERIOD_MONTH'] ?? null,
                    $deal['START_PERIOD_QUARTER'] ?? null,
                    $deal['START_PERIOD_YEAR'] ?? null,
                    $deal['TYPE_ID'] ?? null,
                    $batchId,
                    $exportDate
                ];
                
                if ($exists) {
                    // UPDATE существующей записи
                    $updateSql = "UPDATE [$DB_TABLE_NAME] SET 
                        category_id = ?, created_date = ?, created_time = ?, effective_date = ?, 
                        end_date = ?, end_period_month = ?, end_period_quarter = ?, end_period_year = ?, 
                        owner_id = ?, is_lost = ?, period_month = ?, period_quarter = ?, period_year = ?, 
                        responsible_id = ?, stage = ?, stage_id = ?, stage_semantic = ?, stage_semantic_id = ?, 
                        start_date = ?, start_period_month = ?, start_period_quarter = ?, start_period_year = ?, 
                        type_id = ?, export_batch = ?, export_date = ?
                        WHERE id = ?";
                    
                    // Добавляем ID в конец для WHERE условия
                    $values[] = $deal['ID'] ?? null;
                    $stmt = $pdo->prepare($updateSql);
                } else {
                    // INSERT новой записи
                    $insertSql = "INSERT INTO [$DB_TABLE_NAME] (
                        id, category_id, created_date, created_time, effective_date, end_date, 
                        end_period_month, end_period_quarter, end_period_year, owner_id, is_lost, 
                        period_month, period_quarter, period_year, responsible_id, stage, stage_id, 
                        stage_semantic, stage_semantic_id, start_date, start_period_month, 
                        start_period_quarter, start_period_year, type_id, export_batch, export_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    
                    $stmt = $pdo->prepare($insertSql);
                }
                
                $stmt->execute($values);
                $savedCount++;
                
            } catch (Exception $e) {
                logMessage("Ошибка альтернативного сохранения записи истории ID {$deal['ID']}: " . $e->getMessage(), 'ERROR');
                continue;
            }
        }
        
        $pdo->commit();
        logMessage("Альтернативное сохранение завершено: $savedCount записей (batch: $batchId)", 'INFO');
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
 * Основная функция экспорта истории сделок
 * 
 * @return array Результаты экспорта
 */
function exportAllDeals()
{
    global $YEAR, $START_MONTH, $END_MONTH;
    
    $totalStartTime = logStartOperation("ЭКСПОРТ ИСТОРИИ СДЕЛОК ЗА $YEAR ГОД (месяцы $START_MONTH-$END_MONTH)");
    
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
        
        logMessage("📅 Экспорт истории сделок за месяц: $month/$YEAR", 'INFO');
        
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
                    
                    logMessage("Месяц $month, страница: $dealsCount записей, offset: $offset", 'DEBUG');
                    
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
                    logMessage("В месяце $month больше нет записей истории", 'INFO');
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
        logMessage("Месяц $month завершен. Записей истории: " . count($monthDeals), 'INFO');
        
        // Сохраняем файл по месяцам
        saveToFile([
            'month' => $month,
            'year' => $YEAR,
            'deals_count' => count($monthDeals),
            'deals' => $monthDeals,
            'export_date' => date('Y-m-d H:i:s'),
            'batch_id' => $batchId
        ], "deals_history_{$YEAR}_month_{$month}_{$batchId}.json");
        
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
    ], "deals_history_{$YEAR}_full_export_{$batchId}.json");
    
    logEndOperation("ЭКСПОРТ ИСТОРИИ СДЕЛОК ЗА $YEAR ГОД", $totalStartTime);
    logMessage("📊 ИТОГИ ЭКСПОРТА ИСТОРИИ СДЕЛОК:", 'INFO');
    logMessage("   Всего записей истории: $totalDeals", 'INFO');
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
        logMessage("🚀 ЗАПУСК ЭКСПОРТА ИСТОРИИ СДЕЛОК (cURL ВЕРСИЯ)", 'INFO');
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
        logMessage("✅ ЭКСПОРТ ИСТОРИИ СДЕЛОК УСПЕШНО ЗАВЕРШЕН", 'INFO');
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
    startExport();
}