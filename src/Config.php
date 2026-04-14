<?php
/**
 * Config.php
 * Класс конфигурации для экспорта данных из Битрикс24 в MSSQL
 * Версия 1.0
 */

namespace Pbi\Export;

class Config
{
    private array $bitrixConfig;
    private array $dbConfig;
    private array $exportConfig;
    private array $performanceConfig;
    private array $curlConfig;
    private string $exportDir;

    public function __construct()
    {
        $this->loadEnvironmentVariables();
        $this->initBitrixConfig();
        $this->initDbConfig();
        $this->initExportConfig();
        $this->initPerformanceConfig();
        $this->initCurlConfig();
    }

    /**
     * Загрузка переменных окружения из .env файла
     */
    private function loadEnvironmentVariables(string $envFile = '.env'): void
    {
        if (!file_exists($envFile)) {
            error_log("Файл окружения {$envFile} не найден, используются переменные системы");
            return;
        }

        $lines = file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

        foreach ($lines as $line) {
            if (strpos(trim($line), '#') === 0) {
                continue;
            }

            if (strpos($line, '=') === false) {
                continue;
            }

            list($name, $value) = explode('=', $line, 2);
            $name = trim($name);
            $value = trim($value);

            if (preg_match('/^"([\s\S]*)"$/u', $value, $matches)) {
                $value = $matches[1];
            } elseif (preg_match("/^'([\\s\\S]*)'$/u", $value, $matches)) {
                $value = $matches[1];
            }

            putenv("$name=$value");
            $_ENV[$name] = $value;
            $_SERVER[$name] = $value;
        }
    }

    /**
     * Инициализация конфигурации Bitrix API
     */
    private function initBitrixConfig(): void
    {
        $this->bitrixConfig = [
            'base_url' => getenv('BITRIX_BASE_URL') ?: 'https://crm.ex.ru/local/api/v1/pbi/',
            'token' => getenv('BITRIX_API_TOKEN') ?: 'pbi',
            'timeout' => (int)(getenv('BITRIX_TIMEOUT') ?: 300),
            'retry_attempts' => (int)(getenv('BITRIX_RETRY_ATTEMPTS') ?: 3),
        ];

        if (empty($this->bitrixConfig['token'])) {
            throw new \RuntimeException("BITRIX_API_TOKEN не указан в переменной окружения");
        }
    }

    /**
     * Инициализация конфигурации базы данных MSSQL
     */
    private function initDbConfig(): void
    {
        $this->dbConfig = [
            'server' => getenv('DB_SERVER') ?: '192.168.15.5',
            'database' => getenv('DB_NAME') ?: 'BI',
            'username' => getenv('DB_USER') ?: 'expo',
            'password' => getenv('DB_PASSWORD') ?: 'VN5OoUHtWhAGnX4',
            'port' => (int)(getenv('DB_PORT') ?: 1433),
            'charset' => getenv('DB_CHARSET') ?: 'UTF-8',
        ];

        if (empty($this->dbConfig['password'])) {
            throw new \RuntimeException("DB_PASSWORD не указан в переменной окружения");
        }
    }

    /**
     * Инициализация конфигурации экспорта
     */
    private function initExportConfig(): void
    {
        $this->exportDir = getenv('EXPORT_DIR') ?: '/var/www/api.ex.ru/api/gateway/v1/pbi/logs';
        
        $this->exportConfig = [
            'dir' => $this->exportDir,
            'year' => $this->getYearFromRequest(),
            'start_month' => $this->getStartMonthFromRequest(),
            'end_month' => $this->getEndMonthFromRequest(),
        ];

        $this->validateExportPeriod();
    }

    /**
     * Получение года из GET параметров
     */
    private function getYearFromRequest(): int
    {
        $year = isset($_GET['year']) ? (int)$_GET['year'] : (int)date('Y');
        return $year > 0 ? $year : (int)date('Y');
    }

    /**
     * Получение начального месяца из GET параметров
     */
    private function getStartMonthFromRequest(): int
    {
        $month = isset($_GET['start_month']) ? (int)$_GET['start_month'] : 1;
        return $month > 0 ? $month : 1;
    }

    /**
     * Получение конечного месяца из GET параметров
     */
    private function getEndMonthFromRequest(): int
    {
        $month = isset($_GET['end_month']) ? (int)$_GET['end_month'] : 12;
        return $month > 0 ? $month : 12;
    }

    /**
     * Валидация периода экспорта
     */
    private function validateExportPeriod(): void
    {
        $year = $this->exportConfig['year'];
        $startMonth = $this->exportConfig['start_month'];
        $endMonth = $this->exportConfig['end_month'];

        if ($year < 2025 || $year > 2100) {
            throw new \InvalidArgumentException("Некорректный год экспорта: $year");
        }

        if ($startMonth < 1 || $startMonth > 12 || $endMonth < 1 || $endMonth > 12) {
            throw new \InvalidArgumentException("Некорректный диапазон месяцев: $startMonth-$endMonth");
        }

        if ($startMonth > $endMonth) {
            throw new \InvalidArgumentException("Начальный месяц ($startMonth) не может быть больше конечного ($endMonth)");
        }
    }

    /**
     * Инициализация конфигурации производительности
     */
    private function initPerformanceConfig(): void
    {
        $this->performanceConfig = [
            'memory_limit' => getenv('MEMORY_LIMIT') ?: '512M',
            'batch_size' => (int)(getenv('BATCH_SIZE') ?: 100),
            'request_timeout' => (int)(getenv('REQUEST_TIMEOUT') ?: 300),
            'retry_attempts' => (int)(getenv('RETRY_ATTEMPTS') ?: 3),
            'retry_delay' => (int)(getenv('RETRY_DELAY') ?: 2),
        ];
    }

    /**
     * Инициализация конфигурации cURL
     */
    private function initCurlConfig(): void
    {
        $this->curlConfig = [
            'verify_peer' => filter_var(getenv('SSL_VERIFY_PEER') ?: true, FILTER_VALIDATE_BOOLEAN),
            'ca_file' => getenv('SSL_CA_FILE') ?: null,
            'connect_timeout' => (int)(getenv('CURL_CONNECT_TIMEOUT') ?: 300),
            'follow_redirects' => filter_var(getenv('CURL_FOLLOW_REDIRECTS') ?: true, FILTER_VALIDATE_BOOLEAN),
            'max_redirects' => (int)(getenv('CURL_MAX_REDIRECTS') ?: 3),
            'enable_gzip' => filter_var(getenv('CURL_ENABLE_GZIP') ?: true, FILTER_VALIDATE_BOOLEAN),
            'user_agent' => getenv('CURL_USER_AGENT') ?: 'Bitrix-Export-Script/3.0 (cURL)',
        ];
    }

    /**
     * Получить URL API для указанного метода
     */
    public function getBitrixApiUrl(string $method): string
    {
        $baseUrl = rtrim($this->bitrixConfig['base_url'], '/');
        $method = ltrim($method, '/');
        return "{$baseUrl}/{$method}";
    }

    /**
     * Получить конфигурацию Bitrix
     */
    public function getBitrixConfig(): array
    {
        return $this->bitrixConfig;
    }

    /**
     * Получить токен API Bitrix
     */
    public function getBitrixToken(): string
    {
        return $this->bitrixConfig['token'];
    }

    /**
     * Получить DSN для подключения к MSSQL
     */
    public function getDatabaseDsn(): string
    {
        $config = $this->dbConfig;
        return "sqlsrv:Server={$config['server']},{$config['port']};Database={$config['database']};TrustServerCertificate=yes";
    }

    /**
     * Получить конфигурацию базы данных
     */
    public function getDatabaseConfig(): array
    {
        return $this->dbConfig;
    }

    /**
     * Получить имя пользователя БД
     */
    public function getDatabaseUsername(): string
    {
        return $this->dbConfig['username'];
    }

    /**
     * Получить пароль БД
     */
    public function getDatabasePassword(): string
    {
        return $this->dbConfig['password'];
    }

    /**
     * Получить директорию экспорта
     */
    public function getExportDir(): string
    {
        return $this->exportDir;
    }

    /**
     * Получить конфигурацию экспорта
     */
    public function getExportConfig(): array
    {
        return $this->exportConfig;
    }

    /**
     * Получить год экспорта
     */
    public function getExportYear(): int
    {
        return $this->exportConfig['year'];
    }

    /**
     * Получить начальный месяц экспорта
     */
    public function getExportStartMonth(): int
    {
        return $this->exportConfig['start_month'];
    }

    /**
     * Получить конечный месяц экспорта
     */
    public function getExportEndMonth(): int
    {
        return $this->exportConfig['end_month'];
    }

    /**
     * Получить конфигурацию производительности
     */
    public function getPerformanceConfig(): array
    {
        return $this->performanceConfig;
    }

    /**
     * Получить размер пакета
     */
    public function getBatchSize(): int
    {
        return $this->performanceConfig['batch_size'];
    }

    /**
     * Получить лимит памяти
     */
    public function getMemoryLimit(): string
    {
        return $this->performanceConfig['memory_limit'];
    }

    /**
     * Получить количество попыток повторного запроса
     */
    public function getRetryAttempts(): int
    {
        return $this->performanceConfig['retry_attempts'];
    }

    /**
     * Получить задержку между попытками
     */
    public function getRetryDelay(): int
    {
        return $this->performanceConfig['retry_delay'];
    }

    /**
     * Получить конфигурацию cURL
     */
    public function getCurlConfig(): array
    {
        return $this->curlConfig;
    }

    /**
     * Применить настройки производительности
     */
    public function applyPerformanceSettings(): void
    {
        ini_set('memory_limit', $this->performanceConfig['memory_limit']);
    }

    /**
     * Создать директорию экспорта если не существует
     */
    public function ensureExportDirExists(): bool
    {
        if (!is_dir($this->exportDir)) {
            return mkdir($this->exportDir, 0755, true);
        }
        return true;
    }

    /**
     * Получить путь к файлу лога
     */
    public function getLogFilePath(string $prefix): string
    {
        $this->ensureExportDirExists();
        return $this->exportDir . '/' . $prefix . '_export_' . date('Ymd') . '.log';
    }

    /**
     * Получить путь к файлу данных
     */
    public function getDataFilePath(string $prefix, string $suffix = ''): string
    {
        $this->ensureExportDirExists();
        $filename = $prefix . $suffix . '_' . date('Ymd_His') . '.json';
        return $this->exportDir . '/' . $filename;
    }
}
