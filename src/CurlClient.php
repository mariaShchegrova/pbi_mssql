<?php
/**
 * CurlClient.php
 * HTTP клиент на основе cURL для экспорта данных из Битрикс24
 * Версия 1.0
 */

namespace Pbi\Export;

class CurlClient
{
    private Config $config;
    private Logger $logger;

    public function __construct(Config $config, Logger $logger)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->checkCurlAvailability();
    }

    /**
     * Проверка доступности расширения cURL
     */
    private function checkCurlAvailability(): void
    {
        if (!extension_loaded('curl')) {
            throw new \RuntimeException("cURL расширение не установлено в PHP");
        }

        $version = curl_version();
        $this->logger->debug("cURL версия: " . ($version['version'] ?? 'unknown'));
        $this->logger->debug("SSL поддержка: " . ($version['ssl_version'] ?? 'none'));
    }

    /**
     * Инициализация cURL сессии с настройками безопасности
     *
     * @param string $url URL для запроса
     * @return resource cURL handle
     */
    private function initCurlSession(string $url)
    {
        $ch = curl_init();
        $curlConfig = $this->config->getCurlConfig();
        $performanceConfig = $this->config->getPerformanceConfig();

        // Базовые настройки
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => $curlConfig['follow_redirects'],
            CURLOPT_MAXREDIRS => $curlConfig['max_redirects'],
            CURLOPT_USERAGENT => $curlConfig['user_agent'],
        ]);

        // Настройки сжатия
        if ($curlConfig['enable_gzip']) {
            curl_setopt($ch, CURLOPT_ENCODING, 'gzip, deflate');
        }

        // Настройки таймаутов
        curl_setopt_array($ch, [
            CURLOPT_CONNECTTIMEOUT => $curlConfig['connect_timeout'],
            CURLOPT_TIMEOUT => $performanceConfig['request_timeout'],
        ]);

        // Настройки SSL безопасности
        if ($curlConfig['verify_peer']) {
            curl_setopt_array($ch, [
                CURLOPT_SSL_VERIFYPEER => true,
                CURLOPT_SSL_VERIFYHOST => 2,
            ]);

            if (!empty($curlConfig['ca_file'])) {
                curl_setopt($ch, CURLOPT_CAINFO, $curlConfig['ca_file']);
            }
        } else {
            // Только для разработки!
            curl_setopt_array($ch, [
                CURLOPT_SSL_VERIFYPEER => false,
                CURLOPT_SSL_VERIFYHOST => 0,
            ]);
            $this->logger->warning("ВНИМАНИЕ: SSL верификация отключена");
        }

        // Дополнительные настройки
        curl_setopt_array($ch, [
            CURLOPT_HEADER => false,
            CURLOPT_FAILONERROR => false,
        ]);

        return $ch;
    }

    /**
     * Выполнение HTTP GET запроса с повторными попытками
     *
     * @param string $url URL для запроса
     * @param int|null $maxRetries Максимальное количество попыток
     * @param int|null $retryDelay Базовая задержка между попытками
     * @return array [response, http_code, error, size_download, total_time, effective_url, content_type]
     * @throws \Exception При невозможности выполнить запрос после всех попыток
     */
    public function get(string $url, ?int $maxRetries = null, ?int $retryDelay = null): array
    {
        $maxRetries = $maxRetries ?? $this->config->getRetryAttempts();
        $retryDelay = $retryDelay ?? $this->config->getRetryDelay();
        
        $lastError = null;
        $lastHttpCode = null;

        for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
            $ch = $this->initCurlSession($url);

            $this->logger->debug("cURL попытка $attempt/$maxRetries: $url");

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
                $this->logger->debug("cURL успешно: HTTP $httpCode, время {$totalTime}с, размер {$sizeDownload} байт");

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
            $this->logger->error("cURL ошибка (попытка $attempt): $errorMsg");

            $lastError = $errorMsg;
            $lastHttpCode = $httpCode;

            // Повторная попытка с экспоненциальной задержкой
            if ($attempt < $maxRetries) {
                $delay = $retryDelay * $attempt;
                $this->logger->debug("Повтор через {$delay}сек...");
                sleep($delay);
            }
        }

        // Все попытки исчерпаны
        $finalError = "Не удалось выполнить запрос после $maxRetries попыток. Последняя ошибка: $lastError";
        if ($lastHttpCode) {
            $finalError .= " (HTTP код: $lastHttpCode)";
        }

        throw new \Exception($finalError);
    }

    /**
     * Выполнение HTTP POST запроса с повторными попытками
     *
     * @param string $url URL для запроса
     * @param array|string $data Данные для отправки
     * @param int|null $maxRetries Максимальное количество попыток
     * @param int|null $retryDelay Базовая задержка между попытками
     * @return array [response, http_code, error, size_download, total_time, effective_url, content_type]
     * @throws \Exception При невозможности выполнить запрос после всех попыток
     */
    public function post(string $url, $data, ?int $maxRetries = null, ?int $retryDelay = null): array
    {
        $maxRetries = $maxRetries ?? $this->config->getRetryAttempts();
        $retryDelay = $retryDelay ?? $this->config->getRetryDelay();
        
        $lastError = null;
        $lastHttpCode = null;

        for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
            $ch = $this->initCurlSession($url);

            // Настройки для POST запроса
            if (is_array($data)) {
                curl_setopt($ch, CURLOPT_POST, true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($data));
            } else {
                curl_setopt($ch, CURLOPT_POST, true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
            }

            $this->logger->debug("cURL POST попытка $attempt/$maxRetries: $url");

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
                $this->logger->debug("cURL POST успешно: HTTP $httpCode, время {$totalTime}с, размер {$sizeDownload} байт");

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
            $this->logger->error("cURL POST ошибка (попытка $attempt): $errorMsg");

            $lastError = $errorMsg;
            $lastHttpCode = $httpCode;

            // Повторная попытка с экспоненциальной задержкой
            if ($attempt < $maxRetries) {
                $delay = $retryDelay * $attempt;
                $this->logger->debug("Повтор через {$delay}сек...");
                sleep($delay);
            }
        }

        // Все попытки исчерпаны
        $finalError = "Не удалось выполнить POST запрос после $maxRetries попыток. Последняя ошибка: $lastError";
        if ($lastHttpCode) {
            $finalError .= " (HTTP код: $lastHttpCode)";
        }

        throw new \Exception($finalError);
    }

    /**
     * Тестирование подключения к API
     *
     * @param string $url URL для теста
     * @return bool Успешно ли подключение
     */
    public function testConnection(string $url): bool
    {
        try {
            $result = $this->get($url, 1, 1);
            return $result['http_code'] >= 200 && $result['http_code'] < 300;
        } catch (\Exception $e) {
            $this->logger->error("Тест подключения не удался: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Получить информацию о сервере
     *
     * @param string $url URL для запроса
     * @return array|null Информация о сервере или null при ошибке
     */
    public function getServerInfo(string $url): ?array
    {
        try {
            $result = $this->get($url, 1, 1);
            
            if ($result['http_code'] >= 200 && $result['http_code'] < 300) {
                $data = json_decode($result['response'], true);
                
                return [
                    'http_code' => $result['http_code'],
                    'content_type' => $result['content_type'],
                    'total_time' => $result['total_time'],
                    'size_download' => $result['size_download'],
                    'effective_url' => $result['effective_url'],
                    'response_data' => $data
                ];
            }
            
            return null;
        } catch (\Exception $e) {
            $this->logger->debug("Информация о сервере недоступна: " . $e->getMessage());
            return null;
        }
    }
}
