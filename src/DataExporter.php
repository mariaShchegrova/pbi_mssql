<?php
/**
 * DataExporter.php
 * Базовый класс для экспортеров данных
 * Версия 1.0
 */

namespace Pbi\Export;

abstract class DataExporter
{
    protected Config $config;
    protected Logger $logger;
    protected CurlClient $curlClient;
    protected Database $database;

    public function __construct(Config $config, Logger $logger, CurlClient $curlClient, Database $database)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->curlClient = $curlClient;
        $this->database = $database;
    }

    /**
     * Основной метод экспорта - должен быть реализован в наследнике
     */
    abstract public function export(): array;

    /**
     * Получение данных из API Bitrix24
     *
     * @param string $method Метод API
     * @param array $params Параметры запроса
     * @return array Данные с пагинацией
     */
    protected function fetchFromBitrix(string $method, array $params = []): array
    {
        $url = $this->config->getBitrixApiUrl($method);
        
        // Добавление токена к параметрам
        $params['token'] = $this->config->getBitrixToken();
        
        // Формирование URL с query параметрами
        if (!empty($params)) {
            $url .= (strpos($url, '?') !== false ? '&' : '?') . http_build_query($params);
        }

        try {
            $result = $this->curlClient->get($url);
            
            $data = json_decode($result['response'], true);
            
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new \Exception("Ошибка декодирования JSON: " . json_last_error_msg());
            }

            return [
                'items' => $data['items'] ?? $data['data'] ?? [],
                'total' => $data['total'] ?? count($data['items'] ?? $data['data'] ?? []),
                'has_more' => !empty($data['next']) || ($data['total'] ?? 0) > ($params['limit'] ?? 500)
            ];

        } catch (\Exception $e) {
            $this->logger->error("Ошибка получения данных из Bitrix: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Сохранение данных в файл
     *
     * @param array $data Данные для сохранения
     * @param string $prefix Префикс имени файла
     * @return bool Успешность операции
     */
    protected function saveToFile(array $data, string $prefix): bool
    {
        try {
            $filename = $prefix . '_' . date('Ymd_His') . '.json';
            $fullPath = $this->config->getExportDir() . '/' . $filename;

            $json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR);

            if (file_put_contents($fullPath, $json, LOCK_EX)) {
                $this->logger->success("Файл успешно сохранен: $fullPath");
                return true;
            } else {
                $this->logger->error("Ошибка записи в файл: $fullPath");
                return false;
            }
        } catch (\JsonException $e) {
            $this->logger->error("Ошибка кодирования JSON: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Валидация записи перед сохранением
     *
     * @param array $record Данные записи
     * @param string $idField Поле идентификатора
     * @return bool
     * @throws \InvalidArgumentException При невалидных данных
     */
    protected function validateRecord(array $record, string $idField = 'ID'): bool
    {
        if (!isset($record[$idField]) || !is_numeric($record[$idField])) {
            throw new \InvalidArgumentException("Invalid record ID: " . ($record[$idField] ?? 'null'));
        }

        if ($record[$idField] <= 0) {
            throw new \InvalidArgumentException("Record ID must be positive: " . $record[$idField]);
        }

        // Валидация дат если они присутствуют
        $dateFields = ['DATE_CREATE', 'DATE_MODIFY', 'CLOSEDATE'];
        foreach ($dateFields as $field) {
            if (!empty($record[$field]) && !strtotime($record[$field])) {
                $this->logger->warning("Некорректный формат даты в поле $field: " . $record[$field]);
            }
        }

        return true;
    }

    /**
     * Очистка и нормализация данных записи
     *
     * @param array $record Данные записи
     * @return array Очищенные данные
     */
    protected function sanitizeRecord(array $record): array
    {
        // Конвертация пустых строк в null для числовых полей
        $numericFields = ['ASSIGNED_BY_ID', 'LEAD_ID', 'CATEGORY_ID', 'DEPARTMENT_ID'];
        foreach ($numericFields as $field) {
            if (isset($record[$field]) && $record[$field] === '') {
                $record[$field] = null;
            }
        }

        return $record;
    }

    /**
     * Сохранение записей в базу данных
     *
     * @param array $records Массив записей
     * @param string $tableName Имя таблицы
     * @param array $columns Столбцы таблицы
     * @param string $batchId Идентификатор пакета
     * @return int Количество сохраненных записей
     */
    protected function saveToDatabase(
        array $records,
        string $tableName,
        array $columns,
        string $batchId
    ): int {
        if (empty($records)) {
            $this->logger->info("Нет данных для сохранения в MSSQL");
            return 0;
        }

        $exportDate = date('Y-m-d H:i:s');
        $totalSaved = 0;
        $batchSize = $this->config->getBatchSize();

        // Разбиваем на пачки
        $batches = array_chunk($records, $batchSize);

        $this->logger->info("Сохранение " . count($records) . " записей в " . count($batches) . " пачках");

        foreach ($batches as $batchIndex => $batchRecords) {
            // Добавление мета-данных экспорта
            $processedRecords = [];
            foreach ($batchRecords as $record) {
                $record['EXPORT_BATCH'] = $batchId;
                $record['EXPORT_DATE'] = $exportDate;
                $processedRecords[] = $record;
            }

            $savedInBatch = $this->database->mergeBatch(
                $tableName,
                $columns,
                $processedRecords,
                'ID'
            );

            $totalSaved += $savedInBatch;

            // Освобождаем память
            unset($batchRecords);
        }

        $this->logger->info("Итого сохранено в MSSQL: $totalSaved записей (batch: $batchId)");
        return $totalSaved;
    }

    /**
     * Тестирование подключения к API
     *
     * @return bool
     */
    protected function testConnection(): bool
    {
        $testUrl = $this->config->getBitrixApiUrl('test');
        return $this->curlClient->testConnection($testUrl);
    }

    /**
     * Получение информации о сервере API
     *
     * @return array|null
     */
    protected function getServerInfo(): ?array
    {
        $testUrl = $this->config->getBitrixApiUrl('test');
        return $this->curlClient->getServerInfo($testUrl);
    }

    /**
     * Контроль использования памяти
     */
    protected function checkMemoryUsage(): void
    {
        $currentMemory = memory_get_usage(true);
        $peakMemory = memory_get_peak_usage(true);
        $memoryUsedMB = round($currentMemory / 1024 / 1024, 2);
        $peakMemoryMB = round($peakMemory / 1024 / 1024, 2);

        $this->logger->debug("Использование памяти: {$memoryUsedMB}MB (пик: {$peakMemoryMB}MB)");

        // Сборка мусора при необходимости
        if (function_exists('gc_collect_cycles')) {
            gc_collect_cycles();
        }
    }
}
