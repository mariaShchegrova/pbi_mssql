<?php
/**
 * ActivitiesExporter.php
 * Экспортер активностей из Битрикс24 в MSSQL
 * Версия 1.0
 */

namespace Pbi\Export;

class ActivitiesExporter extends DataExporter
{
    private string $tableName = 'CrmActivity';
    
    private array $columns = [
        'ID',
        'ACTIVITY_TYPE',
        'ACTIVITY_TYPE_NAME',
        'ACTIVITY_URL',
        'AUTHOR_ID',
        'COMPLETED',
        'CREATED',
        'DEADLINE',
        'DESCRIPTION',
        'DESCRIPTION_TYPE',
        'DIRECTION',
        'EDITOR_ID',
        'END_TIME',
        'LAST_UPDATED',
        'OWNER_ID',
        'OWNER_TYPE_ID',
        'OWNER_TYPE_NAME',
        'PRIORITY',
        'PRIORITY_NAME',
        'PROVIDER_PARAMS',
        'PROVIDER_TYPE_ID',
        'RESPONSIBLE_ID',
        'RESULT_MARK',
        'RESULT_SOURCE_ID',
        'RESULT_STATUS',
        'RESULT_STREAM',
        'RESULT_VALUE',
        'START_TIME',
        'SUBJECT',
        'TYPE_ID',
        'EXPORT_BATCH',
        'EXPORT_DATE'
    ];

    /**
     * Создание таблицы активностей если не существует
     */
    private function createTable(): void
    {
        $columnDefinitions = [
            'id' => 'BIGINT NOT NULL',
            'activity_type' => 'NVARCHAR(50) NULL',
            'activity_type_name' => 'NVARCHAR(100) NULL',
            'activity_url' => 'NVARCHAR(500) NULL',
            'author_id' => 'BIGINT NULL',
            'completed' => 'BIT NULL',
            'created' => 'DATETIME2 NULL',
            'deadline' => 'DATETIME2 NULL',
            'description' => 'NVARCHAR(4000) NULL',
            'description_type' => 'NVARCHAR(10) NULL',
            'direction' => 'INT NULL',
            'editor_id' => 'BIGINT NULL',
            'end_time' => 'DATETIME2 NULL',
            'last_updated' => 'DATETIME2 NULL',
            'owner_id' => 'BIGINT NULL',
            'owner_type_id' => 'INT NULL',
            'owner_type_name' => 'NVARCHAR(100) NULL',
            'priority' => 'INT NULL',
            'priority_name' => 'NVARCHAR(50) NULL',
            'provider_params' => 'NVARCHAR(500) NULL',
            'provider_type_id' => 'NVARCHAR(50) NULL',
            'responsible_id' => 'BIGINT NULL',
            'result_mark' => 'INT NULL',
            'result_source_id' => 'INT NULL',
            'result_status' => 'INT NULL',
            'result_stream' => 'INT NULL',
            'result_value' => 'NVARCHAR(1000) NULL',
            'start_time' => 'DATETIME2 NULL',
            'subject' => 'NVARCHAR(500) NULL',
            'type_id' => 'INT NULL',
            'export_batch' => 'NVARCHAR(50) NULL',
            'export_date' => 'DATETIME2 NULL'
        ];

        $this->database->createTableIfNotExists($this->tableName, $columnDefinitions);
    }

    /**
     * Преобразование названия поля Bitrix в название колонки БД
     */
    private function mapBitrixFieldToDbColumn(string $field): string
    {
        $mapping = [
            'ID' => 'ID',
            'ACTIVITY_TYPE' => 'ACTIVITY_TYPE',
            'ACTIVITY_TYPE_NAME' => 'ACTIVITY_TYPE_NAME',
            'ACTIVITY_URL' => 'ACTIVITY_URL',
            'AUTHOR_ID' => 'AUTHOR_ID',
            'COMPLETED' => 'COMPLETED',
            'CREATED' => 'CREATED',
            'DEADLINE' => 'DEADLINE',
            'DESCRIPTION' => 'DESCRIPTION',
            'DESCRIPTION_TYPE' => 'DESCRIPTION_TYPE',
            'DIRECTION' => 'DIRECTION',
            'EDITOR_ID' => 'EDITOR_ID',
            'END_TIME' => 'END_TIME',
            'LAST_UPDATED' => 'LAST_UPDATED',
            'OWNER_ID' => 'OWNER_ID',
            'OWNER_TYPE_ID' => 'OWNER_TYPE_ID',
            'OWNER_TYPE_NAME' => 'OWNER_TYPE_NAME',
            'PRIORITY' => 'PRIORITY',
            'PRIORITY_NAME' => 'PRIORITY_NAME',
            'PROVIDER_PARAMS' => 'PROVIDER_PARAMS',
            'PROVIDER_TYPE_ID' => 'PROVIDER_TYPE_ID',
            'RESPONSIBLE_ID' => 'RESPONSIBLE_ID',
            'RESULT_MARK' => 'RESULT_MARK',
            'RESULT_SOURCE_ID' => 'RESULT_SOURCE_ID',
            'RESULT_STATUS' => 'RESULT_STATUS',
            'RESULT_STREAM' => 'RESULT_STREAM',
            'RESULT_VALUE' => 'RESULT_VALUE',
            'START_TIME' => 'START_TIME',
            'SUBJECT' => 'SUBJECT',
            'TYPE_ID' => 'TYPE_ID'
        ];

        return $mapping[$field] ?? $field;
    }

    /**
     * Получение активностей из API Bitrix24
     */
    private function fetchActivities(int $month, int $year, int $limit = 500, int $offset = 0): array
    {
        $params = [
            'month' => $month,
            'year' => $year,
            'limit' => $limit,
            'offset' => $offset
        ];

        return $this->fetchFromBitrix('get_activities.php', $params);
    }

    /**
     * Подготовка данных активности для сохранения
     */
    private function prepareActivityData(array $activity): array
    {
        $prepared = [];
        
        foreach ($this->columns as $column) {
            if ($column === 'EXPORT_BATCH' || $column === 'EXPORT_DATE') {
                continue; // Эти поля добавляются отдельно
            }
            
            $bitrixField = $this->mapBitrixFieldToDbColumn($column);
            $prepared[$column] = $activity[$bitrixField] ?? null;
        }

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
            if (isset($prepared[$field]) && is_string($prepared[$field])) {
                $prepared[$field] = mb_convert_encoding($prepared[$field], 'UTF-8', 'UTF-8');
                $prepared[$field] = iconv('UTF-8', 'UTF-8//IGNORE', $prepared[$field]);
                $prepared[$field] = preg_replace('/[^\x{0009}\x{000A}\x{000D}\x{0020}-\x{D7FF}\x{E000}-\x{FFFD}]+/u', '', $prepared[$field]);
                
                if (mb_strlen($prepared[$field], 'UTF-8') > $maxLength) {
                    $prepared[$field] = mb_substr($prepared[$field], 0, $maxLength - 3, 'UTF-8') . '...';
                }
            }
        }
        
        // Конвертация текстовых значений в булевы
        $booleanFields = ['COMPLETED'];
        foreach ($booleanFields as $field) {
            if (isset($prepared[$field])) {
                $prepared[$field] = ($prepared[$field] === 'Y') ? 1 : 0;
            }
        }
        
        // Конвертация числовых полей
        $numericFields = [
            'AUTHOR_ID', 'EDITOR_ID', 'RESPONSIBLE_ID', 'OWNER_ID', 
            'OWNER_TYPE_ID', 'PRIORITY', 'RESULT_MARK', 'RESULT_STATUS', 
            'RESULT_STREAM', 'TYPE_ID', 'DIRECTION'
        ];
        
        foreach ($numericFields as $field) {
            if (isset($prepared[$field]) && is_numeric($prepared[$field])) {
                $prepared[$field] = (int)$prepared[$field];
            }
        }

        return $prepared;
    }

    /**
     * Основной метод экспорта активностей
     */
    public function export(): array
    {
        $totalStartTime = $this->logger->startOperation(
            "ЭКСПОРТ АКТИВНОСТЕЙ ЗА {$this->config->getExportYear()} ГОД (месяцы {$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()})"
        );

        $totalActivities = 0;
        $totalSavedToDB = 0;
        $batchId = date('Ymd_His');
        $allActivities = [];

        // Применение настроек производительности
        $this->config->applyPerformanceSettings();

        // Подключение к базе данных
        $this->database->connect();
        
        if ($this->database->isConnected()) {
            $this->createTable();
        } else {
            $this->logger->warning("ВНИМАНИЕ: Работаем без MSSQL, только файлы");
        }

        // Проход по всем месяцам периода
        for ($month = $this->config->getExportStartMonth(); $month <= $this->config->getExportEndMonth(); $month++) {
            $monthStartTime = $this->logger->startOperation("Обработка месяца $month");
            
            $monthActivities = [];
            $offset = 0;
            $limit = 500;

            $this->logger->info("📅 Экспорт месяца: $month/{$this->config->getExportYear()}");

            // Пагинация по страницам
            do {
                try {
                    $pageData = $this->fetchActivities($month, $this->config->getExportYear(), $limit, $offset);

                    if (!empty($pageData['items'])) {
                        $activitiesCount = count($pageData['items']);
                        $monthActivities = array_merge($monthActivities, $pageData['items']);

                        // Сохранение в MSSQL порциями
                        if ($this->database->isConnected()) {
                            $preparedActivities = array_map([$this, 'prepareActivityData'], $pageData['items']);
                            $savedCount = $this->saveToDatabase(
                                $preparedActivities,
                                $this->tableName,
                                $this->columns,
                                $batchId . "_m{$month}"
                            );
                            $totalSavedToDB += $savedCount;
                        }

                        $offset += $limit;

                        $this->logger->debug("Месяц $month, страница: $activitiesCount активностей, offset: $offset");

                        // Пауза между запросами
                        if ($pageData['has_more'] ?? false) {
                            sleep(1);
                        }

                        // Контроль памяти каждые 2000 записей
                        if ($offset % 2000 === 0) {
                            $this->checkMemoryUsage();
                        }

                    } else {
                        $this->logger->info("В месяце $month больше нет активностей");
                        break;
                    }
                } catch (\Exception $e) {
                    $this->logger->error("Критическая ошибка при загрузке месяца $month: " . $e->getMessage());
                    break;
                }
            } while (!empty($pageData['items']) && ($pageData['has_more'] ?? false));

            $allActivities = array_merge($allActivities, $monthActivities);
            $totalActivities += count($monthActivities);

            $this->logger->endOperation("Обработка месяца $month", $monthStartTime);
            $this->logger->info("Месяц $month завершен. Активностей: " . count($monthActivities));

            // Сохранение файла по месяцам
            $this->saveToFile([
                'month' => $month,
                'year' => $this->config->getExportYear(),
                'activities_count' => count($monthActivities),
                'activities' => $monthActivities,
                'export_date' => date('Y-m-d H:i:s'),
                'batch_id' => $batchId
            ], "activities_{$this->config->getExportYear()}_month_{$month}");

            // Освобождение памяти
            unset($monthActivities);
            $this->checkMemoryUsage();
        }

        // Отключение от базы данных
        $this->database->disconnect();

        // Финальная статистика памяти
        $this->checkMemoryUsage();

        // Сохранение общего файла
        $this->saveToFile([
            'total_activities' => $totalActivities,
            'database_saved' => $totalSavedToDB,
            'activities' => $allActivities,
            'export_date' => date('Y-m-d H:i:s'),
            'year' => $this->config->getExportYear(),
            'batch_id' => $batchId,
            'months_exported' => "{$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()}"
        ], "activities_{$this->config->getExportYear()}_full_export");

        $executionTime = round(microtime(true) - $totalStartTime, 2);
        $this->logger->endOperation("ЭКСПОРТ АКТИВНОСТЕЙ ЗА {$this->config->getExportYear()}", $totalStartTime);

        $result = [
            'total_activities' => $totalActivities,
            'database_saved' => $totalSavedToDB,
            'batch_id' => $batchId,
            'execution_time' => $executionTime
        ];

        $this->logger->stats($result);

        return $result;
    }

    /**
     * Запуск экспорта с обработкой ошибок
     */
    public function run(): array
    {
        try {
            $this->logger->section("🚀 ЗАПУСК ЭКСПОРТА АКТИВНОСТЕЙ");
            $this->logger->info("Дата: " . date('Y-m-d H:i:s'));

            // Тестирование подключения
            if (!$this->testConnection()) {
                throw new \Exception("Не удалось установить подключение к Bitrix API");
            }

            // Дополнительная диагностика
            $serverInfo = $this->getServerInfo();
            if ($serverInfo) {
                $this->logger->debug("Информация о сервере: " . json_encode($serverInfo));
            }

            $result = $this->export();

            $this->logger->section("✅ ЭКСПОРТ УСПЕШНО ЗАВЕРШЕН");
            $this->logger->info("Время выполнения: {$result['execution_time']} секунд");

            return $result;

        } catch (\Exception $e) {
            $this->logger->failure("КРИТИЧЕСКАЯ ОШИБКА ЭКСПОРТА: " . $e->getMessage());
            
            // Диагностика при ошибке
            $this->getServerInfo();

            return [
                'error' => $e->getMessage(),
                'total_activities' => 0,
                'database_saved' => 0
            ];
        }
    }
}
