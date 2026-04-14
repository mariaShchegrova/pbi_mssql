<?php
/**
 * LeadsExporter.php
 * Экспортер лидов из Битрикс24 в MSSQL
 * Версия 1.0
 */

namespace Pbi\Export;

class LeadsExporter extends DataExporter
{
    private string $tableName = 'CrmLead';
    
    private array $columns = [
        'ID',
        'DATE_CREATE',
        'DATE_MODIFY',
        'ASSIGNED_BY_ID',
        'SOURCE_ID',
        'SOURCE_DESCRIPTION',
        'SOURCE',
        'STATUS_ID',
        'STATUS',
        'UF_DEPARTMENT_ID',
        'UF_DEPARTMENT',
        'UF_LEAD_DIRECTION',
        'UF_LEAD_DIRECTION_NAME',
        'UF_REASON_NOTCALLING',
        'UF_REASON_NOTCALLING_NAME',
        'UF_SOURCE_HANDCRAFTED',
        'UF_SOURCE_HANDCRAFTED_NAME',
        'UF_LEAD_URL',
        'EXPORT_BATCH',
        'EXPORT_DATE'
    ];

    /**
     * Создание таблицы лидов если не существует
     */
    private function createTable(): void
    {
        $columnDefinitions = [
            'id' => 'BIGINT NOT NULL',
            'date_create' => 'DATETIME2 NULL',
            'date_modify' => 'DATETIME2 NULL',
            'assigned_by_id' => 'INT NULL',
            'source_id' => 'NVARCHAR(50) NULL',
            'source_description' => 'NVARCHAR(MAX) NULL',
            'source' => 'NVARCHAR(100) NULL',
            'status_id' => 'NVARCHAR(50) NULL',
            'status' => 'NVARCHAR(100) NULL',
            'uf_department_id' => 'INT NULL',
            'uf_department' => 'NVARCHAR(100) NULL',
            'uf_lead_direction' => 'NVARCHAR(100) NULL',
            'uf_lead_direction_name' => 'NVARCHAR(100) NULL',
            'uf_reason_notcalling' => 'NVARCHAR(100) NULL',
            'uf_reason_notcalling_name' => 'NVARCHAR(100) NULL',
            'uf_source_handcrafted' => 'NVARCHAR(100) NULL',
            'uf_source_handcrafted_name' => 'NVARCHAR(100) NULL',
            'uf_lead_url' => 'NVARCHAR(100) NULL',
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
            'DATE_CREATE' => 'DATE_CREATE',
            'DATE_MODIFY' => 'DATE_MODIFY',
            'ASSIGNED_BY_ID' => 'ASSIGNED_BY_ID',
            'SOURCE_ID' => 'SOURCE_ID',
            'SOURCE_DESCRIPTION' => 'SOURCE_DESCRIPTION',
            'SOURCE' => 'SOURCE',
            'STATUS_ID' => 'STATUS_ID',
            'STATUS' => 'STATUS',
            'UF_DEPARTMENT_ID' => 'UF_DEPARTMENT_ID',
            'UF_DEPARTMENT' => 'UF_DEPARTMENT',
            'UF_LEAD_DIRECTION' => 'UF_LEAD_DIRECTION',
            'UF_LEAD_DIRECTION_NAME' => 'UF_LEAD_DIRECTION_NAME',
            'UF_REASON_NOTCALLING' => 'UF_REASON_NOTCALLING',
            'UF_REASON_NOTCALLING_NAME' => 'UF_REASON_NOTCALLING_NAME',
            'UF_SOURCE_HANDCRAFTED' => 'UF_SOURCE_HANDCRAFTED',
            'UF_SOURCE_HANDCRAFTED_NAME' => 'UF_SOURCE_HANDCRAFTED_NAME',
            'UF_LEAD_URL' => 'UF_LEAD_URL'
        ];

        return $mapping[$field] ?? $field;
    }

    /**
     * Получение лидов из API Bitrix24
     */
    private function fetchLeads(int $month, int $year, int $limit = 500, int $offset = 0): array
    {
        $params = [
            'month' => $month,
            'year' => $year,
            'limit' => $limit,
            'offset' => $offset
        ];

        return $this->fetchFromBitrix('get_leads.php', $params);
    }

    /**
     * Подготовка данных лида для сохранения
     */
    private function prepareLeadData(array $lead): array
    {
        $prepared = [];
        
        foreach ($this->columns as $column) {
            if ($column === 'EXPORT_BATCH' || $column === 'EXPORT_DATE') {
                continue; // Эти поля добавляются отдельно
            }
            
            $bitrixField = $this->mapBitrixFieldToDbColumn($column);
            $prepared[$column] = $lead[$bitrixField] ?? null;
        }

        return $prepared;
    }

    /**
     * Основной метод экспорта лидов
     */
    public function export(): array
    {
        $totalStartTime = $this->logger->startOperation(
            "ЭКСПОРТ ЛИДОВ ЗА {$this->config->getExportYear()} ГОД (месяцы {$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()})"
        );

        $totalLeads = 0;
        $totalSavedToDB = 0;
        $batchId = date('Ymd_His');
        $allLeads = [];

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
            
            $monthLeads = [];
            $offset = 0;
            $limit = 500;

            $this->logger->info("📅 Экспорт месяца: $month/{$this->config->getExportYear()}");

            // Пагинация по страницам
            do {
                try {
                    $pageData = $this->fetchLeads($month, $this->config->getExportYear(), $limit, $offset);

                    if (!empty($pageData['items'])) {
                        $leadsCount = count($pageData['items']);
                        $monthLeads = array_merge($monthLeads, $pageData['items']);

                        // Сохранение в MSSQL порциями
                        if ($this->database->isConnected()) {
                            $preparedLeads = array_map([$this, 'prepareLeadData'], $pageData['items']);
                            $savedCount = $this->saveToDatabase(
                                $preparedLeads,
                                $this->tableName,
                                $this->columns,
                                $batchId . "_m{$month}"
                            );
                            $totalSavedToDB += $savedCount;
                        }

                        $offset += $limit;

                        $this->logger->debug("Месяц $month, страница: $leadsCount лидов, offset: $offset");

                        // Пауза между запросами
                        if ($pageData['has_more'] ?? false) {
                            sleep(1);
                        }

                        // Контроль памяти каждые 2000 записей
                        if ($offset % 2000 === 0) {
                            $this->checkMemoryUsage();
                        }

                    } else {
                        $this->logger->info("В месяце $month больше нет лидов");
                        break;
                    }
                } catch (\Exception $e) {
                    $this->logger->error("Критическая ошибка при загрузке месяца $month: " . $e->getMessage());
                    break;
                }
            } while (!empty($pageData['items']) && ($pageData['has_more'] ?? false));

            $allLeads = array_merge($allLeads, $monthLeads);
            $totalLeads += count($monthLeads);

            $this->logger->endOperation("Обработка месяца $month", $monthStartTime);
            $this->logger->info("Месяц $month завершен. Лидов: " . count($monthLeads));

            // Сохранение файла по месяцам
            $this->saveToFile([
                'month' => $month,
                'year' => $this->config->getExportYear(),
                'leads_count' => count($monthLeads),
                'leads' => $monthLeads,
                'export_date' => date('Y-m-d H:i:s'),
                'batch_id' => $batchId
            ], "leads_{$this->config->getExportYear()}_month_{$month}");

            // Освобождение памяти
            unset($monthLeads);
            $this->checkMemoryUsage();
        }

        // Отключение от базы данных
        $this->database->disconnect();

        // Финальная статистика памяти
        $this->checkMemoryUsage();

        // Сохранение общего файла
        $this->saveToFile([
            'total_leads' => $totalLeads,
            'database_saved' => $totalSavedToDB,
            'leads' => $allLeads,
            'export_date' => date('Y-m-d H:i:s'),
            'year' => $this->config->getExportYear(),
            'batch_id' => $batchId,
            'months_exported' => "{$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()}"
        ], "leads_{$this->config->getExportYear()}_full_export");

        $executionTime = round(microtime(true) - $totalStartTime, 2);
        $this->logger->endOperation("ЭКСПОРТ ЛИДОВ ЗА {$this->config->getExportYear()}", $totalStartTime);

        $result = [
            'total_leads' => $totalLeads,
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
            $this->logger->section("🚀 ЗАПУСК ЭКСПОРТА ЛИДОВ");
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
                'total_leads' => 0,
                'database_saved' => 0
            ];
        }
    }
}
