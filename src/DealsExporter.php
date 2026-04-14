<?php
/**
 * DealsExporter.php
 * Экспортер сделок из Битрикс24 в MSSQL
 * Версия 1.0
 */

namespace Pbi\Export;

class DealsExporter extends DataExporter
{
    private string $tableName = 'CrmDeal';
    
    private array $columns = [
        'ID',
        'DATE_CREATE',
        'DATE_MODIFY',
        'CLOSEDATE',
        'ASSIGNED_BY_ID',
        'LEAD_ID',
        'SOURCE_ID',
        'SOURCE_DESCRIPTION',
        'CATEGORY_ID',
        'STAGE_ID',
        'SOURCE',
        'CATEGORY',
        'STAGE',
        'UF_DEPARTMENT_ID',
        'UF_VPZ',
        'UF_VPZ_DATE_OLD',
        'UF_VPZ_DATE',
        'UF_TESTDRIVE',
        'UF_TESTDRIVE_DATE',
        'UF_CREDIT_DEAL_ID',
        'UF_CREDIT_DEAL_URL',
        'UF_TRADE_DEAL_ID',
        'UF_TRADE_DEAL_URL',
        'UF_ISSUE_DATE',
        'UF_CONTRACT_DATE',
        'UF_CAR_BRAND',
        'UF_CAR_MODEL',
        'UF_OBSERVER_NAME',
        'UF_EXTERNAL_SYSTEM',
        'UF_EXTERNAL_SYSTEM_NAME',
        'UF_DEAL_FAIL_REASONS',
        'UF_DEAL_FAIL_REASONS_NAME',
        'UF_SOURCE_HANDCRAFTED',
        'UF_SOURCE_HANDCRAFTED_NAME',
        'UF_DEPARTMENT',
        'UF_DEAL_URL',
        'EXPORT_BATCH',
        'EXPORT_DATE'
    ];

    /**
     * Создание таблицы сделок если не существует
     */
    private function createTable(): void
    {
        $columnDefinitions = [
            'id' => 'BIGINT NOT NULL',
            'date_create' => 'DATETIME2 NULL',
            'date_modify' => 'DATETIME2 NULL',
            'closedate' => 'DATETIME2 NULL',
            'assigned_by_id' => 'INT NULL',
            'lead_id' => 'INT NULL',
            'source_id' => 'NVARCHAR(50) NULL',
            'source_description' => 'NVARCHAR(500) NULL',
            'category_id' => 'INT NULL',
            'stage_id' => 'NVARCHAR(50) NULL',
            'source' => 'NVARCHAR(500) NULL',
            'category' => 'NVARCHAR(500) NULL',
            'stage' => 'NVARCHAR(500) NULL',
            'uf_department_id' => 'INT NULL',
            'uf_vpz' => 'NVARCHAR(50) NULL',
            'uf_vpz_date_old' => 'DATETIME2 NULL',
            'uf_vpz_date' => 'DATETIME2 NULL',
            'uf_testdrive' => 'NVARCHAR(50) NULL',
            'uf_testdrive_date' => 'DATETIME2 NULL',
            'uf_credit_deal_id' => 'INT NULL',
            'uf_credit_deal_url' => 'NVARCHAR(2000) NULL',
            'uf_trade_deal_id' => 'INT NULL',
            'uf_trade_deal_url' => 'NVARCHAR(2000) NULL',
            'uf_issue_date' => 'DATETIME2 NULL',
            'uf_contract_date' => 'DATETIME2 NULL',
            'uf_car_brand' => 'NVARCHAR(500) NULL',
            'uf_car_model' => 'NVARCHAR(500) NULL',
            'uf_observer_name' => 'NVARCHAR(1000) NULL',
            'uf_external_system' => 'NVARCHAR(500) NULL',
            'uf_external_system_name' => 'NVARCHAR(500) NULL',
            'uf_deal_fail_reasons' => 'NVARCHAR(500) NULL',
            'uf_deal_fail_reasons_name' => 'NVARCHAR(2000) NULL',
            'uf_source_handcrafted' => 'NVARCHAR(500) NULL',
            'uf_source_handcrafted_name' => 'NVARCHAR(500) NULL',
            'uf_department' => 'NVARCHAR(500) NULL',
            'uf_deal_url' => 'NVARCHAR(2000) NULL',
            'export_batch' => 'NVARCHAR(100) NULL',
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
            'CLOSEDATE' => 'CLOSEDATE',
            'ASSIGNED_BY_ID' => 'ASSIGNED_BY_ID',
            'LEAD_ID' => 'LEAD_ID',
            'SOURCE_ID' => 'SOURCE_ID',
            'SOURCE_DESCRIPTION' => 'SOURCE_DESCRIPTION',
            'CATEGORY_ID' => 'CATEGORY_ID',
            'STAGE_ID' => 'STAGE_ID',
            'SOURCE' => 'SOURCE',
            'CATEGORY' => 'CATEGORY',
            'STAGE' => 'STAGE',
            'UF_DEPARTMENT_ID' => 'UF_DEPARTMENT_ID',
            'UF_VPZ' => 'UF_VPZ',
            'UF_VPZ_DATE_OLD' => 'UF_VPZ_DATE_OLD',
            'UF_VPZ_DATE' => 'UF_VPZ_DATE',
            'UF_TESTDRIVE' => 'UF_TESTDRIVE',
            'UF_TESTDRIVE_DATE' => 'UF_TESTDRIVE_DATE',
            'UF_CREDIT_DEAL_ID' => 'UF_CREDIT_DEAL_ID',
            'UF_CREDIT_DEAL_URL' => 'UF_CREDIT_DEAL_URL',
            'UF_TRADE_DEAL_ID' => 'UF_TRADE_DEAL_ID',
            'UF_TRADE_DEAL_URL' => 'UF_TRADE_DEAL_URL',
            'UF_ISSUE_DATE' => 'UF_ISSUE_DATE',
            'UF_CONTRACT_DATE' => 'UF_CONTRACT_DATE',
            'UF_CAR_BRAND' => 'UF_CAR_BRAND',
            'UF_CAR_MODEL' => 'UF_CAR_MODEL',
            'UF_OBSERVER_NAME' => 'UF_OBSERVER_NAME',
            'UF_EXTERNAL_SYSTEM' => 'UF_EXTERNAL_SYSTEM',
            'UF_EXTERNAL_SYSTEM_NAME' => 'UF_EXTERNAL_SYSTEM_NAME',
            'UF_DEAL_FAIL_REASONS' => 'UF_DEAL_FAIL_REASONS',
            'UF_DEAL_FAIL_REASONS_NAME' => 'UF_DEAL_FAIL_REASONS_NAME',
            'UF_SOURCE_HANDCRAFTED' => 'UF_SOURCE_HANDCRAFTED',
            'UF_SOURCE_HANDCRAFTED_NAME' => 'UF_SOURCE_HANDCRAFTED_NAME',
            'UF_DEPARTMENT' => 'UF_DEPARTMENT',
            'UF_DEAL_URL' => 'UF_DEAL_URL'
        ];

        return $mapping[$field] ?? $field;
    }

    /**
     * Получение сделок из API Bitrix24
     */
    private function fetchDeals(int $month, int $year, int $limit = 500, int $offset = 0): array
    {
        $params = [
            'month' => $month,
            'year' => $year,
            'limit' => $limit,
            'offset' => $offset
        ];

        return $this->fetchFromBitrix('get_deals.php', $params);
    }

    /**
     * Подготовка данных сделки для сохранения
     */
    private function prepareDealData(array $deal): array
    {
        $prepared = [];
        
        foreach ($this->columns as $column) {
            if ($column === 'EXPORT_BATCH' || $column === 'EXPORT_DATE') {
                continue; // Эти поля добавляются отдельно
            }
            
            $bitrixField = $this->mapBitrixFieldToDbColumn($column);
            $prepared[$column] = $deal[$bitrixField] ?? null;
        }

        return $prepared;
    }

    /**
     * Основной метод экспорта сделок
     */
    public function export(): array
    {
        $totalStartTime = $this->logger->startOperation(
            "ЭКСПОРТ СДЕЛОК ЗА {$this->config->getExportYear()} ГОД (месяцы {$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()})"
        );

        $totalDeals = 0;
        $totalSavedToDB = 0;
        $batchId = date('Ymd_His');
        $allDeals = [];

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
            
            $monthDeals = [];
            $offset = 0;
            $limit = 500;

            $this->logger->info("📅 Экспорт месяца: $month/{$this->config->getExportYear()}");

            // Пагинация по страницам
            do {
                try {
                    $pageData = $this->fetchDeals($month, $this->config->getExportYear(), $limit, $offset);

                    if (!empty($pageData['items'])) {
                        $dealsCount = count($pageData['items']);
                        $monthDeals = array_merge($monthDeals, $pageData['items']);

                        // Сохранение в MSSQL порциями
                        if ($this->database->isConnected()) {
                            $preparedDeals = array_map([$this, 'prepareDealData'], $pageData['items']);
                            $savedCount = $this->saveToDatabase(
                                $preparedDeals,
                                $this->tableName,
                                $this->columns,
                                $batchId . "_m{$month}"
                            );
                            $totalSavedToDB += $savedCount;
                        }

                        $offset += $limit;

                        $this->logger->debug("Месяц $month, страница: $dealsCount сделок, offset: $offset");

                        // Пауза между запросами
                        if ($pageData['has_more'] ?? false) {
                            sleep(1);
                        }

                        // Контроль памяти каждые 2000 записей
                        if ($offset % 2000 === 0) {
                            $this->checkMemoryUsage();
                        }

                    } else {
                        $this->logger->info("В месяце $month больше нет сделок");
                        break;
                    }
                } catch (\Exception $e) {
                    $this->logger->error("Критическая ошибка при загрузке месяца $month: " . $e->getMessage());
                    break;
                }
            } while (!empty($pageData['items']) && ($pageData['has_more'] ?? false));

            $allDeals = array_merge($allDeals, $monthDeals);
            $totalDeals += count($monthDeals);

            $this->logger->endOperation("Обработка месяца $month", $monthStartTime);
            $this->logger->info("Месяц $month завершен. Сделок: " . count($monthDeals));

            // Сохранение файла по месяцам
            $this->saveToFile([
                'month' => $month,
                'year' => $this->config->getExportYear(),
                'deals_count' => count($monthDeals),
                'deals' => $monthDeals,
                'export_date' => date('Y-m-d H:i:s'),
                'batch_id' => $batchId
            ], "deals_{$this->config->getExportYear()}_month_{$month}");

            // Освобождение памяти
            unset($monthDeals);
            $this->checkMemoryUsage();
        }

        // Отключение от базы данных
        $this->database->disconnect();

        // Финальная статистика памяти
        $this->checkMemoryUsage();

        // Сохранение общего файла
        $this->saveToFile([
            'total_deals' => $totalDeals,
            'database_saved' => $totalSavedToDB,
            'deals' => $allDeals,
            'export_date' => date('Y-m-d H:i:s'),
            'year' => $this->config->getExportYear(),
            'batch_id' => $batchId,
            'months_exported' => "{$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()}"
        ], "deals_{$this->config->getExportYear()}_full_export");

        $executionTime = round(microtime(true) - $totalStartTime, 2);
        $this->logger->endOperation("ЭКСПОРТ СДЕЛОК ЗА {$this->config->getExportYear()}", $totalStartTime);

        $result = [
            'total_deals' => $totalDeals,
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
            $this->logger->section("🚀 ЗАПУСК ЭКСПОРТА СДЕЛОК");
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
                'total_deals' => 0,
                'database_saved' => 0
            ];
        }
    }
}
