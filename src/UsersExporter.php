<?php
/**
 * UsersExporter.php
 * Экспортер пользователей из Битрикс24 в MSSQL
 * Версия 1.0
 */

namespace Pbi\Export;

class UsersExporter extends DataExporter
{
    private string $tableName = 'CrmUser';
    
    private array $columns = [
        'ID',
        'ACTIVE',
        'BLOCKED',
        'DATE_REGISTER',
        'EMAIL',
        'FULL_NAME',
        'LAST_LOGIN',
        'LAST_NAME',
        'LOGIN',
        'NAME',
        'PERSONAL_BIRTHDAY',
        'PERSONAL_GENDER',
        'PERSONAL_MOBILE',
        'PERSONAL_PHONE',
        'PERSONAL_PHOTO',
        'PHOTO_URL',
        'PROFILE_URL',
        'SECOND_NAME',
        'TIMESTAMP_X',
        'UF_DEPARTMENT_IDS',
        'UF_DEPARTMENT_NAMES',
        'UF_PHONE_INNER',
        'WORK_COMPANY',
        'WORK_DEPARTMENT',
        'WORK_PHONE',
        'WORK_POSITION',
        'EXPORT_BATCH',
        'EXPORT_DATE'
    ];

    /**
     * Создание таблицы пользователей если не существует
     */
    private function createTable(): void
    {
        $columnDefinitions = [
            'id' => 'BIGINT NOT NULL',
            'active' => 'BIT NULL',
            'blocked' => 'BIT NULL',
            'date_register' => 'DATETIME2 NULL',
            'email' => 'NVARCHAR(100) NULL',
            'full_name' => 'NVARCHAR(150) NULL',
            'last_login' => 'DATETIME2 NULL',
            'last_name' => 'NVARCHAR(50) NULL',
            'login' => 'NVARCHAR(50) NULL',
            'name' => 'NVARCHAR(50) NULL',
            'personal_birthday' => 'DATE NULL',
            'personal_gender' => 'NVARCHAR(1) NULL',
            'personal_mobile' => 'NVARCHAR(50) NULL',
            'personal_phone' => 'NVARCHAR(50) NULL',
            'personal_photo' => 'NVARCHAR(20) NULL',
            'photo_url' => 'NVARCHAR(255) NULL',
            'profile_url' => 'NVARCHAR(255) NULL',
            'second_name' => 'NVARCHAR(50) NULL',
            'timestamp_x' => 'DATETIME2 NULL',
            'uf_department_ids' => 'NVARCHAR(100) NULL',
            'uf_department_names' => 'NVARCHAR(500) NULL',
            'uf_phone_inner' => 'NVARCHAR(50) NULL',
            'work_company' => 'NVARCHAR(100) NULL',
            'work_department' => 'NVARCHAR(100) NULL',
            'work_phone' => 'NVARCHAR(50) NULL',
            'work_position' => 'NVARCHAR(100) NULL',
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
            'ACTIVE' => 'ACTIVE',
            'BLOCKED' => 'BLOCKED',
            'DATE_REGISTER' => 'DATE_REGISTER',
            'EMAIL' => 'EMAIL',
            'FULL_NAME' => 'FULL_NAME',
            'LAST_LOGIN' => 'LAST_LOGIN',
            'LAST_NAME' => 'LAST_NAME',
            'LOGIN' => 'LOGIN',
            'NAME' => 'NAME',
            'PERSONAL_BIRTHDAY' => 'PERSONAL_BIRTHDAY',
            'PERSONAL_GENDER' => 'PERSONAL_GENDER',
            'PERSONAL_MOBILE' => 'PERSONAL_MOBILE',
            'PERSONAL_PHONE' => 'PERSONAL_PHONE',
            'PERSONAL_PHOTO' => 'PERSONAL_PHOTO',
            'PHOTO_URL' => 'PHOTO_URL',
            'PROFILE_URL' => 'PROFILE_URL',
            'SECOND_NAME' => 'SECOND_NAME',
            'TIMESTAMP_X' => 'TIMESTAMP_X',
            'UF_DEPARTMENT_IDS' => 'UF_DEPARTMENT_IDS',
            'UF_DEPARTMENT_NAMES' => 'UF_DEPARTMENT_NAMES',
            'UF_PHONE_INNER' => 'UF_PHONE_INNER',
            'WORK_COMPANY' => 'WORK_COMPANY',
            'WORK_DEPARTMENT' => 'WORK_DEPARTMENT',
            'WORK_PHONE' => 'WORK_PHONE',
            'WORK_POSITION' => 'WORK_POSITION'
        ];

        return $mapping[$field] ?? $field;
    }

    /**
     * Получение пользователей из API Bitrix24
     */
    private function fetchUsers(int $month, int $year, int $limit = 500, int $offset = 0): array
    {
        $params = [
            'month' => $month,
            'year' => $year,
            'limit' => $limit,
            'offset' => $offset
        ];

        return $this->fetchFromBitrix('get_users.php', $params);
    }

    /**
     * Подготовка данных пользователя для сохранения
     */
    private function prepareUserData(array $user): array
    {
        $prepared = [];
        
        foreach ($this->columns as $column) {
            if ($column === 'EXPORT_BATCH' || $column === 'EXPORT_DATE') {
                continue; // Эти поля добавляются отдельно
            }
            
            $bitrixField = $this->mapBitrixFieldToDbColumn($column);
            $prepared[$column] = $user[$bitrixField] ?? null;
        }

        // Ограничение длины текстовых полей
        $textFields = [
            'EMAIL' => 100,
            'FULL_NAME' => 150,
            'LAST_NAME' => 50,
            'LOGIN' => 50,
            'NAME' => 50,
            'SECOND_NAME' => 50,
            'WORK_COMPANY' => 100,
            'WORK_DEPARTMENT' => 100,
            'WORK_POSITION' => 100,
            'UF_PHONE_INNER' => 50
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
        $booleanFields = ['ACTIVE', 'BLOCKED'];
        foreach ($booleanFields as $field) {
            if (isset($prepared[$field])) {
                $prepared[$field] = ($prepared[$field] === 'Y') ? 1 : 0;
            }
        }

        return $prepared;
    }

    /**
     * Основной метод экспорта пользователей
     */
    public function export(): array
    {
        $totalStartTime = $this->logger->startOperation(
            "ЭКСПОРТ ПОЛЬЗОВАТЕЛЕЙ ЗА {$this->config->getExportYear()} ГОД (месяцы {$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()})"
        );

        $totalUsers = 0;
        $totalSavedToDB = 0;
        $batchId = date('Ymd_His');
        $allUsers = [];

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
            
            $monthUsers = [];
            $offset = 0;
            $limit = 500;

            $this->logger->info("📅 Экспорт месяца: $month/{$this->config->getExportYear()}");

            // Пагинация по страницам
            do {
                try {
                    $pageData = $this->fetchUsers($month, $this->config->getExportYear(), $limit, $offset);

                    if (!empty($pageData['items'])) {
                        $usersCount = count($pageData['items']);
                        $monthUsers = array_merge($monthUsers, $pageData['items']);

                        // Сохранение в MSSQL порциями
                        if ($this->database->isConnected()) {
                            $preparedUsers = array_map([$this, 'prepareUserData'], $pageData['items']);
                            $savedCount = $this->saveToDatabase(
                                $preparedUsers,
                                $this->tableName,
                                $this->columns,
                                $batchId . "_m{$month}"
                            );
                            $totalSavedToDB += $savedCount;
                        }

                        $offset += $limit;

                        $this->logger->debug("Месяц $month, страница: $usersCount пользователей, offset: $offset");

                        // Пауза между запросами
                        if ($pageData['has_more'] ?? false) {
                            sleep(1);
                        }

                        // Контроль памяти каждые 2000 записей
                        if ($offset % 2000 === 0) {
                            $this->checkMemoryUsage();
                        }

                    } else {
                        $this->logger->info("В месяце $month больше нет пользователей");
                        break;
                    }
                } catch (\Exception $e) {
                    $this->logger->error("Критическая ошибка при загрузке месяца $month: " . $e->getMessage());
                    break;
                }
            } while (!empty($pageData['items']) && ($pageData['has_more'] ?? false));

            $allUsers = array_merge($allUsers, $monthUsers);
            $totalUsers += count($monthUsers);

            $this->logger->endOperation("Обработка месяца $month", $monthStartTime);
            $this->logger->info("Месяц $month завершен. Пользователей: " . count($monthUsers));

            // Сохранение файла по месяцам
            $this->saveToFile([
                'month' => $month,
                'year' => $this->config->getExportYear(),
                'users_count' => count($monthUsers),
                'users' => $monthUsers,
                'export_date' => date('Y-m-d H:i:s'),
                'batch_id' => $batchId
            ], "users_{$this->config->getExportYear()}_month_{$month}");

            // Освобождение памяти
            unset($monthUsers);
            $this->checkMemoryUsage();
        }

        // Отключение от базы данных
        $this->database->disconnect();

        // Финальная статистика памяти
        $this->checkMemoryUsage();

        // Сохранение общего файла
        $this->saveToFile([
            'total_users' => $totalUsers,
            'database_saved' => $totalSavedToDB,
            'users' => $allUsers,
            'export_date' => date('Y-m-d H:i:s'),
            'year' => $this->config->getExportYear(),
            'batch_id' => $batchId,
            'months_exported' => "{$this->config->getExportStartMonth()}-{$this->config->getExportEndMonth()}"
        ], "users_{$this->config->getExportYear()}_full_export");

        $executionTime = round(microtime(true) - $totalStartTime, 2);
        $this->logger->endOperation("ЭКСПОРТ ПОЛЬЗОВАТЕЛЕЙ ЗА {$this->config->getExportYear()}", $totalStartTime);

        $result = [
            'total_users' => $totalUsers,
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
            $this->logger->section("🚀 ЗАПУСК ЭКСПОРТА ПОЛЬЗОВАТЕЛЕЙ");
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
                'total_users' => 0,
                'database_saved' => 0
            ];
        }
    }
}
