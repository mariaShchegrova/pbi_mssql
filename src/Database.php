<?php
/**
 * Database.php
 * Класс для работы с базой данных MSSQL
 * Версия 1.0
 */

namespace Pbi\Export;

class Database
{
    private Config $config;
    private Logger $logger;
    private ?\PDO $pdo = null;

    public function __construct(Config $config, Logger $logger)
    {
        $this->config = $config;
        $this->logger = $logger;
    }

    /**
     * Подключение к MSSQL через PDO
     *
     * @return \PDO|null Объект PDO или null при ошибке
     */
    public function connect(): ?\PDO
    {
        $startTime = $this->logger->startOperation("Подключение к MSSQL");

        try {
            $dsn = $this->config->getDatabaseDsn();
            $username = $this->config->getDatabaseUsername();
            $password = $this->config->getDatabasePassword();

            $this->pdo = new \PDO($dsn, $username, $password, [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC
            ]);

            $this->logger->endOperation("Подключение к MSSQL", $startTime);
            return $this->pdo;

        } catch (\PDOException $e) {
            $this->logger->error("Критическая ошибка подключения к MSSQL: " . $e->getMessage());
            return null;
        }
    }

    /**
     * Получить активное подключение
     *
     * @return \PDO|null
     */
    public function getPdo(): ?\PDO
    {
        return $this->pdo;
    }

    /**
     * Проверка наличия активного подключения
     *
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->pdo !== null;
    }

    /**
     * Закрыть подключение к базе данных
     */
    public function disconnect(): void
    {
        $this->pdo = null;
        $this->logger->debug("Подключение к MSSQL закрыто");
    }

    /**
     * Выполнение SQL запроса с параметрами
     *
     * @param string $sql SQL запрос
     * @param array $params Параметры запроса
     * @return \PDOStatement|false Результат запроса
     */
    public function query(string $sql, array $params = [])
    {
        if (!$this->pdo) {
            $this->logger->error("Нет подключения к базе данных");
            return false;
        }

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);
            return $stmt;
        } catch (\PDOException $e) {
            $this->logger->error("Ошибка выполнения запроса: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Выполнение MERGE запроса для upsert данных
     *
     * @param string $tableName Имя таблицы
     * @param array $columns Столбцы
     * @param array $values Значения
     * @param string $matchColumn Столбец для сопоставления
     * @param array $updateColumns Столбцы для обновления (если не указаны, используются все кроме matchColumn)
     * @return int Количество затронутых строк
     */
    public function merge(
        string $tableName,
        array $columns,
        array $values,
        string $matchColumn = 'id',
        ?array $updateColumns = null
    ): int {
        if (!$this->pdo) {
            $this->logger->error("Нет подключения к базе данных для MERGE операции");
            return 0;
        }

        // Если updateColumns не указаны, используем все колонки кроме matchColumn
        if ($updateColumns === null) {
            $updateColumns = array_filter($columns, fn($col) => $col !== $matchColumn);
        }

        $placeholders = implode(', ', array_fill(0, count($columns), '?'));
        $columnList = implode(', ', $columns);

        // Формирование SET части для UPDATE
        $setParts = [];
        foreach ($updateColumns as $col) {
            $setParts[] = "target.[$col] = source.[$col]";
        }
        $setClause = implode(",\n                    ", $setParts);

        // Формирование INSERT части
        $insertColumns = implode(', ', array_map(fn($col) => "[$col]", $columns));

        $sql = "MERGE [$tableName] WITH (HOLDLOCK) AS target
                USING (VALUES ($placeholders))
                AS source (" . implode(', ', array_map(fn($col) => "[$col]", $columns)) . ")
                ON (target.[$matchColumn] = source.[$matchColumn])
                WHEN MATCHED THEN
                    UPDATE SET
                        $setClause
                WHEN NOT MATCHED THEN
                    INSERT ($insertColumns)
                    VALUES (" . implode(', ', array_map(fn($col) => "source.[$col]", $columns)) . ");";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($values);
            return $stmt->rowCount();
        } catch (\PDOException $e) {
            $this->logger->error("Ошибка MERGE: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Выполнение серии MERGE операций в транзакции
     *
     * @param string $tableName Имя таблицы
     * @param array $columns Столбцы
     * @param array $rows Массив строк данных
     * @param string $matchColumn Столбец для сопоставления
     * @return int Количество сохраненных записей
     */
    public function mergeBatch(
        string $tableName,
        array $columns,
        array $rows,
        string $matchColumn = 'id'
    ): int {
        if (!$this->pdo || empty($rows)) {
            return 0;
        }

        $savedCount = 0;
        $updateColumns = array_filter($columns, fn($col) => $col !== $matchColumn);

        try {
            $this->pdo->beginTransaction();

            foreach ($rows as $row) {
                try {
                    // Формирование значений в правильном порядке колонок
                    $values = [];
                    foreach ($columns as $col) {
                        $values[] = $row[$col] ?? null;
                    }

                    $this->merge($tableName, $columns, $values, $matchColumn, $updateColumns);
                    $savedCount++;

                } catch (\InvalidArgumentException $e) {
                    $this->logger->warning("Пропущена невалидная запись: " . $e->getMessage());
                    continue;
                } catch (\Exception $e) {
                    $this->logger->error("Ошибка обработки записи: " . $e->getMessage());
                    continue;
                }
            }

            $this->pdo->commit();
            return $savedCount;

        } catch (\PDOException $e) {
            $this->pdo->rollBack();
            $this->logger->error("Ошибка транзакции MERGE batch: " . $e->getMessage());

            // Попытка альтернативного сохранения
            return $this->saveAlternative($tableName, $columns, $rows, $matchColumn);
        }
    }

    /**
     * Альтернативный метод сохранения через отдельные INSERT/UPDATE
     *
     * @param string $tableName Имя таблицы
     * @param array $columns Столбцы
     * @param array $rows Массив строк данных
     * @param string $matchColumn Столбец для сопоставления
     * @return int Количество сохраненных записей
     */
    private function saveAlternative(
        string $tableName,
        array $columns,
        array $rows,
        string $matchColumn = 'id'
    ): int {
        $savedCount = 0;

        try {
            $this->pdo->beginTransaction();

            // Подготовка запросов
            $checkSql = "SELECT COUNT(*) FROM [$tableName] WHERE [$matchColumn] = ?";
            $checkStmt = $this->pdo->prepare($checkSql);

            $insertColumns = implode(', ', array_map(fn($col) => "[$col]", $columns));
            $placeholders = implode(', ', array_fill(0, count($columns), '?'));
            $insertSql = "INSERT INTO [$tableName] ($insertColumns) VALUES ($placeholders)";
            $insertStmt = $this->pdo->prepare($insertSql);

            $updateSet = implode(', ', array_map(fn($col) => "[$col] = ?", $columns));
            $updateSql = "UPDATE [$tableName] SET $updateSet WHERE [$matchColumn] = ?";
            $updateStmt = $this->pdo->prepare($updateSql);

            foreach ($rows as $row) {
                try {
                    $matchValue = $row[$matchColumn] ?? null;
                    
                    // Проверка существования записи
                    $checkStmt->execute([$matchValue]);
                    $exists = $checkStmt->fetchColumn() > 0;

                    if ($exists) {
                        // UPDATE
                        $params = [];
                        foreach ($columns as $col) {
                            $params[] = $row[$col] ?? null;
                        }
                        $params[] = $matchValue; // Для WHERE
                        $updateStmt->execute($params);
                    } else {
                        // INSERT
                        $params = [];
                        foreach ($columns as $col) {
                            $params[] = $row[$col] ?? null;
                        }
                        $insertStmt->execute($params);
                    }

                    $savedCount++;

                } catch (\Exception $e) {
                    $this->logger->error("Ошибка альтернативного сохранения: " . $e->getMessage());
                    continue;
                }
            }

            $this->pdo->commit();
            return $savedCount;

        } catch (\PDOException $e) {
            $this->pdo->rollBack();
            $this->logger->error("Критическая ошибка альтернативного сохранения: " . $e->getMessage());
            return 0;
        }
    }

    /**
     * Создание таблицы если не существует
     *
     * @param string $tableName Имя таблицы
     * @param array $columns Определение колонок
     * @return bool Успешность операции
     */
    public function createTableIfNotExists(string $tableName, array $columns): bool
    {
        if (!$this->pdo) {
            $this->logger->error("Нет подключения к базе данных для создания таблицы");
            return false;
        }

        $columnDefs = [];
        foreach ($columns as $name => $type) {
            $columnDefs[] = "[$name] $type";
        }

        $columnDefinitions = implode(",\n        ", $columnDefs);

        $sql = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='$tableName' AND xtype='U')
                CREATE TABLE [$tableName] (
                    $columnDefinitions
                )";

        try {
            $this->pdo->exec($sql);
            $this->logger->info("Таблица $tableName проверена/создана");
            return true;
        } catch (\PDOException $e) {
            $this->logger->error("Ошибка создания таблицы $tableName: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Проверка существования таблицы
     *
     * @param string $tableName Имя таблицы
     * @return bool
     */
    public function tableExists(string $tableName): bool
    {
        if (!$this->pdo) {
            return false;
        }

        try {
            $sql = "SELECT COUNT(*) FROM sysobjects WHERE name=? AND xtype='U'";
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([$tableName]);
            return $stmt->fetchColumn() > 0;
        } catch (\PDOException $e) {
            $this->logger->error("Ошибка проверки таблицы: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Очистка таблицы
     *
     * @param string $tableName Имя таблицы
     * @return bool
     */
    public function truncateTable(string $tableName): bool
    {
        if (!$this->pdo) {
            return false;
        }

        try {
            $this->pdo->exec("TRUNCATE TABLE [$tableName]");
            $this->logger->info("Таблица $tableName очищена");
            return true;
        } catch (\PDOException $e) {
            $this->logger->error("Ошибка очистки таблицы: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Удаление таблицы
     *
     * @param string $tableName Имя таблицы
     * @return bool
     */
    public function dropTable(string $tableName): bool
    {
        if (!$this->pdo) {
            return false;
        }

        try {
            $this->pdo->exec("DROP TABLE IF EXISTS [$tableName]");
            $this->logger->info("Таблица $tableName удалена");
            return true;
        } catch (\PDOException $e) {
            $this->logger->error("Ошибка удаления таблицы: " . $e->getMessage());
            return false;
        }
    }
}
