<?php
/**
 * Logger.php
 * Класс логирования для экспорта данных из Битрикс24 в MSSQL
 * Версия 1.0
 */

namespace Pbi\Export;

class Logger
{
    private string $logDir;
    private string $logFile;
    private bool $echoEnabled;

    public function __construct(string $logDir, string $prefix = 'export', bool $echoEnabled = true)
    {
        $this->logDir = $logDir;
        $this->echoEnabled = $echoEnabled;
        $this->logFile = $this->getLogFilePath($prefix);
        $this->ensureLogDirExists();
    }

    /**
     * Создать директорию логов если не существует
     */
    private function ensureLogDirExists(): void
    {
        if (!is_dir($this->logDir)) {
            mkdir($this->logDir, 0755, true);
        }
    }

    /**
     * Получить путь к файлу лога
     */
    private function getLogFilePath(string $prefix): string
    {
        return $this->logDir . '/' . $prefix . '_export_' . date('Ymd') . '.log';
    }

    /**
     * Установить префикс файла лога
     */
    public function setPrefix(string $prefix): void
    {
        $this->logFile = $this->getLogFilePath($prefix);
    }

    /**
     * Включить/выключить вывод в stdout
     */
    public function setEchoEnabled(bool $enabled): void
    {
        $this->echoEnabled = $enabled;
    }

    /**
     * Записать сообщение в лог
     *
     * @param string $message Сообщение
     * @param string $level Уровень (INFO, ERROR, DEBUG, WARNING)
     * @param bool $echo Выводить в stdout
     */
    public function log(string $message, string $level = 'INFO', ?bool $echo = null): void
    {
        $timestamp = date('Y-m-d H:i:s');
        $formattedMessage = "[$timestamp] [$level] $message\n";

        // Запись в файл
        file_put_contents($this->logFile, $formattedMessage, FILE_APPEND | LOCK_EX);

        // Вывод в stdout если включено
        if ($echo === null ? $this->echoEnabled : $echo) {
            echo $formattedMessage;
        }
    }

    /**
     * Логирование начала операции
     *
     * @param string $operationName Название операции
     * @return float Время начала (для использования с logEndOperation)
     */
    public function startOperation(string $operationName): float
    {
        $this->log("🚀 НАЧАЛО: $operationName", 'INFO');
        return microtime(true);
    }

    /**
     * Логирование завершения операции
     *
     * @param string $operationName Название операции
     * @param float $startTime Время начала (от startOperation)
     */
    public function endOperation(string $operationName, float $startTime): void
    {
        $executionTime = round(microtime(true) - $startTime, 2);
        $this->log("✅ ЗАВЕРШЕНО: $operationName (время: {$executionTime}с)", 'INFO');
    }

    /**
     * Логирование информационного сообщения
     */
    public function info(string $message): void
    {
        $this->log($message, 'INFO');
    }

    /**
     * Логирование ошибки
     */
    public function error(string $message): void
    {
        $this->log($message, 'ERROR');
    }

    /**
     * Логирование отладочного сообщения
     */
    public function debug(string $message): void
    {
        $this->log($message, 'DEBUG');
    }

    /**
     * Логирование предупреждения
     */
    public function warning(string $message): void
    {
        $this->log($message, 'WARNING');
    }

    /**
     * Логирование заголовка раздела
     */
    public function section(string $title): void
    {
        $separator = str_repeat('=', 50);
        $this->log($separator, 'INFO');
        $this->log($title, 'INFO');
        $this->log($separator, 'INFO');
    }

    /**
     * Логирование успешного завершения
     */
    public function success(string $message): void
    {
        $this->log("✅ $message", 'INFO');
    }

    /**
     * Логирование ошибки с деталями
     */
    public function failure(string $message): void
    {
        $this->log("❌ $message", 'ERROR');
    }

    /**
     * Логирование статистики
     *
     * @param array $stats Массив статистики
     */
    public function stats(array $stats): void
    {
        $this->log("📊 СТАТИСТИКА:", 'INFO');
        foreach ($stats as $key => $value) {
            $this->log("   $key: $value", 'INFO');
        }
    }
}
