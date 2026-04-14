<?php
/**
 * bootstrap.php
 * Файл автозагрузки и инициализации для экспортеров PBI
 * Версия 1.0
 */

// Автозагрузка классов PSR-4
spl_autoload_register(function ($class) {
    // Префикс пространства имен
    $prefix = 'Pbi\\Export\\';
    
    // Базовая директория для префикса
    $baseDir = __DIR__ . '/src/';
    
    // Проверка соответствует ли класс пространству имен
    $len = strlen($prefix);
    if (strncmp($prefix, $class, $len) !== 0) {
        return;
    }
    
    // Получение относительного имени класса
    $relativeClass = substr($class, $len);
    
    // Замена разделителя пространства имен на разделитель директорий
    $file = $baseDir . str_replace('\\', '/', $relativeClass) . '.php';
    
    // Если файл существует, подключаем его
    if (file_exists($file)) {
        require $file;
    }
});

/**
 * Создание контейнера зависимостей
 * 
 * @return array Массив с экземплярами классов
 */
function createContainer(): array
{
    // Инициализация конфигурации
    $config = new \Pbi\Export\Config();
    
    // Инициализация логгера
    $logger = new \Pbi\Export\Logger(
        $config->getExportDir(),
        'export',
        true // echo enabled
    );
    
    // Применение настроек производительности
    $config->applyPerformanceSettings();
    
    // Инициализация HTTP клиента
    $curlClient = new \Pbi\Export\CurlClient($config, $logger);
    
    // Инициализация базы данных
    $database = new \Pbi\Export\Database($config, $logger);
    
    return [
        'config' => $config,
        'logger' => $logger,
        'curlClient' => $curlClient,
        'database' => $database
    ];
}

/**
 * Создание экспортера сделок
 * 
 * @param array $container Контейнер зависимостей
 * @return \Pbi\Export\DealsExporter
 */
function createDealsExporter(array $container): \Pbi\Export\DealsExporter
{
    return new \Pbi\Export\DealsExporter(
        $container['config'],
        $container['logger'],
        $container['curlClient'],
        $container['database']
    );
}

/**
 * Создание экспортера лидов
 * 
 * @param array $container Контейнер зависимостей
 * @return \Pbi\Export\LeadsExporter
 */
function createLeadsExporter(array $container): \Pbi\Export\LeadsExporter
{
    return new \Pbi\Export\LeadsExporter(
        $container['config'],
        $container['logger'],
        $container['curlClient'],
        $container['database']
    );
}

/**
 * Создание экспортера пользователей
 * 
 * @param array $container Контейнер зависимостей
 * @return \Pbi\Export\UsersExporter
 */
function createUsersExporter(array $container): \Pbi\Export\UsersExporter
{
    return new \Pbi\Export\UsersExporter(
        $container['config'],
        $container['logger'],
        $container['curlClient'],
        $container['database']
    );
}

/**
 * Создание экспортера активностей
 * 
 * @param array $container Контейнер зависимостей
 * @return \Pbi\Export\ActivitiesExporter
 */
function createActivitiesExporter(array $container): \Pbi\Export\ActivitiesExporter
{
    return new \Pbi\Export\ActivitiesExporter(
        $container['config'],
        $container['logger'],
        $container['curlClient'],
        $container['database']
    );
}

/**
 * Хелпер для быстрого запуска экспорта сделок
 */
function runDealsExport(): array
{
    $container = createContainer();
    $exporter = createDealsExporter($container);
    return $exporter->run();
}

/**
 * Хелпер для быстрого запуска экспорта лидов
 */
function runLeadsExport(): array
{
    $container = createContainer();
    $exporter = createLeadsExporter($container);
    return $exporter->run();
}

/**
 * Хелпер для быстрого запуска экспорта пользователей
 */
function runUsersExport(): array
{
    $container = createContainer();
    $exporter = createUsersExporter($container);
    return $exporter->run();
}

/**
 * Хелпер для быстрого запуска экспорта активностей
 */
function runActivitiesExport(): array
{
    $container = createContainer();
    $exporter = createActivitiesExporter($container);
    return $exporter->run();
}

// Экспорт функций в глобальную область видимости если вызвано не из namespace
if (!function_exists('Pbi\Export\bootstrap')) {
    function bootstrap(): array
    {
        return createContainer();
    }
}
