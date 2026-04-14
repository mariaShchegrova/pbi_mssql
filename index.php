<?php

ini_set("display_errors", "1");
ini_set("display_startup_errors", "1");
ini_set('error_reporting',  E_ERROR);

// Подключение автозагрузчика и базовых классов
require_once __DIR__ . '/bootstrap.php';

$method = trim($_GET['method']);
switch ($method) {
    case 'deals_export':
        $container = createContainer();
        $exporter = createDealsExporter($container);
        $result = $exporter->run();
        header('Content-Type: application/json');
        echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        break;

    case 'deals_history_export':
        // TODO: Реализовать экспортер истории сделок
        header('HTTP/1.1 501 NOT IMPLEMENTED');
        $output = [
            "status" => [
                "code" => 'NOT_IMPLEMENTED',
                "message" => 'Функционал в разработке',
                "detailed_message" => 'Экспортер истории сделок будет доступен позже'
            ]
        ];
        echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        break;

    case 'leads_export':
        // TODO: Реализовать экспортер лидов
        header('HTTP/1.1 501 NOT IMPLEMENTED');
        $output = [
            "status" => [
                "code" => 'NOT_IMPLEMENTED',
                "message" => 'Функционал в разработке',
                "detailed_message" => 'Экспортер лидов будет доступен позже'
            ]
        ];
        echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        break;

    case 'users_export':
        // TODO: Реализовать экспортер пользователей
        header('HTTP/1.1 501 NOT IMPLEMENTED');
        $output = [
            "status" => [
                "code" => 'NOT_IMPLEMENTED',
                "message" => 'Функционал в разработке',
                "detailed_message" => 'Экспортер пользователей будет доступен позже'
            ]
        ];
        echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        break;

    case 'activities_export':
        // TODO: Реализовать экспортер активностей
        header('HTTP/1.1 501 NOT IMPLEMENTED');
        $output = [
            "status" => [
                "code" => 'NOT_IMPLEMENTED',
                "message" => 'Функционал в разработке',
                "detailed_message" => 'Экспортер активностей будет доступен позже'
            ]
        ];
        echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        break;

    default:
        header('HTTP/1.1 400 BAD_REQUEST');
        $output = [
            "status" => [
                "code" => 'ERROR',
                "message" => 'BAD_REQUEST',
                "detailed_message" => 'Вызываемый метод не существует. Доступные методы: deals_export'
            ]
        ];
        echo json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        break;
}
