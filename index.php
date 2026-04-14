<?php

ini_set("display_errors", "1");
ini_set("display_startup_errors", "1");
ini_set('error_reporting',  E_ERROR);

//require_once __DIR__ . '/../prolog.php';
//phpinfo();




$method = trim($_GET['method']);
switch ($method) {
    case 'deals_export':
        require_once 'deals_export_mssql.php';
        start(2025, 1, 1);
        break;

    case 'deals_history_export':
        require_once 'deals_stage_history_export_mssql.php';
        start();
        break;

    case 'leads_export':
        require_once 'leads_export_mssql.php';
        start();
        break;

    case 'users_export':
        require_once 'users_export_mssql.php';
        start();
        break;

    case 'activities_export':
        require_once 'activities_export_mssql.php';
        start();
        break;

    default:
        header('HTTP/1.1 400 BAD_REQUEST');
        $output = array(
            "status" => array(
                "code" => 'ERROR',
                "message" => 'BAD_REQUEST',
                "detailed_message" => 'Вызываемый метод не существует'
            )
        );
        break;
}
