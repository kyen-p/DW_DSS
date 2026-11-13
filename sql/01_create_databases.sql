/*
DW_DSS_Land | 01_create_databases.sql
Khoi tao 2 database. KHONG xoa database/bang/du lieu hien co.
Chay trong SSMS (GO la batch separator cua SSMS/sqlcmd).
*/
IF DB_ID(N'Land_Staging') IS NULL
    EXEC(N'CREATE DATABASE [Land_Staging]');
GO
IF DB_ID(N'DW_Land') IS NULL
    EXEC(N'CREATE DATABASE [DW_Land]');
GO
SELECT name AS DatabaseName, state_desc AS DatabaseStatus
FROM sys.databases
WHERE name IN (N'Land_Staging', N'DW_Land')
ORDER BY name;
GO
