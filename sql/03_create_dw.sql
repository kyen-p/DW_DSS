/*
  DW_DSS_Land | 03_create_dw.sql | V2 TOI GIAN
  Can chay 01_create_databases.sql va 02_create_staging.sql truoc.
  Grain: 1 ListingID (tin dat duy nhat) = 1 dong dbo.Fact_Listing.
  1 Fact + 5 Dimension; phuc vu BR01-BR06; khong co SCD2, GIS, Seller, LandUseClass.
  Chi TAO BANG, KHONG INSERT hay ghi de bang V1.
  Neu da ton tai mot bang, DUNG ngay de tranh nham schema.
*/
USE [DW_Land];
GO
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID(N'dbo.Dim_Time', N'U') IS NOT NULL
       OR OBJECT_ID(N'dbo.Dim_Location', N'U') IS NOT NULL
       OR OBJECT_ID(N'dbo.Dim_Size_Group', N'U') IS NOT NULL
       OR OBJECT_ID(N'dbo.Dim_Price_Segment', N'U') IS NOT NULL
       OR OBJECT_ID(N'dbo.Dim_Land_Profile', N'U') IS NOT NULL
       OR OBJECT_ID(N'dbo.Fact_Listing', N'U') IS NOT NULL
        THROW 51003, N'DW da co bang trung ten. Dung lai va kiem tra schema cu; script V2 khong ghi de.', 1;

    -- 1. THOI GIAN: moi dong la mot thang theo LastUpdatedAt.
    CREATE TABLE dbo.Dim_Time (
        TimeKey INT NOT NULL PRIMARY KEY,       -- YYYYMM; -1 = khong xac dinh
        [Year] SMALLINT NOT NULL,
        [Quarter] TINYINT NOT NULL,
        [Month] TINYINT NOT NULL,
        YearMonth CHAR(7) NOT NULL,            -- YYYY-MM; unknown = Unknown
        MonthName NVARCHAR(20) NOT NULL
    );

    -- 2. DIA BAN: mot dong cho moi to hop tinh goc -> tinh moi -> mien.
    -- Khong tu dong chuyen quan/huyen, xa/phuong cu sang don vi moi.
    CREATE TABLE dbo.Dim_Location (
        LocationKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ProvinceOriginal NVARCHAR(100) NOT NULL,
        ProvinceNew NVARCHAR(100) NOT NULL,
        RegionGroup NVARCHAR(50) NOT NULL,
        CONSTRAINT UQ_Location UNIQUE (ProvinceOriginal, ProvinceNew, RegionGroup)
    );

    -- 3. NHOM DIEN TICH: 6 nhom; [MinArea, MaxArea), MaxArea NULL = khong gioi han.
    CREATE TABLE dbo.Dim_Size_Group (
        SizeKey INT NOT NULL PRIMARY KEY,
        AreaGroup NVARCHAR(40) NOT NULL UNIQUE,
        MinArea DECIMAL(19,4) NOT NULL,
        MaxArea DECIMAL(19,4) NULL,
        SortOrder TINYINT NOT NULL,
        CONSTRAINT CK_Size_Range CHECK (MaxArea IS NULL OR MaxArea > MinArea)
    );

    -- 4. PHAN KHUC GIA: 5 nhom; don vi ty dong; can tren khong bao gom.
    CREATE TABLE dbo.Dim_Price_Segment (
        SegmentKey INT NOT NULL PRIMARY KEY,
        PriceSegment NVARCHAR(40) NOT NULL UNIQUE,
        MinBillion DECIMAL(19,6) NOT NULL,
        MaxBillion DECIMAL(19,6) NULL,
        SortOrder TINYINT NOT NULL,
        CONSTRAINT CK_Segment_Range CHECK (MaxBillion IS NULL OR MaxBillion > MinBillion)
    );

    -- 5. VI TRI DAT: chi can Position cho cau hoi duong chinh vs trong hem.
    CREATE TABLE dbo.Dim_Land_Profile (
        ProfileKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Position NVARCHAR(100) NOT NULL UNIQUE
    );

    -- 6. FACT: 5 khoa ngoai + gia, dien tich, rong/dai, gia/m2, 2 ngay cho BR05.
    -- Price, PriceBillion, Area, Width, Length: dung de loc/tham khao; PricePerM2 KHONG cong don.
    CREATE TABLE dbo.Fact_Listing (
        ListingKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ListingID BIGINT NOT NULL UNIQUE,
        TimeKey INT NOT NULL,
        LocationKey INT NOT NULL,
        SizeKey INT NOT NULL,
        SegmentKey INT NOT NULL,
        ProfileKey INT NOT NULL,
        Price DECIMAL(19,4) NOT NULL,
        PriceBillion DECIMAL(19,6) NOT NULL,
        Area DECIMAL(19,4) NOT NULL,
        Width DECIMAL(19,4) NULL,
        [Length] DECIMAL(19,4) NULL,
        PricePerM2 DECIMAL(19,6) NOT NULL,
        ScrapedAt DATETIME2(0) NULL,
        LastUpdatedAt DATETIME2(0) NULL,
        CONSTRAINT FK_Fact_Time FOREIGN KEY (TimeKey) REFERENCES dbo.Dim_Time(TimeKey),
        CONSTRAINT FK_Fact_Location FOREIGN KEY (LocationKey) REFERENCES dbo.Dim_Location(LocationKey),
        CONSTRAINT FK_Fact_Size FOREIGN KEY (SizeKey) REFERENCES dbo.Dim_Size_Group(SizeKey),
        CONSTRAINT FK_Fact_Segment FOREIGN KEY (SegmentKey) REFERENCES dbo.Dim_Price_Segment(SegmentKey),
        CONSTRAINT FK_Fact_Profile FOREIGN KEY (ProfileKey) REFERENCES dbo.Dim_Land_Profile(ProfileKey),
        CONSTRAINT CK_Fact_Values CHECK (Price > 0 AND Area > 0 AND PricePerM2 > 0),
        CONSTRAINT CK_Fact_Width_Length CHECK (
            (Width IS NULL OR Width > 0) AND
            ([Length] IS NULL OR [Length] > 0)
        )
    );

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO

-- Xac nhan so bang va so cot (chua co du lieu).
SELECT t.name AS TableName, COUNT(c.column_id) AS ColumnCount
FROM sys.tables AS t
JOIN sys.columns AS c ON c.object_id = t.object_id
WHERE t.schema_id = SCHEMA_ID(N'dbo')
  AND t.name IN (N'Dim_Time', N'Dim_Location', N'Dim_Size_Group',
                 N'Dim_Price_Segment', N'Dim_Land_Profile', N'Fact_Listing')
GROUP BY t.name
ORDER BY t.name;
GO
