/*
  DW_DSS_Land | 02_create_staging.sql | V2 TOI GIAN
  Can chay 01_create_databases.sql truoc.
  3 bang: stgRawListing (28 cot CSV + 1 khoa), CleanListing (17 cot), Reject_Log (5 cot).
  Chi TAO BANG moi: khong xoa, khong nap du lieu, khong tu sua bang V1.
  Neu mot bang da ton tai, DUNG ngay de tranh nham schema.
*/
USE [Land_Staging];
GO
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    IF OBJECT_ID(N'dbo.stgRawListing', N'U') IS NOT NULL
       OR OBJECT_ID(N'dbo.CleanListing', N'U') IS NOT NULL
       OR OBJECT_ID(N'dbo.Reject_Log', N'U') IS NOT NULL
        THROW 51002, N'Staging da co bang trung ten. Dung lai va kiem tra schema cu; script V2 khong ghi de.', 1;

    -- 1. RAW: chi them RawRowID de xac dinh tung lan xuat hien trong CSV.
    -- 28 truong khac la du lieu nguon, giu NVARCHAR de chua ep kieu tai Extract.
    CREATE TABLE dbo.stgRawListing (
        RawRowID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Title NVARCHAR(MAX) NULL,
        Price NVARCHAR(255) NULL,
        Area NVARCHAR(255) NULL,
        Location NVARCHAR(MAX) NULL,
        ListingID NVARCHAR(255) NULL,
        LastUpdated NVARCHAR(255) NULL,
        PropertyType NVARCHAR(255) NULL,
        Width NVARCHAR(255) NULL,
        [Length] NVARCHAR(255) NULL,
        Bedrooms NVARCHAR(255) NULL,
        Bathrooms NVARCHAR(255) NULL,
        Floors NVARCHAR(255) NULL,
        Position NVARCHAR(255) NULL,
        Direction NVARCHAR(255) NULL,
        AlleyWidth NVARCHAR(255) NULL,
        RoadType NVARCHAR(255) NULL,
        [Description] NVARCHAR(MAX) NULL,
        Latitude NVARCHAR(255) NULL,
        Longitude NVARCHAR(255) NULL,
        VIPAccount NVARCHAR(255) NULL,
        Avatar NVARCHAR(255) NULL,
        AgentRole NVARCHAR(255) NULL,
        AgentName NVARCHAR(255) NULL,
        AgentListingCount NVARCHAR(255) NULL,
        Province NVARCHAR(255) NULL,
        PropertyTypeSlug NVARCHAR(255) NULL,
        ScrapedAt NVARCHAR(255) NULL,
        LastUpdatedDate NVARCHAR(255) NULL
    );

    -- 2. CLEAN: 1 ListingID = 1 dong sau loc Dat, khu trung va Validation.
    -- SourceRawRowID cho biet dong raw duoc chon; gia du lieu da ep kieu.
    CREATE TABLE dbo.CleanListing (
        ListingID BIGINT NOT NULL PRIMARY KEY,
        SourceRawRowID BIGINT NOT NULL UNIQUE,
        Price DECIMAL(19,4) NOT NULL,            -- trieu dong
        PriceBillion DECIMAL(19,6) NOT NULL,     -- ty dong
        Area DECIMAL(19,4) NOT NULL,             -- m2
        Width DECIMAL(19,4) NULL,                -- chieu rong (m), neu nguon co
        [Length] DECIMAL(19,4) NULL,             -- chieu dai (m), neu nguon co
        PricePerM2 DECIMAL(19,6) NOT NULL,       -- trieu dong/m2
        PriceSegment NVARCHAR(40) NOT NULL,
        AreaGroup NVARCHAR(40) NOT NULL,
        ProvinceOriginal NVARCHAR(100) NOT NULL,
        ProvinceNew NVARCHAR(100) NOT NULL,
        RegionGroup NVARCHAR(50) NOT NULL,
        Position NVARCHAR(100) NOT NULL,
        ScrapedAt DATETIME2(0) NULL,
        LastUpdatedAt DATETIME2(0) NULL,
        TimeKey INT NOT NULL,                    -- YYYYMM, -1 = khong xac dinh
        CONSTRAINT FK_Clean_Source FOREIGN KEY (SourceRawRowID)
            REFERENCES dbo.stgRawListing(RawRowID),
        CONSTRAINT CK_Clean_Values CHECK (Price > 0 AND Area > 0 AND PricePerM2 > 0),
        CONSTRAINT CK_Clean_Width_Length CHECK (
            (Width IS NULL OR Width > 0) AND
            ([Length] IS NULL OR [Length] > 0)
        )
    );

    -- 3. REJECT: ghi dong raw bi loai do loi (co the kem nhieu ly do).
    -- Tin khong thuoc loai Dat la OUT_OF_SCOPE: thong ke rieng trong Python.
    CREATE TABLE dbo.Reject_Log (
        RejectID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        SourceRawRowID BIGINT NOT NULL,
        ListingIDRaw NVARCHAR(255) NULL,
        RejectReason NVARCHAR(500) NOT NULL,
        RejectedAt DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT FK_Reject_Source FOREIGN KEY (SourceRawRowID)
            REFERENCES dbo.stgRawListing(RawRowID)
    );

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO

-- Xac nhan schema vua tao; day KHONG phai cau lenh nap du lieu.
SELECT t.name AS TableName, COUNT(c.column_id) AS ColumnCount
FROM sys.tables AS t
JOIN sys.columns AS c ON c.object_id = t.object_id
WHERE t.schema_id = SCHEMA_ID(N'dbo')
  AND t.name IN (N'stgRawListing', N'CleanListing', N'Reject_Log')
GROUP BY t.name
ORDER BY t.name;
GO
