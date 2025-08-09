CREATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_date]
(
   date_key INT NOT NULL,
   order_date DATE,
   year INT,
   quarter INT,
   month INT,
   day INT
)
GO

SELECT top 3 * from CG_Data_Warehouse.stg.sales_data_sample

TRUNCATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_date];

INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_date] (date_key, order_date, year, quarter, month, day)
SELECT DISTINCT
        CONVERT(INT, CONVERT(VARCHAR(8), CONVERT(DATETIME, ORDERDATE, 101), 112)) AS date_key,
        CAST(CONVERT(DATETIME, ORDERDATE, 101) AS DATE) AS order_date,
    YEAR_ID AS year,
    QTR_ID AS quarter,
    MONTH_ID AS month,
        DAY(CONVERT(DATETIME, ORDERDATE, 101)) AS day
FROM
    CG_Data_Warehouse.stg.sales_data_sample;



CREATE PROC [stg].[stp_load_cg_dim_date]
AS
BEGIN
TRUNCATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_date];

INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_date] (date_key, order_date, year, quarter, month, day)
SELECT DISTINCT
        CONVERT(INT, CONVERT(VARCHAR(8), CONVERT(DATETIME, ORDERDATE, 101), 112)) AS date_key,
        CAST(CONVERT(DATETIME, ORDERDATE, 101) AS DATE) AS order_date,
    YEAR_ID AS year,
    QTR_ID AS quarter,
    MONTH_ID AS month,
        DAY(CONVERT(DATETIME, ORDERDATE, 101)) AS day
FROM
    CG_Data_Warehouse.stg.sales_data_sample;
END
