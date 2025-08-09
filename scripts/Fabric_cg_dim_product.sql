CREATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_product]
(
   product_code VARCHAR(20) NOT NULL,
   product_line VARCHAR(50),
   msrp DECIMAL(10,2),
   product_name VARCHAR(100)
)
GO


INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_product] (product_code, product_line, msrp)
SELECT DISTINCT
    PRODUCTCODE,
    PRODUCTLINE,
    CAST(MSRP AS DECIMAL(10,2))
FROM
    [CG_Data_Warehouse].[stg].[sales_data_sample]
WHERE
    PRODUCTCODE IS NOT NULL;


CREATE PROC [stg].[stp_load_cg_dim_product]
AS
BEGIN
    TRUNCATE TABLE [CG_Data_Warehouse].[stg].[cg_dim_product];

    INSERT INTO [CG_Data_Warehouse].[stg].[cg_dim_product] (product_code, product_line, msrp)
    SELECT DISTINCT
        PRODUCTCODE,
        PRODUCTLINE,
        CAST(MSRP AS DECIMAL(10,2))
    FROM
        [CG_Data_Warehouse].[stg].[sales_data_sample]
    WHERE
        PRODUCTCODE IS NOT NULL;
END
GO