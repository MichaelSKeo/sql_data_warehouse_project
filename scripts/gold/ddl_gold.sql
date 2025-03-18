/*
===============================================================================================================================================================
DDL Script: Create Gold Views
===============================================================================================================================================================

Script Purpose: 
  This script creates views in the Gold layer in the data warehouse.
  The Gold layer presesents the final dimension and fact tables (Star Schema)

  Each view prefers transformations and combines data from Silver layer 
  to provide a clean, enriched, and business-ready dataset. 

Usage: 
  - These views can be queried directly for analytics and reporting. 
===============================================================================================================================================================

*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL 
	DROP VIEW gold.fact_sales; 

GO

CREATE VIEW gold.fact_sales AS
SELECT 
	s. sls_ord_num AS order_number, 
	p. product_key,
	c. customer_key,  
	s. sls_order_dt AS order_date, 
	s. sls_ship_dt AS shipping_date, 
	s. sls_due_dt AS due_date,
	s. sls_sales AS sales_amount, 
	s. sls_quantity AS quantity, 
	s. sls_price AS price
	
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_products p
ON        s. sls_prd_key = p. product_number
LEFT JOIN gold.dim_customers c
ON		  s. sls_cust_id = c. customer_id;

GO

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL 
	DROP VIEW gold.dim_products;  

GO

CREATE VIEW gold.dim_products AS 

SELECT 
	ROW_NUMBER() OVER (ORDER BY prd. prd_start_dt, prd. prd_id) AS product_key, 
	prd. prd_id AS product_id,
	prd. prd_key AS product_number, 
	prd. prd_nm AS product_name, 
	prd. cat_id AS category_id, 
	cat. cat AS category, 
	cat. subcat AS subcategory, 	
	prd. prd_line AS product_line,
	cat. maintenance, 
	prd. prd_cost AS cost,
	prd. prd_start_dt AS start_date

FROM silver.crm_prd_info prd
LEFT JOIN silver.erp_px_cat_g1v2 cat
ON        prd. cat_id = cat. id
WHERE prd. prd_end_dt IS NULL; 

GO

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers; 

GO

CREATE VIEW gold.dim_customers AS 

SELECT 
	ROW_NUMBER() OVER (ORDER BY cus. cst_id) AS customer_key,
	cus. cst_id AS customer_id,
	cus. cst_firstname AS first_name, 
	cus. cst_lastname AS last_name,
	CASE  
		WHEN cus. cst_gndr in ('Male', 'Female') THEN cst_gndr
		WHEN cus. cst_gndr = 'Unknown' and erc. gen in ('Male', 'Female') THEN COALESCE(erc. gen, cst_gndr)
		ELSE 'Unknown'
	END AS gender,  
	cus. cst_marital_status AS marital_status, 
	erl. cntry AS country, 
	erc. bdate AS birth_date, 
	cus. cst_create_date AS create_date

FROM silver.crm_cust_info cus
LEFT JOIN silver.erp_cust_az12 erc 
ON		  cus. cst_key = erc. cid
LEFT JOIN silver.erp_loc_a101 erl 
ON		  cus. cst_key = erl. cid; 
