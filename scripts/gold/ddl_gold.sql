USE DataWarehouse;

CREATE OR ALTER VIEW gold.dim_customers AS 
	SELECT 
		ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		ci.cst_marital_status AS marital_status,
			CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
				 ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,
		ca.BDATE AS birth_date,
		la.CNTRY AS country,
		ci.cst_create_date AS create_date	
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_CUST_AZ12 AS ca
	ON	      ci.cst_key = ca.CID
	LEFT JOIN silver.erp_LOC_A101 AS la
	ON        ci.cst_key = la.CID;

CREATE OR ALTER VIEW gold.dim_products AS
	SELECT 
		ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.CAT AS category,
		pc.SUBCAT AS sub_category,
		pc.MAINTENANCE AS Maintenance,
		pn.prd_line AS product_line,
		pn.prd_cost AS cost,
		pn.prd_start_dt AS start_date
	FROM silver.crm_prd_info AS pn
	LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
	ON        pn.cat_id = pc.ID
	WHERE pn.prd_end_dt IS NULL;

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS ship_date,
sd.sls_due_dt AS due_date,
sd.sls_quantity AS quantity,
sd.sls_price AS price,
sd.sls_sales AS sales_amount
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customers AS cu
ON        sd.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products AS pr
ON		  sd.sls_prd_key= pr.product_number;


-- THE GOLD JOIN
select * 
from gold.fact_sales AS f
LEFT JOIN gold.dim_customers as c
ON		  f.customer_key = c.customer_key
LEFT JOIN gold.dim_products as p
ON		  f.product_key = p.product_key;
