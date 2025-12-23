

create or alter view gold.fact_sales as
(
select 
sd.sls_ord_num as order_number,
pr.id as product_key,
c.customer_key as customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_quantity as quantity,
sd.sls_price as price,
sd.sls_sales as total_sales


from silver.crm_sales_detail sd
left join gold.dim_product pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customer c
on sd.sls_cust_id= c.customer_id
)
