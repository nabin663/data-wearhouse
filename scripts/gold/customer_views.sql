--i have created the views for the customer table 
create or alter view gold.dim_customer as 
select
row_number() over(order by ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number, 
ci.cst_firstName as first_name,
ci.cst_lastName as last_name,
ci.cst_marital_status as marital_status,
case
	when ci.cst_gndr!='n/a' then ci.cst_gndr
	else coalesce(ca.gen, 'n/a')
end as gender, 
ca.bdate as birth_date,
cl.cntry as country,
ci.cst_create_date as created_date





from silver.crm_cust_info as ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 cl
on ci.cst_key = cl.cid


