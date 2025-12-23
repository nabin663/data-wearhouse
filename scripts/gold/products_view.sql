create or alter view gold.dim_product as 
select 
ROW_NUMBER() over(order by pi.prd_start_dt, pi.prd_id asc) as id,
pi.prd_id as product_id, 
pi.prd_key as product_number,
pi.prd_nm as product_name,
pi.prd_cost as product_cost, 
pi.prd_line as product_line,
pi.prd_start_dt as product_start_date,
pi.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance  as maintenance 




from silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 pc
on pi.cat_id= pc.id
where pi.prd_end_dt is null

