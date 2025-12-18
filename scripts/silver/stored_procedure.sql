
CREATE OR ALTER procedure silver.load_silver as
begin
	print'start loading cust_info'
	truncate table silver.crm_cust_info;
	insert into silver.crm_cust_info
	(
	cst_id, 
	cst_key, 
	cst_firstName, 
	cst_lastName, 
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select 
	cst_id,
	cst_key,
	TRIM(cst_firstName) as cst_firstName,
	TRIM(cst_lastName ) as cst_lastName,
	case trim(cst_marital_status )
		when 'M' then 'Married'
		when 'S' then 'Single'
		else 'n/a'

	end as cst_marital_status,
	case trim(cst_gndr)
		when 'M' then 'Male'
		when 'F' then 'Female'
		else 'n/a'

	end as cst_gndr, 
	cst_create_date

	from(select
	*, 
	ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag

	from bronze.crm_cust_info
	where cst_id is not null) t
	where flag=1 
	print'loading cust_info completed'

	--truncate table silver.crm_prd_info;
	--insert into silver.crm_prd_info(
	--prd_id, 
	--cat_id, prd_key,
	--prd_nm,
	--prd_cost,
	--prd_line,
	--prd_start_dt,
	--prd_end_dt

	--)

	--select prd_id,

	--replace(SUBSTRING(prd_key, 1, 5), '-','_') as cat_id,
	--substring(prd_key, 7, len(prd_key)) as prd_key,
	--prd_nm,
	--coalesce(prd_cost, 0) as prd_cost,

	--case trim(UPPER(prd_line) )
	--	when 'M' then 'Mechanical'
	--	when 'R' then 'Road'
	--	when 'S' then 'Other Sale'
	--	when 'T' then 'Technological'
	--	else 'N/A'
	--end as prd_line, 
	--cast(prd_start_dt as date) as prd_start_dt,
	--cast(lead(prd_start_dt) over( partition by prd_key order by prd_start_dt asc )-1  as date)as prd_end_dt



	--from bronze.crm_prd_info
	--it is load t
	print'loading prd_info'

	truncate table silver.crm_prd_info;
	insert into silver.crm_prd_info(
	prd_id, 
	cat_id, prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt

	)

	select prd_id,

	replace(SUBSTRING(prd_key, 1, 5), '-','_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_nm,
	coalesce(prd_cost, 0) as prd_cost,

	case trim(UPPER(prd_line) )
		when 'M' then 'Mechanical'
		when 'R' then 'Road'
		when 'S' then 'Other Sale'
		when 'T' then 'Technological'
		else 'N/A'
	end as prd_line, 
	cast(prd_start_dt as date) as prd_start_dt,
	cast(lead(prd_start_dt) over( partition by prd_key order by prd_start_dt asc )-1  as date)as prd_end_dt



	from bronze.crm_prd_info
	print'loading prd_info completed'

	print'loading sales_detail started'
	--the query to load silver sales detail table
	truncate table silver.crm_sales_detail;
	insert into silver.crm_sales_detail(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales, 
	sls_quantity,
	sls_price
	)
	select 
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	case 
		when len(sls_order_dt)!=8 or sls_order_dt =0 then null
		else cast(CAST(sls_order_dt as varchar) as date)
	end as sls_order_dt, 
	case
		when len(sls_ship_dt)!=8 or sls_order_dt = 0 then null
		else cast(cast(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	case
		when len(sls_due_dt)!=8 or sls_due_dt = 0 then null
		else cast(cast(sls_due_dt as nvarchar) as date)
	end as sls_due_dt,
	case
		when sls_sales is null or sls_sales<=0 or sls_price!= sls_quantity*abs(sls_price)
		then abs(sls_price)*sls_quantity
		else sls_sales
	
	end as sls_sales, 
	sls_quantity,
	case 
		when sls_price is null or sls_price <=0 then abs(sls_sales)/nullif(sls_quantity, 0)
		else sls_price 
	end as sls_price



	from bronze.crm_sales_detail
	print'loading sales_detail completed'

	print'loading erp_cust_az12_started'

	truncate table silver.erp_cust_az12
	insert into silver.erp_cust_az12(
	cid, bdate, gen
	)
	select
	CASE 
		WHEN cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
		else cid
	END as cid, 
	case
		when bdate>getdate() then null
		else bdate
	 end as bdate,
 

		case when upper(trim(gen)) in ('M', 'Male') then 'Male'
		 when upper(trim(gen)) in ('F','Female') then 'Female'
		 else 'N/A'
		end as gen



	from bronze.erp_cust_az12
	print'loading erp_cust_az12_completed'

	print'loading erp_loc_a101_started'

	truncate table silver.erp_loc_a101
	insert into silver.erp_loc_a101(
	cid, cntry)
	select 
	REPLACE(cid, '-', '') as cid ,case 
		when trim(cntry)='DE' THEN 'Germany'
		when trim(cntry) in ('USA', 'US') THEN 'United States'
		when cntry is null or len(trim(cntry))=0 then 'N/A'
		else cntry
	end as cntry


	from bronze.erp_loc_a101
	print'loading erp_loc_a101_completed'

	print'loading erp_px_cat_g1v2started'

	truncate table silver.erp_px_cat_g1v2;
	insert into silver.erp_px_cat_g1v2( id, cat, subcat, maintenance) 
	select id, cat, subcat, maintenance from bronze.erp_px_cat_g1v2
	print'loading erp_px_cat_g1v2completed'

	

end
  


