use master 
go
if exists(select 1 from sys.databases where name = 'DataWarehouse')
begin 
	alter database DataWarehouse
	set SINGLE_USER with rollback immediate ;
	DROP DATABASE DataWarehouse ;
end
create database DataWarehouse;
use DataWarehouse;

create schema bronze;
go 
create schema silver ;
go
create schema gold
go
