

/*
   dgarciab - Diana Maria Garcia Bustamante
   references pages:  https://mode.com/sql-tutorial/sql-string-functions-for-cleaning/
*/

with source as (

	select * from {{source ('main', 'fhv_bases') }}

),
renamed as (

        select 
	TRIM(base_number) as Base_License_Number,
	TRIM(base_name) as Oficial_Base_Name,
        TRIM(dba) as Doing_Business_As,
        TRIM(dba_category) as Business_Category,
        filename
        from source	
)



select * from renamed



