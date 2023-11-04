
/*
   dgarciab - Diana Maria Garcia Bustamante
   references pages:  https://mode.com/sql-tutorial/sql-string-functions-for-cleaning/
   https://mode.com/sql-tutorial/sql-sub-queries/
*/


with source as (

	select * from {{source ('main', 'fhv_tripdata') }}

),
renamed as (

        select 
	TRIM(dispatching_base_num) as Dispaching_Base_License_Num,
        TRIM(pickup_datetime):: TIMESTAMP as Date_Trip_PickUp, 
        TRIM(dropOff_datetime):: TIMESTAMP as Date_Trip_DropOff, 
        TRIM(PUlocationID):: bigint as Zone_ID_Trip_Began,
        TRIM(DOlocationID):: bigint as Zone_ID_Trip_Ended,
        TRIM(Affiliated_base_number) as Affiliated_base_number,
	    /*convert the NULL value in SR_Flah in 0*/
        (SELECT CASE WHEN SR_Flag = 1 THEN 1 ELSE 0 END) AS Share_Ride_Flag,
        filename
        from source	
        /*remove dates that are  located in the future */
        WHERE 
        (pickup_datetime) <= NOW() AND
        (dropOff_datetime) <= NOW()
)

select * from renamed


