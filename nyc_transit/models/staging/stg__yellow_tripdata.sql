
/*
   dgarciab - Diana Maria Garcia Bustamante
   references pages:  https://mode.com/sql-tutorial/sql-string-functions-for-cleaning/
   https://mode.com/sql-tutorial/sql-sub-queries/
*/


with source as (

	select * from {{source ('main', 'yellow_tripdata') }}

),
renamed as (
        select 
	TRIM(VendorID)::int as Record_Provider_ID,
        TRIM(tpep_pickup_datetime):: TIMESTAMP as Date_Meter_Engaged, 
        TRIM(tpep_dropoff_datetime):: TIMESTAMP as Date_Meter_Disengaged, 
        TRIM(store_and_fwd_flag) as Store_forward_Indicator,
        TRIM(RatecodeID)::int as Final_Rate_Code_ID,
        TRIM(PULocationID)::bigint as Location_ID_Meter_Engaged,
        TRIM(DOLocationID)::bigint as Location_ID_Meter_Disengaged,
        TRIM(passenger_count)::bigint as Num_Passanger_inVehicle,
        TRIM(trip_distance)::double as Trip_Distance,
        TRIM(fare_amount)::double as Fare_Amount,  
        TRIM(extra):: double as Extra_Amount,
        TRIM(mta_tax):: double as MTA_Tax,
        TRIM(tip_amount):: double as Tip_Amount,
        TRIM(tolls_amount) :: double Tolls_Amount,
        TRIM(improvement_surcharge):: double as Improvement_Surcharge,
        TRIM(total_amount):: double as Total_Amount_Charged,
        TRIM(payment_type):: int as Payment_Type,
        TRIM(airport_fee):: double as Airport_Fee,
        TRIM(congestion_surcharge):: double as Congestion_Surcharge,
        filename
        from source	
        /*remove negative values */
        WHERE 
        ((trip_distance)>= 0 AND (trip_distance) <=1000) AND
        (fare_amount)>= 0 AND 
        (extra)>=0  AND 
        (mta_tax)>=0 AND
        (tip_amount)>=0 AND
        (improvement_surcharge)>=0 AND
        (total_amount) >=0 AND
        (congestion_surcharge) >=0 AND
        (airport_fee) >=0

)

select * from renamed


