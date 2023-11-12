with source as (

    select * from {{ source('main', 'yellow_tripdata') }}

),

renamed as (

    select
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count::int as passenger_count,
        trip_distance,
        ratecodeid,
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        filename

    from source
        WHERE tpep_pickup_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future
        AND trip_distance >= 0 AND trip_distance <=1000 AND-- drop negative trip_distance  
        -- drop negative values 
        (fare_amount)>= 0 AND 
        (extra)>=0  AND 
        (mta_tax)>=0 AND
        (tip_amount)>=0 AND
        (improvement_surcharge)>=0 AND
        (total_amount) >=0 AND
        (congestion_surcharge) >=0 AND
        (airport_fee) >=0  AND
        (passenger_count) >=0 AND passenger_count <= 9   --- drop negative values  and  passanger counts like 112 


)

select * from renamed



