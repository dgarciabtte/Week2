with source as (

    select * from {{ source('main', 'green_tripdata') }}

),

renamed as (

    select
        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,        
        ratecodeid,
        pulocationid,
        dolocationid,
        passenger_count::int as passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        --ehail_fee, --removed due to 100% null source data
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        filename

    from source
      WHERE lpep_pickup_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future
        AND trip_distance >= 0  AND trip_distance <=1000-- drop negative trip_distance
        AND
        (fare_amount)>= 0 AND 
        (extra)>=0  AND
        (mta_tax)>=0 AND
        (tip_amount)>=0 AND
        (improvement_surcharge)>=0 AND
        (total_amount) >=0 AND
        (congestion_surcharge) >=0 AND
        (lpep_dropoff_datetime) <= TIMESTAMP '2022-12-31' AND
        (passenger_count) >=0 AND passenger_count <= 9   --- drop negative values  and  passanger counts like 112 
      

)

select * from renamed

