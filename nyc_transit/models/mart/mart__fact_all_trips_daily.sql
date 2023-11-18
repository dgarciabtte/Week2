select 
   type,
   date_trunc('day',started_at_ts)::date as date,
   count(*) as trip_count,
   avg(duration_min) as avg_duration_min,
   from   {{ref('mart__fact_all_trips')}}
   group by all



