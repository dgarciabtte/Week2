
-- this query Use Window functions with QUALIFY and ROW_NUMBER to remove
--duplicate rows.
--Prefer rows with a later time stamp

SELECT * FROM 
{{ ref('events') }}
qualify row_number() OVER (PARTITION by event_id
ORDER BY insert_timestamp DESC)
= 1