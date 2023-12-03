-- author dgarciab
--  this query pivot the results by borough.
--  I used the dbt_utils.pivot function to pivot the results by borough
-- source: https://github.com/dbt-labs/dbt-utils#pivot-source
select
  {{ dbt_utils.pivot(
      'Borough',
      dbt_utils.get_column_values(ref('mart__fact_trips_by_borough'), 'Borough'),
      agg='sum',
      then_value='total_trips_by_borough'
  ) }}
from {{ ref('mart__fact_trips_by_borough') }}
group by all
