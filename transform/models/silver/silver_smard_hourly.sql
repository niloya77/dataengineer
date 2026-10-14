-- One row per complete hour with all five SMARD signals side by side.

with deduped as (
    -- Keep the most recently ingested value for each (hour, signal)
    select *
    from {{ ref('bronze_smard') }}
    where value is not null
    qualify row_number() over (partition by timestamp_utc, signal order by ingested_at desc) = 1
),

pivoted as (
    select
        timestamp_utc,
        max(value) filter (where signal = 'price') as price_eur_mwh,
        max(value) filter (where signal = 'solar') as solar_mwh,
        max(value) filter (where signal = 'wind_onshore') as wind_onshore_mwh,
        max(value) filter (where signal = 'wind_offshore') as wind_offshore_mwh,
        max(value) filter (where signal = 'load') as load_mwh
    from deduped
    group by timestamp_utc
)

select
    timestamp_utc,
    timezone('Europe/Berlin', timestamp_utc) as timestamp_local,
    price_eur_mwh,
    solar_mwh,
    wind_onshore_mwh,
    wind_offshore_mwh,
    load_mwh
from pivoted
-- Price is published a day ahead; generation and load arrive later.
-- Only keep hours where every signal is known.
where price_eur_mwh is not null
  and solar_mwh is not null
  and wind_onshore_mwh is not null
  and wind_offshore_mwh is not null
  and load_mwh is not null
