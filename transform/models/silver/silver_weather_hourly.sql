-- One row per (hour, location) with the latest ingested weather values.
select
    timestamp_utc,
    timezone('Europe/Berlin', timestamp_utc) as timestamp_local,
    location,
    temperature_2m as temperature_c,
    wind_speed_100m as wind_speed_100m_ms,
    shortwave_radiation as radiation_wm2,
    cloud_cover as cloud_cover_pct
from {{ ref('bronze_weather') }}
qualify row_number() over (partition by timestamp_utc, location order by ingested_at desc) = 1
