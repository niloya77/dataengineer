-- Raw Open-Meteo rows exactly as ingested, one row per (hour, location, file).
select
    timestamp_utc,
    location,
    temperature_2m,
    wind_speed_100m,
    shortwave_radiation,
    cloud_cover,
    ingested_at,
    filename as source_file
from {{ source('raw', 'weather') }}
