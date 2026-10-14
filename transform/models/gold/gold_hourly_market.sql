-- Central fact table: one row per hour with prices, generation and weather.

with weather as (
    select
        timestamp_utc,
        -- Wind farms are concentrated in the north, solar in the south
        avg(wind_speed_100m_ms) filter (
            where location in ('north_sea', 'schleswig_holstein', 'lower_saxony')
        ) as wind_speed_north_ms,
        avg(radiation_wm2) filter (
            where location in ('bavaria', 'baden_wuerttemberg')
        ) as radiation_south_wm2,
        avg(temperature_c) as temperature_avg_c,
        avg(cloud_cover_pct) as cloud_cover_avg_pct
    from {{ ref('silver_weather_hourly') }}
    group by timestamp_utc
)

select
    s.timestamp_utc,
    s.timestamp_local,
    hour(s.timestamp_local) as hour_local,
    isodow(s.timestamp_local) as weekday,  -- 1 = Monday, 7 = Sunday
    s.price_eur_mwh,
    s.price_eur_mwh < 0 as is_negative_price,
    s.solar_mwh,
    s.wind_onshore_mwh,
    s.wind_offshore_mwh,
    s.solar_mwh + s.wind_onshore_mwh + s.wind_offshore_mwh as renewable_mwh,
    (s.solar_mwh + s.wind_onshore_mwh + s.wind_offshore_mwh) / s.load_mwh as renewable_share,
    s.load_mwh,
    w.wind_speed_north_ms,
    w.radiation_south_wm2,
    w.temperature_avg_c,
    w.cloud_cover_avg_pct
from {{ ref('silver_smard_hourly') }} as s
left join weather as w using (timestamp_utc)
