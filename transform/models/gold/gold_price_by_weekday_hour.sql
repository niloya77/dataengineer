-- Average price and renewable share for each weekday x hour (7 x 24 = 168 rows).
-- Answers: when is electricity cheapest and greenest?
select
    weekday,
    hour_local,
    count(*) as hours,
    round(avg(price_eur_mwh), 2) as avg_price_eur_mwh,
    round(avg(renewable_share), 4) as avg_renewable_share,
    round(avg(is_negative_price::int), 4) as negative_price_rate,
    rank() over (order by avg(price_eur_mwh)) as price_rank
from {{ ref('gold_hourly_market') }}
group by weekday, hour_local
