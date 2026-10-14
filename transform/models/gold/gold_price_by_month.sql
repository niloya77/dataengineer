-- Monthly averages of price and renewable share.
-- Answers: which months are expensive, and when do negative prices cluster?
select
    date_trunc('month', timestamp_local)::date as month,
    count(*) as hours,
    round(avg(price_eur_mwh), 2) as avg_price_eur_mwh,
    round(avg(renewable_share), 4) as avg_renewable_share,
    count(*) filter (where is_negative_price) as negative_price_hours
from {{ ref('gold_hourly_market') }}
group by month
