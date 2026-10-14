-- Price distribution per 10% renewable-share bucket.
-- Answers: how strongly do wind and solar push the price down?
select
    least(floor(renewable_share * 10) / 10, 1.0) as renewable_share_bucket,
    count(*) as hours,
    round(avg(price_eur_mwh), 2) as avg_price_eur_mwh,
    round(median(price_eur_mwh), 2) as median_price_eur_mwh,
    count(*) filter (where is_negative_price) as negative_price_hours
from {{ ref('gold_hourly_market') }}
group by renewable_share_bucket
