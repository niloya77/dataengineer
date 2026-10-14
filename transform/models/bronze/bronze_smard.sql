-- Raw SMARD rows exactly as ingested, one row per (hour, signal, file).
select
    timestamp_utc,
    signal,
    value,
    ingested_at,
    filename as source_file
from {{ source('raw', 'smard') }}
