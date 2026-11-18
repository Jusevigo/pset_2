
SELECT
    d.DATE_KEY,
    s.SYMBOL_KEY,
    silver.CLOSE AS CLOSE_PRICE,
    silver.SOURCE,
    silver.INGESTED_AT

FROM {{ ref('silver_aapl_daily') }} AS silver

INNER JOIN {{ ref('dim_date') }} AS d
    ON silver.DATE = d.FULL_DATE

INNER JOIN {{ ref('dim_symbol') }} AS s
    ON silver.SYMBOL = s.SYMBOL
