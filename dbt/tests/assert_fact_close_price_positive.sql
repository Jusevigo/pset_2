SELECT
    DATE_KEY,
    SYMBOL_KEY,
    CLOSE_PRICE

FROM {{ ref('fact_aapl_daily') }}

WHERE CLOSE_PRICE <= 0