SELECT
    DATE_KEY,
    SYMBOL_KEY,
    COUNT(*) AS REPETICIONES

FROM {{ ref('fact_aapl_daily') }}

GROUP BY
    DATE_KEY,
    SYMBOL_KEY

HAVING COUNT(*) > 1