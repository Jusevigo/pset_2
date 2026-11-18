
WITH symbols AS (

    SELECT DISTINCT
        UPPER(TRIM(SYMBOL)) AS SYMBOL

    FROM {{ ref('silver_aapl_daily') }}

    WHERE SYMBOL IS NOT NULL
        AND TRIM(SYMBOL) <> ''

)

SELECT
    ROW_NUMBER() OVER (ORDER BY SYMBOL) AS SYMBOL_KEY,
    SYMBOL

FROM symbols
