CREATE TABLE IF NOT EXISTS STV2024111143__STAGING.currencies (
    date_update TIMESTAMP,
    currency_code INT,
    currency_code_with INT,
    currency_with_div NUMERIC(5, 3)
);

CREATE PROJECTION IF NOT EXISTS STV2024111143__STAGING.currencies_by_date AS
SELECT * FROM STV2024111143__STAGING.currencies
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES KSAFE 1;