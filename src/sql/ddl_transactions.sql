CREATE TABLE IF NOT EXISTS STV2024111143__STAGING.transactions (
    operation_id VARCHAR(60),
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(30),
    status VARCHAR(30),
    transaction_type VARCHAR(30),
    amount INT,
    transaction_dt TIMESTAMP
);

CREATE PROJECTION IF NOT EXISTS STV2024111143__STAGING.transactions_by_date AS
SELECT * FROM STV2024111143__STAGING.transactions
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES KSAFE 1;