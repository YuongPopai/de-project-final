CREATE TABLE IF NOT EXISTS STV2024111143__DWH.global_metrics (
    date_update DATE,
    currency_from INT,
    amount_total NUMERIC(18, 2),
    cnt_transactions BIGINT,
    avg_transactions_per_account NUMERIC(18, 2),
    cnt_accounts_make_transactions BIGINT
);