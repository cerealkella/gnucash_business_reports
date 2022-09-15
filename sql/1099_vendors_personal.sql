SELECT
    accounts.name,
    transactions.description,
    transactions.post_date,
    cast(splits.value_num AS DOUBLE PRECISION) / splits.value_denom AS amt
FROM
    transactions
    JOIN splits ON transactions.guid = splits.tx_guid
    JOIN accounts ON splits.account_guid = accounts.guid
WHERE
    accounts.name LIKE '%Interest%'
    AND accounts.account_type = 'EXPENSE'
