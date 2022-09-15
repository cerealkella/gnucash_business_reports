SELECT
    accounts.name,
    transactions.post_date,
    sum(
        cast(splits.value_num AS DOUBLE PRECISION) / splits.value_denom
    ) AS amt
FROM
    transactions
    JOIN splits ON transactions.guid = splits.tx_guid
    JOIN accounts ON splits.account_guid = accounts.guid
WHERE
    accounts.code = '510'
GROUP BY
    name,
    post_date