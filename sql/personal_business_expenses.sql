SELECT
    accounts.name AS Acct,
    transactions.post_date,
    transactions.description AS Description,
    splits.memo AS Memo,
    cast (splits.value_num AS DOUBLE PRECISION) / splits.value_denom AS Amt,
    cast (accounts.code AS double precision) AS Deduct_Percentage
FROM
    transactions
    JOIN splits ON transactions.guid = splits.tx_guid
    JOIN accounts ON splits.account_guid = accounts.guid
    JOIN slots ON accounts.guid = slots.obj_guid
    AND slots.name = 'tax-related'
    AND slots.slot_type = 1
WHERE
    accounts.account_type = 'EXPENSE'
ORDER BY
    Acct,
    Post_Date