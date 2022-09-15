SELECT
    transactions.post_date,
    accounts.name,
    accounts.code,
    splits.memo,
    cast(splits.value_num AS DOUBLE PRECISION) / splits.value_denom AS amt,
    employees.id AS emp_id,
    employees.addr_name AS emp_name,
    employees.addr_addr1 AS emp_addr1,
    employees.addr_addr2 AS emp_addr2
FROM
    transactions
    JOIN employees ON transactions.description LIKE employees.addr_name
    JOIN splits ON transactions.guid = splits.tx_guid
    JOIN accounts ON splits.account_guid = accounts.guid
WHERE
    accounts.code LIKE '54%'
