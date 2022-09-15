SELECT
    (
        cast(entries.quantity_num AS DOUBLE PRECISION) / entries.quantity_denom
    ) * (
        cast(entries.b_price_num AS DOUBLE PRECISION) / entries.b_price_denom
    ) AS amt,
    accounts.name AS acct_name,
    accounts.guid AS acct_guid,
    vendors.name AS vendor_name,
    vendors.id AS vendor_id,
    vendors.addr_addr1,
    vendors.addr_addr2,
    vendors.addr_addr3,
    vendors.addr_addr4,
    vendors.addr_phone AS phone,
    invoices.date_posted as post_date
FROM
    invoices
    JOIN jobs ON invoices.owner_guid = jobs.guid
    JOIN entries ON invoices.guid = entries.bill
    JOIN vendors ON vendors.guid = jobs.owner_guid
    JOIN accounts ON accounts.guid = entries.b_acct
WHERE
    vendors.notes LIKE '%1099%'
