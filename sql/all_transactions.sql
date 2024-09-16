/* 
 2024-09-16
 More generic query, not pulling source splits.
 
 */
/*pandas*
timezone = "America/Chicago"
[parse_dates]
post_date = {format = "%Y-%m-%d %H:%M:%S", errors = "coerce", exact = false}
enter_date = "%Y-%m-%d %H:%M:%S"
reconcile_date = "%Y-%m-%d %H:%M:%S"
[dtype]
account_type = "string"
description = "string"
account_code = "string"
*pandas*/

SELECT
	/* accounts */
	a.account_type,
	a.description AS account_desc,
	a.parent_guid,
	a.commodity_guid,
	a.name AS account_name,
	a.code AS account_code,
	a.guid AS account_guid,
	/* transactions */
	t.guid AS tx_guid,
	t.currency_guid,
	t.num AS tx_num,
	t.enter_date,
	t.description,
	t.post_date,
	/* splits */
	s."action" AS split_action,
	s.guid AS split_guid,
	CAST(s.value_num AS DOUBLE PRECISION) / CAST(s.value_denom AS DOUBLE PRECISION) AS amt,
	CAST(s.quantity_num AS DOUBLE PRECISION) / CAST(s.quantity_denom AS DOUBLE PRECISION) AS qty,
	s.reconcile_date,
	s.reconcile_state,
	s.memo AS memo
FROM
	accounts AS a
	JOIN splits AS s ON s.account_guid = a.guid
	JOIN transactions AS t ON t.guid = s.tx_guid
