/* 
 2019-01-12
 Primary Transaction query for GNUCash
 This should be universal for use with
 pandas / python / etc.
 
 The query takes an account guid argument
 which allows it to use it to filter all
 transactions coming in and going out of
 a given account (e.g. Checking, AP, AR)
 
 Modified:
 2019-01-19
 Changed a.guid to account_guid
 Changed post_txn to tx_guid
 Added src_guid
 Sorted
 Added addtl columns
 
 To make this work in a SQL Editor, replace 
 characters in brackets w/valid account guid.
 In my case, use:
 '12eaddcc39718c4582d8b69f22263126'
 
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
	/* source accounts */
	src.src_guid,
	src.src_code,
	src.src_type,
	src.src_name,
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
	CAST(s.value_num AS DOUBLE PRECISION) / CAST(s.value_denom AS DOUBLE PRECISION) {2} AS amt,
	CAST(s.quantity_num AS DOUBLE PRECISION) / CAST(s.quantity_denom AS DOUBLE PRECISION) {2} AS qty,
	s.reconcile_date,
	s.reconcile_state,
	s.memo AS memo
FROM
	accounts AS a
	JOIN splits AS s ON s.account_guid = a.guid
	JOIN transactions AS t ON t.guid = s.tx_guid
	JOIN
	/*Get Split Source Account*/
	(
		SELECT
			DISTINCT tx_guid AS tx,
			accounts.guid AS src_guid,
			accounts.code AS src_code,
			accounts.account_type as src_type,
			accounts.name AS src_name
		FROM
			splits
			JOIN accounts ON accounts.guid = splits.account_guid
		{0}
	) AS src ON src.tx = t.guid
/* WHERE CLAUSE injected dynamically */
{1}
