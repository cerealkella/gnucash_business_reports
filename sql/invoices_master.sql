select
	/* invoices */
	'BILL' as inv_type,
	invoices.guid as inv_guid,
	invoices.id as inv_id,
	invoices.date_posted,
	invoices.date_opened,
	invoices.billing_id,
	invoices.notes,
	invoices.post_txn,
	/* billto/owner */
	billto.id as operation_id,
	billto.name as operation,
	owner_job.vendor_name as org_name,
	owner_job.vendor_notes as org_notes,
	/* accounts */
	accounts.guid as account_guid,
	accounts.code as account_code,
	accounts."name" as account_name,
	/* entries */
	entries.guid as entry_guid,
	entries.b_acct as inv_acct,
	entries.description,
	(entries.quantity_num / cast(entries.quantity_denom as DOUBLE PRECISION)) as quantity,
	(entries.b_price_num / cast(entries.b_price_denom as DOUBLE PRECISION)) as price,
	(entries.quantity_num / cast(entries.quantity_denom as DOUBLE PRECISION)) * (entries.b_price_num / cast(entries.b_price_denom as DOUBLE PRECISION)) as amt,
	entries.action as quantity_type,
	entries.b_paytype as paytype,
	'' disc_how,
	'' disc_type,
	0.0 as disc_amt,
	entries.b_taxable as taxable,
	entries.b_taxincluded as taxincluded,
	entries.b_taxtable as taxtable,
	entries.action as entry_type,
	entries."date" as entry_date
from
	entries
join accounts on
	accounts.guid = entries.b_acct
join invoices on
	invoices.guid = entries.bill
join (
	select
		jobs.id,
		name,
		guid
	from
		jobs) as billto on
	billto.guid = invoices.billto_guid
join (
	select
		jobs.id,
		jobs.name,
		vendors.name as vendor_name,
		vendors.notes as vendor_notes,
		jobs.guid
	from
		jobs
	join vendors on
		vendors.guid = jobs.owner_guid ) as owner_job on
	owner_job.guid = invoices.owner_guid
union select
	/* invoices */
	'INVOICE' as inv_type,
	invoices.guid as inv_guid,
	invoices.id as inv_id,
	invoices.date_posted,
	invoices.date_opened,
	invoices.billing_id,
	invoices.notes,
	invoices.post_txn,
	/* billto/owner */
	owner_job.id as operation_id,
	owner_job.name as operation,
	owner_job.customer_name as org_name,
	owner_job.customer_notes as org_notes,
	/* accounts */
	accounts.guid as account_guid,
	accounts.code as account_code,
	accounts."name" as account_name,
	/* entries */
	entries.guid as entry_guid,
	entries.i_acct as inv_acct,
	entries.description,
	(entries.quantity_num / cast(entries.quantity_denom AS DOUBLE PRECISION)) as quantity,
	(entries.i_price_num / cast(entries.i_price_denom AS DOUBLE PRECISION)) as price,
	(entries.quantity_num / cast(entries.quantity_denom AS DOUBLE PRECISION)) * (entries.i_price_num / cast(entries.i_price_denom AS DOUBLE PRECISION)) as amt,
	entries.action as quantity_type,
	0 as paytype,
	entries.i_disc_how disc_how,
	entries.i_disc_type disc_type,
	(entries.i_discount_num / cast(entries.i_discount_denom AS DOUBLE PRECISION)) as disc_amt,
	entries.i_taxable as taxable,
	entries.i_taxincluded as taxincluded,
	entries.i_taxtable as taxtable,
	entries.action as entry_type,
	entries."date" as entry_date
from
	entries
join accounts on
	accounts.guid = entries.i_acct
join invoices on
	invoices.guid = entries.invoice
join (
	select
		jobs.id,
		jobs.name,
		customers.name as customer_name,
		customers.notes as customer_notes,
		jobs.guid
	from
		jobs
	join customers on
		customers.guid = jobs.owner_guid ) as owner_job on
	owner_job.guid = invoices.owner_guid
