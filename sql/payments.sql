/*
 ~ 2024 = initial creation of SQL
   The following SQL will pull all payment splits in the database.
   This is not terribly useful by itself and is intended to be joined
   with an invoice dataframe to capture whether or not an invoice was paid

   The key to joining the tables is the post_lot field in the invoices table
 */

select payments.payment_amt,
	   invoices.id as invoice_id
 from invoices
join 
(select splits.value_num / cast(splits.value_denom AS DOUBLE PRECISION) as payment_amt,
    splits.lot_guid
from splits
where lot_guid is not null
    and action in ( 'Payment', 'Lot Link')) as payments on lot_guid = invoices.post_lot
