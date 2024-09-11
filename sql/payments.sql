/*
 ~ 2024 = initial creation of SQL
   The following SQL will pull all payment splits in the database.
   This is not terribly useful by itself and is intended to be joined
   with an invoice dataframe to capture whether or not an invoice was paid

   The key to joining the tables is the lot_guid field in the invoices table
 */

select splits.value_num / cast(splits.value_denom AS DOUBLE PRECISION) as payment_amt,
    splits.lot_guid
from splits
where lot_guid is not null
    and action = 'Payment'