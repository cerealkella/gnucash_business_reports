select splits.value_num / cast(splits.value_denom AS DOUBLE PRECISION) as payment_amt,
    splits.lot_guid
from splits
where lot_guid is not null
    and action = 'Payment'