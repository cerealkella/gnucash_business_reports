SELECT accounts.*,
    notes.string_val as account_notes
FROM accounts
    left join (
        select string_val,
            obj_guid
        from slots
        where name = 'notes'
            and slot_type = 4
    ) as notes on notes.obj_guid = accounts.guid