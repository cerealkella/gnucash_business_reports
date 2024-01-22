select
    id as joplin_id,
    parent_id,
    title,
    body,
    source,
    source_application,
    DATETIME(ROUND(user_updated_time / 1000), 'unixepoch', 'localtime') as updated_date,
    DATETIME(ROUND(user_created_time / 1000), 'unixepoch', 'localtime') as created_date
from notes
