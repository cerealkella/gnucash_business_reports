/*
2024-01-24 - JRK
Swiss Army SQL Query for Joplin
*/
/*pandas*
timezone = "America/Chicago"
[parse_dates]
updated_date = {format = "%Y-%m-%d %H:%M:%S", errors = "coerce", exact = false}
created_date = {format = "%Y-%m-%d %H:%M:%S", errors = "coerce", exact = false}
[dtype]
joplin_id = "string"
parent_id = "string"
title = "string"
body = "string"
source = "string"
source_application = "string"
*pandas*/
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
