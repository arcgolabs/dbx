select id, username, email_address
from users
where status = /* status */1
order by id
limit /* Page.Limit */20 offset /* Page.Offset */0
