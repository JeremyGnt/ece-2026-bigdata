select
    o.order_id,
    o.ordered_at,
    u.user_id,
    u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u
    on o.user_id = u.user_id
where cast(o.ordered_at as date) < u.birthdate
