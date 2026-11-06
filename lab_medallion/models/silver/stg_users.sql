with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
),

first_orders as (
    select
        user_id,
        min(cast(ordered_at as date)) as first_order_date
    from {{ ref('stg_orders') }}
    group by user_id
)

select
    u.user_id,
    u.username,
    lower(trim(u.username)) as username_normalized,
    u.name,
    u.sex,
    u.email,

    case
        when u.birthdate is null then null
        when o.first_order_date is null then u.birthdate
        when u.birthdate <= o.first_order_date then u.birthdate
        else null
    end as birthdate,

    case
        when u.birthdate is null then false
        when o.first_order_date is null then true
        else u.birthdate <= o.first_order_date
    end as birthdate_is_valid,

    u.street,
    nullif(
        regexp_extract(
            u.address_line_2,
            '^(.+), [A-Z]{2} \d{5}$', 1
        ), ''
    ) as city,
    regexp_extract(
        u.address_line_2,
        '([A-Z]{2}) (\d{5})$', 1
    ) as state,
    regexp_extract(
        u.address_line_2,
        '([A-Z]{2}) (\d{5})$', 2
    ) as zip_code

from typed u
left join first_orders o
    on u.user_id = o.user_id
