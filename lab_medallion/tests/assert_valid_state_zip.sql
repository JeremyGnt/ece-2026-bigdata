{{ config(severity='warn') }}

select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
left join {{ ref('zip_state_reference') }} z
    on u.zip_code = z.zip_code
    and u.state = z.state
where u.state not in ('AA', 'AE', 'AP')
  and (
      u.zip_code is null
      or not regexp_full_match(u.zip_code, '[0-9]{5}')
      or z.zip_code is null
  )
