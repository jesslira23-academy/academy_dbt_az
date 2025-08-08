with
    int_produto as (
        select *
        from {{ ref('int_produtos') }}
    )

select *
from int_produto