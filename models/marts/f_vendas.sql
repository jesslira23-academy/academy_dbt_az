with
    int_produto as (
        select *
        from {{ ref('int_pedidos') }}
    )

select *
from int_produto