with 
    raw_data as (
        select *
        from {{ source('northwind', 'order_details') }}
    )

    , pedido_item as (
        select
            cast(ID as varchar) as pk_pedido_item
            , cast(ORDERID as varchar) as fk_pedido
            , cast(PRODUCTID as varchar) as fk_produto
            , cast(UNITPRICE as numeric(18,2)) as preco_unidade
            , cast(QUANTITY as int) as quantidade
            , cast(DISCOUNT as numeric(18,2)) as desconto
        from raw_data
    )

select *
from pedido_item