{{
    config(
        materialized='table'
    )
}}

with
    raw_data as (
        select *
        from {{ source('northwind', 'PRODUCTS') }}
    )

    , renomeacao as (
        select 
            cast(ID as int) as pk_produto
           , cast(SUPPLIERID as varchar) as fk_fornecedor
           , cast(CATEGORYID as varchar) as fk_categoria
           , cast(PRODUCTNAME as varchar) as ds_nome_produto
           , cast(QUANTITYPERUNIT as varchar) as ds_quantidade_unidade
           , cast(UNITPRICE as numeric(18,2)) as preco_por_unidade
           , cast(UNITSINSTOCK as int) as unidade_em_estoque
           , cast(UNITSONORDER as int) as unidades_por_pedido
           , cast(REORDERLEVEL as int) as nivel_de_pedido
           , cast(DISCONTINUED as varchar) as eh_descontinuado
        from raw_data
    )

select * 
from renomeacao