with
    raw_data as (
        select *
        from {{ source('northwind', 'CATEGORY') }}
    )

    , renomeacao as (
        select 
           cast(ID as int) as pk_categoria
           , cast(CATEGORYNAME as varchar) as ds_nome_categoria
           , cast(DESCRIPTION as varchar) as ds_descricao_categoria
        from raw_data
    )

select * 
from renomeacao