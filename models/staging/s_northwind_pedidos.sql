with 
    raw_data as (
        select *
        from {{ source('northwind', 'orders') }}
    )

    , pedidos as (
        select
            cast(ID as int) as pk_pedido
            , cast(CUSTOMERID as varchar) as fk_cliente
            , cast(EMPLOYEEID as int) as fk_funcionario
            , cast(SHIPVIA as varchar) as fk_transportadora
            , cast(ORDERDATE as varchar) as data_pedido
            , cast(REQUIREDDATE as varchar) as data_requerida_entrega
            , cast(SHIPPEDDATE as varchar) as data_envio
            , cast(FREIGHT as varchar) as frete
            , cast(SHIPNAME as varchar) as nome_destinatario
            , cast(SHIPADDRESS as varchar) as endereco_destinatario
            , cast(SHIPCITY as varchar) as cidade_destinatario
            , cast(SHIPREGION as varchar) as regiao_destinatario
            , cast(SHIPCOUNTRY as varchar) as pais_destinatario
        from raw_data
    )

select * 
from pedidos