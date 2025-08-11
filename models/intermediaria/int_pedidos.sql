with
    pedido as (
        select *
        from {{ ref('s_northwind_pedidos') }}
    )

    , pedido_item as (
        select *
        from {{ ref('s_northwind_pedidos_item') }}
    )

    , seed_pais as (
        select 
            SHIPCOUNTRY as pais_destinatario
            , DIVISION as divisao_pais
        from {{ ref('seed_depara_pais_division') }}
    )

    , joined as (
        select
            pedido_item.pk_pedido_item
            , pedido_item.fk_pedido
            , pedido_item.fk_produto
            , pedido.fk_cliente
            , pedido.fk_funcionario
            , pedido.fk_transportadora
            , pedido.data_pedido
            , pedido.data_requerida_entrega
            , pedido.data_envio
            , pedido.frete
            , pedido.nome_destinatario
            , pedido.endereco_destinatario
            , pedido.cidade_destinatario
            , pedido.regiao_destinatario
            , pedido.pais_destinatario
            , seed_pais.divisao_pais
            , pedido_item.preco_unidade
            , pedido_item.quantidade
            , pedido_item.desconto
        from pedido_item
        left join pedido
            on pedido.pk_pedido = pedido_item.fk_pedido
        left join seed_pais
            on pedido.pais_destinatario = seed_pais.pais_destinatario
    )

    , metricas as (
    select
        *
        , preco_unidade * quantidade as total_bruto
        , preco_unidade * (1 - desconto) * quantidade as total_liquido
        , case
            when desconto > 0 then true
            else false
        end as teve_desconto
    from joined
    )

select *
from metricas


