with
    produtos as (
       select *
       from {{ ref('s_northwind_produtos') }}

    )
    , categoria as (
       select *
       from {{ ref('s_northwind_categoria') }}
    )
    , juncao as (
       select
           produtos.pk_produto
           , ds_nome_produto
           , ds_quantidade_unidade
           , preco_por_unidade
           , unidade_em_estoque
           , unidades_por_pedido
           , nivel_de_pedido
           , eh_descontinuado
           , categoria.ds_nome_categoria
           , categoria.ds_descricao_categoria
       from produtos
       left join categoria on produtos.fk_categoria = categoria.pk_categoria
    )
select *
from juncao
