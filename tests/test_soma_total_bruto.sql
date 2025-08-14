/*
Este test garante que as vendas brutas de 2012 estejam corretas. 
O valor auditavel = R$ 230,784.68
*/

{{ config(severity = 'warn') }}

with
    vendas2012 as (
        select sum(total_bruto) as soma_total_bruto
        from {{ ref('int_pedidos') }}
        where data_pedido between '2012-01-01' and '2012-12-31' 
    )

select soma_total_bruto
from vendas2012
--where soma_total_bruto != 230784.68
where soma_total_bruto not between 230784 and 230785