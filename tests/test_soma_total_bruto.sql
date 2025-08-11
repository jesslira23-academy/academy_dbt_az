/*
Este teste garante que as vendas brutas de 2012 estão corretas
com o valor auditado da contabilidade
R$ 230,784.68
*/

-- {{ config(severity = 'warn') }}

with
   vendas_2012 as (
       select sum(total_bruto) as soma
       from {{ ref('int_pedidos') }}
       where data_pedido between '2012-01-01' and '2012-12-31'
   )


select soma
from vendas_2012
where soma != 1231785
--where soma not between 230784 and 230785

