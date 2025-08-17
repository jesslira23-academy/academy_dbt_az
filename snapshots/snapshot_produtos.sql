{% snapshot snapshot_produtos %}

{{
    config(
        strategy='check'
        , unique_key='id'
        , check_cols=['id', 'PRODUCTNAME', 'UNITPRICE', 'UNITSINSTOCK']
    )
}}

select 
    ID
    , PRODUCTNAME
    , SUPPLIERID
    , CATEGORYID
    , QUANTITYPERUNIT
    , UNITPRICE
    , UNITSINSTOCK
    , UNITSONORDER
    , REORDERLEVEL
    , DISCONTINUED
from {{ source('schema_pessoal', 'raw_produtos') }}

{% endsnapshot %}