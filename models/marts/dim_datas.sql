with
    seq_datas as (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2025-08-01' as date)",
            end_date="current_date()+1"
        )
        }}
    )

    , datas_detalhes as (
        select
            date_day as data 
            , extract(day from date_day) as dia  
            , extract(month from date_day) as mes  
            , extract(year from date_day) as ano  
            , extract(dow from date_day) + 1 as dia_da_semana           
            , case
                when extract(dow from date_day) in (0, 6) then 'Final de semana'
                else 'Dia útil'
            end as flag_final_de_semana
        from seq_datas
    )

select * 
from datas_detalhes
