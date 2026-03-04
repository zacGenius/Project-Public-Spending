-- Falha se existir algum valor negativo ou zero na tabela final

SELECT *
FROM {{ ref('fato_gastos_por_orgao_ano') }}
WHERE total_gasto <= 0