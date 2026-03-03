SELECT
    orgao_superior,
    orgao,
    valor_pago,
    data_referencia,
    ano_referencia
FROM {{ source('silver', 'gastos') }}
WHERE valor_pago > 0 -- Filtrar pagamentos validos
