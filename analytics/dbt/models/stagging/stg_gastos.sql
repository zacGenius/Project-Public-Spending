SELECT
    orgao_superior,
    orgao,
    pago,
    ano
FROM {{ source('silver', 'gastos') }}
WHERE pago > 0 -- Filtrar pagamentos validos
-- Remove pagamentos zerados ou negativos