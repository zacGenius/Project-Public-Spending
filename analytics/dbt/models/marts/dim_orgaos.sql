SELECT
    {{ normalizar_orgao('orgao') }} AS orgao,
    orgao_superior,
    COUNT(DISTINCT ano) AS anos_com_dados,
    MIN(ano) AS primeiro_ano,
    MAX(ano) AS ultimo_ano,
    SUM(pago) AS total_historico
FROM {{ ref('int_gastos_normalizado') }}
GROUP BY 1, 2
ORDER BY total_historico DESC