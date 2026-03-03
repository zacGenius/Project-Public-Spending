SELECT
    g.orgao_superior,
    COALESCE(d.nome_padrao, g.orgao) AS orgao, 
    g.valor_pago,
    g.data_referencia,
    g.ano_referencia
FROM {{ ref('stg_gastos') }} g
LEFT JOIN {{ ref('de_para_orgaos') }} d 
    ON g.orgao = d.nome_variante