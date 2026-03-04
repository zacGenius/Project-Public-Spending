SELECT
    g.orgao_superior,
    COALESCE(d.nome_padrao, g.orgao) AS orgao, 
    g.pago,
    g.ano
FROM {{ ref('stg_gastos') }} g
LEFT JOIN {{ ref('de_para_orgaos') }} d 
    ON g.orgao = d.nome_variante
    
