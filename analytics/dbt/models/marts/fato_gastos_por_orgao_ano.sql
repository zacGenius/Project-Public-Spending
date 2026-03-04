SELECT
    orgao,
    orgao_superior,
    ano,
    SUM(pago) AS total_gastos,
    COUNT(*) AS qtd_pagamentos,
    AVG(pago) AS ticket_medio
    MIN(pago) AS menor_pagamento,
    MAX(pago) AS maior_pagamento
FROM {{ ref('int_gastos_normalizado') }}
GROUP BY 1, 2, 3