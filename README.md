# Public Spend Analysis

E aí! Eu sou o Isaac Camilo, mas pode me chamar de Zac (GitHub: [zacgenius](https://github.com/zacgenius), LinkedIn: [isaaccami](https://www.linkedin.com/in/isaaccami/)).

Este repo é minha *caixinha de ferramentas* para brincar com dados de gastos públicos do Brasil. Se você está curioso em ver como montar um pipeline completo — da captura do CSV até um dashboard interativo passando por Postgres, dbt e Airflow — tá no lugar certo.

O fluxo básico foi pensado assim:

1. **Ingestão**: baixar os arquivos de gastos e salvar no disco (`data/bronze`).
2. **Transformação**: validar, padronizar e empurrar os dados pra um Postgres (resultado em `data/silver`).
3. **dbt**: modelar tabelas dimensionais/fato, rodar testes e gerar artefatos prontos pra análise.
4. **Visualização**: abrir um app Streamlit com dois painéis simples (gastos por órgão e evolução anual).
5. **Orquestração opcional**: botar tudo isso pra rodar com Airflow diariamente.

Se você só quer entender os conceitos, dá pra olhar os scripts e ler os comentários — tudo tá bem comentado.

---

## 📁 Estrutura da pasta (vale ler com atenção)

```
public_spend/
├── analytics/          # projeto dbt (modelos, macros, seeds, etc.)
│   └── dbt/            # é aqui que você roda `dbt run`/`dbt test`
├── app/                 # código do Streamlit
│   ├── pages/           # cada página do dashboard
│   └── app.py           # ponto de entrada (bastam 2 linhas)
├── data/                # onde os CSVs e os resultados vivem
│   ├── bronze/          # arquivos brutos baixados
│   ├── silver/          # saída dos scripts de transformação
│   └── gold/            # (não usado ainda, destino final imaginado)
├── pipelines/           # scripts Python de ingestão e transformação
│   ├── ingestion/       # pega os arquivos e joga em `data/bronze`
│   └── transformation/  # faz o ETL e carrega o Postgres
├── orchestration/       # DAGs do Airflow (`gastos_pipeline.py`)
├── tests/               # `pytest` para checar tudo
└── docker/              # Dockerfiles e config pra ser reproduzível
    └── postgres/        # script `init.sql` monta o schema vazio
```

Coisa importante: os caminhos `data/bronze` e `data/silver` são hard‑coded nos scripts, então se quiser mudar, é só abrir `pipelines/config.py`.

---

## Como rodar (passo a passo que funciona comigo)

### 1. pega o código

```bash
git clone https://github.com/zacgenius/public_spend.git
cd public_spend
```

### 2. prepara o Python

Use qualquer ambiente virtual. Minha sugestão:

```bash
python -m venv venv        # ou conda, pyenv, etc.
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt  # faz instalar tudo que o pipeline exige
```

Se só for olhar o app do Streamlit, tem outro venv (`venv_streamlit`) com as libs específicas. Ele já está no repo — dá `source venv_streamlit/bin/activate`.

> **Dica:** se pip reclamar de versão incompatível, apague a pasta do venv e refaça. As dependências foram testadas com Python 3.12.

### 3. banco de dados (Postgres)

O jeito mais rápido é com Docker:

```bash
docker-compose up -d --build
```

Isso sobe Postgres + Airflow (webserver, scheduler e worker). O Postgres expõe a porta 5432 e já vem com o schema `public` criado — só olhar `docker/postgres/init.sql` se quiser mudar.

Se você não curte Docker, instale Postgres local e ajuste a string de conexão em `pipelines/config.py` e em `analytics/dbt/profiles.yml` (procura por `HOST`, `USER`, `PASSWORD`).

### 4. puxar e transformar os dados manualmente

```bash
# 1. baixa CSV para data/bronze
python -m pipelines.ingestion.ingestion

# 2. transforma e carrega no Postgres
python -m pipelines.transformation.transformation
```

Se o processo quebrar, olha os logs no terminal — eles mostram o arquivo que falhou e qual coluna está fora do esquema. Ajusta o esquema em `pipelines/transformation/schemas.py` e roda de novo.

### 5. roda o dbt

```bash
cd analytics/dbt
# só precisa fazer tudo isso da primeira vez
dbt deps        # baixa dependências (há um pacote customizado de macros)
dbt seed        # insere o CSV de mapeamento de órgãos
dbt run         # cria/atualiza as tabelas
dbt test        # valida o modelo (nenhum valor nulo, FK válida etc.)
```

Resultados e arquivos de diagnóstico ficam em `analytics/dbt/target/`.

### 6. abre o dashboard

```bash
cd app
streamlit run app.py
```

Vai abrir no `localhost:8501`. Tem duas páginas:

- **Gastos por órgão**: escolhe ano e vê total gasto por cada agência.
- **Evolução temporal**: gráfico de série histórica somando todos os anos.

O app lê direto do Postgres, então tem que rodar os passos anteriores antes.

### 7. deixar automático com Airflow (opcional)

Se você deixou o Docker rodando, o Airflow já está em `http://localhost:8080` (usuário/senha `airflow/airflow`).

O DAG chama os mesmos scripts Python em sequência: ingestão → transformação → dbt. Você pode disparar à mão ou ajustar o `schedule_interval` no arquivo `orchestration/dags/gastos_pipeline.py`.

> O DAG também tem hooks para enviar e‑mail em caso de falha (precisa configurar variáveis de ambiente `SMTP_*`).

---

## Testes

Não deixe de rodar os testes se você fizer alterações no código. Eles dão uma cobertura básica nos módulos de ingestão/transformação.

```bash
pytest tests/ -q
```

Eles usam `pytest-mock` para simular downloads e conexões com o banco. Se algo quebrar, leia o traceback e dê uma olhada nos fixtures dentro de `tests/conftest.py`.

---

## Desenvolvendo

- Quer adicionar outra fonte de dados? crie um novo módulo dentro de `pipelines/ingestion` e adicione uma entrada no `config.py`.
- Para mudar as tabelas dbt crie modelos em `analytics/dbt/models/` seguindo o padrão dos sub‑diretórios (`staging` → `intermediate` → `marts`).
- O app Streamlit é quase um protótipo; basta criar novas páginas em `app/pages/` e usar a conexão Postgres disponibilizada em `app/db.py`.
- Alterou libs? atualiza `requirements.txt` e reaplica o `pip install -r`.

Faz pull request pra mim que eu agradeço.

---

## 📬 Contato

- GitHub: [zacgenius](https://github.com/zacgenius)
- LinkedIn: [isaaccami](https://www.linkedin.com/in/isaaccami/)