# Pipeline ETL — Mortalidade Infantil por UF (IBGE → BigQuery)

Pipeline de dados público que extrai dados oficiais de mortalidade infantil da API SIDRA/IBGE, transforma com validações de qualidade e carrega no BigQuery com CI/CD automatizado via GitHub Actions.

## O que este projeto demonstra

- Extração de API REST pública com tratamento de erros e logging estruturado
- Transformação com validações de qualidade (tipagem, nulos, valores fora de domínio, enriquecimento)
- Carga idempotente no BigQuery (WRITE_TRUNCATE — re-run seguro)
- Testes unitários com pytest (extract + transform)
- CI/CD com GitHub Actions — testes automáticos em todo push e pipeline agendado mensalmente

## Stack

| Camada        | Tecnologia                          |
|---------------|-------------------------------------|
| Extração      | Python · requests · API SIDRA/IBGE  |
| Transformação | pandas · validações customizadas    |
| Carga         | google-cloud-bigquery               |
| Testes        | pytest                              |
| CI/CD         | GitHub Actions                      |
| Destino       | Google BigQuery                     |

## Fonte dos dados

**API SIDRA/IBGE — Tabela 265**
Taxa de mortalidade infantil (por 1.000 nascidos vivos) por Unidade da Federação.
Período: 2000–2022 | Cobertura: 27 UFs

Documentação: https://apisidra.ibge.gov.br/

## Estrutura do projeto

```
ibge-saude-pipeline/
├── src/
│   ├── extract/ibge_api.py       # Extração da API SIDRA
│   ├── transform/clean.py        # Limpeza, validação e enriquecimento
│   ├── load/bigquery_loader.py   # Carga no BigQuery
│   ├── utils/logger.py           # Logger padronizado
│   └── pipeline.py               # Orquestrador principal
├── tests/
│   ├── test_extract.py
│   └── test_transform.py
├── .github/workflows/pipeline.yml
├── .env.example
├── Makefile
└── requirements.txt
```

## Como executar localmente

### 1. Clone e instale as dependências

```bash
git clone https://github.com/rubenscsilva/ibge-saude-pipeline.git
cd ibge-saude-pipeline
pip install -r requirements.txt
```

### 2. Configure as credenciais GCP

```bash
cp .env.example .env
# Edite .env com seu GCP_PROJECT_ID
# Coloque sua credentials.json na raiz (não vai para o Git)
```

### 3. Execute os testes

```bash
make test
```

### 4. Rode o pipeline em modo dry-run (sem carga no BQ)

```bash
make dry-run
```

### 5. Rode o pipeline completo

```bash
make run
```

## CI/CD

O workflow em `.github/workflows/pipeline.yml`:
- Executa os testes a cada push na `main`
- Roda o pipeline completo mensalmente (cron) ou por dispatch manual
- Usa `GCP_CREDENTIALS_JSON` e `GCP_PROJECT_ID` como secrets do repositório

## Schema BigQuery

Tabela: `{project}.ibge_saude.mortalidade_infantil`

| Campo                      | Tipo    | Descrição                                  |
|----------------------------|---------|--------------------------------------------|
| uf_codigo                  | STRING  | Código IBGE da UF                          |
| uf_nome                    | STRING  | Nome da UF                                 |
| ano                        | INTEGER | Ano de referência                          |
| taxa_mortalidade_infantil  | FLOAT   | Taxa por 1.000 nascidos vivos              |
| regiao                     | STRING  | Região geográfica (enriquecimento)         |

---

Dados abertos | Fonte: IBGE/SIDRA | Autor: [Rubens Cristovão](https://linkedin.com/in/rubenscsilva)
