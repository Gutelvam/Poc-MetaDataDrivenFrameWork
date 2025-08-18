# Metadata-Driven Pipeline Framework

Um framework completo para Apache Airflow que permite criar pipelines de dados complexos através de configurações YAML, sem necessidade de programação.

## 🚀 Funcionalidades

- **Configuração por Metadata**: Defina pipelines inteiros via YAML
- **Múltiplas Fontes de Dados**: PostgreSQL, MongoDB, ClickHouse, APIs REST, SFTP, Azure Data Lake
- **Transformações Flexíveis**: SQL, Python, scripts customizados
- **Qualidade de Dados**: Validações automáticas com regras configuráveis
- **Monitoramento Completo**: Integração com Prometheus e Grafana
- **CLI Integrado**: Ferramentas de linha de comando para gestão
- **Auto-descoberta**: DAGs criados automaticamente a partir dos arquivos YAML

## 📋 Pré-requisitos

- Docker e Docker Compose
- Python 3.8+ (para CLI)
- Mínimo 4GB RAM
- 10GB espaço em disco

## 🛠️ Instalação Rápida

### 1. Clone o repositório
```bash
git clone <seu-repositorio>
cd metadata-driven-framework
```

### 2. Execute o setup automatizado
```bash
chmod +x setup_environment.sh
./setup_environment.sh
```

### 3. Acesse as interfaces
- **Airflow**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

## 📁 Estrutura do Projeto

```
📦 metadata-driven-framework/
├── 📂 dags/                     # Código do framework
│   ├── 📂 core/                 # Configurações base
│   ├── 📂 sources/              # Operadores de extração
│   ├── 📂 sinks/                # Operadores de carregamento
│   ├── 📂 transforms/           # Operadores de transformação
│   ├── 📂 quality/              # Operadores de qualidade
│   ├── 📂 monitoring/           # Sistema de monitoramento
│   └── 📄 main.py               # Ponto de entrada principal
├── 📂 metadata/                 # Configurações de pipelines
│   ├── 📄 exemplo_completo.yaml # Pipeline de exemplo
│   └── 📄 seus_pipelines.yaml   # Seus pipelines aqui
├── 📂 config/                   # Configurações do framework
├── 📂 monitoring/               # Dashboards e alertas
├── 📂 scripts/                  # Scripts de inicialização
├── 📄 docker-compose.yml        # Orquestração de serviços
├── 📄 framework-cli.py          # Interface de linha de comando
└── 📄 README.md                 # Esta documentação
```

## 🔧 Criando seu Primeiro Pipeline

### 1. Use o CLI para criar um template
```bash
./framework-cli.py create basic_etl meu_pipeline
```

### 2. Edite o arquivo gerado
```yaml
# metadata/meu_pipeline.yaml
dag_id: meu_pipeline
description: "Meu primeiro pipeline"
pipeline_type: batch
schedule_interval: "0 9 * * *"
start_date: "2024-01-01"

tasks:
  - task_id: extrair_dados
    operator_type: extract
    source:
      name: minha_fonte
      type: postgresql
      connection_id: postgres_dev
      query: "SELECT * FROM minha_tabela WHERE data = '{{ ds }}'"
  
  - task_id: verificar_qualidade
    operator_type: quality_check
    depends_on: [extrair_dados]
    quality_rules:
      - name: dados_nao_vazios
        rule_type: not_null
        column: id
        threshold: 1.0
        severity: critical
  
  - task_id: carregar_dados
    operator_type: load
    depends_on: [verificar_qualidade]
    sink:
      name: destino
      type: postgresql
      connection_id: postgres_dev
      table_name: tabela_destino
      write_mode: append
```

### 3. Valide a configuração
```bash
./framework-cli.py validate meu_pipeline.yaml
```

### 4. O pipeline aparecerá automaticamente no Airflow!

## 🎯 Tipos de Operadores

### Extração (extract)
```yaml
source:
  name: nome_fonte
  type: postgresql|mongodb|clickhouse|rest_api|sftp|file
  connection_id: conexao_airflow
  # Configurações específicas por tipo
```

### Transformação (transform)
```yaml
# SQL Transform
sql_transform: |
  SELECT 
    id,
    UPPER(nome) as nome_upper,
    valor * 1.1 as valor_ajustado
  FROM dados_origem

# Python Transform
python_transform: meu_modulo.minha_funcao

# Script customizado
custom_script: /path/to/script.py
```

### Qualidade (quality_check)
```yaml
quality_rules:
  - name: email_valido
    rule_type: pattern
    column: email
    parameters:
      pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    threshold: 0.95
    severity: warning
```

### Carregamento (load)
```yaml
sink:
  name: destino
  type: postgresql|mongodb|clickhouse|datalake_gen2
  connection_id: conexao_destino
  write_mode: append|overwrite|upsert
  auto_create_table: true
```

## 📊 Monitoramento

### Grafana Dashboards
O framework inclui dashboards pré-configurados:
- **Pipeline Overview**: Visão geral de todos os pipelines
- **Data Quality**: Métricas de qualidade dos dados
- **Performance**: Tempos de execução e recursos
- **Alerts**: Alertas e notificações

### Métricas Automáticas
- Duração de execução de pipelines
- Taxa de sucesso/falha
- Scores de qualidade de dados
- Volume de dados processados
- Utilização de recursos

## 🔍 Comandos CLI

```bash
# Verificar status do framework
./framework-cli.py status

# Criar novo pipeline
./framework-cli.py create basic_etl nome_pipeline

# Validar pipeline
./framework-cli.py validate pipeline.yaml

# Listar pipelines
ls metadata/*.yaml
```

## 🔧 Configurações Avançadas

### Conexões do Airflow
Configure suas conexões de dados no Airflow UI ou via CLI:

```bash
# PostgreSQL
docker-compose exec airflow-webserver airflow connections add \
  'postgres_prod' \
  --conn-type 'postgres' \
  --conn-host 'seu_host' \
  --conn-port '5432' \
  --conn-login 'usuario' \
  --conn-password 'senha' \
  --conn-schema 'database'
```

### Variáveis de Ambiente
Ajuste as configurações no arquivo `.env`:
```bash
# Configurações do Airflow
AIRFLOW_UID=50000
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# URLs de monitoramento
PROMETHEUS_PUSHGATEWAY_URL=http://prometheus:9090
GRAFANA_API_URL=http://grafana:3000
```

## 🚨 Solução de Problemas

### Pipeline não aparece no Airflow
1. Verifique se o arquivo YAML está em `metadata/`
2. Valide a sintaxe: `./framework-cli.py validate seu_pipeline.yaml`
3. Verifique os logs: `docker-compose logs -f airflow-scheduler`

### Erro de conexão
1. Verifique se a conexão está configurada no Airflow
2. Teste a conectividade: `docker-compose exec airflow-webserver airflow connections test conexao_id`

### Performance lenta
1. Ajuste `batch_size` nas configurações de source/sink
2. Configure pools de conexão
3. Use chunks menores para datasets grandes

## 📚 Exemplos Avançados

### Pipeline com Múltiplas Fontes
```yaml
tasks:
  - task_id: extrair_vendas
    operator_type: extract
    source:
      type: postgresql
      query: "SELECT * FROM vendas WHERE data = '{{ ds }}'"
  
  - task_id: extrair_clientes
    operator_type: extract
    source:
      type: mongodb
      collection_name: clientes
      filter_condition: {"ativo": true}
  
  - task_id: juntar_dados
    operator_type: transform
    depends_on: [extrair_vendas, extrair_clientes]
    sql_transform: |
      SELECT 
        v.*,
        c.nome as cliente_nome
      FROM extrair_vendas v
      LEFT JOIN extrair_clientes c ON v.cliente_id = c._id
```

### Pipeline com Validação Complexa
```yaml
quality_rules:
  - name: vendas_positivas
    rule_type: range
    column: valor_venda
    parameters:
      min_value: 0.01
      max_value: 1000000
    threshold: 0.99
    severity: critical
  
  - name: dados_recentes
    rule_type: freshness
    column: timestamp
    parameters:
      max_age_hours: 24
    threshold: 1.0
    severity: error
```

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.

## 🆘 Suporte

- 📧 Email: suporte@framework.com
- 💬 Discord: [Link do servidor]
- 📖 Wiki: [Link da documentação completa]
- 🐛 Issues: [Link para issues do GitHub]

---

⭐ **Se este framework foi útil para você, deixe uma estrela no repositório!**