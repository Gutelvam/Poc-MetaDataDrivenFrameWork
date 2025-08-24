#!/bin/bash

echo "🎯 Configurando Variáveis e Conexões do Airflow"
echo "======================================================="

echo ""
echo "ℹ️  Estes são armazenados na base de dados do Airflow, não como variáveis de ambiente"
echo ""

echo "🔧 Configurando variáveis essenciais do Airflow..."

# Define as 3 variáveis essenciais que estavam causando erros
docker compose exec airflow-apiserver airflow variables set \
    prometheus_pushgateway_url "http://prometheus:9090"

docker compose exec airflow-apiserver airflow variables set \
    grafana_api_url "http://grafana:3000"

docker compose exec airflow-apiserver airflow variables set \
    grafana_api_key "dummy_key_for_testing"

echo ""
echo "✅ Variáveis configuradas com sucesso!"

echo ""
echo "🔧 Configurando a conexão do PostgreSQL 'postgres_dev'..."
# Use o nome do serviço do Docker como host e a porta interna do contêiner
# Substitua 'YOUR_USER', 'YOUR_PASSWORD' e 'YOUR_DB_NAME' pelas suas credenciais reais
docker compose exec airflow-apiserver airflow connections add \
    --conn-json '{
        "conn_id": "postgres_dev",
        "conn_type": "postgres",
        "host": "postgres-dev-1",
        "port": 5432,
        "login": "postgres",
        "password": "postgres",
        "extra": {
            "database": "test_database"
        }
    }'

echo ""
echo "✅ Conexão 'postgres_dev' configurada com sucesso!"

echo ""
echo "📋 Verificando se variáveis e conexões estão armazenadas no Airflow:"
docker compose exec airflow-webserver airflow variables list
docker compose exec airflow-webserver airflow connections list

echo ""
echo "🎉 Concluído! O script agora configura as variáveis e a conexão necessárias."
echo ""
echo "💡 Alternativa: Você também pode configurar estas informações via a UI Web:"
echo "   1. Vá para http://localhost:8080"
echo "   2. Faça login com admin/admin"
echo "   3. Para variáveis, navegue para Admin → Variáveis"
echo "   4. Para conexões, navegue para Admin → Conexões"
echo ""
