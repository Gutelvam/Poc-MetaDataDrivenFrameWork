#!/bin/bash

# =====================================================================
# Environment Setup Script for Metadata-Driven Pipeline Framework
# This script sets up the complete development environment
# =====================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Setting up Metadata-Driven Pipeline Framework Environment..."
echo "Working directory: $SCRIPT_DIR"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    print_status "Docker is installed"
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    print_status "Docker Compose is installed"
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_warning "Python 3 is not installed. CLI functionality will be limited."
    else
        print_status "Python 3 is installed"
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    print_status "Docker is running"
}

# Create necessary directories
create_directories() {
    print_info "Creating directory structure..."
    
    DIRECTORIES=(
        "logs"
        "plugins"
        "config"
        "scripts"
        "monitoring/prometheus"
        "monitoring/grafana/dashboards"
        "monitoring/grafana/provisioning/datasources"
        "monitoring/grafana/provisioning/dashboards"
        "tests"
        "docs"
    )
    
    for dir in "${DIRECTORIES[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            print_status "Created directory: $dir"
        fi
    done
}

# Set proper permissions
set_permissions() {
    print_info "Setting proper permissions..."
    
    # Make scripts executable
    find . -name "*.sh" -type f -exec chmod +x {} \;
    
    # Set Airflow UID if not set
    if [ -z "$AIRFLOW_UID" ]; then
        export AIRFLOW_UID=50000
        echo "AIRFLOW_UID=50000" >> .env
        print_status "Set AIRFLOW_UID to 50000"
    fi
    
    # Create logs directory with proper permissions
    if [ ! -d "logs" ]; then
        mkdir -p logs
    fi
    chmod 755 logs
    
    print_status "Permissions set correctly"
}

# Validate configuration files
validate_configs() {
    print_info "Validating configuration files..."
    
    # Check if required files exist
    REQUIRED_FILES=(
        "docker-compose.yml"
        "Dockerfile"
        ".env"
        "requirements.txt"
        "dags/main.py"
        "dags/core/config.py"
        "dags/metadata/manager.py"
        "framework-cli.py"
    )
    
    for file in "${REQUIRED_FILES[@]}"; do
        if [ ! -f "$file" ]; then
            print_error "Required file missing: $file"
            exit 1
        fi
    done
    
    print_status "All required files present"
    
    # Validate YAML files
    if command -v python3 &> /dev/null; then
        python3 -c "
import yaml
import sys
import os

yaml_files = []
for root, dirs, files in os.walk('metadata'):
    for file in files:
        if file.endswith(('.yaml', '.yml')):
            yaml_files.append(os.path.join(root, file))

for yaml_file in yaml_files:
    try:
        with open(yaml_file, 'r') as f:
            yaml.safe_load(f)
        print(f'✅ Valid YAML: {yaml_file}')
    except Exception as e:
        print(f'❌ Invalid YAML: {yaml_file} - {e}')
        sys.exit(1)
"
        print_status "YAML files validated"
    fi
}

# Initialize Docker environment
init_docker() {
    print_info "Initializing Docker environment..."
    
    # Pull required images
    print_info "Pulling Docker images..."
    docker-compose pull
    
    # Initialize Airflow
    print_info "Initializing Airflow database..."
    docker-compose up airflow-init
    
    print_status "Docker environment initialized"
}

# Start services
start_services() {
    print_info "Starting services..."
    
    # Start all services
    docker-compose up -d
    
    # Wait for services to be ready
    print_info "Waiting for services to be ready..."
    sleep 30
    
    # Check service health
    check_services_health
}

# Check services health
check_services_health() {
    print_info "Checking services health..."
    
    SERVICES=(
        "postgres:5432"
        "redis:6379"
        "airflow-webserver:8080"
        "prometheus:9090"
        "grafana:3000"
    )
    
    for service in "${SERVICES[@]}"; do
        service_name=$(echo $service | cut -d: -f1)
        port=$(echo $service | cut -d: -f2)
        
        if docker-compose ps | grep -q "$service_name.*Up"; then
            print_status "$service_name is running"
        else
            print_warning "$service_name might not be ready yet"
        fi
    done
}

# Setup Airflow connections
setup_airflow_connections() {
    print_info "Setting up Airflow connections..."
    
    # Wait for Airflow to be ready
    sleep 10
    
    # Create connections using Airflow CLI
    docker-compose exec -T airflow-webserver airflow connections add \
        'postgres_dev' \
        --conn-type 'postgres' \
        --conn-host 'postgres-dev' \
        --conn-port '5432' \
        --conn-login 'postgres' \
        --conn-password 'postgres' \
        --conn-schema 'test_database' || true
    
    print_status "Airflow connections configured"
}

# Create example data
create_example_data() {
    print_info "Creating example data..."
    
    # Generate additional sample data
    docker-compose exec -T postgres-dev psql -U postgres -d test_database -c "
        SELECT test_data.generate_daily_orders(CURRENT_DATE - INTERVAL '1 day');
        SELECT test_data.generate_daily_orders(CURRENT_DATE - INTERVAL '2 days');
        SELECT test_data.generate_daily_orders(CURRENT_DATE - INTERVAL '3 days');
    " || print_warning "Could not generate additional sample data"
    
    print_status "Example data created"
}

# Test framework
test_framework() {
    print_info "Testing framework..."
    
    # Test CLI
    if [ -f "framework-cli.py" ] && command -v python3 &> /dev/null; then
        python3 framework-cli.py --help > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            print_status "CLI is working"
        else
            print_warning "CLI might have issues"
        fi
    fi
    
    # Test pipeline validation
    if [ -f "metadata/pipeline_exemplo_completo.yaml" ]; then
        python3 framework-cli.py validate pipeline_exemplo_completo.yaml > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            print_status "Example pipeline is valid"
        else
            print_warning "Example pipeline validation failed"
        fi
    fi
}

# Display access information
show_access_info() {
    echo ""
    echo "🎉 Framework setup completed successfully!"
    echo ""
    echo "📋 Access Information:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🌐 Airflow Web UI:    http://localhost:8080"
    echo "   Username: admin"
    echo "   Password: admin"
    echo ""
    echo "📊 Grafana:           http://localhost:3000"
    echo "   Username: admin"
    echo "   Password: admin"
    echo ""
    echo "📈 Prometheus:        http://localhost:9090"
    echo ""
    echo "🗄️  PostgreSQL (Dev):  localhost:5433"
    echo "   Username: postgres"
    echo "   Password: postgres"
    echo "   Database: test_database"
    echo ""
    echo "🔧 Useful Commands:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "• Check status:       ./framework-cli.py status"
    echo "• Create pipeline:    ./framework-cli.py create basic_etl my_pipeline"
    echo "• Validate pipeline:  ./framework-cli.py validate my_pipeline.yaml"
    echo "• View logs:          docker-compose logs -f airflow-scheduler"
    echo "• Stop services:      docker-compose down"
    echo "• Restart services:   docker-compose restart"
    echo ""
    echo "📚 Next Steps:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "1. Visit Airflow UI and check if example DAGs are loaded"
    echo "2. Run the example pipeline: pipeline_exemplo_completo"
    echo "3. Check Grafana dashboards for monitoring"
    echo "4. Create your own pipeline using the CLI"
    echo ""
}

# Cleanup function
cleanup_on_error() {
    print_error "Setup failed. Cleaning up..."
    docker-compose down || true
}

# Set trap for cleanup on error
trap cleanup_on_error ERR

# Main execution
main() {
    echo "Starting framework setup process..."
    echo ""
    
    check_prerequisites
    create_directories
    set_permissions
    validate_configs
    init_docker
    start_services
    setup_airflow_connections
    create_example_data
    test_framework
    show_access_info
}

# Parse command line arguments
case "${1:-}" in
    "clean")
        print_info "Cleaning up environment..."
        docker-compose down -v
        docker system prune -f
        print_status "Environment cleaned"
        ;;
    "restart")
        print_info "Restarting services..."
        docker-compose restart
        check_services_health
        print_status "Services restarted"
        ;;
    "logs")
        docker-compose logs -f
        ;;
    "status")
        print_info "Checking service status..."
        docker-compose ps
        check_services_health
        ;;
    *)
        main
        ;;
esac