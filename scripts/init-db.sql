-- Airflow Database Initialization Script
-- This script sets up the Airflow database with proper extensions and configurations

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create framework-specific schema for metadata tracking
CREATE SCHEMA IF NOT EXISTS framework_metadata;

-- Create table for tracking pipeline executions
CREATE TABLE IF NOT EXISTS framework_metadata.pipeline_executions (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50) DEFAULT 'running',
    records_processed INTEGER DEFAULT 0,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_dag_id ON framework_metadata.pipeline_executions(dag_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_execution_date ON framework_metadata.pipeline_executions(execution_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status ON framework_metadata.pipeline_executions(status);

-- Create table for data quality results
CREATE TABLE IF NOT EXISTS framework_metadata.data_quality_results (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    rule_name VARCHAR(250) NOT NULL,
    rule_type VARCHAR(100) NOT NULL,
    passed BOOLEAN NOT NULL,
    score DECIMAL(5,4),
    details JSONB,
    severity VARCHAR(50) DEFAULT 'warning',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for data quality results
CREATE INDEX IF NOT EXISTS idx_dq_results_dag_task ON framework_metadata.data_quality_results(dag_id, task_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_execution_date ON framework_metadata.data_quality_results(execution_date);

-- Create table for framework metrics
CREATE TABLE IF NOT EXISTS framework_metadata.framework_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(250) NOT NULL,
    metric_value DECIMAL(15,6),
    metric_type VARCHAR(100),
    labels JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for metrics
CREATE INDEX IF NOT EXISTS idx_framework_metrics_name ON framework_metadata.framework_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_framework_metrics_timestamp ON framework_metadata.framework_metrics(timestamp);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION framework_metadata.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for pipeline_executions table
DROP TRIGGER IF EXISTS update_pipeline_executions_updated_at ON framework_metadata.pipeline_executions;
CREATE TRIGGER update_pipeline_executions_updated_at
    BEFORE UPDATE ON framework_metadata.pipeline_executions
    FOR EACH ROW
    EXECUTE FUNCTION framework_metadata.update_updated_at_column();

-- Grant permissions to airflow user
GRANT USAGE ON SCHEMA framework_metadata TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA framework_metadata TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA framework_metadata TO airflow;

-- Create default connections for the framework
-- These will be created via Airflow CLI in the init process