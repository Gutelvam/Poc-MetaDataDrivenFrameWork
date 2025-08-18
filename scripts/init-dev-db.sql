-- Development Database Initialization Script
-- This script creates sample data for testing the framework

-- Create test schema
CREATE SCHEMA IF NOT EXISTS test_data;

-- Create sample source table
CREATE TABLE IF NOT EXISTS test_data.exemplo_tabela (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    valor DECIMAL(10,2) NOT NULL,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    categoria VARCHAR(100),
    ativo BOOLEAN DEFAULT true,
    metadata JSONB
);

-- Insert sample data
INSERT INTO test_data.exemplo_tabela (nome, valor, categoria, metadata) VALUES
('Produto A', 150.00, 'eletrônicos', '{"marca": "TechCorp", "modelo": "X1"}'),
('Produto B', 89.99, 'casa', '{"marca": "HomePlus", "cor": "azul"}'),
('Produto C', 250.50, 'eletrônicos', '{"marca": "TechCorp", "modelo": "X2"}'),
('Produto D', 45.75, 'livros', '{"autor": "João Silva", "paginas": 320}'),
('Produto E', 199.90, 'roupas', '{"marca": "Fashion", "tamanho": "M"}'),
('Produto F', 75.00, 'casa', '{"marca": "HomePlus", "material": "madeira"}'),
('Produto G', 320.00, 'eletrônicos', '{"marca": "NewTech", "garantia": "2 anos"}'),
('Produto H', 12.50, 'livros', '{"autor": "Maria Santos", "genero": "ficção"}'),
('Produto I', 450.00, 'eletrônicos', '{"marca": "Premium", "tipo": "smartphone"}'),
('Produto J', 65.30, 'roupas', '{"marca": "Classic", "cor": "preto"}');

-- Create customers table for more complex examples
CREATE TABLE IF NOT EXISTS test_data.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    registration_date DATE DEFAULT CURRENT_DATE,
    country VARCHAR(50),
    city VARCHAR(100),
    age INTEGER,
    customer_segment VARCHAR(50),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0.00,
    last_order_date DATE,
    is_active BOOLEAN DEFAULT true
);

-- Insert sample customers
INSERT INTO test_data.customers (first_name, last_name, email, country, city, age, customer_segment, total_orders, total_spent, last_order_date) VALUES
('João', 'Silva', 'joao.silva@email.com', 'Brasil', 'São Paulo', 35, 'premium', 15, 2500.00, '2024-01-15'),
('Maria', 'Santos', 'maria.santos@email.com', 'Brasil', 'Rio de Janeiro', 28, 'regular', 8, 890.50, '2024-01-10'),
('Pedro', 'Oliveira', 'pedro.oliveira@email.com', 'Brasil', 'Belo Horizonte', 42, 'premium', 22, 3200.75, '2024-01-18'),
('Ana', 'Costa', 'ana.costa@email.com', 'Brasil', 'Porto Alegre', 31, 'regular', 12, 1450.25, '2024-01-12'),
('Carlos', 'Ferreira', 'carlos.ferreira@email.com', 'Brasil', 'Salvador', 39, 'vip', 35, 5600.00, '2024-01-20'),
('Lucia', 'Pereira', 'lucia.pereira@email.com', 'Brasil', 'Fortaleza', 26, 'new', 3, 245.90, '2024-01-08'),
('Roberto', 'Lima', 'roberto.lima@email.com', 'Brasil', 'Recife', 45, 'premium', 18, 2890.40, '2024-01-16'),
('Fernanda', 'Alves', 'fernanda.alves@email.com', 'Brasil', 'Brasília', 33, 'regular', 9, 1120.80, '2024-01-14'),
('Ricardo', 'Mendes', 'ricardo.mendes@email.com', 'Brasil', 'Curitiba', 37, 'premium', 25, 4100.60, '2024-01-19'),
('Juliana', 'Rocha', 'juliana.rocha@email.com', 'Brasil', 'Goiânia', 29, 'regular', 7, 650.30, '2024-01-11');

-- Create orders table for ETL examples
CREATE TABLE IF NOT EXISTS test_data.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES test_data.customers(customer_id),
    order_date DATE DEFAULT CURRENT_DATE,
    order_amount DECIMAL(10,2) NOT NULL,
    product_category VARCHAR(100),
    payment_method VARCHAR(50),
    order_status VARCHAR(50) DEFAULT 'completed',
    shipping_country VARCHAR(50),
    discount_amount DECIMAL(8,2) DEFAULT 0.00,
    tax_amount DECIMAL(8,2) DEFAULT 0.00
);

-- Insert sample orders
INSERT INTO test_data.orders (customer_id, order_date, order_amount, product_category, payment_method, shipping_country, discount_amount, tax_amount) VALUES
(1, '2024-01-15', 299.90, 'eletrônicos', 'cartão_credito', 'Brasil', 29.99, 23.99),
(2, '2024-01-10', 89.50, 'livros', 'pix', 'Brasil', 0.00, 7.16),
(3, '2024-01-18', 450.00, 'eletrônicos', 'cartão_credito', 'Brasil', 45.00, 36.00),
(4, '2024-01-12', 125.75, 'casa', 'cartão_debito', 'Brasil', 12.58, 10.06),
(5, '2024-01-20', 680.00, 'roupas', 'cartão_credito', 'Brasil', 68.00, 54.40),
(1, '2024-01-16', 199.90, 'livros', 'pix', 'Brasil', 0.00, 15.99),
(6, '2024-01-08', 75.30, 'casa', 'pix', 'Brasil', 0.00, 6.02),
(7, '2024-01-16', 320.50, 'eletrônicos', 'cartão_credito', 'Brasil', 32.05, 25.64),
(2, '2024-01-11', 155.80, 'roupas', 'cartão_debito', 'Brasil', 15.58, 12.46),
(8, '2024-01-14', 89.90, 'livros', 'pix', 'Brasil', 0.00, 7.19);

-- Create events table for real-time processing examples
CREATE TABLE IF NOT EXISTS test_data.events (
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    customer_id INTEGER,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    page_url VARCHAR(500),
    product_id INTEGER,
    session_id VARCHAR(255),
    user_agent TEXT,
    ip_address INET,
    event_data JSONB,
    processed BOOLEAN DEFAULT false
);

-- Insert sample events
INSERT INTO test_data.events (event_type, customer_id, page_url, product_id, session_id, ip_address, event_data) VALUES
('page_view', 1, '/produtos/eletronicos', NULL, 'sess_001', '192.168.1.100', '{"referrer": "google.com", "device": "mobile"}'),
('product_view', 1, '/produto/123', 123, 'sess_001', '192.168.1.100', '{"category": "eletrônicos", "price": 299.90}'),
('add_to_cart', 1, '/carrinho', 123, 'sess_001', '192.168.1.100', '{"quantity": 1, "value": 299.90}'),
('page_view', 2, '/produtos/livros', NULL, 'sess_002', '192.168.1.101', '{"referrer": "direct", "device": "desktop"}'),
('purchase', 1, '/checkout/success', 123, 'sess_001', '192.168.1.100', '{"order_id": 1001, "total": 299.90, "payment": "cartão_credito"}'),
('login', 3, '/login', NULL, 'sess_003', '192.168.1.102', '{"method": "email", "success": true}'),
('search', 2, '/busca', NULL, 'sess_002', '192.168.1.101', '{"query": "python programming", "results": 45}'),
('logout', 3, '/logout', NULL, 'sess_003', '192.168.1.102', '{"session_duration": 1800}');

-- Create data quality test table
CREATE TABLE IF NOT EXISTS test_data.quality_test (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    value INTEGER,
    email VARCHAR(255),
    phone VARCHAR(20),
    created_date DATE,
    score DECIMAL(5,2),
    category VARCHAR(50),
    is_valid BOOLEAN DEFAULT true
);

-- Insert data with quality issues for testing
INSERT INTO test_data.quality_test (name, value, email, phone, created_date, score, category, is_valid) VALUES
('Produto Válido', 100, 'valido@email.com', '11999999999', '2024-01-15', 95.50, 'A', true),
(NULL, 200, 'email_sem_nome@email.com', '11888888888', '2024-01-16', 87.30, 'B', true),  -- Nome nulo
('Produto Duplicado', 150, 'duplicado@email.com', '11777777777', '2024-01-17', 92.10, 'A', true),
('Produto Duplicado', 150, 'duplicado@email.com', '11777777777', '2024-01-17', 92.10, 'A', true),  -- Duplicado
('Produto Inválido', -50, 'invalido', '123', '2024-01-18', -10.00, 'X', false),  -- Valores inválidos
('Produto OK', 75, 'ok@email.com', '11666666666', '2024-01-19', 78.90, 'C', true),
('', 0, '@email.com', '', '2024-01-20', 0.00, '', true),  -- Campos vazios
('Produto Limite', 999999, 'limite@email.com', '11555555555', '2024-01-21', 100.00, 'A', true);  -- Valor no limite

-- Create materialized view for analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS test_data.customer_summary AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    c.customer_segment,
    c.country,
    c.city,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.order_amount), 0) AS total_spent,
    MAX(o.order_date) AS last_order_date,
    AVG(o.order_amount) AS avg_order_value
FROM test_data.customers c
LEFT JOIN test_data.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.customer_segment, c.country, c.city;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON test_data.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON test_data.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_events_customer_id ON test_data.events(customer_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON test_data.events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type ON test_data.events(event_type);

-- Create function to generate random test data
CREATE OR REPLACE FUNCTION test_data.generate_daily_orders(date_param DATE DEFAULT CURRENT_DATE)
RETURNS INTEGER AS $$
DECLARE
    order_count INTEGER := 0;
    customer_ids INTEGER[];
    i INTEGER;
BEGIN
    -- Get active customer IDs
    SELECT ARRAY(SELECT customer_id FROM test_data.customers WHERE is_active = true) INTO customer_ids;
    
    -- Generate 5-15 random orders for the given date
    FOR i IN 1..(5 + FLOOR(RANDOM() * 10)) LOOP
        INSERT INTO test_data.orders (
            customer_id, 
            order_date, 
            order_amount, 
            product_category, 
            payment_method, 
            shipping_country,
            discount_amount,
            tax_amount
        ) VALUES (
            customer_ids[1 + FLOOR(RANDOM() * array_length(customer_ids, 1))],
            date_param,
            ROUND((50 + RANDOM() * 450)::NUMERIC, 2),
            CASE FLOOR(RANDOM() * 4) 
                WHEN 0 THEN 'eletrônicos'
                WHEN 1 THEN 'livros'
                WHEN 2 THEN 'roupas'
                ELSE 'casa'
            END,
            CASE FLOOR(RANDOM() * 3)
                WHEN 0 THEN 'cartão_credito'
                WHEN 1 THEN 'cartão_debito'
                ELSE 'pix'
            END,
            'Brasil',
            ROUND((RANDOM() * 50)::NUMERIC, 2),
            ROUND((RANDOM() * 40)::NUMERIC, 2)
        );
        order_count := order_count + 1;
    END LOOP;
    
    RETURN order_count;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT USAGE ON SCHEMA test_data TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test_data TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA test_data TO postgres;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA test_data TO postgres;