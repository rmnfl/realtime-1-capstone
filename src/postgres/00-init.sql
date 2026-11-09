-- Исходные таблицы заказов, позиций и платежей.
CREATE SCHEMA IF NOT EXISTS capstone;

CREATE TABLE IF NOT EXISTS capstone.orders (
    dataset_id text NOT NULL,
    id text NOT NULL,
    campaign_id text NOT NULL,
    status text NOT NULL,
    amount_cents bigint NOT NULL,
    version integer NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (dataset_id, id)
);
CREATE TABLE IF NOT EXISTS capstone.order_items (
    dataset_id text NOT NULL,
    id text NOT NULL,
    order_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL,
    unit_price_cents bigint NOT NULL,
    PRIMARY KEY (dataset_id, id),
    FOREIGN KEY (dataset_id, order_id)
        REFERENCES capstone.orders (dataset_id, id)
);
CREATE TABLE IF NOT EXISTS capstone.payments (
    dataset_id text NOT NULL,
    id text NOT NULL,
    order_id text NOT NULL,
    kind text NOT NULL,
    amount_cents bigint NOT NULL,
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY (dataset_id, id),
    FOREIGN KEY (dataset_id, order_id)
        REFERENCES capstone.orders (dataset_id, id)
);

-- Полный старый образ строки для UPDATE и DELETE.
ALTER TABLE capstone.orders REPLICA IDENTITY FULL;
ALTER TABLE capstone.order_items REPLICA IDENTITY FULL;
ALTER TABLE capstone.payments REPLICA IDENTITY FULL;

CREATE PUBLICATION capstone_pub
FOR TABLE capstone.orders, capstone.order_items, capstone.payments;

-- Пользователь Debezium: репликация и чтение для snapshot.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';

GRANT USAGE ON SCHEMA capstone TO debezium;
GRANT SELECT ON capstone.orders TO debezium;
GRANT SELECT ON capstone.order_items TO debezium;
GRANT SELECT ON capstone.payments TO debezium;

-- Пользователь exporter: чтение статистики PostgreSQL.
CREATE ROLE monitor WITH LOGIN PASSWORD 'monitor';

GRANT pg_monitor TO monitor;
GRANT CONNECT ON DATABASE capstone TO monitor;
