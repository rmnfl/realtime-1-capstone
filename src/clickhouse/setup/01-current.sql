-- -----------------------------------------------------------------------------
-- orders — заказы
-- -----------------------------------------------------------------------------

-- Заказы: выбираем максимальную бизнес-версию, включая состояние удаления.
CREATE TABLE IF NOT EXISTS capstone.orders_current (
    dataset_id   String,
    id           String,
    campaign_id  String,
    status       String,
    amount_cents Int64,
    version      UInt32,
    created_at   DateTime64(6, 'UTC'),
    updated_at   DateTime64(6, 'UTC'),
    is_deleted   UInt8,
    _source_lsn  UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (dataset_id, id);

CREATE MATERIALIZED VIEW IF NOT EXISTS capstone.orders_raw_to_current
TO capstone.orders_current AS
SELECT
    dataset_id,
    id,
    campaign_id,
    status,
    amount_cents,
    version,
    created_at,
    updated_at,
    is_deleted,
    _source_lsn
FROM capstone.orders_raw;

CREATE VIEW IF NOT EXISTS capstone.orders_latest AS
SELECT *
FROM capstone.orders_current FINAL;

-- -----------------------------------------------------------------------------
-- order_items — позиции заказов
-- -----------------------------------------------------------------------------

-- Позиции: выбираем состояние по позиции в WAL.
CREATE TABLE IF NOT EXISTS capstone.order_items_current (
    dataset_id       String,
    id               String,
    order_id         String,
    sku              String,
    quantity         Int32,
    unit_price_cents Int64,
    is_deleted       UInt8,
    _source_lsn      UInt64
)
ENGINE = ReplacingMergeTree(_source_lsn)
ORDER BY (dataset_id, id);

CREATE MATERIALIZED VIEW IF NOT EXISTS capstone.order_items_raw_to_current
TO capstone.order_items_current AS
SELECT
    dataset_id,
    id,
    order_id,
    sku,
    quantity,
    unit_price_cents,
    is_deleted,
    _source_lsn
FROM capstone.order_items_raw;

CREATE VIEW IF NOT EXISTS capstone.order_items_latest AS
SELECT *
FROM capstone.order_items_current FINAL;

-- -----------------------------------------------------------------------------
-- payments — оплаты и возвраты
-- -----------------------------------------------------------------------------

-- Платежи неизменяемы: повтор одного ключа содержит тот же бизнес-факт.
CREATE TABLE IF NOT EXISTS capstone.payments_current (
    dataset_id   String,
    id           String,
    order_id     String,
    kind         String,
    amount_cents Int64,
    occurred_at  DateTime64(6, 'UTC'),
    _source_lsn  UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (dataset_id, id);

CREATE MATERIALIZED VIEW IF NOT EXISTS capstone.payments_raw_to_current
TO capstone.payments_current AS
SELECT
    dataset_id,
    id,
    order_id,
    kind,
    amount_cents,
    occurred_at,
    _source_lsn
FROM capstone.payments_raw;

CREATE VIEW IF NOT EXISTS capstone.payments_latest AS
SELECT *
FROM capstone.payments_current FINAL;
