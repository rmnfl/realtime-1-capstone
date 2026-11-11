"""Проверяет snapshot, доставку изменений и дедупликацию на запущенном стенде."""

import argparse
import asyncio
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import psycopg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import KafkaError

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = START + timedelta(days=1)
TABLES = ("orders", "order_items", "payments")
ENDPOINTS = (
    "orders/summary", "campaigns/summary", "products/summary",
    "orders/timeseries", "payments/timeseries",
)


def build_order_summary(created, paid, cancelled, amount):
    count = created + paid + cancelled
    return {
        "orders_count": count,
        "created_orders_count": created,
        "paid_orders_count": paid,
        "cancelled_orders_count": cancelled,
        "paid_amount_cents": amount,
        "avg_paid_amount_cents": amount / paid if paid else None,
        "paid_share": paid / count if count else None,
        "cancelled_share": cancelled / count if count else None,
    }


def build_payment_total(captures=0, captured=0, refunds=0, refunded=0):
    return {
        "captures_count": captures, "captured_amount_cents": captured,
        "refunds_count": refunds, "refunded_amount_cents": refunded,
        "net_amount_cents": captured - refunded,
    }


def build_product_summary(sku, orders, units, amount):
    return {
        "sku": sku, "paid_orders_count": orders,
        "units_count": units, "items_amount_cents": amount,
    }


def build_expected_metrics(stage):
    if stage == "snapshot":
        total = build_order_summary(3, 6, 3, 101000)
        campaigns = [
            {"campaign_id": name, **build_order_summary(1, 2, 1, amount)}
            for name, amount in (("beta", 54000), ("alpha", 30000), ("organic", 17000))
        ]
        products = [build_product_summary("sku002", 2, 2, 40000), build_product_summary("sku003", 2, 3, 36000),
                    build_product_summary("sku001", 3, 5, 25000)]
        orders = {9: build_order_summary(0, 2, 0, 30000), 10: build_order_summary(1, 0, 1, 0),
                  12: build_order_summary(0, 2, 0, 54000), 13: build_order_summary(1, 0, 1, 0),
                  15: build_order_summary(0, 2, 0, 17000), 16: build_order_summary(1, 0, 1, 0)}
        payments = {9: build_payment_total(2, 30000), 12: build_payment_total(2, 54000),
                    13: build_payment_total(1, 10000), 14: build_payment_total(0, 0, 2, 14000),
                    15: build_payment_total(2, 17000)}
    else:
        paid = stage == "pay"
        refunded = stage == "refund"
        empty = stage in ("empty", "delete")
        total = build_order_summary(int(not paid and not refunded and not empty), int(paid),
                        int(refunded), 10000 if paid else 0)
        campaigns = [] if empty else [{"campaign_id": "alpha", **total}]
        products = [build_product_summary("sku001", 1, 1, 6000), build_product_summary("sku002", 1, 2, 4000)] if paid else []
        orders = {} if empty else {12: total}
        payments = {12: build_payment_total(1, 10000)} if paid or refunded else {}
        if refunded:
            payments[14] = build_payment_total(0, 0, 1, 10000)

    order_series, payment_series = [], []
    cumulative = 0
    for hour in range(24):
        bucket = (START + timedelta(hours=hour)).isoformat()
        row = orders.get(hour, build_order_summary(0, 0, 0, 0)).copy()
        row.pop("paid_share")
        row.pop("cancelled_share")
        cumulative += row["paid_amount_cents"]
        order_series.append({"bucket_start": bucket, **row,
                             "cumulative_paid_amount_cents": cumulative})
        payment_series.append({"bucket_start": bucket, **payments.get(hour, build_payment_total())})
    payment_total = {key: sum(row[key] for row in payments.values()) for key in build_payment_total()}
    return dict(zip(ENDPOINTS, (total, campaigns, products, order_series, payment_series))), [
        {"bucket_start": START.isoformat(), **payment_total}
    ]


def assert_equal(actual, wanted, path="data"):
    if isinstance(wanted, dict):
        if not isinstance(actual, dict) or actual.keys() != wanted.keys():
            raise AssertionError(f"{path}: поля {actual!r}, ожидалось {wanted!r}")
        for key, value in wanted.items():
            assert_equal(actual[key], value, f"{path}.{key}")
    elif isinstance(wanted, list):
        if not isinstance(actual, list) or len(actual) != len(wanted):
            raise AssertionError(f"{path}: {actual!r}, ожидалось {wanted!r}")
        for index, value in enumerate(wanted):
            assert_equal(actual[index], value, f"{path}[{index}]")
    elif path.endswith("bucket_start"):
        if datetime.fromisoformat(actual) != datetime.fromisoformat(wanted):
            raise AssertionError(f"{path}: {actual!r} != {wanted!r}")
    elif isinstance(wanted, (int, float)) and isinstance(actual, (int, float)):
        if not math.isclose(actual, wanted, rel_tol=1e-9, abs_tol=1e-9):
            raise AssertionError(f"{path}: {actual!r} != {wanted!r}")
    elif actual != wanted:
        raise AssertionError(f"{path}: {actual!r} != {wanted!r}")


def wait_for(check, description, timeout=60, announce=False):
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = check()
            if announce:
                print(f"OK: {description}", flush=True)
            return result
        except (AssertionError, httpx.HTTPError) as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Не дождались: {description}\n{error}") from error
            time.sleep(1)


def fetch_metrics(client, dataset):
    base = f"http://127.0.0.1:{os.getenv('API_PORT', '18001')}"
    answers = {}
    for endpoint in (*ENDPOINTS, "payments/total"):
        params = {"from": START.isoformat(), "to": END.isoformat()}
        route = endpoint
        if endpoint.endswith("summary"):
            params["refresh"] = "true"
        elif endpoint == "payments/total":
            route = "payments/timeseries"
            params["interval"] = "total"
        response = client.get(f"{base}/v1/datasets/{dataset}/metrics/{route}", params=params)
        response.raise_for_status()
        answers[endpoint] = response.json()["data"]
    return answers


def verify_metrics(client, dataset, stage):
    wanted, total = build_expected_metrics(stage)
    wanted["payments/total"] = total

    def check():
        answers = fetch_metrics(client, dataset)
        for endpoint, value in wanted.items():
            assert_equal(answers[endpoint], value, endpoint)
        return answers

    return wait_for(check, f"{dataset}: ожидаемое состояние {stage} во всех ответах API")


def print_table(headers, rows):
    widths = [max(len(str(row[i])) for row in [headers, *rows]) for i in range(len(headers))]
    for index, row in enumerate([headers, *rows]):
        if index == 1:
            print("-+-".join("-" * width for width in widths))
        print(" | ".join(str(value).ljust(width) for value, width in zip(row, widths)))


def build_metric_overview(answers):
    orders = answers["orders/summary"]
    payments = answers["payments/total"][0]
    products = answers["products/summary"]
    return {
        "Заказов": orders["orders_count"],
        "В статусе created": orders["created_orders_count"],
        "В статусе paid": orders["paid_orders_count"],
        "В статусе cancelled": orders["cancelled_orders_count"],
        "Сумма оплаченных заказов, коп.": orders["paid_amount_cents"],
        "SKU в оплаченных заказах": len(products),
        "Единиц в оплаченных заказах": sum(row["units_count"] for row in products),
        "Оплаты: число / коп.": f"{payments['captures_count']} / {payments['captured_amount_cents']}",
        "Возвраты: число / коп.": f"{payments['refunds_count']} / {payments['refunded_amount_cents']}",
        "Оплаты минус возвраты, коп.": payments["net_amount_cents"],
    }


def print_comparison(before, after):
    current = build_metric_overview(after)
    if before is None:
        print_table(("Показатель", "В API"), list(current.items()))
    else:
        previous = build_metric_overview(before)
        print_table(("Показатель", "До", "После"), [
            (name, previous[name], value) for name, value in current.items()
        ])
    print("Проверены все пять эндпоинтов, оба ряда за 24 часа и итог платежей.", flush=True)


STEPS = {
    "create": (
        "Создаём заказ",
        "В PostgreSQL появится заказ o1 на 10000 копеек и две товарные позиции: "
        "1 × 6000 и 2 × 2000.",
        "Один заказ со статусом created. Оплаченных товаров и платежей пока нет.",
        "empty",
    ),
    "pay": (
        "Оплачиваем заказ",
        "Одной транзакцией меняем статус o1 на paid, увеличиваем version до 2 "
        "и записываем capture на 10000 копеек.",
        "Сумму оплаченных заказов 10000 копеек, два SKU, три единицы товара и одну оплату.",
        "create",
    ),
    "refund": (
        "Возвращаем деньги",
        "Одной транзакцией меняем статус o1 на cancelled, увеличиваем version до 3 "
        "и записываем refund на 10000 копеек.",
        "Один заказ со статусом cancelled и нулевую сумму оплаченных заказов. "
        "Оплата и возврат останутся в истории; их разница равна нулю.",
        "pay",
    ),
    "delete": (
        "Физически удаляем неоплаченный заказ",
        "Удаляем две позиции и сам заказ из PostgreSQL одной транзакцией.",
        "Ноль заказов. В ClickHouse останется последняя версия с is_deleted=1.",
        "create",
    ),
}


def run_step(client, dataset, stage):
    title, action, expectation, previous = STEPS[stage]
    print(f"\n{title}\n{action}\nОжидаем в API: {expectation}", flush=True)
    if stage == "create":
        history = query_clickhouse(client, """
            SELECT count() AS n FROM capstone.orders_raw
            WHERE dataset_id = {dataset:String} FORMAT JSONEachRow
        """, dataset)
        if int(history[0]["n"]) != 0:
            raise ValueError(f"Датасет {dataset} уже использовался. Выберите новое имя.")
    before = verify_metrics(client, dataset, previous)
    apply_source_change(dataset, stage)
    after = verify_metrics(client, dataset, stage)
    if stage == "delete":
        def check_deleted():
            orders = query_clickhouse(client, """
                SELECT id, version, is_deleted FROM capstone.orders_latest
                WHERE dataset_id = {dataset:String} ORDER BY id FORMAT JSONEachRow
            """, dataset)
            items = query_clickhouse(client, """
                SELECT id, is_deleted FROM capstone.order_items_latest
                WHERE dataset_id = {dataset:String} ORDER BY id FORMAT JSONEachRow
            """, dataset)
            assert_equal(orders, [{"id": "o1", "version": 2, "is_deleted": 1}], "orders_latest")
            assert_equal(
                items,
                [{"id": "i1", "is_deleted": 1}, {"id": "i2", "is_deleted": 1}],
                "order_items_latest",
            )

        wait_for(check_deleted, "флаги удаления в ClickHouse")
        print("ClickHouse: заказ с version=2 и обе позиции имеют is_deleted=1.")
    print_comparison(before, after)


def run_snapshot(client):
    print("\nИсходный snapshot\nВ PostgreSQL до запуска Debezium было 12 заказов, "
          "14 позиций и 9 платежных операций.\n"
          "Ожидаем эти данные в API после начального snapshot.", flush=True)
    answers = verify_metrics(client, "demo", "snapshot")

    def check_snapshot():
        rows = []
        for table, wanted in zip(TABLES, (12, 14, 9)):
            count = int(query_clickhouse(client, f"""
                SELECT uniqExact(id) AS n FROM capstone.{table}_raw
                WHERE dataset_id = {{dataset:String}} AND _operation = 'r'
                FORMAT JSONEachRow
            """, "demo")[0]["n"])
            if count != wanted:
                raise AssertionError(f"{table}: snapshot содержит {count} ключей, ожидалось {wanted}")
            rows.append((table, count))
        return rows

    rows = wait_for(check_snapshot, "snapshot-события трёх таблиц в raw")
    print_table(("Таблица PostgreSQL", "Ключей в raw с op=r"), rows)
    print_comparison(None, answers)


def apply_source_change(dataset, stage):
    dsn = f"postgresql://admin:admin@127.0.0.1:{os.getenv('PG_PORT', '55434')}/capstone"
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            if stage == "create":
                cursor.execute("SELECT 1 FROM capstone.orders WHERE dataset_id = %s LIMIT 1", (dataset,))
                if cursor.fetchone():
                    raise ValueError(f"Датасет {dataset} уже занят. Выберите новый dataset_id.")
                cursor.executemany("""
                    INSERT INTO capstone.orders
                        (dataset_id, id, campaign_id, status, amount_cents, version, created_at, updated_at)
                    VALUES (%s, %s, 'alpha', 'created', %s, 1, %s, now())
                """, [(dataset, "o1", 10000, START + timedelta(hours=12))])
                cursor.executemany("""
                    INSERT INTO capstone.order_items
                        (dataset_id, id, order_id, sku, quantity, unit_price_cents)
                    VALUES (%s, %s, 'o1', %s, %s, %s)
                """, [(dataset, "i1", "sku001", 1, 6000),
                      (dataset, "i2", "sku002", 2, 2000)])
            elif stage == "delete":
                cursor.execute("""
                    SELECT 1 FROM capstone.orders
                    WHERE dataset_id = %s AND id = 'o1' AND status = 'created' AND version = 1
                    FOR UPDATE
                """, (dataset,))
                if not cursor.fetchone():
                    raise ValueError("Физическое удаление в демо выполняется только для неоплаченного o1.")
                cursor.execute("DELETE FROM capstone.order_items WHERE dataset_id = %s AND order_id = 'o1'", (dataset,))
                cursor.execute("DELETE FROM capstone.orders WHERE dataset_id = %s AND id = 'o1'", (dataset,))
            else:
                previous, status, kind, version, hour = (
                    ("created", "paid", "capture", 1, 12) if stage == "pay"
                    else ("paid", "cancelled", "refund", 2, 14)
                )
                cursor.execute("""
                    UPDATE capstone.orders
                    SET status = %s, version = version + 1, updated_at = now()
                    WHERE dataset_id = %s AND id = 'o1' AND status = %s AND version = %s
                """, (status, dataset, previous, version))
                if cursor.rowcount != 1:
                    raise ValueError(f"Для {stage} заказ o1 должен иметь статус {previous}, version={version}.")
                cursor.execute("""
                    INSERT INTO capstone.payments
                        (dataset_id, id, order_id, kind, amount_cents, occurred_at)
                    VALUES (%s, %s, 'o1', %s, 10000, %s)
                """, (dataset, "p1" if stage == "pay" else "p2", kind,
                      START + timedelta(hours=hour, minutes=1 if stage == "pay" else 0)))
    print("Изменения сохранены в PostgreSQL. Ждём результат в API…", flush=True)


def query_clickhouse(client, query, dataset):
    response = client.post(
        f"http://127.0.0.1:{os.getenv('CLICKHOUSE_HTTP_PORT', '18133')}",
        auth=("admin", "admin"), params={"param_dataset": dataset}, content=query,
    )
    response.raise_for_status()
    return [json.loads(line) for line in response.text.splitlines()]


async def replay_kafka_events(records):
    broker = f"127.0.0.1:{os.getenv('KAFKA_PORT', '19093')}"
    consumer = AIOKafkaConsumer(bootstrap_servers=broker, enable_auto_commit=False)
    producer = AIOKafkaProducer(bootstrap_servers=broker, acks="all")
    await consumer.start()
    try:
        messages = []
        for record in records:
            partition = TopicPartition(record["topic"], record["partition"])
            consumer.assign([partition])
            consumer.seek(partition, record["offset"])
            message = await asyncio.wait_for(consumer.getone(), timeout=15)
            if message.offset != record["offset"] or message.value is None:
                raise ValueError(f"Исходное сообщение Kafka недоступно: {record}")
            key = json.loads(message.key)
            envelope = json.loads(message.value)
            row = envelope.get("after") or envelope.get("before")
            if key["dataset_id"] != record["dataset_id"] or row["dataset_id"] != record["dataset_id"]:
                raise ValueError(f"Сообщение Kafka не соответствует raw: {record}")
            messages.append(message)
        await producer.start()
        try:
            for message in messages:
                result = await producer.send_and_wait(
                    message.topic, key=message.key, value=message.value,
                    partition=message.partition, headers=list(message.headers),
                )
                if result.offset <= message.offset:
                    raise ValueError("Kafka не присвоила повтору новый offset.")
        finally:
            await producer.stop()
    finally:
        await consumer.stop()


def run_duplicates(client, dataset):
    print("\nПовторно доставляем события\nОтправляем в Kafka те же ключи и Debezium envelope. "
          "Событие со статусом paid и версией 2 придёт раньше старого события created с версией 1.\n"
          "Ожидаем: строк в raw станет больше, но в API останется один оплаченный заказ, "
          "сумма 10000, три единицы товара и одна оплата.", flush=True)
    before_api = verify_metrics(client, dataset, "pay")
    # Replay the newest events first, so old order versions arrive after them.
    records = []
    before = {}
    for table in TABLES:
        rows = query_clickhouse(client, f"""
            SELECT dataset_id, _kafka_topic AS topic, _kafka_partition AS partition,
                   _kafka_offset AS offset
            FROM capstone.{table}_raw
            WHERE dataset_id = {{dataset:String}}
            GROUP BY dataset_id, topic, partition, offset
            ORDER BY partition, offset DESC
            FORMAT JSONEachRow
        """, dataset)
        if not rows:
            raise ValueError(f"Нет событий {table} для {dataset}.")
        for row in rows:
            row["partition"] = int(row["partition"])
            row["offset"] = int(row["offset"])
        records.extend(rows)
        before[table] = int(query_clickhouse(client, f"""
            SELECT count() AS n FROM capstone.{table}_raw
            WHERE dataset_id = {{dataset:String}} FORMAT JSONEachRow
        """, dataset)[0]["n"])
    asyncio.run(replay_kafka_events(records))

    def check_raw():
        counts = {}
        for table in TABLES:
            counts[table] = int(query_clickhouse(client, f"""
                SELECT count() AS n FROM capstone.{table}_raw
                WHERE dataset_id = {{dataset:String}} FORMAT JSONEachRow
            """, dataset)[0]["n"])
            sent = sum(row["topic"] == f"capstone.capstone.{table}" for row in records)
            if counts[table] < before[table] + sent:
                raise AssertionError(f"{table}_raw: {counts[table]}, ожидалось >= {before[table] + sent}")
        return counts

    after = wait_for(check_raw, "повторы из Kafka записаны в raw", announce=True)
    print_table(("Таблица", "Строк до", "Строк после"), [
        (f"{table}_raw", before[table], after[table]) for table in TABLES
    ])
    print_comparison(before_api, verify_metrics(client, dataset, "pay"))


def print_dataset_info(dataset):
    print(f"\nДатасет: {dataset}\nПериод API: 2026-01-01T00:00:00Z — 2026-01-02T00:00:00Z", flush=True)


def run_pipeline(client, dataset):
    print_dataset_info(dataset)
    run_step(client, dataset, "create")
    run_step(client, dataset, "pay")
    run_duplicates(client, dataset)
    run_step(client, dataset, "refund")


def run_delete(client):
    dataset = f"delete_{uuid4().hex[:12]}"
    print("\nДополнительный эпизод: физическое удаление. "
          "Для него создаём отдельный неоплаченный заказ.", flush=True)
    print_dataset_info(dataset)
    run_step(client, dataset, "create")
    run_step(client, dataset, "delete")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "step",
        choices=("all", "snapshot", "pipeline", "create", "pay", "refund", "duplicates", "delete", "show"),
        help="Шаг демо; all выполняет полный прогон",
    )
    parser.add_argument("--dataset", help="Датасет для истории заказа или просмотра ответов API")
    args = parser.parse_args(argv)
    if args.step in ("pay", "refund", "duplicates", "show") and args.dataset is None:
        parser.error("Укажите --dataset; для истории заказа используйте имя из demo-create.")
    if args.step in ("snapshot", "delete") and args.dataset is not None:
        parser.error("Этот шаг не принимает --dataset: snapshot использует demo, delete создаёт новый датасет.")
    if args.dataset is not None:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", args.dataset):
            parser.error("--dataset: 1–64 символа a-z, 0-9, _ и -, первый — буква или цифра")
        if args.dataset == "demo" and args.step != "show":
            parser.error("Для контрольного сценария выберите датасет, отличный от demo.")
    args.dataset = args.dataset or f"check_{uuid4().hex[:12]}"
    return args


def main():
    args = parse_args()
    dataset = args.dataset
    with httpx.Client(timeout=10) as client:
        if args.step in ("all", "snapshot"):
            run_snapshot(client)
        if args.step in ("all", "pipeline"):
            run_pipeline(client, dataset)
        if args.step in ("all", "delete"):
            run_delete(client)
        elif args.step == "show":
            print(json.dumps(fetch_metrics(client, dataset), ensure_ascii=False, indent=2))
        elif args.step == "duplicates":
            print_dataset_info(dataset)
            run_duplicates(client, dataset)
        elif args.step in ("create", "pay", "refund"):
            print_dataset_info(dataset)
            run_step(client, dataset, args.step)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, httpx.HTTPError, psycopg.Error, KafkaError, TimeoutError) as error:
        print(f"Ошибка демо: {error}", file=sys.stderr)
        sys.exit(1)
