"""uv run --group generator src/generator/data_load.py --help"""

import argparse
import asyncio
import json
import math
import os
import random
import re
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from itertools import islice
from uuid import uuid4

import psycopg
from aiokafka import AIOKafkaProducer
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn


def generate_events(args, totals):
    rng = random.Random(args.seed)
    anchor = datetime.fromisoformat(args.start_date).replace(tzinfo=UTC)
    sequence = 0
    # Kafka: synthetic positions must not be mixed with WAL events for the same keys.
    lsn_base = time.time_ns()
    deadline = time.monotonic() + args.duration if args.duration is not None else None

    def build_event(table, op, before, after):
        nonlocal sequence
        sequence += 1
        row = after if after is not None else before
        return {
            "source_lsn": lsn_base + sequence,
            "source_ts_ms": time.time_ns() // 1_000_000,
            "table": table,
            "op": op,
            "key": {"dataset_id": args.dataset, "id": row["id"]},
            "before": before,
            "after": after,
        }

    for number in range(args.orders):
        if deadline is not None and time.monotonic() >= deadline:
            break
        order_id = f"o{args.start_id + number:09d}"
        created = anchor + timedelta(seconds=rng.randrange(args.days * 86400))
        items = [
            {
                "dataset_id": args.dataset,
                "id": f"{order_id}_i{i}",
                "order_id": order_id,
                "sku": f"sku{rng.randrange(100):03d}",
                "quantity": rng.randint(1, 3),
                "unit_price_cents": rng.randint(100, 10000),
            }
            for i in range(rng.randint(1, 10))
        ]
        amount = sum(item["quantity"] * item["unit_price_cents"] for item in items)
        order = {
            "dataset_id": args.dataset,
            "id": order_id,
            "campaign_id": f"campaign{rng.randrange(args.campaigns):03d}",
            "status": "created",
            "amount_cents": amount,
            "version": 1,
            "created_at": created.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            ),
            "updated_at": created.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            ),
        }
        yield build_event("orders", "c", None, order)
        for item in items:
            yield build_event("order_items", "c", None, item)
        totals["orders_created"] += 1

        if rng.random() >= args.paid_rate:
            if rng.random() < args.delete_rate:
                for item in items:
                    yield build_event("order_items", "d", item, None)
                yield build_event("orders", "d", order, None)
                totals["deleted_orders"] += 1
            else:
                totals["unpaid_orders"] += 1
            continue

        paid_at = (
            (created + timedelta(seconds=1))
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        paid = {
            **order,
            "status": "paid",
            "version": 2,
            "updated_at": paid_at,
        }
        yield build_event("orders", "u", order, paid)
        payment = {
            "dataset_id": args.dataset,
            "id": f"{order_id}_capture",
            "order_id": order_id,
            "kind": "capture",
            "amount_cents": amount,
            "occurred_at": paid_at,
        }
        yield build_event("payments", "c", None, payment)
        totals["captured_amount_cents"] += amount

        if rng.random() >= args.refund_rate:
            totals["paid_orders"] += 1
            totals["paid_amount_cents"] += amount
            continue

        refunded_at = (
            (created + timedelta(days=1))
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        cancelled = {
            **paid,
            "status": "cancelled",
            "version": 3,
            "updated_at": refunded_at,
        }
        yield build_event("orders", "u", paid, cancelled)
        refund = {
            **payment,
            "id": f"{order_id}_refund",
            "kind": "refund",
            "occurred_at": refunded_at,
        }
        yield build_event("payments", "c", None, refund)
        totals["refunded_amount_cents"] += amount
        totals["cancelled_orders"] += 1


class Postgres:
    def __init__(self, args):
        self.args = args
        self.conn = None
        self.statements = {}

    async def open(self):
        self.conn = await psycopg.AsyncConnection.connect(
            self.args.dsn,
            connect_timeout=10,
        )

    def build_statement(self, table, op, columns):
        key = (table, op)
        if key in self.statements:
            return self.statements[key]

        target = sql.Identifier("capstone", table)
        match op:
            case "c":
                statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    target,
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )
            case "u":
                assignments = sql.SQL(", ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(column))
                    for column in columns
                )
                statement = sql.SQL(
                    "UPDATE {} SET {} WHERE dataset_id = %s AND id = %s"
                ).format(target, assignments)
            case "d":
                statement = sql.SQL(
                    "DELETE FROM {} WHERE dataset_id = %s AND id = %s"
                ).format(target)
            case _:
                raise ValueError(f"Неизвестная операция: {op}")

        self.statements[key] = statement
        return statement

    async def send(self, batch):
        async with self.conn.transaction(), self.conn.pipeline():
            for event in batch:
                table, op = event["table"], event["op"]
                row = event["after"] if event["after"] is not None else event["before"]
                statement = self.build_statement(table, op, list(row))
                values = list(row.values()) if op in ("c", "u") else []
                if op in ("u", "d"):
                    values.extend([row["dataset_id"], row["id"]])
                await self.conn.execute(statement, values)
        return len(batch)

    async def close(self):
        if self.conn is not None:
            await self.conn.close()


def build_kafka_value(event):
    return {
        "before": event["before"],
        "after": event["after"],
        "source": {
            "name": "generator",
            "schema": "capstone",
            "table": event["table"],
            "snapshot": "false",
            "lsn": event["source_lsn"],
            "ts_ms": event["source_ts_ms"],
        },
        "op": event["op"],
        "ts_ms": time.time_ns() // 1_000_000,
    }


class Kafka:
    def __init__(self, args):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=args.brokers,
            enable_idempotence=True,
            request_timeout_ms=15000,
        )
        self.prefix = args.topic_prefix
        self.duplicates = args.duplicate_rate
        self.rng = random.Random(args.seed + 1)

    async def open(self):
        async with asyncio.timeout(20):
            await self.producer.start()

    async def send(self, batch):
        pending = []
        try:
            async with asyncio.timeout(20):
                for event in batch:
                    key = json.dumps(event["key"], separators=(",", ":"))
                    value = json.dumps(build_kafka_value(event), separators=(",", ":"))
                    copies = 2 if self.rng.random() < self.duplicates else 1
                    for _ in range(copies):
                        future = await self.producer.send(
                            f"{self.prefix}.{event['table']}",
                            key=key.encode(),
                            value=value.encode(),
                        )
                        pending.append(future)
                # Wait for acknowledgements for the entire batch.
                await asyncio.gather(*pending)
            return len(pending)
        finally:
            # Collect exceptions from completed sends and cancel pending sends.
            for future in pending:
                if not future.done():
                    future.cancel()
                elif not future.cancelled():
                    future.exception()

    async def close(self):
        async with asyncio.timeout(20):
            await self.producer.stop()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Генерирует заказы, позиции, оплаты и возвраты; отправляет их в PostgreSQL или Kafka.",
        epilog=(
            "Просмотр событий: just data-load --orders 3 --dry-run. "
            "Запуск нагрузки: just data-load --orders 10000 --rate 1000. "
            "Для нового прогона используйте новый датасет. "
            "Kafka mode sends compatible Debezium envelopes directly to CDC topics; PostgreSQL is unchanged."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "target",
        choices=["postgres", "kafka"],
        help="Приёмник событий: PostgreSQL или Kafka",
    )
    parser.add_argument(
        "--dataset",
        default=f"shop_{uuid4().hex[:12]}",
        help="Датасет; по умолчанию создаётся новое имя",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed для воспроизводимой последовательности",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=10000,
        help="Максимальное число заказов",
    )
    parser.add_argument(
        "--start-id",
        type=int,
        default=0,
        help="Начальный номер заказа; при продолжении датасета берите новый диапазон ID",
    )
    parser.add_argument(
        "--duration",
        type=float,
        help="Ограничить загрузку по времени в секундах; текущий заказ завершается полностью",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1000,
        help="Изменений строк/с; 0 — без ограничения",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Размер пакета изменений; в PostgreSQL — одна транзакция",
    )
    parser.add_argument(
        "--campaigns",
        type=int,
        default=32,
        help="Число рекламных кампаний",
    )
    parser.add_argument(
        "--start-date",
        default="2026-01-01",
        help="Начальная дата создания заказов, YYYY-MM-DD, UTC",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=28,
        help="Число суток для распределения дат создания заказов",
    )
    parser.add_argument(
        "--paid-rate",
        type=float,
        default=0.8,
        help="Доля оплачиваемых заказов, от 0 до 1",
    )
    parser.add_argument(
        "--refund-rate",
        type=float,
        default=0.1,
        help="Доля оплаченных заказов с полным возвратом, от 0 до 1",
    )
    parser.add_argument(
        "--delete-rate",
        type=float,
        default=0.05,
        help="Доля удаляемых неоплаченных заказов, от 0 до 1",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0,
        help="Kafka: вероятность повторной отправки события, от 0 до 1",
    )
    parser.add_argument(
        "--dsn", help="Подключение к PostgreSQL; можно через POSTGRES_DSN"
    )
    parser.add_argument(
        "--brokers",
        default="localhost:19093",
        help="Адреса брокеров Kafka, например localhost:19093",
    )
    parser.add_argument(
        "--topic-prefix",
        default="capstone.capstone",
        help="Префикс топиков: <prefix>.orders, .order_items, .payments",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="JSONL в stdout, без подключений, пауз и повторов",
    )
    args = parser.parse_args(argv)
    args.dsn = args.dsn or os.environ.get("POSTGRES_DSN")
    for name in ("orders", "batch_size", "campaigns", "days"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} должен быть больше 0")
    if not math.isfinite(args.rate) or args.rate < 0:
        parser.error("--rate должен быть конечным числом >= 0")
    if args.start_id < 0:
        parser.error("--start-id должен быть >= 0")
    if args.duration is not None and (not math.isfinite(args.duration) or args.duration <= 0):
        parser.error("--duration должен быть конечным числом > 0")
    for name in ("paid_rate", "refund_rate", "delete_rate", "duplicate_rate"):
        if not 0 <= getattr(args, name) <= 1:
            parser.error(f"--{name.replace('_', '-')} должен быть от 0 до 1")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", args.dataset):
        parser.error(
            "--dataset: 1–64 символа a-z, 0-9, _ и -, первый — буква или цифра"
        )
    try:
        anchor = datetime.strptime(args.start_date, "%Y-%m-%d")
        if anchor.strftime("%Y-%m-%d") != args.start_date:
            raise ValueError("Используйте YYYY-MM-DD")
        anchor + timedelta(days=args.days + 1)
    except (ValueError, OverflowError):
        parser.error("Неверная дата или слишком большой --days")
    if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,200}", args.topic_prefix):
        parser.error("--topic-prefix: 1–200 characters from a-z, A-Z, 0-9, _, . and -")
    if args.target == "postgres" and args.duplicate_rate:
        parser.error(
            "--duplicate-rate применим только к Kafka: повторный UPDATE не является повторной доставкой"
        )
    if args.target == "postgres" and not args.dry_run and not args.dsn:
        parser.error("Укажите --dsn или POSTGRES_DSN")
    return args


def print_report(totals, changes: int, records: int, elapsed: float):
    report = [
        f"Готово: {changes} изменений, {records} записей за {elapsed:.2f} с.",
        "Что должно получиться после обработки всего датасета:",
        json.dumps(dict(totals), ensure_ascii=False, indent=2),
    ]
    for line in report:
        print(line, file=sys.stderr)


async def run(args):
    totals = Counter(
        {
            "orders_created": 0,
            "paid_orders": 0,
            "paid_amount_cents": 0,
            "captured_amount_cents": 0,
            "refunded_amount_cents": 0,
            "cancelled_orders": 0,
            "deleted_orders": 0,
            "unpaid_orders": 0,
        }
    )
    source = generate_events(args, totals)
    sink = None
    changes = records = 0
    started = reported = time.monotonic()
    console = Console(stderr=True, highlight=False, markup=False)
    progress = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        TextColumn("изменений: {task.completed:,.0f}"),
        TextColumn("записей: {task.fields[records]:,}"),
        TextColumn("{task.fields[speed]:.0f}/с"),
        TimeElapsedColumn(),
        console=console,
        disable=args.dry_run or not console.is_terminal,
        redirect_stdout=False,
        redirect_stderr=False,
    )
    print(
        f"Датасет {args.dataset}",
        f"seed={args.seed}",
        f"заказов={args.orders}",
        f"цель={args.target}",
        sep=". ",
        file=sys.stderr,
    )
    try:
        if not args.dry_run:
            sink = Postgres(args) if args.target == "postgres" else Kafka(args)
            await sink.open()
        with progress:
            task = progress.add_task("Отправка", total=None, records=0, speed=0)
            while batch := list(islice(source, args.batch_size)):
                batch_started = time.monotonic()
                if sink is None:
                    for event in batch:
                        record = {
                            "topic": f"{args.topic_prefix}.{event['table']}",
                            "key": event["key"],
                            "value": build_kafka_value(event),
                        }
                        sys.stdout.write(
                            json.dumps(
                                record, ensure_ascii=False, separators=(",", ":")
                            )
                            + "\n"
                        )
                    records += len(batch)
                else:
                    records += await sink.send(batch)
                changes += len(batch)
                if args.rate and not args.dry_run:
                    # Slow receivers reduce throughput; do not compensate with a burst.
                    await asyncio.sleep(
                        max(
                            0,
                            len(batch) / args.rate - (time.monotonic() - batch_started),
                        )
                    )
                now = time.monotonic()
                progress.update(
                    task,
                    completed=changes,
                    records=records,
                    speed=changes / (now - started),
                )
                if (args.dry_run or not console.is_terminal) and now - reported >= 2:
                    print(
                        f"{'Сформировано' if args.dry_run else 'Подтверждено'} изменений: {changes}; записей: {records}; "
                        f"скорость: {changes / (now - started):.0f}/с; прошло {now - started:.1f}с",
                        file=sys.stderr,
                    )
                    reported = now
    except Exception as error:
        message = str(error) or type(error).__name__
        if args.dsn:
            try:
                password = conninfo_to_dict(args.dsn).get("password")
                if password:
                    message = message.replace(password, "***")
            except Exception:
                message = type(error).__name__
        print(f"Отправка не завершена: {message}", file=sys.stderr)
        print(
            "Часть датасета могла сохраниться; для нового прогона используйте новый --dataset.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    finally:
        if sink is not None:
            await sink.close()
    print_report(totals, changes, records, time.monotonic() - started)


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        print(
            "Остановлено. Для нового прогона используйте новый --dataset на всякий случай.",
            file=sys.stderr,
        )
        sys.exit(130)
    except Exception as error:
        print(
            f"Отправка не завершена ({type(error).__name__}).",
            "Проверьте доступность и конфигурацию kafka/postgres, права и уникальность --dataset.",
            file=sys.stderr,
        )
        sys.exit(1)
