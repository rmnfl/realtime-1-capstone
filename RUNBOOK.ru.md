# Ранбук

[English](RUNBOOK.md) | **Русский**

Диагностика сбоев и восстановление работы сервисов.
Выполняйте команды из корня репозитория. Конфигурация сервисов находится
в [compose.yaml](compose.yaml). SQL-скрипты ClickHouse лежат в [src/clickhouse](src/clickhouse).
В HTTP-примерах указаны порты по умолчанию. Если меняли их в `.env`,
подставьте свои значения. Команды `just` загружают `.env` автоматически.

[Первичная проверка](#первичная-проверка) · [API](#api-недоступен) ·
[ClickHouse](#api-возвращает-503) · [Redis](#redis-недоступен) ·
[PostgreSQL](#postgresql-недоступен) · [Kafka](#kafka-недоступна) ·
[Connect](#коннектор-или-задача-не-running) ·
[Старые данные](#api-возвращает-старые-данные) ·
[Мониторинг](#не-поступают-метрики) · [Проверка восстановления](#после-восстановления)

## Первичная проверка

До перезапуска сохраните время сбоя, датасет, параметры запроса,
ожидаемый результат и вывод команд. Ниже замените `demo` и период своими значениями.

```sh
just status
just logs api connect clickhouse
just api-ping
curl -i --max-time 12 --get \
  'http://127.0.0.1:18001/v1/datasets/demo/metrics/orders/summary' \
  --data-urlencode 'from=2026-01-01T00:00:00Z' \
  --data-urlencode 'to=2026-01-02T00:00:00Z' \
  --data-urlencode 'refresh=true'
```

Команда `just api-ping` должна вернуть `{"status":"ok"}`.
Для запроса сводки ожидается HTTP `200`.
Успешный `/ping` говорит только о доступности процесса API.
Параметр `refresh=true` обходит кэш, но использует уже загруженные данные ClickHouse.

| Наблюдение | Что проверить |
|---|---|
| `/ping` не отвечает | [API](#api-недоступен) |
| `/ping` работает, сводка возвращает `503` | [ClickHouse и ошибку SQL](#api-возвращает-503) |
| `429` | Параллелизм нагрузки: API допускает до 4 одновременных запросов к ClickHouse и ждёт свободного места не дольше 0,5 с |
| `422` | Параметры запроса; тело ответа указывает ошибку валидации |
| Ответ `200`, в логах ошибки Redis | [Redis](#redis-недоступен); API может продолжать работу без кэша |
| Ответ `200`, значения не меняются | [Доставку события](#api-возвращает-старые-данные) |
| Ошибка только на графике или плитке | [Сбор метрик](#не-поступают-метрики) и прямую проверку сервиса |

Не удаляйте volumes, коннектор, слот репликации или consumer group
и не сбрасывайте offsets ради перезапуска: можно потерять данные или позицию чтения.
При повторном падении разберите причину по логам.

## API недоступен

Алерт `ApiUnavailable` срабатывает, когда Prometheus не может получить `/metrics`.
Проверьте сам API и соединение между ним и Prometheus.

```sh
just logs api
just status
```

После устранения причины остановки запустите API:

```sh
just api-start
just api-ping
```

Убедитесь, что контейнер в статусе `healthy`, а `/ping` возвращает `{"status":"ok"}`.
Повторите запрос сводки из начала ранбука.
Если ответ `503`, перейдите к следующему разделу.
Если API отвечает напрямую, но алерт активен, проверьте target `api` в Prometheus.

## API возвращает 503

Алерт `ApiErrors` срабатывает, когда доля 5xx на одном бизнес-эндпоинте
превышает 50% за последние 30 секунд и остаётся выше порога ещё 10 секунд.

```sh
just logs api clickhouse
just ch --query 'SELECT 1'
```

Если ClickHouse доступен, проверьте подключение с учётной записью API:

```sh
docker compose exec -T clickhouse clickhouse-client \
  --user reader --password reader --database capstone --query 'SELECT 1'
```

Ожидаемый результат `1` подтверждает доступ к серверу, но не наличие витрин. Найдите в логах API и ClickHouse причину ошибки:
отсутствующую таблицу, нехватку прав, неверный SQL или тайм-аут.
Запросы API лежат в [src/api/sql](src/api/sql).
Команда `just clickhouse-setup` использует `IF NOT EXISTS` и не меняет существующие таблицы.

Если контейнер ClickHouse остановлен, сначала проверьте подключённый volume:

```sh
docker inspect --format '{{json .Mounts}}' "$(docker compose ps -a -q clickhouse)"
```

При ошибке диска или повреждении таблиц сначала проверьте хранилище.
Если причина устранена и подключён прежний volume, запустите сервис:

```sh
just clickhouse-start
just ch --query 'SELECT 1'
```

Повторите запрос сводки с `refresh=true`. Ожидается HTTP `200`.
Обычные запросы могли отвечать из Redis до истечения TTL при недоступном ClickHouse. После запуска проверьте
[обработку событий Kafka](#api-возвращает-старые-данные).

## Redis недоступен

Отказ Redis отслеживают `RedisUnavailable` и `RedisErrors`.
Второй алерт реагирует на ошибки обращений API к кэшу;
если таких запросов нет, он не сработает.

```sh
just logs redis redis-exporter api
docker compose exec -T redis redis-cli PING
```

Redis должен ответить `PONG`. Если контейнер остановлен, запустите его:

```sh
just redis-start
docker compose exec -T redis redis-cli PING
```

Сделайте запрос сводки с `refresh=true`, затем два запроса с теми же параметрами
без refresh. До истечения TTL поле `meta.computed_at` должно совпадать:
API возвращает ответ из кэша. В логах должны перестать появляться
ошибки `Redis cache`. Если Redis отвечает, но экспортер не подключается,
проверьте его логи и target `redis` отдельно.

Без Redis API обращается к ClickHouse напрямую и может отвечать `200`,
но нагрузка на ClickHouse растёт.
Очищать кэш для восстановления соединения не нужно.

## PostgreSQL недоступен

Алерты: `PostgresUnavailable`, возможен `CdcTaskNotRunning`.
API при этом может отвечать из ClickHouse, но новых данных из источника не будет.

```sh
just logs postgres postgres-exporter connect
just pg -c 'SELECT 1'
```

Запрос должен вернуть `1`. После устранения причины остановки запустите PostgreSQL:

```sh
just postgres-start
just pg -c 'SELECT 1'
just cdc-status
```

Проверьте слот репликации и объём удерживаемого WAL:

```sh
just pg -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots WHERE slot_name = 'capstone_slot';"
```

Когда коннектор читает WAL, у слота `capstone_slot` должно быть `active=true`.
Сравните `confirmed_flush_lsn` до и после изменения в источнике:
после его обработки значение должно увеличиться.
Пока Connect не работает, слот может удерживать WAL. Проверьте место на диске:

```sh
docker compose exec -T postgres df -h /var/lib/postgresql/data
```

Если слот утрачен или нужный участок WAL уже удалён, перезапуск не вернёт
пропущенные изменения. Сохраните логи и позиции чтения.
Перед повторным snapshot подготовьте план сверки с ClickHouse. Snapshot загрузит
текущие строки источника, но не восстановит информацию о ранее удалённых записях.

## Kafka недоступна

```sh
just logs kafka connect clickhouse
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

В списке должны быть топики `capstone.capstone.orders`,
`capstone.capstone.order_items` и `capstone.capstone.payments`.
После устранения причины остановки запустите брокер:

```sh
just kafka-start
just cdc-status
```

Проверьте список топиков, offsets и данные API.
После восстановления Kafka у задачи Connect может остаться статус `RUNNING`,
хотя новые события не поступают. Если исходное изменение есть,
но offsets стоят, изучите логи Connect.
После устранения причины выполните `just connect-restart` и повторите сверку.

При длительном простое проверьте диски Kafka и PostgreSQL.
Connect может перестать подтверждать чтение WAL, ClickHouse перестанет
получать события из Kafka. Если volume Kafka утрачен, сначала определите,
какие данные и offsets потеряны. Пересоздание топиков не вернёт историю.

## Коннектор или задача не RUNNING

За состояние задачи отвечает алерт `CdcTaskNotRunning`.

```sh
just cdc-status
just logs connect
```

У коннектора должен быть `connector.state=RUNNING`, у задачи `tasks[0].state=RUNNING`.
Если задача в `FAILED`, прочитайте `trace` и сопоставьте его с логами Connect.
В зависимости от ошибки проверьте подключение к PostgreSQL или Kafka, права и слот репликации.

Если остановлен Connect, запустите его:

```sh
just connect-start
just cdc-status
```

Если причина устранена, а задача в `FAILED`, перезапустите worker.
Это также можно сделать при `RUNNING`, если проверка источника, offsets
и логов подтвердила, что обработка новых событий остановилась:

```sh
just connect-restart
```

Команда перезапускает Connect целиком; в стенде один коннектор.
Повторно проверьте оба статуса и прохождение изменений до ClickHouse.
Статус `RUNNING` не гарантирует обработку новых событий.

При ответе REST API `404` выясните причину отсутствия коннектора
и проверьте сохранность данных Kafka, offsets и слота репликации.
Команда `just cdc-register` регистрирует конфигурацию;
она не восстанавливает потерянное состояние CDC.

## API возвращает старые данные

### Исключить кэш и проверить источник

Сравните сводку с refresh и без него при одинаковых параметрах.
Если с `refresh=true` данные обновились, дождитесь TTL 30 секунд или используйте refresh.
Чтение не продлевает TTL. Новое значение `computed_at` подтверждает новый расчёт,
но не свежесть исходных данных.

Если оба ответа старые, проверьте запись в PostgreSQL.
В запросе подставьте датасет и ID заказа:

```sh
just pg -c "SELECT id, status, amount_cents, version, updated_at
FROM capstone.orders WHERE dataset_id = 'show_01' AND id = 'o1';"
just cdc-status
```

Проверьте, что транзакция закоммичена, а версия заказа увеличилась.
Для платежей проверьте `capstone.payments`, для позиций `capstone.order_items`.
Если изменения в источнике нет, искать причину в CDC пока рано.

### Проверить Kafka и consumer ClickHouse

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group capstone-orders-clickhouse
```

Для позиций используется группа `capstone-order-items-clickhouse`,
для платежей `capstone-payments-clickhouse`.
Выполните проверку ещё раз через 10 секунд и сравните offsets.

| Наблюдение | Следующее действие |
|---|---|
| При ожидаемых событиях `LOG-END-OFFSET` не растёт | Проверить источник, Connect и его ошибки |
| Конец журнала растёт, `CURRENT-OFFSET` стоит | Проверить consumer ClickHouse и MV |
| Обе позиции растут, lag увеличивается | Сравнить скорость поступления и обработки, проверить ресурсы ClickHouse |
| Committed offsets догнали нужную границу, ответ неверен | Сверить raw, последние версии и SQL API |
| Группа или committed offset отсутствует | Проверить имя группы и состояние Kafka-таблицы; не сбрасывать offsets |

Алерт `ClickHouseConsumerLag` срабатывает, если суммарный lag группы
по топику больше нуля в течение 30 секунд. Нулевой lag сам по себе
не подтверждает свежесть: Connect мог ещё не опубликовать изменение в Kafka.

В ClickHouse 25.8 при проверке удаления обнаружен отдельный случай:
консьюмер прочитал последний tombstone, но не закоммитил его offset.
Lag остаётся равным 1 и алерт активен, хотя DELETE уже обработан.
Сравните `assignments.current_offset` из запроса ниже с committed offset
и концом партиции. Прочитайте последнее сообщение по этим координатам
и проверьте удаление в `*_latest` и API.
Исключать его из сверки бизнес-событий можно только после проверки `value=null`.
Следующее обычное сообщение в той же партиции продвигает committed offset.
Сбрасывать offsets не нужно.

```sh
just ch --query "SELECT table, assignments.topic, assignments.partition_id,
  assignments.current_offset, last_poll_time, exceptions.time, exceptions.text
FROM system.kafka_consumers WHERE database = 'capstone' FORMAT Vertical"
just ch --query "SELECT name, engine FROM system.tables
WHERE database = 'capstone' ORDER BY name"
just ch --query "SELECT table FROM system.detached_tables
WHERE database = 'capstone'"
```

Проверьте наличие Kafka-таблиц `*_cdc_queue`, MV `*_cdc_to_raw`
и `*_raw_to_current`, таблиц raw/current и представлений latest.
Определения находятся в [setup](src/clickhouse/setup).
Проверяйте время ошибок: старые исключения сохраняются в истории консьюмера
после восстановления обработки.

Если MV отключена, выясните причину перед `ATTACH`.
Новая MV обрабатывает только новые вставки: создание её поверх непустого raw
не загрузит старые строки в current. Для них нужен backfill.
Перед ним сверьте диапазон данных и правила дедупликации.

Если простой был длительным, проверьте, какие offsets ещё доступны в топике:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic capstone.capstone.orders --time -2
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 --entity-type topics \
  --entity-name capstone.capstone.orders --describe --all
docker compose exec -T kafka df -h /var/lib/kafka/data
```

Первая команда выводит earliest offset партиций.
Если он уже больше committed offset, часть неподтверждённых сообщений
удалена из Kafka. При этом данные могли попасть в ClickHouse до commit offset.
Сохраните позиции и сверьте сами записи, прежде чем планировать повторную загрузку.

### Сверить raw, latest и API

```sh
just ch --query "SELECT id, status, amount_cents, version, is_deleted,
  _source_lsn, _operation, _kafka_partition, _kafka_offset, _ingested_at
FROM capstone.orders_raw WHERE dataset_id = 'show_01' AND id = 'o1'
ORDER BY version, _ingested_at"
just ch --query "SELECT id, status, amount_cents, version, is_deleted
FROM capstone.orders_latest WHERE dataset_id = 'show_01' AND id = 'o1'"
```

Если версии нет в raw, проверьте CDC и чтение Kafka.
Если она есть в raw, но отсутствует в latest, проверьте MV из raw в current
и правило выбора версии. Если latest верен, проверьте период, кампанию, фильтр `is_deleted` и SQL API.

Для позиций сравнивайте `_source_lsn` и `is_deleted`.
Для платежей проверьте ID, тип и сумму каждой операции:
`payments` обрабатывается как append-only журнал.
У физически удалённого заказа в latest должна остаться версия с `is_deleted=1`.

## Не поступают метрики

Алерты: `PipelineMetricsUnavailable`, `PipelineMetricsCollectionFailed`.

```sh
just status
just logs prometheus postgres-exporter redis-exporter kafka-exporter
just prometheus-config-check
```

На странице [Targets](http://127.0.0.1:19005/targets) найдите нужный job
и прочитайте `Last Error`. Всего настроено восемь jobs: `api`, `postgres`,
`redis`, `kafka`, `connect`, `kafka-consumers`, `clickhouse` и `pipeline`.
Статус target `UP` подтверждает получение метрик. Для проверки самих PostgreSQL
и Redis используются `pg_up` и `redis_up`.
Ошибка target `pipeline` может быть вызвана запросом к `pipeline_metrics`,
даже если ClickHouse отвечает.

Алерт `PipelineMetricsCollectionFailed` сообщает об ошибке сбора внутри
экспортера PostgreSQL или JMX-агента Kafka/Connect.
Проверьте их логи, права и конфигурацию.
Если Prometheus остановлен, он не вычисляет алерты.

После правки конфигурации Prometheus выполните:

```sh
just prometheus-config-reload
```

Команда проверяет правила и отправляет Prometheus SIGHUP для перечитывания
конфигурации. Остановленный контейнер сначала запустите через
`docker compose up -d --wait prometheus`.
Если метрики поступают, но в Grafana нет данных, проверьте подключение
к Prometheus, выбранный период и `just logs grafana`.
Источник данных и дашборды настроены через
[provisioning](monitoring/grafana/provisioning).

## После восстановления

1. Повторите прямую проверку отказавшего сервиса и запрос API с `refresh=true`.
2. Для каждой партиции запишите offset последнего нужного бизнес-события
   плюс один. Дождитесь, пока committed offset достигнет этой границы.
   Обычно ориентиром служит `LOG-END-OFFSET`; последний tombstone проверьте
   отдельно по инструкции выше. При продолжающемся потоке
   ориентируйтесь на записанную границу.
3. Сверьте записи с источником или сохранёнными ожиданиями: версии, статусы,
   суммы и удаления. Совпадения одной итоговой суммы недостаточно для проверки полноты.
4. Сделайте новое изменение в источнике, проверьте его в API и повторите события.
5. Убедитесь, что метрики поступают, а связанные алерты перешли в `Inactive`
   на странице [Alerts](http://127.0.0.1:19005/alerts).

Проверьте новые события и дедупликацию в новом датасете:

```sh
just demo-create recovery_01
just demo-pay recovery_01
just demo-duplicates recovery_01
```

В API должны остаться один оплаченный заказ на 10000 копеек,
два SKU, три единицы товара и одна оплата. Дубли добавляют строки в raw,
но не меняют результаты пяти эндпоинтов. Этот сценарий проверяет новые события;
накопившиеся за время сбоя данные сверьте отдельно в шагах 2 и 3.

Если `demo-pay` не дождался API и завершился по тайм-ауту, транзакция в PostgreSQL
могла уже закоммититься. Проверьте источник и `just demo-show recovery_01`.
Не повторяйте оплату до выяснения состояния заказа и платежа.

Алерты с окном 30 секунд снимаются не сразу после устранения ошибки.
Продолжайте выполнять запросы, проверяйте ответы и поступление новых метрик.
Пустой график или исчезнувшая серия не означают, что сервис восстановлен.

Запишите причину, действия, время восстановления и результат сверки.
Если проверена часть данных, укажите датасет и диапазон.

Последний прогон выполнен 11 ноября 2026 года. Проверены остановка и запуск
PostgreSQL, Kafka, Connect, ClickHouse, API и Redis, отключение MV загрузки
из Kafka и ошибка прав экспортера PostgreSQL. Также выполнен перезапуск
всего стенда через `just down` и `just start` с сохранением volumes.
После каждого отказа сверены источник и ответы всех пяти эндпоинтов.
Все девять алертов сработали и снялись после восстановления.
После запуска Kafka новые события не поступали в течение 45 секунд,
хотя задача оставалась в `RUNNING`. Перезапуск через `just connect-restart`
возобновил обработку. Общий перезапуск сохранил данные и offsets,
повторного snapshot не было.
