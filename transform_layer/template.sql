DROP VIEW IF EXISTS analytics.series_mv;
DROP TABLE IF EXISTS analytics.series_kafka;
DROP TABLE IF EXISTS analytics.series;

CREATE TABLE analytics.series_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.series',
  kafka_group_name = 'ch_series_v3',   -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.series
(
  id Int64,
  series_id String,
  ticker String,
  slug String,
  title String,
  series_type String,
  recurrence String,
  active Nullable(UInt8),
  closed Nullable(UInt8),
  published_at Nullable(DateTime64(3)),
  updated_at Nullable(DateTime64(3)),
  start_date Nullable(DateTime64(3)),
  created_at Nullable(DateTime64(3)),

  _op String,
  _lsn UInt64,
  _ts  DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (series_id, id);

CREATE MATERIALIZED VIEW analytics.series_mv
TO analytics.series
AS
SELECT
  toInt64OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  JSON_VALUE(payload, '$.payload.series_id') AS series_id,
  JSON_VALUE(payload, '$.payload.ticker') AS ticker,
  JSON_VALUE(payload, '$.payload.slug') AS slug,
  JSON_VALUE(payload, '$.payload.title') AS title,
  JSON_VALUE(payload, '$.payload.series_type') AS series_type,
  JSON_VALUE(payload, '$.payload.recurrence') AS recurrence,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.active')) AS active,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.closed')) AS closed,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.published_at')) AS published_at,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.updated_at')) AS updated_at,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.start_date')) AS start_date,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.created_at')) AS created_at,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.series_kafka
WHERE payload != '';



DROP VIEW IF EXISTS analytics.order_book_snapshots_mv;
DROP TABLE IF EXISTS analytics.order_book_snapshots_kafka;
DROP TABLE IF EXISTS analytics.order_book_snapshots;

CREATE TABLE analytics.order_book_snapshots_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.order_book_snapshots',
  kafka_group_name = 'ch_order_book_snapshots_v2', -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.order_book_snapshots
(
  id Int64,
  token_id String,
  top_price Nullable(Float64),
  top_size Nullable(Float64),
  book String,  -- Debezium sends this JSON as a string; keep as-is or parse later
  side String,
  snapshot_timestamp DateTime64(3),

  _op String,
  _lsn UInt64,
  _ts DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (token_id, id);

CREATE MATERIALIZED VIEW analytics.order_book_snapshots_mv
TO analytics.order_book_snapshots
AS
SELECT
  toInt64OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  JSON_VALUE(payload, '$.payload.token_id') AS token_id,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.top_price')) AS top_price,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.top_size')) AS top_size,
  JSON_VALUE(payload, '$.payload.book') AS book,
  JSON_VALUE(payload, '$.payload.side') AS side,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.snapshot_timestamp')) AS snapshot_timestamp,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.order_book_snapshots_kafka
WHERE payload != '';



DROP VIEW IF EXISTS analytics.order_book_updates_mv;
DROP TABLE IF EXISTS analytics.order_book_updates_kafka;
DROP TABLE IF EXISTS analytics.order_book_updates;

CREATE TABLE analytics.order_book_updates_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.order_book_updates',
  kafka_group_name = 'ch_order_book_updates_v2',  -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.order_book_updates
(
  id Int64,
  token_id String,
  price Float64,
  size Float64,
  side String,
  update_timestamp DateTime64(3),
  send_timestamp Nullable(DateTime64(3)),
  arrival_timestamp Nullable(DateTime64(3)),

  _op String,
  _lsn UInt64,
  _ts DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (token_id, id);

CREATE MATERIALIZED VIEW analytics.order_book_updates_mv
TO analytics.order_book_updates
AS
SELECT
  toInt64OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  JSON_VALUE(payload, '$.payload.token_id') AS token_id,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.price')) AS price,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.size')) AS size,
  JSON_VALUE(payload, '$.payload.side') AS side,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.update_timestamp')) AS update_timestamp,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.send_timestamp')) AS send_timestamp,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.arrival_timestamp')) AS arrival_timestamp,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.order_book_updates_kafka
WHERE payload != '';


DROP VIEW IF EXISTS analytics.events_mv;
DROP TABLE IF EXISTS analytics.events_kafka;
DROP TABLE IF EXISTS analytics.events;

CREATE TABLE analytics.events_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.events',
  kafka_group_name = 'ch_events_v2',   -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.events
(
  id Int64,
  event_id String,
  series_id Nullable(String),
  parent_event_id Nullable(String),
  ticker Nullable(String),
  slug Nullable(String),
  title Nullable(String),
  description Nullable(String),
  resolution_source Nullable(String),
  category Nullable(String),
  subcategory Nullable(String),
  active Nullable(UInt8),
  closed Nullable(UInt8),
  archived Nullable(UInt8),
  new Nullable(UInt8),
  featured Nullable(UInt8),
  restricted Nullable(UInt8),
  cyom Nullable(UInt8),
  enable_order_book Nullable(UInt8),
  neg_risk Nullable(UInt8),
  enable_neg_risk Nullable(UInt8),
  neg_risk_augmented Nullable(UInt8),
  automatically_active Nullable(UInt8),
  automatically_resolved Nullable(UInt8),
  creation_date Nullable(DateTime64(3)),
  create_at Nullable(DateTime64(3)),
  updated_at Nullable(DateTime64(3)),
  start_date Nullable(DateTime64(3)),
  start_time Nullable(DateTime64(3)),
  end_date Nullable(DateTime64(3)),
  closed_time Nullable(DateTime64(3)),
  finished_timestamp Nullable(DateTime64(3)),
  volume Nullable(Float64),
  open_interest Nullable(Float64),
  liquidity Nullable(Float64),
  volume_24hr Nullable(Float64),
  volume_1wk Nullable(Float64),
  volume_1mo Nullable(Float64),
  liquidity_amm Nullable(Float64),
  liquidity_clob Nullable(Float64),
  comment_count Nullable(Int32),

  _op String,
  _lsn UInt64,
  _ts DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (event_id, id);

CREATE MATERIALIZED VIEW analytics.events_mv
TO analytics.events
AS
SELECT
  toInt64OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  JSON_VALUE(payload, '$.payload.event_id') AS event_id,
  JSON_VALUE(payload, '$.payload.series_id') AS series_id,
  JSON_VALUE(payload, '$.payload.parent_event_id') AS parent_event_id,
  JSON_VALUE(payload, '$.payload.ticker') AS ticker,
  JSON_VALUE(payload, '$.payload.slug') AS slug,
  JSON_VALUE(payload, '$.payload.title') AS title,
  JSON_VALUE(payload, '$.payload.description') AS description,
  JSON_VALUE(payload, '$.payload.resolution_source') AS resolution_source,
  JSON_VALUE(payload, '$.payload.category') AS category,
  JSON_VALUE(payload, '$.payload.subcategory') AS subcategory,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.active')) AS active,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.closed')) AS closed,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.archived')) AS archived,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.new')) AS new,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.featured')) AS featured,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.restricted')) AS restricted,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.cyom')) AS cyom,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.enable_order_book')) AS enable_order_book,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.neg_risk')) AS neg_risk,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.enable_neg_risk')) AS enable_neg_risk,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.neg_risk_augmented')) AS neg_risk_augmented,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.automatically_active')) AS automatically_active,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.automatically_resolved')) AS automatically_resolved,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.creation_date')) AS creation_date,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.create_at')) AS create_at,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.updated_at')) AS updated_at,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.start_date')) AS start_date,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.start_time')) AS start_time,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.end_date')) AS end_date,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.closed_time')) AS closed_time,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.finished_timestamp')) AS finished_timestamp,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume')) AS volume,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.open_interest')) AS open_interest,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.liquidity')) AS liquidity,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_24hr')) AS volume_24hr,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_1wk')) AS volume_1wk,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_1mo')) AS volume_1mo,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.liquidity_amm')) AS liquidity_amm,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.liquidity_clob')) AS liquidity_clob,
  toInt32OrNull(JSON_VALUE(payload, '$.payload.comment_count')) AS comment_count,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.events_kafka
WHERE payload != '';



DROP VIEW IF EXISTS analytics.markets_mv;
DROP TABLE IF EXISTS analytics.markets_kafka;
DROP TABLE IF EXISTS analytics.markets;

CREATE TABLE analytics.markets_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.markets',
  kafka_group_name = 'ch_markets_v2',  -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.markets
(
  id Int64,
  market_id String,
  event_id Nullable(String),
  condition_id Nullable(String),
  slug Nullable(String),
  resolution_source Nullable(String),
  game_id Nullable(String),
  sports_market_type Nullable(String),
  question Nullable(String),
  description Nullable(String),
  category Nullable(String),
  subcategory Nullable(String),
  market_type Nullable(String),
  market_maker_address Nullable(String),
  outcomes Array(String),
  clob_token_ids Array(String),
  active Nullable(UInt8),
  closed Nullable(UInt8),
  fpmm_live Nullable(UInt8),
  ready Nullable(UInt8),
  funded Nullable(UInt8),
  approved Nullable(UInt8),
  neg_risk Nullable(UInt8),
  neg_risk_other Nullable(UInt8),
  accepting_orders Nullable(UInt8),
  holding_rewards_enabled Nullable(UInt8),
  enable_order_book Nullable(UInt8),
  comments_enabled Nullable(UInt8),
  uma_resolution_status Nullable(String),
  rewards_min_size Nullable(Float64),
  rewards_max_spread Nullable(Float64),
  clob_rewards Nullable(Float64),
  seconds_delay Nullable(Float64),
  lower_bound Nullable(Float64),
  upper_bound Nullable(Float64),
  order_price_min_tick_size Nullable(Float64),
  order_min_size Nullable(Float64),
  maker_base_fee Nullable(Float64),
  taker_base_fee Nullable(Float64),
  spread Nullable(Float64),
  last_trade_price Nullable(Float64),
  best_bid Nullable(Float64),
  best_ask Nullable(Float64),
  fee Nullable(Float64),
  start_date Nullable(DateTime64(3)),
  end_date Nullable(DateTime64(3)),
  created_at Nullable(DateTime64(3)),
  updated_at Nullable(DateTime64(3)),
  closed_time Nullable(DateTime64(3)),
  game_start_time Nullable(DateTime64(3)),
  event_start_time Nullable(DateTime64(3)),
  accepting_orders_timestamp Nullable(DateTime64(3)),
  liquidity Nullable(Float64),
  liquidity_num Nullable(Float64),
  volume Nullable(Float64),
  volume_num Nullable(Float64),
  volume_24h Nullable(Float64),
  volume_1wk Nullable(Float64),
  volume_1mo Nullable(Float64),
  volume_1yr Nullable(Float64),

  _op String,
  _lsn UInt64,
  _ts DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (market_id, id);

CREATE MATERIALIZED VIEW analytics.markets_mv
TO analytics.markets
AS
SELECT
  toInt64OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  JSON_VALUE(payload, '$.payload.market_id') AS market_id,
  JSON_VALUE(payload, '$.payload.event_id') AS event_id,
  JSON_VALUE(payload, '$.payload.condition_id') AS condition_id,
  JSON_VALUE(payload, '$.payload.slug') AS slug,
  JSON_VALUE(payload, '$.payload.resolution_source') AS resolution_source,
  JSON_VALUE(payload, '$.payload.game_id') AS game_id,
  JSON_VALUE(payload, '$.payload.sports_market_type') AS sports_market_type,
  JSON_VALUE(payload, '$.payload.question') AS question,
  JSON_VALUE(payload, '$.payload.description') AS description,
  JSON_VALUE(payload, '$.payload.category') AS category,
  JSON_VALUE(payload, '$.payload.subcategory') AS subcategory,
  JSON_VALUE(payload, '$.payload.market_type') AS market_type,
  JSON_VALUE(payload, '$.payload.market_maker_address') AS market_maker_address,
  JSONExtract(payload, 'payload.outcomes', 'Array(String)') AS outcomes,
  JSONExtract(payload, 'payload.clob_token_ids', 'Array(String)') AS clob_token_ids,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.active')) AS active,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.closed')) AS closed,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.fpmm_live')) AS fpmm_live,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.ready')) AS ready,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.funded')) AS funded,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.approved')) AS approved,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.neg_risk')) AS neg_risk,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.neg_risk_other')) AS neg_risk_other,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.accepting_orders')) AS accepting_orders,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.holding_rewards_enabled')) AS holding_rewards_enabled,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.enable_order_book')) AS enable_order_book,
  toUInt8OrNull(JSON_VALUE(payload, '$.payload.comments_enabled')) AS comments_enabled,
  JSON_VALUE(payload, '$.payload.uma_resolution_status') AS uma_resolution_status,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.rewards_min_size')) AS rewards_min_size,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.rewards_max_spread')) AS rewards_max_spread,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.clob_rewards')) AS clob_rewards,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.seconds_delay')) AS seconds_delay,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.lower_bound')) AS lower_bound,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.upper_bound')) AS upper_bound,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.order_price_min_tick_size')) AS order_price_min_tick_size,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.order_min_size')) AS order_min_size,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.maker_base_fee')) AS maker_base_fee,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.taker_base_fee')) AS taker_base_fee,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.spread')) AS spread,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.last_trade_price')) AS last_trade_price,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.best_bid')) AS best_bid,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.best_ask')) AS best_ask,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.fee')) AS fee,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.start_date')) AS start_date,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.end_date')) AS end_date,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.created_at')) AS created_at,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.updated_at')) AS updated_at,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.closed_time')) AS closed_time,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.game_start_time')) AS game_start_time,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.event_start_time')) AS event_start_time,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.accepting_orders_timestamp')) AS accepting_orders_timestamp,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.liquidity')) AS liquidity,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.liquidity_num')) AS liquidity_num,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume')) AS volume,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_num')) AS volume_num,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_24h')) AS volume_24h,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_1wk')) AS volume_1wk,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_1mo')) AS volume_1mo,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.volume_1yr')) AS volume_1yr,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.markets_kafka
WHERE payload != '';


DROP VIEW IF EXISTS analytics.tokens_mv;
DROP TABLE IF EXISTS analytics.tokens_kafka;
DROP TABLE IF EXISTS analytics.tokens;

CREATE TABLE analytics.tokens_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.tokens',
  kafka_group_name = 'ch_tokens_v2',  -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.tokens
(
  id Int32,
  token_id Nullable(String),
  market_id String,
  outcome Nullable(String),

  _op String,
  _lsn UInt64,
  _ts DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (market_id, id);

CREATE MATERIALIZED VIEW analytics.tokens_mv
TO analytics.tokens
AS
SELECT
  toInt32OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  JSON_VALUE(payload, '$.payload.token_id') AS token_id,
  JSON_VALUE(payload, '$.payload.market_id') AS market_id,
  JSON_VALUE(payload, '$.payload.outcome') AS outcome,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.tokens_kafka
WHERE payload != '';

DROP VIEW IF EXISTS analytics.chainlink_prices_mv;
DROP TABLE IF EXISTS analytics.chainlink_prices_kafka;
DROP TABLE IF EXISTS analytics.chainlink_prices;

CREATE TABLE analytics.chainlink_prices_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.chainlink_prices',
  kafka_group_name = 'ch_chainlink_prices_v1', -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.chainlink_prices
(
  id Int64,
  ingested_at DateTime64(3),
  source String,
  symbol String,
  value Float64,
  full_accuracy_value String,
  update_timestamp DateTime64(3),
  send_timestamp Nullable(DateTime64(3)),
  arrival_timestamp Nullable(DateTime64(3)),
  raw_payload String,

  _op String,
  _lsn UInt64,
  _ts DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (symbol, update_timestamp, id);

CREATE MATERIALIZED VIEW analytics.chainlink_prices_mv
TO analytics.chainlink_prices
AS
SELECT
  toInt64OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.ingested_at')) AS ingested_at,
  JSON_VALUE(payload, '$.payload.source') AS source,
  JSON_VALUE(payload, '$.payload.symbol') AS symbol,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.value')) AS value,
  JSON_VALUE(payload, '$.payload.full_accuracy_value') AS full_accuracy_value,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.update_timestamp')) AS update_timestamp,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.send_timestamp')) AS send_timestamp,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.arrival_timestamp')) AS arrival_timestamp,
  JSON_VALUE(payload, '$.payload.raw_payload') AS raw_payload,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.chainlink_prices_kafka
WHERE payload != '';




DROP TABLE IF EXISTS analytics.binance_prices_kafka;
DROP VIEW IF EXISTS analytics.binance_prices_mv;
DROP TABLE IF EXISTS analytics.binance_prices;

CREATE TABLE analytics.binance_prices_kafka
(
  payload String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'postgres.public.binance_prices',
  kafka_group_name = 'ch_binance_prices_v1', -- new group to re-read from start
  kafka_format = 'JSONAsString',
  kafka_handle_error_mode = 'stream';

CREATE TABLE analytics.binance_prices
(
  id Int64,
  ingested_at DateTime64(3),
  source String,
  symbol String,
  value Float64,
  full_accuracy_value String,
  update_timestamp DateTime64(3),
  send_timestamp Nullable(DateTime64(3)),
  arrival_timestamp Nullable(DateTime64(3)),
  raw_payload String,

  _op String,
  _lsn UInt64,
  _ts DateTime64(3)
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (symbol, update_timestamp, id);

CREATE MATERIALIZED VIEW analytics.binance_prices_mv
TO analytics.binance_prices
AS
SELECT
  toInt64OrNull(JSON_VALUE(payload, '$.payload.id')) AS id,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.ingested_at')) AS ingested_at,
  JSON_VALUE(payload, '$.payload.source') AS source,
  JSON_VALUE(payload, '$.payload.symbol') AS symbol,
  toFloat64OrNull(JSON_VALUE(payload, '$.payload.value')) AS value,
  JSON_VALUE(payload, '$.payload.full_accuracy_value') AS full_accuracy_value,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.update_timestamp')) AS update_timestamp,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.send_timestamp')) AS send_timestamp,
  parseDateTime64BestEffortOrNull(JSON_VALUE(payload, '$.payload.arrival_timestamp')) AS arrival_timestamp,
  JSON_VALUE(payload, '$.payload.raw_payload') AS raw_payload,
  coalesce(JSON_VALUE(payload, '$.__op'), JSON_VALUE(payload, '$.payload.__op'), '') AS _op,
  coalesce(
    toUInt64OrNull(JSON_VALUE(payload, '$.__lsn')),
    toUInt64OrNull(JSON_VALUE(payload, '$.payload.__lsn')),
    toUInt64(0)
  ) AS _lsn,
  coalesce(
    fromUnixTimestamp64Milli(
      coalesce(
        toInt64OrNull(JSON_VALUE(payload, '$.__ts_ms')),
        toInt64OrNull(JSON_VALUE(payload, '$.payload.__ts_ms'))
      )
    ),
    toDateTime64(0, 3)
  ) AS _ts
FROM analytics.binance_prices_kafka
WHERE payload != '';
