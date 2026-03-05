CREATE DATABASE IF NOT EXISTS quantify
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE quantify;

CREATE TABLE IF NOT EXISTS bot_runs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  run_uid VARCHAR(128) NOT NULL,
  generated_at_iso VARCHAR(64) NOT NULL,
  generated_at_local VARCHAR(64) NULL,
  date_key VARCHAR(16) NULL,
  source_file VARCHAR(255) NULL,
  total_rows INT NULL,
  buy_count INT NULL,
  hold_count INT NULL,
  reduce_count INT NULL,
  discovery_found INT NULL,
  discovery_skip INT NULL,
  progress_stage VARCHAR(64) NULL,
  progress_city VARCHAR(64) NULL,
  progress_city_index INT NULL,
  progress_total_cities INT NULL,
  payload_json LONGTEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_bot_runs_run_uid (run_uid),
  KEY idx_bot_runs_generated_at_iso (generated_at_iso),
  KEY idx_bot_runs_date_key (date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS bot_actions (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  run_id BIGINT UNSIGNED NOT NULL,
  action_index INT NOT NULL,
  city VARCHAR(64) NULL,
  action_date VARCHAR(32) NULL,
  date_label VARCHAR(32) NULL,
  action_signal VARCHAR(16) NULL,
  action_side VARCHAR(8) NULL,
  label VARCHAR(255) NULL,
  token_id VARCHAR(128) NULL,
  opposite_token_id VARCHAR(128) NULL,
  condition_id TEXT NULL,
  question TEXT NULL,
  market_price DOUBLE NULL,
  fair_prob DOUBLE NULL,
  edge DOUBLE NULL,
  edge_ratio DOUBLE NULL,
  hold_reason TEXT NULL,
  exit_reason VARCHAR(255) NULL,
  raw_json LONGTEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_bot_actions_run_idx (run_id, action_index),
  KEY idx_bot_actions_signal (action_signal),
  KEY idx_bot_actions_city_date (city, action_date),
  KEY idx_bot_actions_token (token_id),
  CONSTRAINT fk_bot_actions_run_id
    FOREIGN KEY (run_id) REFERENCES bot_runs(id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS bot_diagnostics (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  diag_uid VARCHAR(128) NOT NULL,
  generated_at_iso VARCHAR(64) NOT NULL,
  generated_at_local VARCHAR(64) NULL,
  mode VARCHAR(64) NULL,
  buy_count INT NULL,
  hold_count INT NULL,
  reduce_count INT NULL,
  rows_count INT NULL,
  payload_json LONGTEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_bot_diagnostics_uid (diag_uid),
  KEY idx_bot_diagnostics_generated_at_iso (generated_at_iso)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS fact_bot_actions (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  event_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
  city VARCHAR(50) NULL COMMENT '城市',
  date_label VARCHAR(50) NULL COMMENT '日期标签',
  condition_id TEXT NULL COMMENT '条件ID',
  token_id VARCHAR(100) NULL COMMENT 'Token ID',
  trade_signal VARCHAR(20) NULL COMMENT '动作信号(BUY/HOLD/REDUCE)',
  market_price DOUBLE NULL COMMENT '市场价格',
  fair_prob DOUBLE NULL COMMENT '模型公平概率',
  edge DOUBLE NULL COMMENT '边际优势',
  dynamic_buy_usdc DOUBLE NULL COMMENT '动态买入金额(USDC)',
  hold_reason TEXT NULL COMMENT '持有/未交易原因',
  PRIMARY KEY (id),
  KEY idx_fact_actions_time (event_ts),
  KEY idx_fact_actions_city_date (city, date_label),
  KEY idx_fact_actions_signal (trade_signal),
  KEY idx_fact_actions_token (token_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS dim_bot_diagnostics (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  event_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
  city VARCHAR(50) NULL COMMENT '城市',
  date_label VARCHAR(50) NULL COMMENT '日期标签',
  forecast_max DOUBLE NULL COMMENT '预测最高温',
  confidence_score DOUBLE NULL COMMENT '预测置信度',
  disagreement_index DOUBLE NULL COMMENT '多源分歧指数',
  yes_token_id VARCHAR(100) NULL COMMENT 'YES Token ID',
  yes_market_price DOUBLE NULL COMMENT 'YES市场价',
  yes_fair_prob DOUBLE NULL COMMENT 'YES公平概率',
  yes_edge DOUBLE NULL COMMENT 'YES边际优势',
  no_token_id VARCHAR(100) NULL COMMENT 'NO Token ID',
  no_market_price DOUBLE NULL COMMENT 'NO市场价',
  no_fair_prob DOUBLE NULL COMMENT 'NO公平概率',
  no_edge DOUBLE NULL COMMENT 'NO边际优势',
  PRIMARY KEY (id),
  KEY idx_dim_diag_time (event_ts),
  KEY idx_dim_diag_city_date (city, date_label)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
