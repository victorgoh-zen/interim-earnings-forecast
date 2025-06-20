USE CATALOG exploration;
USE SCHEMA earnings_forecast;

CREATE OR REPLACE TABLE daily_mtm_scenario_prices (
  model_id SMALLINT NOT NULL,
  sample_id SMALLINT NOT NULL,
  interval_date DATE NOT NULL,
  period_id SMALLINT NOT NULL,
  region_number TINYINT NOT NULL,
  rrp FLOAT NOT NULL,
  CONSTRAINT pk_mtm_scenario PRIMARY KEY (model_id, sample_id, interval_date, period_id, region_number),
  CONSTRAINT fk_mtm_model FOREIGN KEY (model_id) REFERENCES scenario_modelling.price_models(model_id),
  CONSTRAINT fk_mtm_sample FOREIGN KEY (model_id, sample_id) REFERENCES scenario_modelling.price_model_sample_details(model_id, sample_id),
  CONSTRAINT fk_mtm_region FOREIGN KEY (region_number) REFERENCES scenario_modelling.region_numbers(region_number)
);

CREATE OR REPLACE TABLE daily_mtm_scenario_earnings (
  group STRING NOT NULL,
  product_id INT NOT NULL,
  deal_id INT NOT NULL,
  deal_name STRING NOT NULL,
  status STRING NOT NULL,
  deal_date DATE NOT NULL,
  strategy STRING NOT NULL,
  regionid STRING NOT NULL,
  interval_date DATE NOT NULL,
  period_id SMALLINT NOT NULL,
  buy_sell STRING NOT NULL,
  volume_mwh FLOAT NOT NULL,
  rrp FLOAT NOT NULL,
  cost_amount FLOAT NOT NULL,
  income_amount FLOAT NOT NULL
);

CREATE OR REPLACE TABLE scenario_generation_profiles (
  model_id SMALLINT NOT NULL,
  product_id INTEGER NOT NULL,
  year SMALLINT NOT NULL,
  month_id SMALLINT NOT NULL,
  day_id SMALLINT NOT NULL,
  period_id SMALLINT NOT NULL,
  generation_mwh FLOAT NOT NULL,
  CONSTRAINT pk_generation_profile PRIMARY KEY (product_id, year, month_id, day_id, period_id),
  CONSTRAINT fk_generation_model FOREIGN KEY (model_id) REFERENCES scenario_modelling.price_models(model_id)
);

CREATE OR REPLACE TABLE scenario_load_profiles (
  model_id SMALLINT NOT NULL,
  product_id INTEGER NOT NULL,
  jurisdiction_id TINYINT NOT NULL,
  year SMALLINT NOT NULL,
  month_id SMALLINT NOT NULL,
  day_id SMALLINT NOT NULL,
  period_id SMALLINT NOT NULL,
  load_mwh FLOAT NOT NULL,
  CONSTRAINT pk_load_profile PRIMARY KEY (product_id, jurisdiction_id, year, month_id, day_id, period_id),
  CONSTRAINT fk_load_model FOREIGN KEY (model_id) REFERENCES scenario_modelling.price_models(model_id),
  CONSTRAINT fk_load_jurisdiction FOREIGN KEY (jurisdiction_id) REFERENCES scenario_modelling.jurisdictions(jurisdiction_id)
);

CREATE OR REPLACE TABLE deal_settlement_details (
  deal_id SMALLINT NOT NULL,
  product_id SMALLINT NOT NULL,
  deal_type STRING NOT NULL,
  buy_sell STRING NOT NULL,
  region_number TINYINT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  quantity DOUBLE,
  price DOUBLE,
  strike DOUBLE,
  tolling_fee DOUBLE,
  floor DOUBLE,
  turndown DOUBLE,
  lgc_price DOUBLE,
  lgc_percentage DOUBLE,
  CONSTRAINT pk_deal_settlement PRIMARY KEY (deal_id, product_id, start_date),
  CONSTRAINT fk_deal_settlement_region FOREIGN KEY (region_number) REFERENCES scenario_modelling.region_numbers(region_number)
);

CREATE OR REPLACE TABLE rate_calendar (
  product_id SMALLINT NOT NULL,
  jurisdiction_id TINYINT NOT NULL,
  interval_date DATE NOT NULL,
  period_id SMALLINT NOT NULL,
  rate FLOAT NOT NULL,
  CONSTRAINT pk_rate_calendar PRIMARY KEY (product_id, jurisdiction_id, interval_date, period_id),
  CONSTRAINT fk_rate_calendar_jurisdiction FOREIGN KEY (jurisdiction_id) REFERENCES scenario_modelling.jurisdictions(jurisdiction_id)
);
