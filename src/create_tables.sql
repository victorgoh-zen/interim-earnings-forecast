USE CATALOG exploration;
USE SCHEMA earnings_forecast;

CREATE OR REPLACE TABLE daily_mtm_scenario_prices (
  model_id SMALLINT NOT NULL,
  sample_id SMALLINT NOT NULL,
  interval_date DATE NOT NULL,
  period_id SMALLINT NOT NULL,
  region_number TINYINT NOT NULL,
  rrp FLOAT NOT NULL,
  CONSTRAINT pk_mtm_scenario PRIMARY KEY (interval_date, period_id, region_number),
  CONSTRAINT fk_mtm_model FOREIGN KEY (model_id) REFERENCES scenario_modelling.price_models(model_id),
  CONSTRAINT fk_mtm_sample FOREIGN KEY (model_id, sample_id) REFERENCES scenario_modelling.price_model_sample_details(model_id, sample_id),
  CONSTRAINT fk_mtm_region FOREIGN KEY (region_number) REFERENCES scenario_modelling.region_numbers(region_number)
);

CREATE OR REPLACE TABLE daily_mtm_scenario_earnings (
  product_id SMALLINT NOT NULL,
  deal_id SMALLINT NOT NULL,
  instrument_id SMALLINT NOT NULL,
  region_number TINYINT NOT NULL,
  buy BOOLEAN NOT NULL,
  interval_date DATE NOT NULL,
  period_id SMALLINT NOT NULL,
  volume_mwh FLOAT NOT NULL,
  income FLOAT NOT NULL,
  cost FLOAT NOT NULL,
  CONSTRAINT pk_mtm_earnings  PRIMARY KEY (product_id, deal_id, instrument_id, region_number, interval_date, period_id),
  CONSTRAINT fk_mtm_earnings_region FOREIGN KEY (region_number) REFERENCES scenario_modelling.region_numbers(region_number),
  CONSTRAINT fk_mtm_earnings_instrument FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
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
  instrument STRING NOT NULL,
  buy BOOLEAN NOT NULL,
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

CREATE OR REPLACE TABLE retail_rate_calendar (
  product_id SMALLINT NOT NULL,
  jurisdiction_id TINYINT NOT NULL,
  interval_date DATE NOT NULL,
  period_id SMALLINT NOT NULL,
  rate FLOAT NOT NULL,
  CONSTRAINT pk_rate_calendar PRIMARY KEY (product_id, jurisdiction_id, interval_date, period_id),
  CONSTRAINT fk_rate_calendar_jurisdiction FOREIGN KEY (jurisdiction_id) REFERENCES scenario_modelling.jurisdictions(jurisdiction_id)
);

CREATE OR REPLACE TABLE instruments (
  instrument_id SMALLINT NOT NULL,
  instrument STRING NOT NULL,
  CONSTRAINT instrument_pk PRIMARY KEY (instrument_id)
);

INSERT INTO instruments (instrument_id, instrument) VALUES 
  (0, "flat_energy_swap"),
  (1, "flat_energy_cap"),
  (2, "profiled_energy_swap"),
  (3, "profiled_energy_cap"),
  (4, "ppa_energy"),
  (5, "asset_toll_energy"),
  (6, "generation_lgc"),
  (7, "retail_energy"),
  (8, "retail_lgc");
