USE CATALOG exploration;
USE SCHEMA scenario_modelling;

CREATE OR REPLACE TABLE daily_mtm_scenario_prices (
  model_id SMALLINT NOT NULL,
  sample_id SMALLINT NOT NULL,
  interval_date DATE NOT NULL,
  period_id SMALLINT NOT NULL,
  region_number TINYINT NOT NULL,
  rrp FLOAT,
  CONSTRAINT pk_mtm_scenario PRIMARY KEY (model_id, sample_id, interval_date, period_id, region_number),
  CONSTRAINT fk_mtm_model FOREIGN KEY (model_id) REFERENCES price_models(model_id),
  CONSTRAINT fk_mtm_sample FOREIGN KEY (model_id, sample_id) REFERENCES price_model_sample_details(model_id, sample_id),
  CONSTRAINT fk_mtm_region FOREIGN KEY (region_number) REFERENCES region_numbers(region_number)
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