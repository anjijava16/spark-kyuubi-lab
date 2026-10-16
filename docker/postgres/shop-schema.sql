-- Tables for the Spark examples. Spark jobs create/overwrite the rest
-- (orders, daily_sales, ...) themselves.

CREATE TABLE IF NOT EXISTS customers (
  customer_id  INTEGER PRIMARY KEY,
  full_name    TEXT    NOT NULL,
  email        TEXT    NOT NULL,
  region       TEXT    NOT NULL,
  tier         TEXT    NOT NULL,
  signup_date  DATE    NOT NULL
);

INSERT INTO customers (customer_id, full_name, email, region, tier, signup_date) VALUES
  (1, 'Asha Rao', 'asha.rao@example.com', 'South', 'silver', DATE '2025-06-15'),
  (2, 'Ben Smith', 'ben.smith@example.com', 'North', 'bronze', DATE '2025-11-30'),
  (3, 'Chen Li', 'chen.li@example.com', 'North', 'silver', DATE '2026-02-25'),
  (4, 'Diego Garcia', 'diego.garcia@example.com', 'North', 'gold', DATE '2025-10-26'),
  (5, 'Emma Brown', 'emma.brown@example.com', 'North', 'bronze', DATE '2025-04-20'),
  (6, 'Farah Khan', 'farah.khan@example.com', 'West', 'bronze', DATE '2025-08-11'),
  (7, 'Gopal Iyer', 'gopal.iyer@example.com', 'North', 'gold', DATE '2025-05-04'),
  (8, 'Hana Sato', 'hana.sato@example.com', 'North', 'gold', DATE '2025-08-06'),
  (9, 'Ivan Petrov', 'ivan.petrov@example.com', 'South', 'gold', DATE '2025-03-05'),
  (10, 'Jia Wang', 'jia.wang@example.com', 'North', 'gold', DATE '2025-11-18'),
  (11, 'Kofi Mensah', 'kofi.mensah@example.com', 'West', 'bronze', DATE '2025-10-27'),
  (12, 'Lena Weber', 'lena.weber@example.com', 'South', 'bronze', DATE '2026-05-15'),
  (13, 'Mateo Lopez', 'mateo.lopez@example.com', 'South', 'silver', DATE '2025-10-13'),
  (14, 'Nora Kim', 'nora.kim@example.com', 'South', 'gold', DATE '2025-08-03'),
  (15, 'Omar Haddad', 'omar.haddad@example.com', 'East', 'gold', DATE '2025-03-02'),
  (16, 'Priya Nair', 'priya.nair@example.com', 'South', 'bronze', DATE '2026-02-22'),
  (17, 'Quinn Taylor', 'quinn.taylor@example.com', 'South', 'silver', DATE '2025-10-25'),
  (18, 'Ravi Shah', 'ravi.shah@example.com', 'North', 'gold', DATE '2025-02-19'),
  (19, 'Sara Silva', 'sara.silva@example.com', 'South', 'silver', DATE '2025-01-31'),
  (20, 'Tomas Novak', 'tomas.novak@example.com', 'West', 'silver', DATE '2025-12-15'),
  (21, 'Uma Das', 'uma.das@example.com', 'West', 'silver', DATE '2025-08-27'),
  (22, 'Victor Costa', 'victor.costa@example.com', 'South', 'bronze', DATE '2025-06-03'),
  (23, 'Wen Zhou', 'wen.zhou@example.com', 'South', 'bronze', DATE '2025-12-24'),
  (24, 'Ximena Ruiz', 'ximena.ruiz@example.com', 'East', 'gold', DATE '2025-10-22'),
  (25, 'Yusuf Aydin', 'yusuf.aydin@example.com', 'East', 'gold', DATE '2025-09-11'),
  (26, 'Zoe Martin', 'zoe.martin@example.com', 'East', 'gold', DATE '2025-08-18'),
  (27, 'Arjun Reddy', 'arjun.reddy@example.com', 'North', 'gold', DATE '2025-02-07'),
  (28, 'Bea Moore', 'bea.moore@example.com', 'South', 'silver', DATE '2025-08-03'),
  (29, 'Carlos Diaz', 'carlos.diaz@example.com', 'West', 'silver', DATE '2025-03-19'),
  (30, 'Divya Menon', 'divya.menon@example.com', 'North', 'gold', DATE '2025-01-21')
ON CONFLICT (customer_id) DO NOTHING;

-- Target of the upsert examples (13_postgres_to_postgres.py and the
-- order-tracking stream). Primary keys make ON CONFLICT possible.
CREATE TABLE IF NOT EXISTS customer_ltv (
  customer_id   INTEGER PRIMARY KEY,
  orders        BIGINT        NOT NULL,
  revenue       NUMERIC(12,2) NOT NULL,
  last_order    DATE,
  updated_at    TIMESTAMP     NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_tracking (
  order_id      TEXT PRIMARY KEY,
  customer_id   INTEGER,
  status        TEXT        NOT NULL,
  status_time   TIMESTAMP   NOT NULL,
  amount        NUMERIC(12,2),
  city          TEXT,
  carrier       TEXT,
  events_seen   INTEGER     NOT NULL,
  updated_at    TIMESTAMP   NOT NULL DEFAULT now()
);
