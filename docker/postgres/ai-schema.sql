-- AI tables in "shop" (run as user app by pg-init; idempotent).
--   people, tickets, products      sample rows for jev(): plain-language WHERE / ranking / routing
--   kb_articles                    help-center articles, embedded for semantic search
--   product_embeddings             vectors written by Spark (18_pgvector_embeddings.py)
--   ticket_routing                 jev results written back by Spark (19_jev_tickets_to_lake.py)

CREATE TABLE IF NOT EXISTS people (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  country     TEXT,
  occupation  TEXT,
  age         INTEGER
);
INSERT INTO people VALUES
  (1, 'Lukas Bauer', 'Germany', 'nurse', 34),
  (2, 'Aiko Tanaka', 'Japan', 'software engineer', 29),
  (3, 'Sofia Rossi', 'Italy', 'architect', 41),
  (4, 'Carlos Mendes', 'Portugal', 'physician', 52),
  (5, 'Priya Raman', 'India', 'data analyst', 31),
  (6, 'Emma Johansson', 'Sweden', 'teacher', 45),
  (7, 'Liam O''Brien', 'Ireland', 'pharmacist', 38),
  (8, 'Chen Wei', 'China', 'accountant', 47),
  (9, 'Amara Okafor', 'Nigeria', 'surgeon', 50),
  (10, 'Hugo Lefevre', 'France', 'chef', 27),
  (11, 'Olivia Smith', 'USA', 'lawyer', 36),
  (12, 'Mateo Garcia', 'Spain', 'hospital administrator', 58),
  (13, 'Noah Williams', 'Canada', 'electrician', 33),
  (14, 'Zofia Nowak', 'Poland', 'therapist', 44),
  (15, 'Kenji Sato', 'Japan', 'pilot', 39),
  (16, 'Fatima Al-Sayed', 'Egypt', 'pharmacist', 30),
  (17, 'Anna de Vries', 'Netherlands', 'product manager', 35),
  (18, 'Diego Fernandez', 'Mexico', 'doctor', 61),
  (19, 'Ingrid Hansen', 'Norway', 'marine biologist', 42),
  (20, 'Ravi Kumar', 'India', 'clinic nurse', 26)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS tickets (
  id          INTEGER PRIMARY KEY,
  customer_id INTEGER,
  subject     TEXT NOT NULL,
  body        TEXT NOT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT now()
);
INSERT INTO tickets (id, customer_id, subject, body) VALUES
  (1, 3, 'Charged twice this month', 'My card was charged twice for the same subscription. This is the third time. I want a refund now.'),
  (2, 7, 'App crashes on login', 'Since the last update the app crashes every time I try to login on Android.'),
  (3, 12, 'Suspicious login from another country', 'I got an email about a login from Brazil. I did not do this. Is my account hacked?'),
  (4, 5, 'Quote for 50 seats', 'We would like pricing for an enterprise plan with 50 seats and a demo next week.'),
  (5, 1, 'Invoice address wrong', 'Can you update the billing address on my last invoice? Thanks!'),
  (6, 9, 'Page loading forever', 'The dashboard page is very slow and sometimes shows a 500 error.'),
  (7, 14, 'Absolutely unacceptable service', 'Furious. Nobody answers, the export is broken for two weeks. Worst support ever, I will cancel.'),
  (8, 20, 'Reset my password', 'I forgot my password and the reset email never arrives. Please help.'),
  (9, 2, 'Discount for nonprofits?', 'Do you offer a discount for nonprofit organisations on the annual plan?'),
  (10, 18, 'Refund for unused months', 'I cancelled in March but was still billed for April and May. Please refund.'),
  (11, 6, 'API timeout', 'Our integration gets timeout errors from the API since yesterday 14:00 UTC.'),
  (12, 11, 'Phishing email pretending to be you', 'I received a phishing email using your logo asking for my card details.'),
  (13, 25, 'Upgrade to Pro', 'How do I upgrade my plan to Pro and what happens to my trial?'),
  (14, 4, 'Still waiting for my refund!!!', 'This is ridiculous. I was promised a refund three weeks ago and still nothing. Terrible.'),
  (15, 16, 'Sync not working', 'Files are not syncing between my laptop and phone anymore.'),
  (16, 27, 'Enable 2FA for team', 'How can I force two-factor authentication (2FA) for everyone on my team?'),
  (17, 8, 'Wrong VAT on invoice', 'The VAT on invoice 2026-118 is wrong, we are a registered business in the EU.'),
  (18, 22, 'Install fails on Windows', 'The installer stops at 80% with error code 0x80070005.'),
  (19, 13, 'Contract renewal', 'Our contract ends next month, can sales send a renewal quote with the new pricing?'),
  (20, 30, 'Account locked after fraud alert', 'My account is locked after a fraud alert and I cannot access my data. Urgent.')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS products (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  category    TEXT,
  price       NUMERIC(10,2),
  description TEXT
);
INSERT INTO products VALUES
  (1, 'Basic Plastic Water Bottle', 'kitchen', 4.99, 'Cheap everyday bottle, BPA free plastic.'),
  (2, 'Handcrafted Italian Leather Bag', 'fashion', 1890.00, 'Luxury handcrafted bag in Italian calf leather, limited edition.'),
  (3, 'Stainless Steel Chef Knife', 'kitchen', 129.00, 'Professional steel knife for daily cooking, solid and reliable.'),
  (4, 'Cashmere Scarf', 'fashion', 420.00, 'Soft cashmere scarf, designer collection.'),
  (5, 'USB-C Charging Cable', 'electronics', 9.50, 'Basic cable, 1 m, fast charging.'),
  (6, 'Noise Cancelling Headphones', 'electronics', 349.00, 'Premium over-ear headphones with active noise cancelling.'),
  (7, 'Swiss Automatic Watch', 'fashion', 6200.00, 'Exclusive swiss automatic movement, gold case, sapphire glass.'),
  (8, 'Cotton T-Shirt', 'fashion', 15.00, 'Standard cotton t-shirt, everyday wear.'),
  (9, 'Espresso Machine Pro', 'kitchen', 899.00, 'Premium espresso machine with steam wand, pro quality.'),
  (10, 'Paper Notebook', 'office', 3.20, 'Budget lined notebook, 80 pages.'),
  (11, 'Ergonomic Office Chair', 'office', 540.00, 'Premium mesh chair with lumbar support, quality build.'),
  (12, 'Silk Pajama Set', 'fashion', 780.00, 'Luxury silk pajamas, handcrafted finish.'),
  (13, 'Wireless Mouse', 'electronics', 24.00, 'Reliable wireless mouse for everyday use.'),
  (14, 'Cast Iron Skillet', 'kitchen', 45.00, 'Solid cast iron pan, lasts forever.'),
  (15, 'Gold Plated Fountain Pen', 'office', 1150.00, 'Designer fountain pen with gold nib, exclusive gift box.'),
  (16, 'Winter Down Jacket', 'fashion', 260.00, 'Warm down jacket for cold winter days, waterproof shell.'),
  (17, 'Trail Running Shoes', 'sports', 140.00, 'Lightweight running shoes with grippy sole for trails.'),
  (18, 'Yoga Mat', 'sports', 30.00, 'Basic non-slip yoga mat, 6 mm.'),
  (19, 'Carbon Road Bike', 'sports', 4800.00, 'Premium carbon frame road bike, pro components.'),
  (20, 'Wool Winter Hat', 'fashion', 35.00, 'Warm wool hat for winter, one size.')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS kb_articles (
  id          INTEGER PRIMARY KEY,
  title       TEXT NOT NULL,
  body        TEXT NOT NULL,
  embedding   vector(512)
);
INSERT INTO kb_articles (id, title, body) VALUES
  (1, 'How refunds work', 'Refunds for double charges or unused months go back to the original card within 5-10 days.'),
  (2, 'Update billing address or VAT number', 'Change the billing address and VAT id on invoices from Settings > Billing.'),
  (3, 'App crashes after update', 'If the app crashes on login after an update, clear the cache or reinstall the latest version.'),
  (4, 'Reset your password', 'Use Forgot password on the login page; check spam if the reset email does not arrive.'),
  (5, 'Secure your account with 2FA', 'Turn on two-factor authentication (2FA) and review suspicious login alerts.'),
  (6, 'Report phishing', 'Forward phishing emails that use our logo to security@example.com; we never ask for card details.'),
  (7, 'Enterprise plans and quotes', 'Sales prepares quotes for enterprise plans, seats, renewals and nonprofit discounts.'),
  (8, 'Upgrade or change your plan', 'Upgrade from trial to Pro from the plan page; your data stays.'),
  (9, 'API errors and timeouts', 'Timeouts and 500 errors from the API: check the status page and retry with backoff.'),
  (10, 'Sync problems', 'If files are not syncing between devices, sign out and in again on each device.'),
  (11, 'Installing on Windows', 'Installer errors like 0x80070005 mean missing permissions: run it as administrator.'),
  (12, 'Locked accounts and fraud alerts', 'Accounts locked after a fraud alert are reviewed by security within 24 hours.')
ON CONFLICT (id) DO NOTHING;
-- embed the help articles right here in SQL (Spark embeds the products, see job 18)
UPDATE kb_articles SET embedding = lab_embed(title || ' ' || body) WHERE embedding IS NULL;

CREATE TABLE IF NOT EXISTS product_embeddings (
  product_id  INTEGER PRIMARY KEY REFERENCES products(id),
  model       TEXT NOT NULL,
  embedding   vector(512) NOT NULL,
  updated_at  TIMESTAMP NOT NULL DEFAULT now()
);
-- HNSW index for cosine distance (<=>). With 20 rows Postgres will still pick
-- a seq scan; the index is here so the same DDL works when the table grows.
CREATE INDEX IF NOT EXISTS product_embeddings_hnsw
  ON product_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS kb_articles_hnsw
  ON kb_articles USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS ticket_routing (
  ticket_id   INTEGER PRIMARY KEY,
  team        TEXT NOT NULL,
  angry_p     DOUBLE PRECISION,
  urgency     DOUBLE PRECISION,
  kb_article  INTEGER,
  routed_at   TIMESTAMP NOT NULL DEFAULT now()
);
