-- minimal stand-in for the tables spark_jobs/feature_gold.py lands in production, so CI can
-- run the real dbt build + tests without a spark cluster. the rows are internally
-- consistent -- every categoryid resolves, every session has its events -- so the
-- relationship and uniqueness tests exercise real joins rather than trivially passing.
-- _loaded_at is now() so source freshness is green.

DROP TABLE IF EXISTS raw_events, raw_sessions, raw_category_tree, item_latest, cooccur_pairs;

CREATE TABLE raw_events (
    visitorid  BIGINT,
    event      TEXT,
    itemid     BIGINT,
    categoryid BIGINT,
    event_date DATE,
    session_id TEXT,
    _loaded_at TIMESTAMPTZ
);
INSERT INTO raw_events VALUES
    (1, 'view',        100, 1016, DATE '2015-06-01', 's1', now()),
    (1, 'addtocart',   100, 1016, DATE '2015-06-01', 's1', now()),  -- carted, never bought
    (2, 'view',        200, 1338, DATE '2015-06-01', 's2', now()),
    (2, 'transaction', 200, 1338, DATE '2015-06-01', 's2', now());

CREATE TABLE raw_sessions (
    session_id   TEXT,
    visitorid    BIGINT,
    start_time   TIMESTAMP,
    end_time     TIMESTAMP,
    event_count  BIGINT,
    has_purchase BOOLEAN,
    session_date DATE,
    _loaded_at   TIMESTAMPTZ
);
INSERT INTO raw_sessions VALUES
    ('s1', 1, TIMESTAMP '2015-06-01 10:00', TIMESTAMP '2015-06-01 10:05', 2, false, DATE '2015-06-01', now()),
    ('s2', 2, TIMESTAMP '2015-06-01 11:00', TIMESTAMP '2015-06-01 11:03', 2, true,  DATE '2015-06-01', now());

CREATE TABLE raw_category_tree (categoryid BIGINT, parentid BIGINT);
INSERT INTO raw_category_tree VALUES (213, NULL), (1016, 213), (1338, 213);

CREATE TABLE item_latest (itemid BIGINT, categoryid BIGINT);
INSERT INTO item_latest VALUES (100, 1016), (200, 1338);

CREATE TABLE cooccur_pairs (item_a BIGINT, item_b BIGINT, pair_type TEXT, weight BIGINT);
INSERT INTO cooccur_pairs VALUES
    (100, 200, 'view', 2),
    (100, 200, 'purchase', 2);
