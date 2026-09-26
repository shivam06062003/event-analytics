-- Realistic demo data, generated INSIDE ClickHouse (millions of rows in seconds,
-- no Python loop). Randomness is hash-based (cityHash64(user, salt)), so the
-- same inputs always produce the same data: reproducible benchmarks.
-- Parameters: {project:UUID}, {users:UInt64}, {start:DateTime64(3)}

-- Signups spread over 8 weeks, each user on a random plan.
INSERT INTO events (project_id, event_id, event, distinct_id, user_id, anonymous_id, timestamp,
                    client_timestamp, received_at, properties, context, ip, kafka_partition, kafka_offset)
SELECT {project:UUID}, generateUUIDv4(), 'signup', uid, uid, NULL, ts, NULL, ts,
       concat('{"plan":"', arrayElement(['free', 'free', 'pro', 'team'], 1 + cityHash64(n, 'plan') % 4), '"}'),
       '{}', NULL, 0, 0
FROM (SELECT number AS n, concat('user-', toString(n)) AS uid,
             {start:DateTime64(3)} + toIntervalSecond(cityHash64(n, 'signup') % (56 * 86400)) AS ts
      FROM numbers({users:UInt64}));

-- 0-19 page views in the two days after signup.
INSERT INTO events (project_id, event_id, event, distinct_id, user_id, anonymous_id, timestamp,
                    client_timestamp, received_at, properties, context, ip, kafka_partition, kafka_offset)
SELECT {project:UUID}, generateUUIDv4(), 'page_viewed', uid, uid, NULL,
       signup + toIntervalSecond(cityHash64(n, i, 'pv') % 172800), NULL, signup,
       concat('{"path":"', arrayElement(['/', '/pricing', '/docs', '/blog'], 1 + cityHash64(n, i) % 4), '"}'),
       '{}', NULL, 0, 0
FROM (SELECT number AS n, concat('user-', toString(n)) AS uid,
             {start:DateTime64(3)} + toIntervalSecond(cityHash64(n, 'signup') % (56 * 86400)) AS signup,
             arrayJoin(range(cityHash64(n, 'views') % 20)) AS i
      FROM numbers({users:UInt64}));

-- Funnel: 40% start checkout within a day; 55% of those purchase within hours.
INSERT INTO events (project_id, event_id, event, distinct_id, user_id, anonymous_id, timestamp,
                    client_timestamp, received_at, properties, context, ip, kafka_partition, kafka_offset)
SELECT {project:UUID}, generateUUIDv4(), step, uid, uid, NULL, ts, NULL, ts, props, '{}', NULL, 0, 0
FROM (
    SELECT number AS n, concat('user-', toString(n)) AS uid,
           {start:DateTime64(3)} + toIntervalSecond(cityHash64(n, 'signup') % (56 * 86400)) AS signup,
           arrayJoin([
               ('checkout_started', signup + toIntervalSecond(3600 + cityHash64(n, 'co') % 82800), '{}',
                cityHash64(n, 'p_co') % 100 < 40),
               ('purchase', signup + toIntervalSecond(90000 + cityHash64(n, 'pu') % 14400),
                concat('{"value":', toString(9 + cityHash64(n, 'val') % 491), '}'),
                cityHash64(n, 'p_co') % 100 < 40 AND cityHash64(n, 'p_pu') % 100 < 55)
           ]) AS t,
           t.1 AS step, t.2 AS ts, t.3 AS props, t.4 AS happens
    FROM numbers({users:UInt64})
)
WHERE happens;

-- Retention: return visits in weeks 1-8, less likely each week.
INSERT INTO events (project_id, event_id, event, distinct_id, user_id, anonymous_id, timestamp,
                    client_timestamp, received_at, properties, context, ip, kafka_partition, kafka_offset)
SELECT {project:UUID}, generateUUIDv4(), 'page_viewed', uid, uid, NULL, ts, NULL, ts,
       '{"path":"/app"}', '{}', NULL, 0, 0
FROM (
    SELECT number AS n, concat('user-', toString(n)) AS uid,
           {start:DateTime64(3)} + toIntervalSecond(cityHash64(n, 'signup') % (56 * 86400)) AS signup,
           arrayJoin(range(1, 9)) AS week,
           signup + toIntervalSecond(week * 604800 + cityHash64(n, week) % 604800) AS ts
    FROM numbers({users:UInt64})
)
WHERE cityHash64(n, week, 'ret') % 100 < (45 - week * 5);
