DROP TABLE IF EXISTS workflow_audit_log;
DROP TABLE IF EXISTS workflow_schedule;

CREATE TABLE workflow_audit_log (
                                    log_id SERIAL PRIMARY KEY,
                                    workflow_ref TEXT,
                                    operation TEXT,
                                    state_snapshot TEXT,
                                    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE workflow_schedule (
                                   schedule_id TEXT PRIMARY KEY,
                                   workflow_ref_id TEXT,
                                   schedule_snapshot TEXT,
                                   created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO workflow_audit_log (workflow_ref, operation, state_snapshot, created_at)
VALUES
    ('sch-1', 'create', 'snapshot_v1', NOW() - INTERVAL '5 days'),
    ('sch-1', 'update', 'snapshot_v2', NOW() - INTERVAL '2 days'),
    ('sch-1', 'update', 'snapshot_v3', NOW() - INTERVAL '1 day');

INSERT INTO workflow_audit_log (workflow_ref, operation, state_snapshot, created_at)
VALUES
    ('sch-2-ref', 'update', 'snapshot_v_new', NOW());

INSERT INTO workflow_schedule (schedule_id, workflow_ref_id, schedule_snapshot, created_at)
VALUES
    ('sch-2', 'sch-2-ref', 'snapshot_v_old', NOW() - INTERVAL '3 days');

INSERT INTO workflow_audit_log (workflow_ref, operation, state_snapshot, created_at)
VALUES
    ('sch-3', 'update', 'snapshot_v_now', NOW());

INSERT INTO workflow_schedule (schedule_id, workflow_ref_id, schedule_snapshot, created_at)
VALUES
    ('sch-3', NULL, 'snapshot_v_original', NOW() - INTERVAL '2 days');

INSERT INTO workflow_audit_log (workflow_ref, operation, state_snapshot, created_at)
VALUES
    ('sch-4', 'update', 'snapshot_v_lost', NOW());


WITH updates AS (
    SELECT *
    FROM workflow_audit_log
    WHERE operation = 'update'
),
     previous_audit AS (
         SELECT
             e.log_id AS update_id,
             sub.state_snapshot AS previous_snapshot,
             1 AS source_level
         FROM updates e
                  JOIN LATERAL (
             SELECT state_snapshot
             FROM workflow_audit_log
             WHERE workflow_ref = e.workflow_ref
               AND operation IN ('create', 'update')
               AND state_snapshot IS NOT NULL
               AND created_at < e.created_at
             ORDER BY created_at DESC
                 LIMIT 1
    ) sub ON TRUE
    ),
    previous_schedule_ref AS (
SELECT
    e.log_id AS update_id,
    s.schedule_snapshot AS previous_snapshot,
    2 AS source_level
FROM updates e
    JOIN workflow_schedule s ON s.workflow_ref_id = e.workflow_ref
WHERE NOT EXISTS (
    SELECT 1 FROM previous_audit WHERE previous_audit.update_id = e.log_id
    )
    ),
    previous_schedule_id AS (
SELECT
    e.log_id AS update_id,
    s.schedule_snapshot AS previous_snapshot,
    3 AS source_level
FROM updates e
    JOIN workflow_schedule s ON s.schedule_id = e.workflow_ref
WHERE NOT EXISTS (
    SELECT 1 FROM previous_audit WHERE previous_audit.update_id = e.log_id
    )
  AND NOT EXISTS (
    SELECT 1 FROM previous_schedule_ref WHERE previous_schedule_ref.update_id = e.log_id
    )
    ),
    all_previous_versions AS (
SELECT * FROM previous_audit
UNION ALL
SELECT * FROM previous_schedule_ref
UNION ALL
SELECT * FROM previous_schedule_id
    )

SELECT
    e.log_id AS update_id,
    e.workflow_ref,
    e.state_snapshot AS current_snapshot,
    p.previous_snapshot,
    p.source_level
FROM updates e
         LEFT JOIN all_previous_versions p ON e.log_id = p.update_id
ORDER BY e.log_id;