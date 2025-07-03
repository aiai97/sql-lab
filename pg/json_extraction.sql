CREATE TABLE workflow_audit (
                                workflow_id TEXT,
                                operation_type TEXT,
                                state_snapshot TEXT,
                                modified_by TEXT,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO workflow_audit (workflow_id, operation_type, state_snapshot, modified_by)
VALUES (
           'ref-123',
           'update',
           '{
           "scheduleTasks": [
             { "parameterField": "111111222" },
             { "parameterField": "114455111222" }
           ],
           "deliveryChannel": [
             "434455111222",
             "114455111222"
           ]
         }',
           'user1'
       );

SELECT
    workflow_id,
    operation_type,
    (
        SELECT string_agg(task->>'parameterField', ',')
        FROM jsonb_array_elements(state_snapshot::jsonb->'scheduleTasks') AS task
    ) AS task_parameters,
    (
        SELECT string_agg(value::text, ',')
        FROM jsonb_array_elements_text(state_snapshot::jsonb->'deliveryChannel') AS value
        ) AS delivery_channels
FROM workflow_audit;