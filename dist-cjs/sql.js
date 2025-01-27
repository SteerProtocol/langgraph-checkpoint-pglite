"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DELETE_THREAD_CHECKPOINTS_SQL = exports.DELETE_THREAD_BLOBS_SQL = exports.DELETE_THREAD_WRITES_SQL = exports.DELETE_CHECKPOINT_SQL = exports.DELETE_CHECKPOINT_BLOBS_SQL = exports.DELETE_CHECKPOINT_WRITES_SQL = exports.INSERT_CHECKPOINT_WRITES_SQL = exports.UPSERT_CHECKPOINT_WRITES_SQL = exports.UPSERT_CHECKPOINTS_SQL = exports.UPSERT_CHECKPOINT_BLOBS_SQL = exports.SELECT_SQL = void 0;
const langgraph_checkpoint_1 = require("@langchain/langgraph-checkpoint");
exports.SELECT_SQL = `
select
    thread_id,
    checkpoint,
    checkpoint_ns,
    checkpoint_id,
    parent_checkpoint_id,
    metadata,
    (
        select array_agg(array[bl.channel::bytea, bl.type::bytea, bl.blob])
        from jsonb_each_text(checkpoint -> 'channel_versions')
        inner join checkpoint_blobs bl
            on bl.thread_id = checkpoints.thread_id
            and bl.checkpoint_ns = checkpoints.checkpoint_ns
            and bl.channel = jsonb_each_text.key
            and bl.version = jsonb_each_text.value
    ) as channel_values,
    (
        select
        array_agg(array[cw.task_id::text::bytea, cw.channel::bytea, cw.type::bytea, cw.blob] order by cw.task_id, cw.idx)
        from checkpoint_writes cw
        where cw.thread_id = checkpoints.thread_id
            and cw.checkpoint_ns = checkpoints.checkpoint_ns
            and cw.checkpoint_id = checkpoints.checkpoint_id
    ) as pending_writes,
    (
        select array_agg(array[cw.type::bytea, cw.blob] order by cw.idx)
        from checkpoint_writes cw
        where cw.thread_id = checkpoints.thread_id
            and cw.checkpoint_ns = checkpoints.checkpoint_ns
            and cw.checkpoint_id = checkpoints.parent_checkpoint_id
            and cw.channel = '${langgraph_checkpoint_1.TASKS}'
    ) as pending_sends
from checkpoints `;
exports.UPSERT_CHECKPOINT_BLOBS_SQL = `
    INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO NOTHING
`;
exports.UPSERT_CHECKPOINTS_SQL = `
    INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id)
    DO UPDATE SET
        checkpoint = EXCLUDED.checkpoint,
        metadata = EXCLUDED.metadata;
`;
exports.UPSERT_CHECKPOINT_WRITES_SQL = `
    INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO UPDATE SET
        channel = EXCLUDED.channel,
        type = EXCLUDED.type,
        blob = EXCLUDED.blob;
`;
exports.INSERT_CHECKPOINT_WRITES_SQL = `
    INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, idx) DO NOTHING
`;
exports.DELETE_CHECKPOINT_WRITES_SQL = `
    DELETE FROM checkpoint_writes
    WHERE thread_id = $1
    AND checkpoint_ns = $2
    AND checkpoint_id = $3
`;
exports.DELETE_CHECKPOINT_BLOBS_SQL = `
    DELETE FROM checkpoint_blobs
    WHERE thread_id = $1
    AND checkpoint_ns = $2
    AND NOT EXISTS (
        SELECT 1 FROM checkpoints c
        WHERE c.thread_id = checkpoint_blobs.thread_id
        AND c.checkpoint_ns = checkpoint_blobs.checkpoint_ns
        AND c.checkpoint_id != $3
        AND (c.checkpoint->>'channel_versions')::jsonb @> jsonb_build_object(channel, version)
    )
`;
exports.DELETE_CHECKPOINT_SQL = `
    DELETE FROM checkpoints
    WHERE thread_id = $1
    AND checkpoint_ns = $2
    AND checkpoint_id = $3
`;
exports.DELETE_THREAD_WRITES_SQL = `
    DELETE FROM checkpoint_writes
    WHERE thread_id = $1
    AND checkpoint_ns = $2
`;
exports.DELETE_THREAD_BLOBS_SQL = `
    DELETE FROM checkpoint_blobs
    WHERE thread_id = $1
    AND checkpoint_ns = $2
`;
exports.DELETE_THREAD_CHECKPOINTS_SQL = `
    DELETE FROM checkpoints
    WHERE thread_id = $1
    AND checkpoint_ns = $2
`;
