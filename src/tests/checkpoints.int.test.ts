/* eslint-disable no-process-env */
import { describe, it, expect, beforeEach, afterAll } from "@jest/globals";
import { PGlite } from '@electric-sql/pglite';
import {
  Checkpoint,
  CheckpointTuple,
  uuid6,
} from "@langchain/langgraph-checkpoint";
import { PgLiteSaver } from "../index.js";

const checkpoint1: Checkpoint = {
  v: 1,
  id: uuid6(-1),
  ts: "2024-04-19T17:19:07.952Z",
  channel_values: {
    someKey1: "someValue1",
  },
  channel_versions: {
    someKey1: 1,
    someKey2: 1,
  },
  versions_seen: {
    someKey3: {
      someKey4: 1,
    },
  },
  pending_sends: [],
};

const checkpoint2: Checkpoint = {
  v: 1,
  id: uuid6(1),
  ts: "2024-04-20T17:19:07.952Z",
  channel_values: {
    someKey1: "someValue2",
  },
  channel_versions: {
    someKey1: 1,
    someKey2: 2,
  },
  versions_seen: {
    someKey3: {
      someKey4: 2,
    },
  },
  pending_sends: [],
};

const pgLiteSavers: PgLiteSaver[] = [];

describe("PgLiteSaver", () => {
  let pgLiteSaver: PgLiteSaver;

  beforeEach(async () => {
    // Create a new in-memory database for each test
    const dbName = `memory://${Date.now()}_${Math.floor(Math.random() * 1000)}`;
    pgLiteSaver = PgLiteSaver.fromConnString(dbName);
    pgLiteSavers.push(pgLiteSaver);
    await pgLiteSaver.setup();
  });

  afterAll(async () => {
    // Close all database connections
    await Promise.all(pgLiteSavers.map((saver) => saver.end()));
  });

  it("should initialize correctly with an existing PGlite instance", async () => {
    const dbName = `memory://${Date.now()}_${Math.floor(Math.random() * 1000)}`;
    const db = new PGlite(dbName);
    const saver = PgLiteSaver.fromInstance(db);
    pgLiteSavers.push(saver);
    
    await saver.setup();
    
    // Test basic operations with the instance-based saver
    const config = { configurable: { thread_id: "instance-test" } };
    
    // Verify initial state
    const undefinedCheckpoint = await saver.getTuple(config);
    expect(undefinedCheckpoint).toBeUndefined();
    
    // Save a checkpoint
    await saver.put(
      config,
      checkpoint1,
      { source: "update", step: 1, writes: null, parents: {} },
      checkpoint1.channel_versions
    );
    
    // Verify checkpoint was saved
    const savedCheckpoint = await saver.getTuple(config);
    expect(savedCheckpoint?.checkpoint).toEqual(checkpoint1);
    expect(savedCheckpoint?.metadata).toEqual({
      source: "update",
      step: 1,
      writes: null,
      parents: {},
    });
  });

  it("should save and retrieve checkpoints correctly", async () => {
    // get undefined checkpoint
    const undefinedCheckpoint = await pgLiteSaver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(undefinedCheckpoint).toBeUndefined();

    // save first checkpoint
    const runnableConfig = await pgLiteSaver.put(
      { configurable: { thread_id: "1" } },
      checkpoint1,
      { source: "update", step: -1, writes: null, parents: {} },
      checkpoint1.channel_versions
    );
    expect(runnableConfig).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: checkpoint1.id,
      },
    });

    // add some writes
    await pgLiteSaver.putWrites(
      {
        configurable: {
          checkpoint_id: checkpoint1.id,
          checkpoint_ns: "",
          thread_id: "1",
        },
      },
      [["bar", "baz"]],
      "foo"
    );

    // get first checkpoint tuple
    const firstCheckpointTuple = await pgLiteSaver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(firstCheckpointTuple?.config).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: checkpoint1.id,
      },
    });
    expect(firstCheckpointTuple?.checkpoint).toEqual(checkpoint1);
    expect(firstCheckpointTuple?.metadata).toEqual({
      source: "update",
      step: -1,
      writes: null,
      parents: {},
    });
    expect(firstCheckpointTuple?.parentConfig).toBeUndefined();
    expect(firstCheckpointTuple?.pendingWrites).toEqual([
      ["foo", "bar", "baz"],
    ]);

    // save second checkpoint
    await pgLiteSaver.put(
      {
        configurable: {
          thread_id: "1",
          checkpoint_id: "2024-04-18T17:19:07.952Z",
        },
      },
      checkpoint2,
      { source: "update", step: -1, writes: null, parents: {} },
      checkpoint2.channel_versions
    );

    // verify that parentTs is set and retrieved correctly for second checkpoint
    const secondCheckpointTuple = await pgLiteSaver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(secondCheckpointTuple?.metadata).toEqual({
      source: "update",
      step: -1,
      writes: null,
      parents: {},
    });
    expect(secondCheckpointTuple?.parentConfig).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: "2024-04-18T17:19:07.952Z",
      },
    });

    // list checkpoints
    const checkpointTupleGenerator = pgLiteSaver.list({
      configurable: { thread_id: "1" },
    });
    const checkpointTuples: CheckpointTuple[] = [];
    for await (const checkpoint of checkpointTupleGenerator) {
      checkpointTuples.push(checkpoint);
    }
    expect(checkpointTuples.length).toBe(2);
    const checkpointTuple1 = checkpointTuples[0];
    const checkpointTuple2 = checkpointTuples[1];
    expect(checkpointTuple1.checkpoint.ts).toBe("2024-04-20T17:19:07.952Z");
    expect(checkpointTuple2.checkpoint.ts).toBe("2024-04-19T17:19:07.952Z");
  });

  it("should delete a single checkpoint correctly", async () => {
    // Save a checkpoint first
    await pgLiteSaver.put(
      { configurable: { thread_id: "delete-test" } },
      checkpoint1,
      { source: "update", step: 1, writes: null, parents: {} },
      checkpoint1.channel_versions
    );

    // Add some writes
    await pgLiteSaver.putWrites(
      {
        configurable: {
          checkpoint_id: checkpoint1.id,
          checkpoint_ns: "",
          thread_id: "delete-test",
        },
      },
      [["bar", "baz"]],
      "foo"
    );

    // Verify checkpoint exists
    const savedCheckpoint = await pgLiteSaver.getTuple({
      configurable: { thread_id: "delete-test" },
    });
    expect(savedCheckpoint).toBeDefined();
    expect(savedCheckpoint?.checkpoint).toEqual(checkpoint1);

    // Delete the checkpoint
    await pgLiteSaver.delete({
      configurable: {
        thread_id: "delete-test",
        checkpoint_ns: "",
        checkpoint_id: checkpoint1.id,
      },
    });

    // Verify checkpoint was deleted
    const deletedCheckpoint = await pgLiteSaver.getTuple({
      configurable: { thread_id: "delete-test" },
    });
    expect(deletedCheckpoint).toBeUndefined();
  });

  it("should delete all checkpoints in a thread correctly", async () => {
    // Save multiple checkpoints
    await pgLiteSaver.put(
      { configurable: { thread_id: "thread-delete-test" } },
      checkpoint1,
      { source: "update", step: 1, writes: null, parents: {} },
      checkpoint1.channel_versions
    );

    await pgLiteSaver.put(
      {
        configurable: {
          thread_id: "thread-delete-test",
          checkpoint_id: checkpoint1.id,
        },
      },
      checkpoint2,
      { source: "update", step: 2, writes: null, parents: {} },
      checkpoint2.channel_versions
    );

    // Add writes to both checkpoints
    await pgLiteSaver.putWrites(
      {
        configurable: {
          checkpoint_id: checkpoint1.id,
          checkpoint_ns: "",
          thread_id: "thread-delete-test",
        },
      },
      [["bar1", "baz1"]],
      "foo1"
    );

    await pgLiteSaver.putWrites(
      {
        configurable: {
          checkpoint_id: checkpoint2.id,
          checkpoint_ns: "",
          thread_id: "thread-delete-test",
        },
      },
      [["bar2", "baz2"]],
      "foo2"
    );

    // Verify checkpoints exist
    const checkpointTupleGenerator = pgLiteSaver.list({
      configurable: { thread_id: "thread-delete-test" },
    });
    const checkpointTuples: CheckpointTuple[] = [];
    for await (const checkpoint of checkpointTupleGenerator) {
      checkpointTuples.push(checkpoint);
    }
    expect(checkpointTuples.length).toBe(2);

    // Delete all checkpoints in the thread
    await pgLiteSaver.deleteThread("thread-delete-test");

    // Verify all checkpoints were deleted
    const deletedCheckpointTupleGenerator = pgLiteSaver.list({
      configurable: { thread_id: "thread-delete-test" },
    });
    const deletedCheckpointTuples: CheckpointTuple[] = [];
    for await (const checkpoint of deletedCheckpointTupleGenerator) {
      deletedCheckpointTuples.push(checkpoint);
    }
    expect(deletedCheckpointTuples.length).toBe(0);
  });
});
