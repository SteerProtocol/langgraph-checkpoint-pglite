import { PgLiteSaver } from '../index.js';
import { rm } from 'fs/promises';
import { join } from 'path';

const DB_PATH = './test-db';

async function testPersistentStorage() {
  // Clean up any existing database
  try {
    await rm(DB_PATH, { recursive: true, force: true });
  } catch (e) {
    // Ignore if doesn't exist
  }

  // First session: Create and save data
  console.log("Session 1: Creating and saving data...");
  const checkpointer1 = PgLiteSaver.fromConnString(`file://${DB_PATH}`);
  await checkpointer1.setup();

  const checkpoint = {
    v: 1,
    id: "persistent-test-1",
    ts: new Date().toISOString(),
    channel_values: {
      persistent_key: "This data should persist"
    },
    channel_versions: {
      persistent_key: 1
    },
    versions_seen: {
      persistent_key: {
        value: 1
      }
    },
    pending_sends: []
  };

  const config = {
    configurable: {
      thread_id: "persistent-thread",
      checkpoint_ns: "persistent-test"
    }
  };

  await checkpointer1.put(
    config,
    checkpoint,
    { source: "persistence-test", step: 1, writes: null, parents: {} },
    checkpoint.channel_versions
  );

  await checkpointer1.end();
  console.log("Session 1: Data saved and connection closed.");

  // Second session: Verify data persisted
  console.log("\nSession 2: Verifying data persistence...");
  const checkpointer2 = PgLiteSaver.fromConnString(`file://${DB_PATH}`);
  await checkpointer2.setup();

  const retrieved = await checkpointer2.getTuple(config);
  console.log("Retrieved data from persistent storage:", 
    JSON.stringify(retrieved?.checkpoint?.channel_values, null, 2));

  await checkpointer2.end();
  console.log("Session 2: Verification complete.");

  // Clean up
  await rm(DB_PATH, { recursive: true });
}

// Run the test
testPersistentStorage().catch(console.error); 