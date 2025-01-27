import { PgLiteSaver } from '../index.js';

async function testCheckpointer() {
  // Create an in-memory database for testing
  const checkpointer = PgLiteSaver.fromConnString("memory://test-db");
  
  // Initialize the database
  await checkpointer.setup();

  // Create a test checkpoint
  const checkpoint = {
    v: 1,
    id: "test-checkpoint-1",
    ts: new Date().toISOString(),
    channel_values: {
      test_key: "test_value"
    },
    channel_versions: {
      test_key: 1
    },
    versions_seen: {
      test_key: {
        value: 1
      }
    },
    pending_sends: []
  };

  // Save the checkpoint
  const config = {
    configurable: {
      thread_id: "test-thread",
      checkpoint_ns: "test-namespace"
    }
  };

  console.log("Saving checkpoint...");
  await checkpointer.put(
    config,
    checkpoint,
    { source: "test", step: 1, writes: null, parents: {} },
    checkpoint.channel_versions
  );

  // Retrieve the checkpoint
  console.log("Retrieving checkpoint...");
  const retrieved = await checkpointer.getTuple(config);
  console.log("Retrieved checkpoint:", JSON.stringify(retrieved, null, 2));

  // List all checkpoints
  console.log("\nListing all checkpoints:");
  for await (const cp of checkpointer.list(config)) {
    console.log("- Checkpoint ID:", cp.checkpoint.id);
  }

  // Clean up
  await checkpointer.end();
}

// Run the test
testCheckpointer().catch(console.error); 