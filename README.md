# @steerprotocol/langgraph-checkpoint-pglite

Implementation of a [LangGraph.js](https://github.com/langchain-ai/langgraphjs) CheckpointSaver that uses PGlite for local, in-memory, or browser-based PostgreSQL storage.

## Features

- 🚀 Works in Node.js, Bun, Deno, and browsers
- 💾 Supports in-memory, file system, and IndexedDB storage
- 🔄 No external PostgreSQL server required
- 🌐 Perfect for local development and browser-based applications

## Installation

```bash
npm install @steerprotocol/langgraph-checkpoint-pglite
```

## Usage

```ts
import { PostgresSaver } from "@steerprotocol/langgraph-checkpoint-pglite";

const writeConfig = {
  configurable: {
    thread_id: "1",
    checkpoint_ns: ""
  }
};
const readConfig = {
  configurable: {
    thread_id: "1"
  }
};

// Create an in-memory database
const checkpointer = PostgresSaver.fromConnString("memory://");

// Or use file system storage (Node.js/Bun)
// const checkpointer = PostgresSaver.fromConnString("file://./my-database");

// Or use IndexedDB storage (Browser)
// const checkpointer = PostgresSaver.fromConnString("idb://my-database");

// You must call .setup() the first time you use the checkpointer:
await checkpointer.setup();

const checkpoint = {
  v: 1,
  ts: "2024-07-31T20:14:19.804150+00:00",
  id: "1ef4f797-8335-6428-8001-8a1503f9b875",
  channel_values: {
    my_key: "meow",
    node: "node"
  },
  channel_versions: {
    __start__: 2,
    my_key: 3,
    start:node: 3,
    node: 3
  },
  versions_seen: {
    __input__: {},
    __start__: {
      __start__: 1
    },
    node: {
      start:node: 2
    }
  },
  pending_sends: [],
}

// store checkpoint
await checkpointer.put(writeConfig, checkpoint, {}, {});

// load checkpoint
await checkpointer.get(readConfig);

// list checkpoints
for await (const checkpoint of checkpointer.list(readConfig)) {
  console.log(checkpoint);
}

// Don't forget to close the connection when done
await checkpointer.end();
```

## Storage Options

PGlite supports multiple storage backends:

### In-Memory (Ephemeral)
```ts
const checkpointer = PostgresSaver.fromConnString("memory://my-db");
```

### File System (Node.js/Bun)
```ts
const checkpointer = PostgresSaver.fromConnString("file://./path/to/data");
```

### IndexedDB (Browser)
```ts
const checkpointer = PostgresSaver.fromConnString("idb://my-database");
```

## Testing

The tests use in-memory databases, so no setup is required. Just run:

```bash
npm test
```

## License

MIT
