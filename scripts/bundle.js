import { writeFileSync, mkdirSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = join(__dirname, '..');

// Ensure directories exist
mkdirSync(join(ROOT_DIR, 'dist'), { recursive: true });
mkdirSync(join(ROOT_DIR, 'dist-cjs'), { recursive: true });

// Create ESM entry point
writeFileSync(
  join(ROOT_DIR, 'index.js'),
  `export * from './dist/index.js';\n`
);

// Create CJS entry point
writeFileSync(
  join(ROOT_DIR, 'index.cjs'),
  `module.exports = require('./dist-cjs/index.js');\n`
);

// Create type definitions
writeFileSync(
  join(ROOT_DIR, 'index.d.ts'),
  `export * from './dist/index.js';\n`
);

writeFileSync(
  join(ROOT_DIR, 'index.d.cts'),
  `export * from './dist-cjs/index.d.ts';\n`
); 