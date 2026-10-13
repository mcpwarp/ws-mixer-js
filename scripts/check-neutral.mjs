#!/usr/bin/env node
// Import-boundary check over the BUILT package (run after `npm run build`):
// starting from the platform-neutral entries dist/index.js,
// dist/mcpwarp/index.js and dist/testing/index.js, follows every relative
// import (esbuild's shared chunks) and fails if anything reachable imports a
// bare specifier (`node:*`, a Node builtin, an npm package) or reaches
// dist/node/. test/boundary.test.ts checks the same rule on the sources.

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dist = path.join(root, "dist");
const entries = ["index.js", "mcpwarp/index.js", "testing/index.js"].map((p) => path.join(dist, p));

const SPECIFIER_RE = /(?:\bimport|\bexport)\s*(?:type\s+)?(?:[\w*{}\s,$]+?\s*from\s*)?["']([^"']+)["']|\bimport\s*\(\s*["']([^"']+)["']\s*\)/g;

const missing = entries.filter((e) => !existsSync(e));
if (missing.length > 0) {
  console.error(`check-neutral: missing ${missing.map((m) => path.relative(root, m)).join(", ")} -- run \`npm run build\` first`);
  process.exit(1);
}

const seen = new Set();
const violations = [];
const queue = [...entries];
while (queue.length > 0) {
  const file = queue.pop();
  if (seen.has(file)) continue;
  seen.add(file);
  const rel = path.relative(root, file);
  if (file.startsWith(path.join(dist, "node") + path.sep)) violations.push(`${rel} (under dist/node) is reachable`);
  const text = readFileSync(file, "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
  for (const m of text.matchAll(SPECIFIER_RE)) {
    const spec = m[1] ?? m[2];
    if (!spec.startsWith(".")) {
      violations.push(`${rel} imports ${JSON.stringify(spec)}`);
      continue;
    }
    const target = path.resolve(path.dirname(file), spec);
    if (!existsSync(target)) {
      violations.push(`${rel} imports ${JSON.stringify(spec)}, which does not exist`);
      continue;
    }
    queue.push(target);
  }
}

if (violations.length > 0) {
  console.error("check-neutral: the platform-neutral entries reach non-neutral imports:");
  for (const v of violations) console.error(`  ${v}`);
  process.exit(1);
}
console.log(`check-neutral: ok (${seen.size} files reachable from ${entries.length} neutral entries, all relative imports)`);
