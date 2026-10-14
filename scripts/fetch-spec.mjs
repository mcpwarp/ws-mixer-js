#!/usr/bin/env node
// Shallow-clones github.com/mcpwarp/ws-mixer-spec at the tag pinned in
// spec.pin into .spec/ (gitignored). No-op if .spec/ already checked out at
// that pin. This is what CI runs before the fixture-reading test suites
// (test/frame.test.ts, test/control.test.ts, test/sequence.test.ts,
// test/spec-envelope-fixtures.test.ts, test/spec-bridging-fixtures.test.ts,
// test/spec-tunnel-fixtures.test.ts, test/spec-status-text-fixtures.test.ts).
// The resolver those suites share (test/helpers/spec-dir.ts) tries
// WSMIXER_SPEC_DIR, then .spec/ while its .pin equals spec.pin, then an
// ../ws-mixer-spec sibling checkout while it is exactly at that tag.

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const pin = readFileSync(path.join(repoRoot, "spec.pin"), "utf8").trim();
const dest = path.join(repoRoot, ".spec");
const pinStampPath = path.join(dest, ".pin");

if (existsSync(pinStampPath) && readFileSync(pinStampPath, "utf8").trim() === pin) {
  console.log(`fetch-spec: .spec/ already at ${pin}`);
  process.exit(0);
}

rmSync(dest, { recursive: true, force: true });

console.log(`fetch-spec: cloning ws-mixer-spec@${pin} into .spec/`);
execFileSync(
  "git",
  ["clone", "--depth", "1", "--branch", pin, "https://github.com/mcpwarp/ws-mixer-spec.git", dest],
  { stdio: "inherit" },
);

const { writeFileSync } = await import("node:fs");
writeFileSync(pinStampPath, pin + "\n");
console.log(`fetch-spec: done (${pin})`);
