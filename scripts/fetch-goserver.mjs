#!/usr/bin/env node
// Shallow-clones github.com/mcpwarp/ws-mixer-go at the tag pinned in
// goserver.pin into .goserver/ (gitignored), for test/interop.test.ts to
// build cmd/testserver from. No-op if .goserver/ already checked out at
// that pin. For local dev, WSMIXER_GO_DIR (or an ../ws-mixer-go sibling
// checkout, as produced by the monorepo extraction) is tried first -- see
// the resolver in test/interop.test.ts.

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const pin = readFileSync(path.join(repoRoot, "goserver.pin"), "utf8").trim();
const dest = path.join(repoRoot, ".goserver");
const pinStampPath = path.join(dest, ".pin");

if (existsSync(pinStampPath) && readFileSync(pinStampPath, "utf8").trim() === pin) {
  console.log(`fetch-goserver: .goserver/ already at ${pin}`);
  process.exit(0);
}

rmSync(dest, { recursive: true, force: true });

console.log(`fetch-goserver: cloning ws-mixer-go@${pin} into .goserver/`);
execFileSync(
  "git",
  ["clone", "--depth", "1", "--branch", pin, "https://github.com/mcpwarp/ws-mixer-go.git", dest],
  { stdio: "inherit" },
);

writeFileSync(pinStampPath, pin + "\n");
console.log(`fetch-goserver: done (${pin})`);
