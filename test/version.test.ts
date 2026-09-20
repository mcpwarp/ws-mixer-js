/**
 * SDK_VERSION (src/client.ts) is agent/client metadata that goes on the wire
 * twice -- `hello.agent.sdk_version` and the dial's `User-Agent` header --
 * so it must not drift from package.json's own version. This test exists
 * purely so a release bump that forgets to update the hand-maintained
 * constant fails CI instead of silently going stale on the wire.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { SDK_VERSION } from "../src/client.js";

describe("SDK_VERSION", () => {
  it("matches package.json's version", () => {
    const here = dirname(fileURLToPath(import.meta.url));
    const pkg = JSON.parse(readFileSync(join(here, "..", "package.json"), "utf8")) as { version: string };
    expect(SDK_VERSION).toBe(pkg.version);
  });
});
