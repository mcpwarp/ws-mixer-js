/**
 * Runs every spec/fixtures/control/**\/*.json case through two independent
 * checkers, per spec/README.md's "SDK runtime validators assert wire_valid;
 * schema/CI tooling asserts schema_valid":
 *
 *  - `wire_valid`: this SDK's own hand-written validator (`parseControl`) --
 *    the wire-legal behaviour a real client must match.
 *  - `schema_valid`: the strict `spec/control.schema.json` (2020-12,
 *    `additionalProperties: false`), compiled with `ajv/dist/2020` (never the
 *    bare `ajv` import, which is draft-07 and fails open -- decision 9,
 *    OVERVIEW.md section 6).
 *
 * The two intentionally diverge on exactly one shape: a well-formed message
 * carrying an extra unrecognized field is wire_valid (forward-compatibility,
 * WIRE.md section 2.7) but not schema_valid (the strict schema's
 * additionalProperties:false typo-catcher).
 */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { Ajv2020 } from "ajv/dist/2020.js";
import { describe, expect, it } from "vitest";
import { parseControl } from "../src/control.js";
import { resolveSpecDir } from "./helpers/spec-dir.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const specResult = resolveSpecDir(here);
const controlDir = path.join(specResult.dir, "fixtures/control");
const schemaPath = path.join(specResult.dir, "control.schema.json");

// `$anchor` on every $defs entry plus per-property `description` strings
// trip ajv's strict-mode heuristics without being schema errors -- strict:
// false is exactly the escape hatch OVERVIEW.md section 6 anticipates.
const ajv = new Ajv2020({ strict: false, allErrors: true });
const validateControlSchema = specResult.available ? ajv.compile(JSON.parse(readFileSync(schemaPath, "utf8"))) : null;

interface ControlFixture {
  description: string;
  message?: Record<string, unknown>;
  raw?: string;
  schema_valid: boolean;
  wire_valid: boolean;
  expect?: { error_code?: string; message_contains?: string };
}

function walk(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walk(p));
    else if (entry.name.endsWith(".json")) out.push(p);
  }
  return out;
}

describe("control fixtures", () => {
  if (!specResult.available) {
    it.skip(`spec fixtures unavailable: ${specResult.reason}`, () => {});
    return;
  }
  const files = walk(controlDir);
  const validate = validateControlSchema!;

  it("loaded fixtures for all seven message types plus envelope", () => {
    const types = new Set(files.map((f) => path.basename(path.dirname(path.dirname(f)))));
    for (const t of ["hello", "welcome", "ping", "pong", "drain", "error", "app", "envelope"]) {
      expect(types.has(t), `missing fixtures for ${t}`).toBe(true);
    }
  });

  for (const file of files) {
    const rel = path.relative(controlDir, file);
    it(rel, () => {
      const fixture = JSON.parse(readFileSync(file, "utf8")) as ControlFixture;

      if (fixture.raw !== undefined) {
        // The one fixture whose bytes never parse as JSON at all: never
        // reaches the schema (which needs a parsed instance) either.
        expect(() => JSON.parse(fixture.raw!)).toThrow();
        expect(() => parseControl(fixture.raw!)).toThrow();
        return;
      }

      const payload = JSON.stringify(fixture.message);
      if (fixture.wire_valid) {
        expect(() => parseControl(payload)).not.toThrow();
      } else {
        expect(() => parseControl(payload)).toThrow();
      }

      const schemaValid = validate(fixture.message);
      expect(schemaValid, JSON.stringify(validate.errors)).toBe(fixture.schema_valid);
    });
  }
});
