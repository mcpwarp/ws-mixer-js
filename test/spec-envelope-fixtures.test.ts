/**
 * spec/fixtures/mcpwarp/envelope/{valid,invalid}/*.json (MCPWARP.md section
 * 10.2) against `spec/mcpwarp.schema.json` (ajv, draft 2020-12) and this
 * SDK's envelope codec. `schema_valid` is the strict schema's verdict;
 * `wire_valid`/`expect` is the agent's: an edge->agent op decodes (defaults
 * per section 2.3), fails with MALFORMED_ENVELOPE, or decodes as `unknown`
 * (UNSUPPORTED_VERSION / UNKNOWN_OP); a valid register/unregister equals
 * the encoder's output byte for byte. Agent->edge cases whose verdict is the
 * edge's (`SCHEMA_*` hints) are schema-checked only.
 */
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { Ajv2020 } from "ajv/dist/2020.js";
import { decodeEnvelope, encodeRegister, encodeUnregister, isValidServiceName, type ServiceKind } from "../src/mcpwarp/envelope.js";
import { McpwarpError } from "../src/mcpwarp/errors.js";
import { resolveSpecDir } from "./helpers/spec-dir.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const specResult = resolveSpecDir(here);
const envDir = path.join(specResult.dir, "fixtures/mcpwarp/envelope");
const schemaPath = path.join(specResult.dir, "mcpwarp.schema.json");
const available = specResult.available && existsSync(envDir) && existsSync(schemaPath);
const skipReason = specResult.reason ?? `no fixtures/mcpwarp/envelope under ${specResult.dir} (spec.pin older than v0.6.0?)`;

interface EnvelopeFixture {
  description: string;
  op: string;
  message: Record<string, unknown>;
  schema_valid: boolean;
  wire_valid: boolean;
  expect?: { error_code: string; message_contains?: string };
}

const AGENT_TO_EDGE = new Set(["register", "unregister"]);

function load(kind: "valid" | "invalid"): Array<{ file: string; fixture: EnvelopeFixture }> {
  if (!available) return [];
  return readdirSync(path.join(envDir, kind))
    .filter((f) => f.endsWith(".json"))
    .sort()
    .map((file) => ({ file, fixture: JSON.parse(readFileSync(path.join(envDir, kind, file), "utf8")) as EnvelopeFixture }));
}

// strict: false for the same reason as test/control.test.ts: descriptive keywords are not schema errors.
const validateSchema = available ? new Ajv2020({ strict: false, allErrors: true }).compile(JSON.parse(readFileSync(schemaPath, "utf8"))) : null;

function decode(message: Record<string, unknown>): { ok: true; value: ReturnType<typeof decodeEnvelope> } | { ok: false; error: unknown } {
  try {
    return { ok: true, value: decodeEnvelope({ mcpwarp: message }) };
  } catch (error) {
    return { ok: false, error };
  }
}

/** What section 2.3's defaults make of a well-formed edge->agent message. */
function expectedDecode(m: Record<string, any>): unknown {
  switch (m.op) {
    case "registered":
      return {
        op: "registered",
        services: (m.services ?? []).map((s: any) => ({ name: s.name, id: s.id, url: s.url, created: s.created ?? false })),
        errors: (m.errors ?? []).map((e: any) => ({ name: e.name, code: e.code, message: e.message ?? "", ...(e.id !== undefined ? { id: e.id } : {}) })),
      };
    case "unregistered":
      return { op: "unregistered", services: (m.services ?? []).map((s: any) => ({ name: s.name, id: s.id ?? "" })) };
    case "disable":
      return { op: "disable", id: m.id, reason: m.reason ?? "disabled" };
    case "enable":
      return { op: "enable", id: m.id, name: m.name };
    case "conflict":
      return { op: "conflict", id: m.id, name: m.name, reason: m.reason ?? "taken_over" };
    case "error":
      return { op: "error", code: m.code, message: m.message ?? "" };
    default:
      throw new Error(`no expected decode for op ${String(m.op)}`);
  }
}

const valid = load("valid");
const invalid = load("invalid");

describe.skipIf(!available)("spec envelope fixtures: valid", () => {
  it("the corpus is there", () => {
    expect(valid.length).toBeGreaterThan(0);
  });

  for (const { file, fixture } of valid) {
    it(`${file}: ${fixture.description}`, () => {
      expect(fixture.schema_valid).toBe(true);
      expect(fixture.wire_valid).toBe(true);
      expect(validateSchema!(fixture.message), JSON.stringify(validateSchema!.errors)).toBe(true);
      const m = fixture.message as Record<string, any>;
      if (AGENT_TO_EDGE.has(fixture.op)) {
        const svc = m.services[0];
        const encoded = fixture.op === "register" ? encodeRegister(svc.name, svc.kind as ServiceKind) : encodeUnregister(svc.name);
        expect(JSON.stringify(encoded)).toBe(JSON.stringify({ mcpwarp: m }));
        expect(isValidServiceName(svc.name)).toBe(true);
        return;
      }
      const r = decode(m);
      expect(r.ok, r.ok ? "" : String((r as { error: unknown }).error)).toBe(true);
      expect((r as { value: unknown }).value).toEqual(expectedDecode(m));
    });
  }
});

describe.skipIf(!available)("spec envelope fixtures: invalid", () => {
  it("the corpus is there", () => {
    expect(invalid.length).toBeGreaterThan(0);
  });

  for (const { file, fixture } of invalid) {
    it(`${file}: ${fixture.description}`, () => {
      expect(fixture.schema_valid).toBe(false);
      expect(validateSchema!(fixture.message)).toBe(false);
      const code = fixture.expect!.error_code;
      const m = fixture.message as Record<string, any>;

      if (AGENT_TO_EDGE.has(fixture.op)) {
        // The edge's verdict; what the agent side can check is its own name rule and the decoder ignoring v/op it does not know.
        if (code === "INVALID_NAME") expect(isValidServiceName(m.services[0].name)).toBe(false);
        if (code === "UNSUPPORTED_VERSION" || code === "UNKNOWN_OP") expect(decode(m)).toEqual({ ok: true, value: { op: "unknown", rawOp: m.op, v: m.v } });
        return;
      }

      const r = decode(m);
      if (fixture.wire_valid) {
        expect(code.startsWith("SCHEMA_"), code).toBe(true);
        expect(r.ok, r.ok ? "" : String((r as { error: unknown }).error)).toBe(true);
        expect((r as { value: unknown }).value).toEqual(expectedDecode(m));
        return;
      }
      if (code === "UNSUPPORTED_VERSION" || code === "UNKNOWN_OP") {
        expect(r).toEqual({ ok: true, value: { op: "unknown", rawOp: m.op, v: m.v } });
        return;
      }
      expect(code).toBe("MALFORMED_ENVELOPE");
      expect(r.ok).toBe(false);
      const err = (r as { error: unknown }).error;
      expect(err).toBeInstanceOf(McpwarpError);
      expect((err as McpwarpError).code).toBe("MALFORMED_ENVELOPE");
      if (fixture.expect!.message_contains) expect((err as McpwarpError).message).toContain(fixture.expect!.message_contains);
    });
  }
});

describe.skipIf(available)("spec envelope fixtures (unavailable)", () => {
  // CI fetches the spec at spec.pin, so a missing corpus there is a broken pin, not a local setup gap.
  if (process.env.CI) it("spec fixtures are required in CI", () => expect.fail(skipReason));
  else it.skip(skipReason, () => {});
});
