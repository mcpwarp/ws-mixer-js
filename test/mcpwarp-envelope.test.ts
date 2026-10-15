import { describe, expect, it } from "vitest";
import { z } from "zod";
import { decodeEnvelope, encodeRegister, encodeUnregister, INTEGRATION_ID_RE, SERVICE_NAME_RE, TRANSIENT_REGISTER_ERRORS } from "../src/mcpwarp/envelope.js";
import { McpwarpError } from "../src/mcpwarp/errors.js";
import { AppErrorCode, RegisterErrorCode } from "../src/mcpwarp/index.js";
import * as mcpwarp from "../src/mcpwarp/index.js";
import * as root from "../src/index.js";

describe("mcpwarp envelope encoding", () => {
  it("register and unregister are byte-exact with the tunnel's shapes", () => {
    expect(JSON.stringify(encodeRegister("anki", "http"))).toBe('{"mcpwarp":{"v":1,"op":"register","services":[{"name":"anki","kind":"http"}]}}');
    expect(JSON.stringify(encodeUnregister("anki"))).toBe('{"mcpwarp":{"v":1,"op":"unregister","services":[{"name":"anki"}]}}');
  });

  it("INTEGRATION_ID_RE accepts exactly what zod's z.uuid() accepts, once lowercased", () => {
    const zodUuid = z.uuid();
    const samples = [
      "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10",
      "0F6C2A8E-4C1B-4B7E-9A4E-2F1D9B7C3A10",
      "00000000-0000-0000-0000-000000000000",
      "ffffffff-ffff-ffff-ffff-ffffffffffff",
      "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
      "0f6c2a8e-4c1b-1b7e-8a4e-2f1d9b7c3a10",
      "0f6c2a8e-4c1b-8b7e-ba4e-2f1d9b7c3a10",
      "0f6c2a8e-4c1b-0b7e-9a4e-2f1d9b7c3a10",
      "0f6c2a8e-4c1b-9b7e-9a4e-2f1d9b7c3a10",
      "0f6c2a8e-4c1b-4b7e-7a4e-2f1d9b7c3a10",
      "0f6c2a8e-4c1b-4b7e-ca4e-2f1d9b7c3a10",
      "0f6c2a8e4c1b4b7e9a4e2f1d9b7c3a10",
      "{0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10}",
      " 0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10",
      "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a1g",
      "my-zotero-plugin",
      "",
    ];
    for (const s of samples) expect(INTEGRATION_ID_RE.test(s.toLowerCase()), s).toBe(zodUuid.safeParse(s.toLowerCase()).success);
    expect(samples.filter((s) => INTEGRATION_ID_RE.test(s.toLowerCase()))).toHaveLength(7);
  });

  it("SERVICE_NAME_RE matches the tunnel's name rule", () => {
    for (const ok of ["a", "anki", "a-b", "x1", "1x"]) expect(SERVICE_NAME_RE.test(ok)).toBe(true);
    for (const bad of ["", "-a", "a-", "A", "a_b", "a.b"]) expect(SERVICE_NAME_RE.test(bad)).toBe(false);
  });

  it("SERVICE_NAME_MAX, isValidServiceName and webSocketFactory are public from ./mcpwarp (MCPWARP.md sections 2.4, 9)", () => {
    expect(mcpwarp.SERVICE_NAME_MAX).toBe(30);
    expect(mcpwarp.isValidServiceName("a".repeat(30))).toBe(true);
    expect(mcpwarp.isValidServiceName("a".repeat(31))).toBe(false);
    expect(mcpwarp.isValidServiceName("zotero-library-notes-search-01")).toBe(true);
    for (const bad of ["", "a-", "-a", "Anki", "a b", 7, null, undefined]) expect(mcpwarp.isValidServiceName(bad)).toBe(false);
    expect(mcpwarp.webSocketFactory).toBe(root.webSocketFactory);
  });

  it("error-code constants: exported from ./mcpwarp, each value equals its key, lists match the tunnel's (DECISIONS.md)", () => {
    // registered.errors[].code: the tunnel's own codes, then the pass-through RPC codes.
    const register = [
      "INVALID_NAME", "USERNAME_REQUIRED", "QUOTA_EXCEEDED", "CONFLICT", "SERVER_DISABLED", "INTERNAL",
      "NO_RESPONDERS", "TIMEOUT", "ABORTED", "CONNECTION_LOST", "MALFORMED_REPLY", "BAD_REQUEST",
      "UNAUTHENTICATED", "FORBIDDEN", "NOT_IMPLEMENTED", "UNAVAILABLE", "SERVICE_ERROR",
    ];
    const app = ["BAD_REQUEST", "UNSUPPORTED_VERSION", "UNKNOWN_OP", "TOO_MANY_SERVICES", "OVERLOADED", "CONNECTION_LIMIT"];
    expect(Object.keys(RegisterErrorCode).sort()).toEqual([...register].sort());
    expect(Object.keys(AppErrorCode).sort()).toEqual([...app].sort());
    for (const table of [RegisterErrorCode, AppErrorCode]) {
      for (const [k, v] of Object.entries(table)) expect(v).toBe(k);
    }
    for (const code of TRANSIENT_REGISTER_ERRORS) expect(register).toContain(code);
  });

  it("transient register errors are the retryable RPC set", () => {
    expect([...TRANSIENT_REGISTER_ERRORS].sort()).toEqual(
      ["ABORTED", "CONNECTION_LOST", "INTERNAL", "MALFORMED_REPLY", "NO_RESPONDERS", "SERVICE_ERROR", "TIMEOUT", "UNAVAILABLE"].sort(),
    );
  });
});

describe("decodeEnvelope", () => {
  it("registered: services and errors, with defaults", () => {
    const env = decodeEnvelope({
      mcpwarp: {
        v: 1,
        op: "registered",
        services: [{ name: "anki", id: "srv_1", url: "https://anki-u.mcpwarp.io/mcp", created: true }, { name: "b", id: "srv_2", url: "https://b-u.mcpwarp.io/mcp", created: null }],
        errors: [{ name: "c", code: "CONFLICT" }, { name: "d", code: "SERVER_DISABLED", message: "off", id: "srv_4" }],
      },
    });
    expect(env).toEqual({
      op: "registered",
      services: [
        { name: "anki", id: "srv_1", url: "https://anki-u.mcpwarp.io/mcp", created: true },
        { name: "b", id: "srv_2", url: "https://b-u.mcpwarp.io/mcp", created: false },
      ],
      errors: [
        { name: "c", code: "CONFLICT", message: "" },
        { name: "d", code: "SERVER_DISABLED", message: "off", id: "srv_4" },
      ],
    });
  });

  it("registered.services[].url missing or empty -> MALFORMED_ENVELOPE (MCPWARP.md section 2.8 rule 8)", () => {
    for (const url of [undefined, "", null, 7]) {
      const svc: Record<string, unknown> = { name: "anki", id: "srv_1", created: true };
      if (url !== undefined) svc.url = url;
      try {
        decodeEnvelope({ mcpwarp: { v: 1, op: "registered", services: [svc] } });
        expect.unreachable(JSON.stringify(svc));
      } catch (e) {
        expect(e).toBeInstanceOf(McpwarpError);
        expect((e as McpwarpError).code).toBe("MALFORMED_ENVELOPE");
        expect((e as McpwarpError).message).toBe("registered.services[].url must be a non-empty string");
      }
    }
  });

  it("registered.services[] entry: a multi-defect entry reports the first of name, id, url, created (MCPWARP.md section 2.8 rule 10)", () => {
    const cases: Array<[Record<string, unknown>, string]> = [
      [{ name: 7, url: "", created: "yes" }, "registered.services[].name must be a string"],
      [{ name: "anki", id: 7, url: "", created: "yes" }, "registered.services[].id must be a string"],
      [{ name: "anki", id: "srv_anki", url: "", created: "yes" }, "registered.services[].url must be a non-empty string"],
      [{ name: "anki", id: "srv_anki", url: "https://x", created: "yes" }, "registered.services[].created must be a boolean"],
    ];
    for (const [svc, message] of cases) {
      try {
        decodeEnvelope({ mcpwarp: { v: 1, op: "registered", services: [svc] } });
        expect.unreachable(JSON.stringify(svc));
      } catch (e) {
        expect(e).toBeInstanceOf(McpwarpError);
        expect((e as McpwarpError).code).toBe("MALFORMED_ENVELOPE");
        expect((e as McpwarpError).message).toBe(message);
      }
    }
  });

  it("registered with no services/errors arrays defaults both to []", () => {
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "registered" } })).toEqual({ op: "registered", services: [], errors: [] });
  });

  it("unregistered, disable (reason default), enable, error (message default)", () => {
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "unregistered", services: [{ name: "a", id: "srv" }] } })).toEqual({
      op: "unregistered",
      services: [{ name: "a", id: "srv" }],
    });
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "disable", id: "srv" } })).toEqual({ op: "disable", id: "srv", reason: "disabled" });
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "enable", id: "srv", name: "a" } })).toEqual({ op: "enable", id: "srv", name: "a" });
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "error", code: "OVERLOADED" } })).toEqual({ op: "error", code: "OVERLOADED", message: "" });
  });

  it("conflict: id, name, reason (default taken_over)", () => {
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "conflict", id: "srv_1", name: "anki", reason: "taken_over" } })).toEqual({
      op: "conflict",
      id: "srv_1",
      name: "anki",
      reason: "taken_over",
    });
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "conflict", id: "srv_1", name: "anki" } })).toEqual({ op: "conflict", id: "srv_1", name: "anki", reason: "taken_over" });
  });

  it("v:2 and unknown ops decode as unknown", () => {
    expect(decodeEnvelope({ mcpwarp: { v: 2, op: "registered" } })).toEqual({ op: "unknown", rawOp: "registered", v: 2 });
    expect(decodeEnvelope({ mcpwarp: { v: 1, op: "future" } })).toEqual({ op: "unknown", rawOp: "future", v: 1 });
  });

  it("a body without an object-valued mcpwarp key is not ours", () => {
    expect(decodeEnvelope({ other: 1 })).toBeUndefined();
    expect(decodeEnvelope({ mcpwarp: "x" })).toBeUndefined();
    expect(decodeEnvelope({ mcpwarp: [1] })).toBeUndefined();
  });

  it("a known op with wrong field types throws MALFORMED_ENVELOPE", () => {
    for (const body of [
      { mcpwarp: { v: 1, op: "registered", services: [{ name: 1, id: "x" }] } },
      { mcpwarp: { v: 1, op: "registered", services: "nope" } },
      { mcpwarp: { v: 1, op: "registered", services: [{ name: "a", id: "x", created: "yes" }] } },
      { mcpwarp: { v: 1, op: "disable" } },
      { mcpwarp: { v: 1, op: "enable", id: "x" } },
      { mcpwarp: { v: 1, op: "error", code: 5 } },
      { mcpwarp: { v: 1, op: "conflict", name: "a" } },
      { mcpwarp: { v: 1, op: "conflict", id: "x" } },
      { mcpwarp: { v: 1, op: "conflict", id: "x", name: 1 } },
      { mcpwarp: { v: 1, op: "conflict", id: "x", name: "a", reason: 7 } },
    ]) {
      try {
        decodeEnvelope(body);
        expect.unreachable(JSON.stringify(body));
      } catch (e) {
        expect(e).toBeInstanceOf(McpwarpError);
        expect((e as McpwarpError).code).toBe("MALFORMED_ENVELOPE");
      }
    }
  });
});
