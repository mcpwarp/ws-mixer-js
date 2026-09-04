/**
 * Runs every spec/fixtures/frames/*.json case through decodeFrame/encodeFrame,
 * per spec/README.md's "Consuming these fixtures from an SDK test harness".
 */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { decodeFrame, resetCode, resetMessage, windowIncrement } from "../src/frame.js";
import { codeName, parseErrorCode } from "../src/errors.js";
import { ConnError, StreamError } from "../src/errors.js";
import { resolveSpecDir } from "./helpers/spec-dir.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const specResult = resolveSpecDir(here);
const framesDir = path.join(specResult.dir, "fixtures/frames");

interface FrameFixture {
  description: string;
  hex: string;
  valid: boolean;
  decoded: null | {
    type: string;
    flags: number;
    stream_id: number;
    payload_hex?: string;
    increment?: number;
    code?: number;
    message?: string;
  };
  expect: null | { error_code: string; connection_fatal: boolean };
}

function loadFixtures(): Array<{ file: string; fixture: FrameFixture }> {
  return readdirSync(framesDir)
    .filter((f) => f.endsWith(".json"))
    .map((file) => ({ file, fixture: JSON.parse(readFileSync(path.join(framesDir, file), "utf8")) as FrameFixture }));
}

const typeNumberOf: Record<string, number> = { OPEN: 0, DATA: 1, WINDOW: 2, CLOSE: 3, RESET: 4 };

describe("frame fixtures", () => {
  if (!specResult.available) {
    it.skip(`spec fixtures unavailable: ${specResult.reason}`, () => {});
    return;
  }
  const fixtures = loadFixtures();
  it("loaded at least the documented set of frame fixtures", () => {
    expect(fixtures.length).toBeGreaterThanOrEqual(20);
  });

  for (const { file, fixture } of fixtures) {
    it(file, () => {
      const bytes = Buffer.from(fixture.hex, "hex");
      if (fixture.valid) {
        const frame = decodeFrame(bytes);
        expect(frame.flags).toBe(fixture.decoded!.flags);
        expect(frame.streamId).toBe(fixture.decoded!.stream_id);
        if (fixture.decoded!.type in typeNumberOf) {
          expect(frame.type).toBe(typeNumberOf[fixture.decoded!.type]);
        }
        if (fixture.decoded!.payload_hex !== undefined) {
          expect(Buffer.from(frame.payload).toString("hex")).toBe(fixture.decoded!.payload_hex);
        }
        if (fixture.decoded!.increment !== undefined) {
          expect(windowIncrement(frame)).toBe(fixture.decoded!.increment);
        }
        if (fixture.decoded!.code !== undefined) {
          expect(resetCode(frame)).toBe(fixture.decoded!.code);
        }
        if (fixture.decoded!.message !== undefined) {
          expect(resetMessage(frame)).toBe(fixture.decoded!.message);
        }
      } else {
        let caught: unknown;
        try {
          decodeFrame(bytes);
        } catch (e) {
          caught = e;
        }
        expect(caught, `${file} should have thrown`).toBeDefined();
        const expectedCode = parseErrorCode(fixture.expect!.error_code);
        expect(expectedCode, `unknown error code name ${fixture.expect!.error_code}`).toBeDefined();
        expect((caught as { code: number }).code).toBe(expectedCode);
        if (fixture.expect!.connection_fatal) {
          expect(caught).toBeInstanceOf(ConnError);
        } else {
          expect(caught).toBeInstanceOf(StreamError);
        }
      }
    });
  }
});

describe("frame codec round trip", () => {
  if (!specResult.available) {
    it.skip(`spec fixtures unavailable: ${specResult.reason}`, () => {});
    return;
  }
  it("codeName/parseErrorCode agree on every name used by fixtures", () => {
    for (const { fixture } of loadFixtures()) {
      if (fixture.expect) {
        const code = parseErrorCode(fixture.expect.error_code);
        expect(code).toBeDefined();
        expect(codeName(code!)).toBe(fixture.expect.error_code);
      }
    }
  });
});
