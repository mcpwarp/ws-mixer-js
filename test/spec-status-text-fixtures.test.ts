/**
 * spec/fixtures/mcpwarp/status-text (MCPWARP.md sections 7 and 10.5): the
 * shipped tables must equal the per-locale JSON string for string, every
 * cases.json input must describe exactly as expected, and rules.json must
 * match the resolution order and colon joiners. status-text.ts keeps its own
 * tables (no fs at runtime); this is what keeps them in sync.
 */
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { describeStatus, statusLocales, TunnelStatusCode, type TunnelStatus } from "../src/mcpwarp/index.js";
import { resolveSpecDir } from "./helpers/spec-dir.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const specResult = resolveSpecDir(here);
const dir = path.join(specResult.dir, "fixtures/mcpwarp/status-text");
const load = (file: string): any => JSON.parse(readFileSync(path.join(dir, file), "utf8"));

describe("mcpwarp status-text fixtures", () => {
  if (!specResult.available) {
    // CI fetches the spec at spec.pin, so a missing corpus there is a broken pin, not a local setup gap.
    if (process.env.CI) it("spec fixtures are required in CI", () => expect.fail(specResult.reason));
    else it.skip(`spec fixtures unavailable: ${specResult.reason}`, () => {});
    return;
  }
  const rules = load("rules.json");
  const cases: Array<{ description: string; input: { status: TunnelStatus; locale: string | null }; expect: { label: string; detail?: string } }> = load("cases.json").cases;

  it("rules.json locales are statusLocales, in resolution order", () => {
    expect([...statusLocales]).toEqual(rules.locales);
    expect([...statusLocales]).toEqual(rules.localeResolution.order);
    expect(describeStatus({ state: "off" }, { locale: "zh" })).toEqual(describeStatus({ state: "off" }, { locale: rules.zhAlias }));
  });

  for (const locale of rules.locales as string[]) {
    it(`${locale}.json equals the shipped table (every legal pair, string for string)`, () => {
      const table: Record<string, { label: string; detail?: string }> = load(`${locale}.json`);
      expect(Object.keys(table).sort()).toEqual(Object.keys(load("en.json")).sort());
      for (const [key, want] of Object.entries(table)) {
        expect(key).toMatch(new RegExp(rules.keyFormat.pattern));
        const [state, code] = key.split(".") as [TunnelStatus["state"], string];
        const status: TunnelStatus = code === "none" ? { state } : { state, code: code as TunnelStatus["code"] & string };
        expect(describeStatus(status, { locale }), `${locale}:${key}`).toStrictEqual(want);
        if (code === "none") continue;
        // The raw sentence, unhidden by the equal-to-label rule: an empty one, or one only equal to the label, must not pass.
        const raw = describeStatus(status, { locale, messages: { states: { [state]: "\u0000" } } }).detail;
        expect(raw, `${locale}:${key} sentence`).toBe(want.detail ?? want.label);
      }
      const codes = new Set(Object.keys(table).map((k) => k.split(".")[1]));
      for (const c of Object.values(TunnelStatusCode)) expect(codes.has(c), `${locale} covers ${c}`).toBe(true);
    });

    it(`${locale}: INVALID_CONFIG composition uses rules.json's colon joiner and rewrite`, () => {
      const sentence = describeStatus({ state: "error", code: "INVALID_CONFIG" }, { locale }).detail!;
      const generic = rules.invalidConfigComposition.genericLastError as string;
      expect(describeStatus({ state: "error", code: "INVALID_CONFIG", lastError: generic }, { locale }).detail).toBe(sentence);
      const rewrite = rules.lastErrorColonRewrite;
      const lastError = "Bad value:\t x: y";
      expect(describeStatus({ state: "error", code: "INVALID_CONFIG", lastError }, { locale }).detail).toBe(
        sentence + rules.colonJoiner[locale] + lastError.replace(new RegExp(rewrite.pattern, rewrite.flags), rewrite.replacement),
      );
    });
  }

  it("newInSpec strings are shipped", () => {
    for (const entry of rules.newInSpec as string[]) {
      const [locale, key] = entry.split(":") as [string, string];
      const [state, code] = key.split(".");
      expect(describeStatus({ state, code } as TunnelStatus, { locale })).toStrictEqual(load(`${locale}.json`)[key]);
    }
  });

  it("the host-side off pattern", () => {
    const { status, locale, expect: want } = rules.hostOffPattern.example;
    expect(describeStatus(status, { locale })).toStrictEqual(want);
  });

  for (const c of cases) {
    it(`case: ${c.description}`, () => {
      const got = c.input.locale === null ? describeStatus(c.input.status) : describeStatus(c.input.status, { locale: c.input.locale });
      expect(got).toStrictEqual(c.expect);
    });
  }
});
