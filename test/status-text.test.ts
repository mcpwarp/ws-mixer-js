/** describeStatus(): shipped tables, locale resolution, overrides, detail rules. */
import { describe, expect, it } from "vitest";
import { describeStatus, statusLocales, TunnelStatusCode, type StatusLocale, type TunnelState, type TunnelStatus } from "../src/mcpwarp/index.js";

const STATES: TunnelState[] = ["off", "connecting", "online", "disabled", "conflict", "error"];
const CODES = Object.values(TunnelStatusCode);

// Strings that are the same as English by design (loanword or same word).
const SAME_AS_EN: Partial<Record<StatusLocale, string[]>> = {
  de: ["states.online"],
  es: ["states.error"],
};

const msg = (locale: string) => ({
  states: Object.fromEntries(STATES.map((s) => [s, describeStatus({ state: s }, { locale }).label])),
  codes: Object.fromEntries(CODES.map((c) => [c, describeStatus({ state: "error", code: c }, { locale }).detail])),
});

describe("describeStatus tables", () => {
  it("ships en, de, es, fr, ja, zh-CN", () => {
    expect([...statusLocales]).toEqual(["en", "de", "es", "fr", "ja", "zh-CN"]);
    expect(CODES).toHaveLength(12);
  });

  it("every locale has a non-empty string for all 6 states and 12 codes", () => {
    for (const locale of statusLocales) {
      const m = msg(locale);
      for (const s of STATES) expect(typeof m.states[s] === "string" && m.states[s]!.length > 0, `${locale} states.${s}`).toBe(true);
      for (const c of CODES) expect(typeof m.codes[c] === "string" && m.codes[c]!.length > 0, `${locale} codes.${c}`).toBe(true);
    }
  });

  it("no translated string equals the English one except the listed ones", () => {
    const en = msg("en");
    for (const locale of statusLocales) {
      if (locale === "en") continue;
      const m = msg(locale);
      const same = [
        ...STATES.filter((s) => m.states[s] === en.states[s]).map((s) => `states.${s}`),
        ...CODES.filter((c) => m.codes[c] === en.codes[c]).map((c) => `codes.${c}`),
      ];
      expect(same, locale).toEqual(SAME_AS_EN[locale] ?? []);
    }
  });

  it("lint: no ASCII '...', no trailing '.' or '。', no leading or trailing whitespace", () => {
    for (const locale of statusLocales) {
      const m = msg(locale);
      const all: Array<[string, string]> = [...STATES.map((s) => [`states.${s}`, m.states[s]!] as [string, string]), ...CODES.map((c) => [`codes.${c}`, m.codes[c]!] as [string, string])];
      for (const [key, s] of all) {
        expect(s.includes("..."), `${locale} ${key}`).toBe(false);
        expect(/[.。]$/.test(s), `${locale} ${key}`).toBe(false);
        expect(s, `${locale} ${key}`).toBe(s.trim());
      }
    }
  });

  it("statusLocales is frozen", () => {
    expect(Object.isFrozen(statusLocales)).toBe(true);
  });

  it("English strings", () => {
    expect(describeStatus({ state: "connecting", code: "RETRYING", lastError: "x." })).toEqual({ label: "Connecting…", detail: "Connection failed, retrying" });
    expect(describeStatus({ state: "error", code: "QUOTA_EXCEEDED", lastError: "x." })).toEqual({ label: "Error", detail: "Plan limit reached" });
  });

  it("French is accented", () => {
    expect(describeStatus({ state: "error", code: "CONFLICT" }, { locale: "fr" })).toEqual({ label: "Erreur", detail: "Nom déjà utilisé" });
    expect(describeStatus({ state: "error", code: "DISCONNECTED_FATAL" }, { locale: "fr" }).detail).toBe("Échec de la connexion à mcpwarp");
  });
});

describe("describeStatus locale resolution", () => {
  const label = (locale: unknown) => describeStatus({ state: "off" }, { locale: locale as string }).label;

  it("exact tags, case-insensitive, '_' as '-'", () => {
    expect(label("de")).toBe("Aus");
    expect(label("DE")).toBe("Aus");
    expect(label("zh-cn")).toBe("已关闭");
    expect(label("zh_CN")).toBe("已关闭");
    expect(label(" fr ")).toBe("Désactivé");
    expect(label("en-US")).toBe("Off");
    expect(label("EN_us")).toBe("Off");
  });

  it("falls back to the language subtag", () => {
    expect(label("de-AT")).toBe("Aus");
    expect(label("ja-JP")).toBe("オフ");
    expect(label("es_MX")).toBe("Desactivado");
    expect(label("zh")).toBe("已关闭");
    expect(label("zh-TW")).toBe("已关闭");
  });

  it("anything else is English", () => {
    expect(label("pt_BR")).toBe("Off");
    expect(label("xx")).toBe("Off");
    expect(label("")).toBe("Off");
    expect(label(undefined)).toBe("Off");
    expect(label(42)).toBe("Off");
    expect(label(null)).toBe("Off");
    expect(label({ toString: () => "de" })).toBe("Off");
    expect(label("__proto__")).toBe("Off");
    expect(label("constructor")).toBe("Off");
    expect(label("toString")).toBe("Off");
    expect(describeStatus({ state: "off" }).label).toBe("Off");
  });
});

describe("describeStatus detail", () => {
  it("online and off have no detail", () => {
    expect(describeStatus({ state: "online", url: "https://x.mcpwarp.com/mcp" }, { locale: "de" })).toEqual({ label: "Online" });
    expect(describeStatus({ state: "off" }, { locale: "ja" })).toEqual({ label: "オフ" });
    expect("detail" in describeStatus({ state: "off" })).toBe(false);
  });

  it("INVALID_CONFIG appends lastError after a per-locale colon", () => {
    const s: TunnelStatus = { state: "error", code: "INVALID_CONFIG", lastError: "The service name is not set." };
    expect(describeStatus(s)).toEqual({ label: "Error", detail: "Invalid settings: The service name is not set." });
    expect(describeStatus(s, { locale: "de" })).toEqual({ label: "Fehler", detail: "Ungültige Einstellungen: The service name is not set." });
    expect(describeStatus(s, { locale: "fr" }).detail).toBe("Paramètres non valides : The service name is not set.");
    expect(describeStatus(s, { locale: "ja" }).detail).toBe("設定が無効です：The service name is not set.");
    expect(describeStatus(s, { locale: "zh-CN" }).detail).toBe("设置无效：The service name is not set.");
    expect(describeStatus({ state: "error", code: "INVALID_CONFIG" }).detail).toBe("Invalid settings");
    expect(describeStatus({ state: "error", code: "INVALID_CONFIG", lastError: "" }).detail).toBe("Invalid settings");
  });

  it("INVALID_CONFIG does not repeat itself or produce two colons", () => {
    const invalid = (lastError: string) => ({ state: "error", code: "INVALID_CONFIG", lastError }) as const;
    expect(describeStatus(invalid("The tunnel settings are invalid."))).toEqual({ label: "Error", detail: "Invalid settings" });
    expect(describeStatus(invalid("The tunnel settings are invalid."), { locale: "ja" }).detail).toBe("設定が無効です");
    expect(describeStatus(invalid("Invalid settings"))).toEqual({ label: "Error", detail: "Invalid settings" });
    expect(describeStatus(invalid("Invalid settings: no token."))).toEqual({ label: "Error", detail: "Invalid settings: no token." });
    const name = "The service name is invalid: use lowercase letters, digits and dashes (not at the start or end), at most 63 characters.";
    expect(describeStatus(invalid(name)).detail).toBe(
      "Invalid settings: The service name is invalid; use lowercase letters, digits and dashes (not at the start or end), at most 63 characters.",
    );
    expect(describeStatus(invalid(name), { locale: "de" }).detail!.split(":")).toHaveLength(2);
    expect(describeStatus(invalid("Bad url https://x.example/mcp.")).detail).toBe("Invalid settings: Bad url https://x.example/mcp.");
  });

  it("an empty lastError gives no detail", () => {
    expect(describeStatus({ state: "error", lastError: "" })).toEqual({ label: "Error" });
    expect(describeStatus({ state: "off", lastError: "" }, { locale: "de" })).toEqual({ label: "Aus" });
  });

  it("other codes use only the localized sentence", () => {
    expect(describeStatus({ state: "error", code: "UNAUTHORIZED", lastError: "The token was rejected." }, { locale: "fr" }).detail).toBe("Jeton d'accès refusé");
  });

  it("a detail equal to the label is left out", () => {
    for (const locale of statusLocales) {
      const disabled = describeStatus({ state: "disabled", code: "DISABLED", lastError: "This service is disabled in the mcpwarp dashboard." }, { locale });
      const conflict = describeStatus({ state: "conflict", code: "TAKEN_OVER", lastError: "Another connection took over this name." }, { locale });
      expect("detail" in disabled, `${locale} disabled`).toBe(false);
      expect("detail" in conflict, `${locale} conflict`).toBe(false);
    }
    expect(describeStatus({ state: "disabled", code: "DISABLED" }, { messages: { codes: { DISABLED: "Turned off by an admin" } } })).toEqual({
      label: "Disabled in the mcpwarp dashboard",
      detail: "Turned off by an admin",
    });
  });

  it("a code-less status uses lastError", () => {
    expect(describeStatus({ state: "off", lastError: "No window to connect from." }, { locale: "de" })).toEqual({ label: "Aus", detail: "No window to connect from." });
    expect(describeStatus({ state: "error", lastError: "Boom." }).detail).toBe("Boom.");
  });

  it("an unknown code falls back to lastError or nothing, without throwing", () => {
    const bogus = (code: string, lastError?: string) => ({ state: "error", code, lastError }) as unknown as TunnelStatus;
    expect(describeStatus(bogus("NEW_CODE", "Something new."))).toEqual({ label: "Error", detail: "Something new." });
    expect(describeStatus(bogus("NEW_CODE"))).toEqual({ label: "Error" });
    expect(describeStatus(bogus("toString"))).toEqual({ label: "Error" });
    expect(describeStatus(bogus("__proto__", "x."))).toEqual({ label: "Error", detail: "x." });
    expect(describeStatus({ state: "__proto__" as never })).toEqual({ label: "__proto__" });
  });
});

describe("describeStatus overrides", () => {
  it("replaces only the given keys, over the resolved locale", () => {
    const opts = { locale: "de", messages: { states: { error: "Problem" }, codes: { CONFLICT: "Name vergeben" } } };
    expect(describeStatus({ state: "error", code: "CONFLICT" }, opts)).toEqual({ label: "Problem", detail: "Name vergeben" });
    expect(describeStatus({ state: "off" }, opts).label).toBe("Aus");
    expect(describeStatus({ state: "error", code: "INTERNAL" }, opts).detail).toBe("Unerwarteter Fehler");
  });

  it("does not leak into later calls or the shipped tables", () => {
    describeStatus({ state: "off" }, { messages: { states: { off: "Stopped" } } });
    expect(describeStatus({ state: "off" }).label).toBe("Off");
  });

  it("ignores non-string values and garbage", () => {
    const messages = { states: { off: 1, online: "Up" }, codes: "nope" } as never;
    expect(describeStatus({ state: "off" }, { messages }).label).toBe("Off");
    expect(describeStatus({ state: "online" }, { messages }).label).toBe("Up");
    expect(describeStatus({ state: "error", code: "INTERNAL" }, { messages }).detail).toBe("Unexpected error");
  });

  it("ignores a JSON-parsed __proto__ key", () => {
    const messages = { states: JSON.parse('{"__proto__":{"off":"P"}}') };
    expect(describeStatus({ state: "off" }, { messages }).label).toBe("Off");
    expect(({} as Record<string, unknown>).off).toBeUndefined();
  });
});
