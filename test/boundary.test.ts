/**
 * Import boundary (docs/DECISIONS.md D-2026-10-12-01): nothing reachable
 * from the platform-neutral entries (`.`, `./mcpwarp`, `./testing`) may
 * import anything but a relative path -- no `node:*`, no Node builtin, no
 * npm package -- and none of it may reach `src/node/**`. Walks the TS
 * sources; scripts/check-neutral.mjs does the same over the built dist.
 */
import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const here = path.dirname(fileURLToPath(import.meta.url));
const src = path.resolve(here, "../src");
const ENTRIES = ["index.ts", "mcpwarp/index.ts", "testing/index.ts"].map((p) => path.join(src, p));

const SPECIFIER_RE = /(?:\bimport|\bexport)\s+(?:type\s+)?(?:[\w*{}\s,$]+?\s+from\s+)?["']([^"']+)["']|\bimport\s*\(\s*["']([^"']+)["']\s*\)/g;

function specifiers(file: string): string[] {
  const text = readFileSync(file, "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
  const out: string[] = [];
  for (const m of text.matchAll(SPECIFIER_RE)) out.push((m[1] ?? m[2])!);
  return out;
}

function resolveRelative(from: string, spec: string): string {
  const base = path.resolve(path.dirname(from), spec);
  for (const candidate of [base.replace(/\.js$/, ".ts"), `${base}.ts`, path.join(base, "index.ts")]) {
    if (existsSync(candidate)) return candidate;
  }
  throw new Error(`unresolvable import ${spec} in ${from}`);
}

function walk(): { files: Set<string>; violations: string[] } {
  const files = new Set<string>();
  const violations: string[] = [];
  const queue = [...ENTRIES];
  while (queue.length > 0) {
    const file = queue.pop()!;
    if (files.has(file)) continue;
    files.add(file);
    if (file.startsWith(path.join(src, "node") + path.sep)) violations.push(`${path.relative(src, file)} (under src/node) is reachable`);
    for (const spec of specifiers(file)) {
      if (!spec.startsWith(".")) {
        violations.push(`${path.relative(src, file)} imports ${JSON.stringify(spec)}`);
        continue;
      }
      queue.push(resolveRelative(file, spec));
    }
  }
  return { files, violations };
}

describe("platform-neutral import boundary", () => {
  it("the neutral entries reach only relative imports, and never src/node", () => {
    const { files, violations } = walk();
    expect(violations).toEqual([]);
    expect(files.size).toBeGreaterThan(20);
    expect([...files].some((f) => f.endsWith(path.join("src", "client.ts")))).toBe(true);
    expect([...files].some((f) => f.endsWith(path.join("testing", "fake-edge.ts")))).toBe(true);
  });

  it("the specifier scan sees every import form", () => {
    const sample = [
      'import { a } from "./a.js";',
      'import type { B } from "node:http";',
      'export { c } from "./c.js";',
      'export * from "ws";',
      'import "./side-effect.js";',
      'const m = await import("node:fs");',
      "import {\n  x,\n  y,\n} from \"./multi.js\";",
    ].join("\n");
    const found: string[] = [];
    for (const m of sample.matchAll(SPECIFIER_RE)) found.push((m[1] ?? m[2])!);
    expect(found).toEqual(["./a.js", "node:http", "./c.js", "ws", "./side-effect.js", "node:fs", "./multi.js"]);
  });
});
