import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "mcpwarp/index": "src/mcpwarp/index.ts",
    "node/index": "src/node/index.ts",
    "testing/index": "src/testing/index.ts",
  },
  // ESM only: the sub-path entries share chunks (one copy of each class, so
  // `instanceof` holds across `.`/`./mcpwarp`/`./node`/`./testing`), which
  // needs esbuild's code splitting, and tsup only splits ESM properly.
  format: ["esm"],
  // Declarations are generated separately (see package.json's "build:types"
  // script) via a plain `tsc --emitDeclarationOnly`, not here: tsup's
  // bundled `dts: true` pass ignores `stripInternal` entirely (verified
  // against tsup 8.5.1 -- its dts bundler never reads that compiler option,
  // with or without passing it through `dts: { compilerOptions }`), so it
  // was leaking every `@internal`-tagged member straight into the public
  // .d.ts. Plain `tsc` on the same tsconfig.json honours `stripInternal`
  // correctly.
  dts: false,
  sourcemap: true,
  clean: true,
  target: "es2022",
  // Keep `node:` specifiers in ./node as written (tsup strips them by default).
  removeNodeProtocol: false,
  // Each entry also gets bare `import "../chunk-*.js"` lines for the chunks it
  // reaches transitively (esbuild's evaluation-order imports), which is why
  // package.json has no `"sideEffects": false`: with it, esbuild consumers warn
  // `ignored-bare-import` on every one.
  splitting: true,
  // Only so `node:` builtins stay external for ./node; neutrality of the
  // other entries is enforced separately (scripts/check-neutral.mjs).
  platform: "node",
});
