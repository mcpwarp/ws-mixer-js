import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
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
  target: "node20",
  splitting: false,
});
