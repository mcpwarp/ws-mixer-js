/**
 * Resolves the checkout of ws-mixer-spec that test/frame.test.ts,
 * test/control.test.ts and test/sequence.test.ts read fixtures from.
 * Post-split, the spec is a separate repo -- resolution order:
 *
 *  1. `WSMIXER_SPEC_DIR` env var, when set. ws-mixer-js's own convention for
 *     this variable is the spec subdir itself (`$WSMIXER_SPEC_DIR/fixtures`
 *     exists directly), but ws-mixer-go's convention (wsmixer/specdir_test.go)
 *     is repo-root (`$WSMIXER_SPEC_DIR/spec/fixtures`) -- both are tolerated,
 *     preferring the direct form and falling back to `$WSMIXER_SPEC_DIR/spec`
 *     when only that one exists. Fatal (no further fallback) if neither exists.
 *  2. `.spec/spec` at the repo root, as populated by `npm run fetch-spec`
 *     (shallow clone at the tag in `spec.pin`). This is what CI uses.
 *  3. `../ws-mixer-spec/spec`, a sibling checkout -- convenient for local
 *     dev against a repo extracted from the same former monorepo tree.
 *
 * If none resolve, the caller should skip its suite with `reason`, not
 * fail or silently pass.
 */
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

export interface SpecDirResult {
  /** Root of the ws-mixer-spec checkout's `spec/` directory. */
  dir: string;
  available: boolean;
  reason?: string;
}

/** `here` is `path.dirname(fileURLToPath(import.meta.url))` of the caller (i.e. `test/`). */
export function resolveSpecDir(here: string): SpecDirResult {
  const repoRoot = path.resolve(here, "..");

  const envDir = process.env.WSMIXER_SPEC_DIR?.trim();
  if (envDir) {
    if (existsSync(path.join(envDir, "fixtures"))) return { dir: envDir, available: true };
    const repoRootForm = path.join(envDir, "spec");
    if (existsSync(path.join(repoRootForm, "fixtures"))) return { dir: repoRootForm, available: true };
    return {
      dir: envDir,
      available: false,
      reason: `WSMIXER_SPEC_DIR="${envDir}" has neither fixtures/ nor spec/fixtures/ under it`,
    };
  }

  const fetched = path.resolve(repoRoot, ".spec/spec");
  if (existsSync(fetched)) return { dir: fetched, available: true };

  const sibling = path.resolve(repoRoot, "../ws-mixer-spec/spec");
  if (existsSync(sibling)) return { dir: sibling, available: true };

  let pin = "?";
  try {
    pin = readFileSync(path.join(repoRoot, "spec.pin"), "utf8").trim();
  } catch {
    // spec.pin missing is itself unusual, but not this function's problem to raise.
  }

  return {
    dir: fetched,
    available: false,
    reason: `spec fixtures not found -- run 'npm run fetch-spec' (clones ws-mixer-spec@${pin} into .spec/), set WSMIXER_SPEC_DIR to a local checkout, or check out ../ws-mixer-spec as a sibling`,
  };
}
