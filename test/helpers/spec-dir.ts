/**
 * Resolves the checkout of ws-mixer-spec that test/frame.test.ts,
 * test/control.test.ts, test/sequence.test.ts and the test/spec-*.test.ts
 * suites read fixtures from.
 * Post-split, the spec is a separate repo -- resolution order:
 *
 *  1. `WSMIXER_SPEC_DIR` env var, when set. ws-mixer-js's own convention for
 *     this variable is the spec subdir itself (`$WSMIXER_SPEC_DIR/fixtures`
 *     exists directly), but ws-mixer-go's convention (wsmixer/specdir_test.go)
 *     is repo-root (`$WSMIXER_SPEC_DIR/spec/fixtures`) -- both are tolerated,
 *     preferring the direct form and falling back to `$WSMIXER_SPEC_DIR/spec`
 *     when only that one exists. Fatal (no further fallback) if neither exists.
 *  2. `.spec/spec` at the repo root, as populated by `npm run fetch-spec`
 *     (shallow clone at the tag in `spec.pin`), only while `.spec/.pin`
 *     equals `spec.pin`. This is what CI uses.
 *  3. `../ws-mixer-spec/spec`, a sibling checkout -- convenient for local
 *     dev against a repo extracted from the same former monorepo tree --
 *     only while its HEAD is exactly the `spec.pin` tag with no local
 *     changes under `spec/` (`git describe --tags --exact-match`, `git
 *     status`); otherwise it is skipped with a console note. With neither
 *     usable, the result is unavailable, naming the tags found.
 *
 * If none resolve, the caller should skip its suite with `reason`, not
 * fail or silently pass.
 */
import { execFileSync } from "node:child_process";
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

  let pin = "?";
  try {
    pin = readFileSync(path.join(repoRoot, "spec.pin"), "utf8").trim();
  } catch {
    // spec.pin missing is itself unusual, but not this function's problem to raise.
  }

  const fetched = path.resolve(repoRoot, ".spec/spec");
  let fetchedPin: string | undefined;
  if (existsSync(fetched)) {
    try {
      fetchedPin = readFileSync(path.resolve(repoRoot, ".spec/.pin"), "utf8").trim();
    } catch {
      fetchedPin = "an unknown tag";
    }
    if (fetchedPin === pin) return { dir: fetched, available: true };
  }

  const sibling = path.resolve(repoRoot, "../ws-mixer-spec/spec");
  let siblingNote: string | undefined;
  if (existsSync(sibling)) {
    const at = gitTag(path.dirname(sibling));
    if (at === pin) return { dir: sibling, available: true };
    siblingNote = `../ws-mixer-spec is at ${at}, spec.pin is ${pin}`;
    console.info(`spec fixtures: not using ${siblingNote} (set WSMIXER_SPEC_DIR to use it anyway)`);
  }

  const fix = "run 'npm run fetch-spec'";
  return {
    dir: fetched,
    available: false,
    reason:
      fetchedPin !== undefined
        ? `.spec/ is at ${fetchedPin}${siblingNote ? ` and ${siblingNote}` : `, spec.pin is ${pin}`} -- ${fix}`
        : siblingNote !== undefined
          ? `${siblingNote} and there is no .spec/ -- ${fix}`
          : `spec fixtures not found -- ${fix} (clones ws-mixer-spec@${pin} into .spec/), set WSMIXER_SPEC_DIR to a local checkout, or check out ../ws-mixer-spec as a sibling`,
  };
}

/** The tag `repo`'s HEAD is exactly at, with "+ local changes" when `spec/` differs from it. */
function gitTag(repo: string): string {
  const git = (...args: string[]) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  let tag: string;
  try {
    tag = git("describe", "--tags", "--exact-match");
  } catch {
    return "no tag";
  }
  try {
    return git("status", "--porcelain", "--untracked-files=all", "--", "spec") === "" ? tag : `${tag} + local changes`;
  } catch {
    return `${tag} (status unknown)`;
  }
}
