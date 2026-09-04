#!/usr/bin/env node
// prepublishOnly guard: refuse to publish anywhere except the public npm registry.
//
// npm only exports npm_config_registry into lifecycle script env when the
// registry was explicitly overridden (CLI flag, npm_config_registry env,
// or an .npmrc entry) -- it is absent for the built-in default. That
// override takes priority over package.json's publishConfig.registry at
// actual publish time, so we honour it first and fall back to
// publishConfig.registry (the value npm will use when nothing overrides it).

import { readFileSync } from "node:fs";

const REQUIRED_HOST = "registry.npmjs.org";

function readPublishConfigRegistry() {
  const pkgPath = process.env.npm_package_json;
  if (!pkgPath) return undefined;
  try {
    const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
    return pkg.publishConfig?.registry;
  } catch {
    return undefined;
  }
}

const envRegistry = process.env.npm_config_registry;
const publishConfigRegistry = readPublishConfigRegistry();
const effectiveRegistry = envRegistry || publishConfigRegistry;

if (!effectiveRegistry) {
  console.error(
    "check-registry: could not determine the publish registry (no npm_config_registry env, no publishConfig.registry in package.json). Aborting.",
  );
  process.exit(1);
}

let host;
try {
  host = new URL(effectiveRegistry).host;
} catch {
  console.error(`check-registry: publish registry "${effectiveRegistry}" is not a valid URL. Aborting.`);
  process.exit(1);
}

if (host !== REQUIRED_HOST) {
  console.error(
    `check-registry: refusing to publish to "${effectiveRegistry}" (host "${host}"). ` +
      `This package may only be published to https://${REQUIRED_HOST}/. ` +
      `Unset npm_config_registry / remove any --registry override and try again.`,
  );
  process.exit(1);
}

console.log(`check-registry: publishing to ${effectiveRegistry} - OK`);
