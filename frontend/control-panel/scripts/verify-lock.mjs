import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptRoot = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(scriptRoot, "..");
const packageJson = JSON.parse(
  await readFile(resolve(frontendRoot, "package.json"), "utf8"),
);
const packageLock = JSON.parse(
  await readFile(resolve(frontendRoot, "package-lock.json"), "utf8"),
);
const lockedRoot = packageLock.packages?.[""];

if (!lockedRoot) {
  throw new Error("package-lock.json is missing its root package record.");
}

for (const section of ["dependencies", "devDependencies"]) {
  const expected = packageJson[section] ?? {};
  const actual = lockedRoot[section] ?? {};
  if (JSON.stringify(expected) !== JSON.stringify(actual)) {
    throw new Error(`package-lock.json ${section} is stale.`);
  }
}

console.log("frontend lock verification passed");