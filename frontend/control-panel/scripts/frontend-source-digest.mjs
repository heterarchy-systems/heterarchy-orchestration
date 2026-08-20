import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { relative, resolve } from "node:path";

const ROOT_FILES = [
  "app.ts",
  "next.config.ts",
  "package.json",
  "tsconfig.json",
  "tsconfig.legacy.json",
];
const SOURCE_DIRECTORIES = ["app", "components"];

async function collectFiles(frontendRoot) {
  const files = ROOT_FILES.map((path) => resolve(frontendRoot, path));

  async function visit(directory) {
    const entries = await readdir(directory, { withFileTypes: true });
    for (const entry of entries) {
      const path = resolve(directory, entry.name);
      if (entry.isDirectory()) {
        await visit(path);
      } else if (entry.isFile()) {
        files.push(path);
      }
    }
  }

  for (const directory of SOURCE_DIRECTORIES) {
    await visit(resolve(frontendRoot, directory));
  }
  return files.sort();
}

export async function frontendSourceDigest(frontendRoot) {
  const hash = createHash("sha256");
  for (const path of await collectFiles(frontendRoot)) {
    hash.update(relative(frontendRoot, path));
    hash.update("\0");
    hash.update(await readFile(path));
    hash.update("\0");
  }
  return hash.digest("hex");
}