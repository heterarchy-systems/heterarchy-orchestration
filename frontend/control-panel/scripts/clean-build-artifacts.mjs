import { rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptRoot = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(scriptRoot, "..");
const mode = process.argv[2] ?? "all";
const targetNames = mode === "next" ? [".next"] : [".next", "out"];

for (const targetName of targetNames) {
  await rm(resolve(frontendRoot, targetName), {
    recursive: true,
    force: true,
  });
}
