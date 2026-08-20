import { cp, mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { frontendSourceDigest } from "./frontend-source-digest.mjs";

const scriptRoot = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(scriptRoot, "..");
const exportRoot = resolve(frontendRoot, "out");
const staticRoot = resolve(
  frontendRoot,
  "../../chattocodex/orchestrator_backend/control_panel/static",
);

await rm(staticRoot, { recursive: true, force: true });
await mkdir(staticRoot, { recursive: true });

for (const name of ["index.html", "app.js", "_next"]) {
  await cp(resolve(exportRoot, name), resolve(staticRoot, name), {
    recursive: true,
    force: true,
  });
}

await writeFile(
  resolve(staticRoot, "frontend-source.sha256"),
  `${await frontendSourceDigest(frontendRoot)}\n`,
  "utf8",
);