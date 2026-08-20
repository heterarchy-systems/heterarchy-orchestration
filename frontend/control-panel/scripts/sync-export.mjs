import { cp, mkdir, rename, rm, writeFile } from "node:fs/promises";
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
const stagedStaticRoot = `${staticRoot}.staged-${process.pid}`;

await rm(stagedStaticRoot, { recursive: true, force: true });
await mkdir(stagedStaticRoot, { recursive: true });

for (const name of ["index.html", "app.js", "_next"]) {
  await cp(resolve(exportRoot, name), resolve(stagedStaticRoot, name), {
    recursive: true,
    force: true,
  });
}

await writeFile(
  resolve(stagedStaticRoot, "frontend-source.sha256"),
  `${await frontendSourceDigest(frontendRoot)}\n`,
  "utf8",
);
await rm(staticRoot, { recursive: true, force: true });
await rename(stagedStaticRoot, staticRoot);
