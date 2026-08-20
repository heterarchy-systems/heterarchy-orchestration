import { readFile, readdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { frontendSourceDigest } from "./frontend-source-digest.mjs";

const scriptRoot = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(scriptRoot, "..");
const packagedRoot = resolve(
  frontendRoot,
  "../../chattocodex/orchestrator_backend/control_panel/static",
);

const expectedDigest = await frontendSourceDigest(frontendRoot);
const packagedDigest = (
  await readFile(resolve(packagedRoot, "frontend-source.sha256"), "utf8")
).trim();
if (packagedDigest !== expectedDigest) {
  throw new Error(
    "Packaged Control Panel assets are stale; run 'make frontend-build'.",
  );
}

const indexHtml = await readFile(resolve(packagedRoot, "index.html"), "utf8");
if (!indexHtml.includes('/static/app.js')) {
  throw new Error("Packaged Control Panel index is missing the legacy client bridge.");
}
if (!indexHtml.includes('/static/_next/')) {
  throw new Error("Packaged Control Panel index is missing Next.js static assets.");
}

const nextEntries = await readdir(resolve(packagedRoot, "_next"), {
  recursive: true,
});
if (nextEntries.length === 0) {
  throw new Error("Packaged Control Panel Next.js asset directory is empty.");
}

console.log("frontend export sync verification passed");