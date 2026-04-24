import fs from "node:fs/promises";
import path from "node:path";
import { ensureValidRdToken } from "../lib/rd-auth.mjs";
import { runNodeScript } from "../lib/run-script.mjs";
import { logInfo, logError } from "../lib/logger.mjs";

const STATUS_FILE = path.resolve("data/last-run.json");

async function saveRunStatus(status) {
  await fs.mkdir(path.dirname(STATUS_FILE), { recursive: true });
  await fs.writeFile(
    STATUS_FILE,
    JSON.stringify(
      {
        ...status,
        updated_at: new Date().toISOString(),
      },
      null,
      2
    ),
    "utf-8"
  );
}

async function main() {
  const startedAt = new Date().toISOString();
  await logInfo(`=== PIPELINE START ${startedAt} ===`);

  try {
    await ensureValidRdToken();

    await runNodeScript("Buscar leads Meta", "./scripts/index.mjs");
    await runNodeScript("Enriquecer leads com RD", "./scripts/enrich-leads.mjs");
    await runNodeScript("Gerar dados do dashboard", "./scripts/build-dashboard-data.mjs");

    await saveRunStatus({
      status: "success",
      started_at: startedAt,
      finished_at: new Date().toISOString(),
    });

    await logInfo("=== PIPELINE SUCCESS ===");
  } catch (error) {
    await saveRunStatus({
      status: "error",
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      error: error.message,
    });

    await logError("=== PIPELINE FAILED ===", error);
    process.exit(1);
  }
}

main();