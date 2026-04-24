import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { logInfo, logError } from "./logger.mjs";

const execFileAsync = promisify(execFile);

export async function runNodeScript(name, scriptPath) {
  const startedAt = Date.now();
  await logInfo(`Iniciando etapa: ${name}`);

  try {
    const { stdout, stderr } = await execFileAsync("node", [scriptPath], {
      env: process.env,
      maxBuffer: 1024 * 1024 * 20,
    });

    if (stdout?.trim()) {
      await logInfo(`[${name}] stdout:\n${stdout}`);
    }

    if (stderr?.trim()) {
      await logInfo(`[${name}] stderr:\n${stderr}`);
    }

    const elapsed = Date.now() - startedAt;
    await logInfo(`Etapa finalizada: ${name} (${elapsed} ms)`);
  } catch (error) {
    await logError(`Erro na etapa: ${name}`, error);
    throw error;
  }
}