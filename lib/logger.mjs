import fs from "node:fs/promises";
import path from "node:path";

const LOG_DIR = path.resolve("logs");
const LOG_FILE = path.join(LOG_DIR, "pipeline.log");

async function write(level, message) {
  await fs.mkdir(LOG_DIR, { recursive: true });
  const line = `[${new Date().toISOString()}] [${level}] ${message}\n`;
  await fs.appendFile(LOG_FILE, line, "utf-8");
  console.log(line.trim());
}

export async function logInfo(message) {
  await write("INFO", message);
}

export async function logWarn(message) {
  await write("WARN", message);
}

export async function logError(message, error = null) {
  const details = error?.stack || error?.message || "";
  await write("ERROR", details ? `${message}\n${details}` : message);
}