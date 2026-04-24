import fs from "node:fs/promises";
import path from "node:path";
import { logInfo, logWarn, logError } from "./logger.mjs";

const TOKEN_FILE = path.resolve("data/rd-token.json");

async function ensureDataDir() {
  await fs.mkdir(path.dirname(TOKEN_FILE), { recursive: true });
}

async function readTokenFile() {
  try {
    const raw = await fs.readFile(TOKEN_FILE, "utf-8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function writeTokenFile(tokenData) {
  await ensureDataDir();
  await fs.writeFile(TOKEN_FILE, JSON.stringify(tokenData, null, 2), "utf-8");
}

function isTokenExpired(tokenData) {
  if (!tokenData?.expires_at) return true;

  const now = Date.now();
  const safetyWindowMs = 5 * 60 * 1000;
  return now >= tokenData.expires_at - safetyWindowMs;
}

export async function refreshRdToken(refreshToken) {
  if (!refreshToken) {
    throw new Error("Refresh token do RD não encontrado.");
  }

  await logInfo("Renovando token do RD...");

  const response = await fetch("https://api.rd.services/auth/token", {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      client_id: process.env.RD_CLIENT_ID,
      client_secret: process.env.RD_CLIENT_SECRET,
      refresh_token: refreshToken,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Falha ao renovar token RD: ${response.status} - ${text}`);
  }

  const json = await response.json();
  const expiresIn = Number(json.expires_in || 86400);

  const tokenData = {
    access_token: json.access_token,
    refresh_token: json.refresh_token,
    expires_in: expiresIn,
    expires_at: Date.now() + expiresIn * 1000,
    updated_at: new Date().toISOString(),
  };

  await writeTokenFile(tokenData);
  await logInfo("Token do RD renovado com sucesso.");

  process.env.RD_ACCESS_TOKEN = tokenData.access_token;
  process.env.RD_REFRESH_TOKEN = tokenData.refresh_token;

  return tokenData;
}

export async function ensureValidRdToken() {
  const saved = await readTokenFile();

  if (saved && !isTokenExpired(saved)) {
    process.env.RD_ACCESS_TOKEN = saved.access_token;
    process.env.RD_REFRESH_TOKEN = saved.refresh_token;
    return saved.access_token;
  }

  const refreshToken = saved?.refresh_token || process.env.RD_REFRESH_TOKEN;

  if (!refreshToken) {
    throw new Error(
      "Nenhum refresh token do RD encontrado. Gere um token inicial primeiro."
    );
  }

  const tokenData = await refreshRdToken(refreshToken);
  return tokenData.access_token;
}

export async function rdFetch(url, options = {}, retry = true) {
  const accessToken = await ensureValidRdToken();

  const response = await fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      Authorization: `Bearer ${accessToken}`,
      accept: "application/json",
    },
  });

  if (response.status === 401 && retry) {
    await logWarn("RD retornou 401. Tentando renovar token e repetir requisição.");

    const saved = await readTokenFile();
    const refreshToken = saved?.refresh_token || process.env.RD_REFRESH_TOKEN;

    const newToken = await refreshRdToken(refreshToken);

    return fetch(url, {
      ...options,
      headers: {
        ...(options.headers || {}),
        Authorization: `Bearer ${newToken.access_token}`,
        accept: "application/json",
      },
    });
  }

  return response;
}