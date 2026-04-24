import fs from "node:fs/promises";
import path from "node:path";
import { rdFetch } from "../lib/rd-auth.mjs";
import { logInfo, logWarn, logError } from "../lib/logger.mjs";

const INPUT_FILE = path.resolve("data/normalized-leads.json");
const OUTPUT_FILE = path.resolve("data/enriched-leads.json");

const META_TOKEN = process.env.META_ACCESS_TOKEN;
const META_API_VERSION = process.env.META_API_VERSION || "v22.0";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function extractEmailFromLead(lead) {
  if (!lead?.email) return null;
  return String(lead.email).trim().toLowerCase();
}

function extractRdSummary(rdContact) {
  if (!rdContact) {
    return {
      rd_found: false,
      rd_email: null,
      rd_funil_vendas: null,
      rd_etapa: null,
      rd_motivo_perda: null,
      rd_responsavel: null,
      rd_faturamento: null,
      rd_segmento: null,
      rd_numero_funcionarios: null,
      rd_cargo: null,
      rd_valor: null,
      rd_origem: null,
    };
  }

  return {
    rd_found: true,
    rd_email: rdContact.email ?? null,
    rd_funil_vendas: rdContact.cf_plug_deal_pipeline ?? null,
    rd_etapa: rdContact.cf_plug_funnel_stage ?? null,
    rd_motivo_perda: rdContact.cf_plug_lost_reason ?? null,
    rd_responsavel: rdContact.cf_plug_contact_owner ?? null,
    rd_faturamento: rdContact.cf_faturamento ?? null,
    rd_segmento: rdContact.cf_segmento ?? null,
    rd_numero_funcionarios: rdContact.cf_numero_de_funcionarios ?? null,
    rd_cargo: rdContact.cf_cargo_2 ?? null,
    rd_valor: rdContact.cf_plug_opportunity_value ?? null,
    rd_origem: rdContact.cf_plug_opportunity_origin ?? null,
  };
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// RD Station
// ---------------------------------------------------------------------------

async function fetchRdContactByEmail(email, attempt = 1) {
  const url = `https://api.rd.services/platform/contacts/email:${encodeURIComponent(email)}`;
  const response = await rdFetch(url);

  if (response.status === 404) return null;

  if (response.status === 429) {
    if (attempt <= 5) {
      const waitMs = attempt * 2000;
      await logWarn(
        `RD rate limit para ${email}. Tentando novamente em ${waitMs}ms (tentativa ${attempt}/5)...`
      );
      await sleep(waitMs);
      return fetchRdContactByEmail(email, attempt + 1);
    }
    throw new Error(`RD 429: API rate limit exceeded após ${attempt - 1} retries`);
  }

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`RD ${response.status}: ${text}`);
  }

  return response.json();
}

// ---------------------------------------------------------------------------
// Meta Ads
//
// CORREÇÃO: a versão anterior usava "adset{id,name}" como field inline, que
// NÃO funciona na Graph API para objetos relacionados. O correto é buscar
// adset_id e campaign_id do ad e depois chamar cada objeto separadamente,
// exatamente como já faz o index.mjs.
// ---------------------------------------------------------------------------

async function metaGet(resourcePath, attempt = 1) {
  if (!META_TOKEN) return null;

  const url = `https://graph.facebook.com/${META_API_VERSION}/${resourcePath}&access_token=${encodeURIComponent(META_TOKEN)}`;
  const response = await fetch(url);

  if (response.status === 404) return null;

  if (response.status === 429 || response.status >= 500) {
    if (attempt <= 5) {
      const waitMs = attempt * 1500;
      await logWarn(
        `Meta retry (${response.status}) em ${waitMs}ms (tentativa ${attempt}/5)...`
      );
      await sleep(waitMs);
      return metaGet(resourcePath, attempt + 1);
    }
  }

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Meta ${response.status}: ${text}`);
  }

  return response.json();
}

async function fetchMetaAdById(adId) {
  if (!adId || !META_TOKEN) return null;

  try {
    // Passo 1: busca ad com adset_id e campaign_id
    const ad = await metaGet(`${encodeURIComponent(adId)}?fields=id,name,adset_id,campaign_id`);
    if (!ad) return null;

    let adset_name = null;
    let campaign_name = null;

    // Passo 2: busca nome do adset
    if (ad.adset_id) {
      const adset = await metaGet(`${encodeURIComponent(ad.adset_id)}?fields=id,name`);
      adset_name = adset?.name ?? null;
    }

    // Passo 3: busca nome da campanha
    if (ad.campaign_id) {
      const campaign = await metaGet(`${encodeURIComponent(ad.campaign_id)}?fields=id,name`);
      campaign_name = campaign?.name ?? null;
    }

    return {
      ad_id: ad.id ?? adId,
      ad_name: ad.name ?? null,
      adset_id: ad.adset_id ?? null,
      adset_name,
      campaign_id: ad.campaign_id ?? null,
      campaign_name,
    };
  } catch (error) {
    await logWarn(`Não foi possível buscar metadados do ad ${adId}: ${error.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Concorrência controlada
// ---------------------------------------------------------------------------

async function mapWithConcurrency(items, limit, asyncFn) {
  const results = new Array(items.length);
  let currentIndex = 0;

  async function worker() {
    while (true) {
      const index = currentIndex++;
      if (index >= items.length) return;
      results[index] = await asyncFn(items[index], index);
    }
  }

  const workers = Array.from({ length: limit }, () => worker());
  await Promise.all(workers);
  return results;
}

async function readJsonIfExists(filePath, fallback) {
  try {
    const raw = await fs.readFile(filePath, "utf-8");
    return JSON.parse(raw);
  } catch (error) {
    if (error.code === "ENOENT") return fallback;
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Merge
//
// CORREÇÃO: a versão anterior usava `currentLead.ad_name ?? existingLead.ad_name`
// mas null explícito satisfaz ??, então um lead que veio com ad_name: null do
// index.mjs nunca caía no fallback do cache. firstNonEmpty() ignora null/""/undefined.
// ---------------------------------------------------------------------------

function firstNonEmpty(...values) {
  for (const v of values) {
    if (v !== null && v !== undefined && v !== "") return v;
  }
  return null;
}

function mergeLeadData(currentLead, existingLead) {
  return {
    // base do cache
    ...existingLead,
    // campos frescos do normalized sempre vencem
    ...currentLead,

    // identidade: sempre do normalized atual
    id: currentLead.id,
    created_time: currentLead.created_time,
    ad_id: currentLead.ad_id,
    form_id: currentLead.form_id,
    form_name: currentLead.form_name,
    platform: currentLead.platform,
    is_organic: currentLead.is_organic,
    lead_status: currentLead.lead_status,
    email: currentLead.email,
    nome: currentLead.nome,
    empresa: currentLead.empresa,
    segmento: currentLead.segmento,
    faturamento: currentLead.faturamento,
    telefone: currentLead.telefone,
    cargo: currentLead.cargo,
    quantidade_de_funcionarios: currentLead.quantidade_de_funcionarios,
    desafio_principal: currentLead.desafio_principal,
    fields: currentLead.fields,
    field_data_raw: currentLead.field_data_raw,

    // campanha: firstNonEmpty ignora null, ao contrário de ??
    ad_name: firstNonEmpty(currentLead.ad_name, existingLead.ad_name, existingLead.meta_ad_name),
    adset_id: firstNonEmpty(currentLead.adset_id, existingLead.adset_id),
    adset_name: firstNonEmpty(currentLead.adset_name, existingLead.adset_name, existingLead.meta_adset_name),
    campaign_id: firstNonEmpty(currentLead.campaign_id, existingLead.campaign_id),
    campaign_name: firstNonEmpty(currentLead.campaign_name, existingLead.campaign_name, existingLead.meta_campaign_name),
  };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  try {
    await logInfo("Lendo leads normalizados...");
    const normalizedLeads = JSON.parse(await fs.readFile(INPUT_FILE, "utf-8"));

    await logInfo("Lendo base enriquecida anterior...");
    const previousEnriched = await readJsonIfExists(OUTPUT_FILE, []);
    const previousById = new Map(previousEnriched.map((lead) => [lead.id, lead]));

    // CORREÇÃO: leads do cache são reaproveitados SÓ se já tinham meta_found: true
    // OU se não possuem ad_id (não há o que buscar). Caso contrário, re-enrich.
    const leadsToProcess = [];
    const reusedLeads = [];

    for (const lead of normalizedLeads) {
      const existing = previousById.get(lead.id);

      if (existing) {
        const metaOk = existing.meta_found === true;
        const semAdId = !lead.ad_id;

        if (metaOk || semAdId) {
          reusedLeads.push(mergeLeadData(lead, existing));
        } else {
          // tinha ad_id mas meta não foi encontrado antes: tenta de novo
          leadsToProcess.push(lead);
        }
      } else {
        leadsToProcess.push(lead);
      }
    }

    await logInfo(`Total normalizados:          ${normalizedLeads.length}`);
    await logInfo(`Reaproveitados do cache:      ${reusedLeads.length}`);
    await logInfo(`Novos / re-enrich:            ${leadsToProcess.length}`);

    if (!META_TOKEN) {
      await logWarn("META_ACCESS_TOKEN não definido — enriquecimento Meta será pulado.");
    }

    const emailCache = new Map();
    const metaAdCache = new Map();

    const enrichedNew = await mapWithConcurrency(leadsToProcess, 1, async (lead, index) => {
      const email = extractEmailFromLead(lead);
      let rdContact = null;
      let metaAdData = null;

      try {
        // RD Station
        if (email) {
          if (emailCache.has(email)) {
            rdContact = emailCache.get(email);
          } else {
            rdContact = await fetchRdContactByEmail(email);
            emailCache.set(email, rdContact);
          }
        } else {
          await logWarn(`Lead sem email — índice ${index} (id: ${lead.id})`);
        }

        // Meta Ads
        if (lead.ad_id && META_TOKEN) {
          if (metaAdCache.has(lead.ad_id)) {
            metaAdData = metaAdCache.get(lead.ad_id);
          } else {
            metaAdData = await fetchMetaAdById(lead.ad_id);
            metaAdCache.set(lead.ad_id, metaAdData);
          }
        }

        const rdSummary = extractRdSummary(rdContact);

        // Campos de campanha: prioriza o que veio do index.mjs, depois o que a Meta retornou
        const ad_name = firstNonEmpty(lead.ad_name, metaAdData?.ad_name);
        const adset_name = firstNonEmpty(lead.adset_name, metaAdData?.adset_name);
        const campaign_name = firstNonEmpty(lead.campaign_name, metaAdData?.campaign_name);

        return {
          ...lead,

          ad_name,
          adset_id: firstNonEmpty(lead.adset_id, metaAdData?.adset_id),
          adset_name,
          campaign_id: firstNonEmpty(lead.campaign_id, metaAdData?.campaign_id),
          campaign_name,

          ...rdSummary,

          meta_found: metaAdData !== null,
          meta_ad_name: metaAdData?.ad_name ?? null,
          meta_adset_name: metaAdData?.adset_name ?? null,
          meta_campaign_name: metaAdData?.campaign_name ?? null,

          rd_contact: rdContact,
          meta_ad: metaAdData,

          rd_status: email ? (rdContact ? "found" : "not_found") : "no_email",
          meta_status: lead.ad_id ? (metaAdData ? "found" : "not_found") : "no_ad_id",
        };
      } catch (error) {
        await logError(`Erro ao enriquecer lead ${email || lead.id}`, error);

        return {
          ...lead,
          ...extractRdSummary(rdContact),
          meta_found: false,
          meta_ad_name: null,
          meta_adset_name: null,
          meta_campaign_name: null,
          rd_contact: rdContact,
          meta_ad: null,
          rd_status: "error",
          meta_status: "error",
          rd_error: error.message,
        };
      }
    });

    const finalById = new Map();
    for (const lead of [...reusedLeads, ...enrichedNew]) {
      finalById.set(lead.id, lead);
    }

    const finalEnriched = normalizedLeads
      .map((lead) => finalById.get(lead.id) ?? lead)
      .sort((a, b) => new Date(a.created_time || 0) - new Date(b.created_time || 0));

    await fs.writeFile(OUTPUT_FILE, JSON.stringify(finalEnriched, null, 2), "utf-8");
    await logInfo(`Arquivo gerado: ${OUTPUT_FILE} (${finalEnriched.length} leads)`);

    // Diagnóstico
    const metaFound = finalEnriched.filter((l) => l.meta_found).length;
    const rdFound = finalEnriched.filter((l) => l.rd_found).length;
    await logInfo(`Meta: ${metaFound} encontrados | RD: ${rdFound} encontrados`);
  } catch (error) {
    await logError("Erro geral ao enriquecer leads", error);
    throw error;
  }
}

main();
