import fs from "node:fs/promises";
import path from "node:path";
import { logInfo, logError, logWarn } from "../lib/logger.mjs";

const RAW_OUTPUT_FILE = path.resolve("data/raw-leads.json");
const NORMALIZED_OUTPUT_FILE = path.resolve("data/normalized-leads.json");

// ---------------------------------------------------------------------------
// Dois tokens distintos:
// META_PAGE_TOKEN  — token de página, acessa leadgen_forms e leads
// META_ACCESS_TOKEN — token de usuário do sistema, acessa ads/adsets/campaigns
// ---------------------------------------------------------------------------

async function metaFetch(url) {
  const response = await fetch(url);

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Erro Meta ${response.status}: ${text}`);
  }

  return response.json();
}

async function fetchMetaForms() {
  const pageId = process.env.META_PAGE_ID;
  const pageToken = process.env.META_PAGE_TOKEN || process.env.META_ACCESS_TOKEN;

  if (!pageId) throw new Error("META_PAGE_ID não definido.");
  if (!pageToken) throw new Error("META_PAGE_TOKEN não definido.");

  let url =
    `https://graph.facebook.com/v23.0/${pageId}/leadgen_forms` +
    `?fields=id,name,status` +
    `&limit=100` +
    `&access_token=${encodeURIComponent(pageToken)}`;

  const allForms = [];

  while (url) {
    const json = await metaFetch(url);
    if (Array.isArray(json.data)) allForms.push(...json.data);
    url = json.paging?.next ?? null;
  }

  return allForms;
}

async function fetchMetaLeadsByFormId(formId) {
  const pageToken = process.env.META_PAGE_TOKEN || process.env.META_ACCESS_TOKEN;
  const fields = ["id", "created_time", "ad_id", "form_id", "field_data"].join(",");

  let url =
    `https://graph.facebook.com/v23.0/${formId}/leads` +
    `?fields=${encodeURIComponent(fields)}` +
    `&limit=100` +
    `&access_token=${encodeURIComponent(pageToken)}`;

  const allLeads = [];

  while (url) {
    const json = await metaFetch(url);
    if (Array.isArray(json.data)) allLeads.push(...json.data);
    url = json.paging?.next ?? null;
  }

  return allLeads;
}

async function fetchMetaObjectName(objectId, adsToken) {
  if (!objectId) return null;

  try {
    const url =
      `https://graph.facebook.com/v23.0/${objectId}` +
      `?fields=id,name` +
      `&access_token=${encodeURIComponent(adsToken)}`;

    const json = await metaFetch(url);
    return { id: json.id ?? objectId, name: json.name ?? null };
  } catch (error) {
    await logWarn(`Não foi possível buscar nome do objeto ${objectId}: ${error.message}`);
    return { id: objectId, name: null };
  }
}

async function fetchMetaAdData(adId, adsToken) {
  if (!adId) {
    return { ad_id: null, ad_name: null, adset_id: null, adset_name: null, campaign_id: null, campaign_name: null };
  }

  try {
    const adUrl =
      `https://graph.facebook.com/v23.0/${adId}` +
      `?fields=id,name,adset_id,campaign_id` +
      `&access_token=${encodeURIComponent(adsToken)}`;

    const ad = await metaFetch(adUrl);

    const adset_id = ad.adset_id ?? null;
    const campaign_id = ad.campaign_id ?? null;

    let adset_name = null;
    let campaign_name = null;

    if (adset_id) {
      const adset = await fetchMetaObjectName(adset_id, adsToken);
      adset_name = adset?.name ?? null;
    }

    if (campaign_id) {
      const campaign = await fetchMetaObjectName(campaign_id, adsToken);
      campaign_name = campaign?.name ?? null;
    }

    return {
      ad_id: ad.id ?? adId,
      ad_name: ad.name ?? null,
      adset_id,
      adset_name,
      campaign_id,
      campaign_name,
    };
  } catch (error) {
    await logWarn(`Não foi possível buscar metadados do anúncio ${adId}: ${error.message}`);
    return { ad_id: adId, ad_name: null, adset_id: null, adset_name: null, campaign_id: null, campaign_name: null };
  }
}

function normalizeFieldName(name = "") {
  return String(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "");
}

function extractFieldData(fieldData = []) {
  const fields = {};
  for (const field of fieldData) {
    const key = normalizeFieldName(field?.name);
    const value = Array.isArray(field?.values) ? field.values[0] : null;
    if (key) fields[key] = value ?? null;
  }
  return fields;
}

function normalizeLead(lead, extra = {}) {
  const fields = extractFieldData(lead.field_data);

  return {
    id: lead.id,
    created_time: lead.created_time,

    ad_id: extra.ad_id ?? lead.ad_id ?? null,
    ad_name: extra.ad_name ?? null,
    adset_id: extra.adset_id ?? null,
    adset_name: extra.adset_name ?? null,
    campaign_id: extra.campaign_id ?? null,
    campaign_name: extra.campaign_name ?? null,

    form_id: lead.form_id ?? null,
    form_name: extra.form_name ?? null,

    platform: "Meta Ads",
    is_organic: false,
    lead_status: "novo",

    email: fields.email ?? fields.e_mail ?? fields.work_email ?? null,
    nome: fields.nome ?? fields.full_name ?? fields.name ?? null,
    empresa: fields.empresa ?? fields.company ?? fields.company_name ?? fields.nome_da_empresa ?? null,
    segmento: fields.segmento ?? fields.segment ?? fields.qual_o_segmento_da_sua_empresa ?? null,
    faturamento: fields.faturamento ?? fields.revenue ?? fields.qual_o_faturamento_medio_mensal_da_sua_empresa ?? null,
    telefone: fields.telefone ?? fields.phone ?? fields.work_phone_number ?? null,
    cargo: fields.cargo ?? null,
    quantidade_de_funcionarios: fields.quantidade_de_funcionarios ?? null,
    desafio_principal:
      fields.quail_o_principal_desafio_da_sua_empresa ??
      fields.qual_o_principal_desafio_da_sua_empresa ??
      null,

    fields,
    field_data_raw: lead.field_data ?? [],
  };
}

async function main() {
  try {
    const adsToken = process.env.META_ACCESS_TOKEN;
    const pageToken = process.env.META_PAGE_TOKEN || adsToken;

    if (!adsToken) await logWarn("META_ACCESS_TOKEN não definido — metadados de anúncio serão pulados.");
    if (!pageToken) throw new Error("META_PAGE_TOKEN não definido.");

    await logInfo("Buscando formulários da Meta...");
    const forms = await fetchMetaForms();
    await logInfo(`Formulários encontrados: ${forms.length}`);

    const allLeads = [];
    const seen = new Set();
    const adCache = new Map();
    const formNameMap = new Map(forms.map((form) => [form.id, form.name]));

    for (const form of forms) {
      await logInfo(`Buscando leads do formulário: ${form.name || form.id}`);
      const leads = await fetchMetaLeadsByFormId(form.id);

      for (const lead of leads) {
        if (seen.has(lead.id)) continue;
        seen.add(lead.id);
        allLeads.push(lead);
      }
    }

    const createdTimes = allLeads.map((l) => l.created_time).filter(Boolean).sort();
    await logInfo(`Total leads coletados: ${allLeads.length}`);
    await logInfo(`Primeiro: ${createdTimes[0]} | Último: ${createdTimes[createdTimes.length - 1]}`);

    const normalizedLeads = [];

    for (let index = 0; index < allLeads.length; index++) {
      const lead = allLeads[index];

      let adMeta = { ad_id: lead.ad_id ?? null, ad_name: null, adset_id: null, adset_name: null, campaign_id: null, campaign_name: null };

      if (lead.ad_id && adsToken) {
        if (!adCache.has(lead.ad_id)) {
          adCache.set(lead.ad_id, await fetchMetaAdData(lead.ad_id, adsToken));
        }
        adMeta = adCache.get(lead.ad_id);
      }

      const parsed = normalizeLead(lead, {
        ...adMeta,
        form_name: formNameMap.get(lead.form_id) ?? null,
      });

      if (!parsed.email) {
        console.warn(`Lead sem email no índice ${index}`, { id: parsed.id, campos: Object.keys(parsed.fields) });
      }

      normalizedLeads.push(parsed);
    }

    normalizedLeads.sort((a, b) => new Date(a.created_time || 0) - new Date(b.created_time || 0));

    await fs.mkdir(path.dirname(RAW_OUTPUT_FILE), { recursive: true });
    await fs.writeFile(RAW_OUTPUT_FILE, JSON.stringify(allLeads, null, 2), "utf-8");
    await fs.writeFile(NORMALIZED_OUTPUT_FILE, JSON.stringify(normalizedLeads, null, 2), "utf-8");

    await logInfo(`Leads brutos: ${RAW_OUTPUT_FILE} (${allLeads.length})`);
    await logInfo(`Leads normalizados: ${NORMALIZED_OUTPUT_FILE} (${normalizedLeads.length})`);

    // Diagnóstico
    const comAdName = normalizedLeads.filter((l) => l.ad_name).length;
    await logInfo(`Leads com ad_name preenchido: ${comAdName} / ${normalizedLeads.length}`);
  } catch (error) {
    await logError("Erro ao buscar leads da Meta", error);
    throw error;
  }
}

main();
