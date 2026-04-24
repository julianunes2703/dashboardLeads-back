import fs from "node:fs/promises";
import path from "node:path";
import { logInfo, logError } from "../lib/logger.mjs";

const INPUT_FILE = path.resolve("data/enriched-leads.json");
const OUTPUT_FILE = path.resolve("data/dashboard-data.json");

// ---------------------------------------------------------------------------
// Configuração de investimento por plataforma
// Edite aqui quando os valores mudarem — um único lugar para ambos os arquivos
// ---------------------------------------------------------------------------
const PLATFORM_CONFIG = {
  face: { investimento: 8045, brasil: 90 },
  insta: { investimento: 9335, brasil: 430 },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function normalizeText(value = "") {
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function firstNonEmpty(...values) {
  for (const v of values) {
    if (v !== null && v !== undefined && v !== "") return v;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Regras de negócio — alinhadas com a planilha
// ---------------------------------------------------------------------------

function isICP(lead) {
  const lost = normalizeText(lead.rd_motivo_perda || "");
  if (lost.includes("fora do icp")) return false;

  const faturamento = normalizeText(lead.rd_faturamento || lead.faturamento || "");
  const cargo = normalizeText(lead.rd_cargo || lead.cargo || "");
  const segmento = normalizeText(lead.rd_segmento || lead.segmento || "");

  // Planilha: "ICP = Proprietário com ou sem sócio com faturamento > 100K"
  // → precisa de faturamento OU cargo — mas sem segmento e faturamento retorna false
  if (!faturamento && !cargo) return false;

  const faturamentoOk = [
    "100 a 200k",
    "200 a 500k",
    "500 a 1 milhao",
    "500 a 1 milhão",
    "1 a 2 milhoes",
    "1 a 2 milhões",
    "2 a 5 milhoes",
    "2 a 5 milhões",
    "acima",
  ].some((term) => faturamento.includes(term));

  const cargoOk = [
    "proprietario",
    "proprietária",
    "proprietário",
    "dono",
    "dona",
    "socio",
    "sócio",
    "sócia",
    "ceo",
    "founder",
  ].some((term) => cargo.includes(term));

  return faturamentoOk || cargoOk;
}

// Planilha: "Avançou = Qualquer estágio que não 'tentativas' ou 'sem contato'"
function advancedInFunnel(lead) {
  const etapa = normalizeText(lead.rd_etapa || "");
  if (!etapa) return false;

  const etapasNaoAvancou = [
    "tentativas de contato",
    "tentativa",
    "sem contato",
    "novo",
    "lead",
    "entrada",
    "triagem",
  ];

  return !etapasNaoAvancou.some((term) => etapa.includes(term));
}

// Planilha: "Fez reunião = Proposta enviada ou positivado"
function hasMeeting(lead) {
  const etapa = normalizeText(lead.rd_etapa || "");

  return [
    "proposta enviada",
    "positivado",
    "positivados",
    "reuniao",
    "reunião",
    "diagnostico",
    "diagnóstico",
    "apresentacao",
    "apresentação",
    "call",
    "discovery",
    "demo",
  ].some((term) => etapa.includes(term));
}

// Planilha: "Venda = 'Positivados'"
function hasSale(lead) {
  const etapa = normalizeText(lead.rd_etapa || "");
  const funil = normalizeText(lead.rd_funil_vendas || "");
  const combined = `${etapa} ${funil}`;

  return [
    "positivado",
    "positivados",
    "ganho",
    "ganha",
    "ganhou",
    "fechado ganho",
    "venda",
    "vendido",
    "closed won",
    "won",
    "oportunidade ganha",
    "cliente",
  ].some((term) => combined.includes(term));
}

// ---------------------------------------------------------------------------
// Plataforma
//
// A separação face/insta depende dos nomes de anúncio/adset/campanha conterem
// marcadores como [FEED] ou [STORIES]/instagram. Com o enrich-leads.mjs
// corrigido, esses campos chegam preenchidos para a maioria dos leads.
// ---------------------------------------------------------------------------

function getPlatform(lead) {
  const raw = normalizeText(lead.platform || lead.plataforma || lead.source_platform || "");

  const adName = normalizeText(lead.ad_name || "");
  const adsetName = normalizeText(lead.adset_name || "");
  const campaignName = normalizeText(lead.campaign_name || "");
  const formName = normalizeText(lead.form_name || "");

  const combined = `${raw} ${adName} ${adsetName} ${campaignName} ${formName}`;

  // Instagram primeiro (mais específico)
  if (
    combined.includes("instagram") ||
    combined.includes("insta") ||
    combined.includes("[stories]") ||
    combined.includes("stories")
  ) {
    return "insta";
  }

  // Facebook
  if (
    combined.includes("facebook") ||
    combined.includes("[feed]") ||
    combined.includes("face")
  ) {
    return "face";
  }

  // Genérico Meta (sem marcador de placement)
  if (combined.includes("meta")) {
    return "meta";
  }

  return "nao identificado";
}

// ---------------------------------------------------------------------------
// Nome de campanha — prioridade: ad_name > adset_name > campaign_name > form_name
// Alinhado com a planilha que usa "Ad Name" como linha da tabela
// ---------------------------------------------------------------------------

function getCampaignLabel(lead) {
  return (
    firstNonEmpty(lead.ad_name, lead.adset_name, lead.campaign_name, lead.form_name) ||
    "Sem campanha identificada"
  );
}

function extractLeadDate(lead) {
  return lead.created_time || lead.created_at || new Date().toISOString();
}

// ---------------------------------------------------------------------------
// Sumarizações
// ---------------------------------------------------------------------------

function buildCampaignMap(leads, totalLeadsICP) {
  const map = new Map();

  for (const lead of leads) {
    const name = getCampaignLabel(lead);
    const platform = getPlatform(lead);
    const key = `${platform}__${name}`;

    if (!map.has(key)) {
      map.set(key, {
        name,
        platform,
        leads: 0,
        noIcp: 0,
        avancouIcp: 0,
        reuniaoIcp: 0,
        avancouTotal: 0,
        fezReuniaoTotal: 0,
        vendas: 0,
      });
    }

    const row = map.get(key);
    row.leads += 1;
    if (!lead.is_icp) row.noIcp += 1;
    if (lead.is_icp && lead.advanced_funnel) row.avancouIcp += 1;
    if (lead.is_icp && lead.meeting) row.reuniaoIcp += 1;
    if (lead.advanced_funnel) row.avancouTotal += 1;
    if (lead.meeting) row.fezReuniaoTotal += 1;
    if (lead.sale) row.vendas += 1;
  }

  const totalLeads = leads.length;

  return Array.from(map.values())
    .map((row) => ({
      ...row,
      // % Leads = leads desta campanha / total geral
      leadsPercent: totalLeads > 0 ? (row.leads / totalLeads) * 100 : 0,
      // % Avançou ICP = avançouIcp desta campanha / total ICP geral (igual à planilha)
      avancouIcpPercent: totalLeadsICP > 0 ? (row.avancouIcp / totalLeadsICP) * 100 : 0,
      // % Reunião ICP = reuniãoIcp / avançouIcp desta campanha
      reuniaoIcpPercent: row.avancouIcp > 0 ? (row.reuniaoIcp / row.avancouIcp) * 100 : 0,
      // % Avançou total = avancouTotal / leads desta campanha
      avancouTotalPercent: row.leads > 0 ? (row.avancouTotal / row.leads) * 100 : 0,
      // % Fez reunião total = fezReuniaoTotal / avancouTotal desta campanha
      fezReuniaoTotalPercent: row.avancouTotal > 0 ? (row.fezReuniaoTotal / row.avancouTotal) * 100 : 0,
      // % Venda = vendas / fezReuniaoTotal desta campanha
      vendasPercent: row.fezReuniaoTotal > 0 ? (row.vendas / row.fezReuniaoTotal) * 100 : 0,
      // % Leads no ICP = noIcp / leads desta campanha
      noIcpPercent: row.leads > 0 ? (row.noIcp / row.leads) * 100 : 0,
    }))
    .sort((a, b) => b.leads - a.leads);
}

function buildPlatformData(leads, totalVendas) {
  const map = new Map();

  for (const lead of leads) {
    const platform = getPlatform(lead);

    if (!map.has(platform)) {
      map.set(platform, {
        platform,
        leads: 0,
        noIcp: 0,
        avancouIcp: 0,
        reuniaoIcp: 0,
        avancouTotal: 0,
        fezReuniaoTotal: 0,
        vendas: 0,
      });
    }

    const row = map.get(platform);
    row.leads += 1;
    if (!lead.is_icp) row.noIcp += 1;
    if (lead.is_icp && lead.advanced_funnel) row.avancouIcp += 1;
    if (lead.is_icp && lead.meeting) row.reuniaoIcp += 1;
    if (lead.advanced_funnel) row.avancouTotal += 1;
    if (lead.meeting) row.fezReuniaoTotal += 1;
    if (lead.sale) row.vendas += 1;
  }

  return Array.from(map.values())
    .map((row) => {
      const cfg = PLATFORM_CONFIG[row.platform] || { investimento: 0, brasil: 0 };
      const custoVenda = row.vendas > 0 ? cfg.investimento / row.vendas : 0;
      const percentualVendas = totalVendas > 0 ? (row.vendas / totalVendas) * 100 : 0;

      return {
        ...row,
        investimento: cfg.investimento,
        brasil: cfg.brasil,
        custoVenda,
        percentualVendas,
        // percentuais de funil por plataforma
        noIcpPercent: row.leads > 0 ? (row.noIcp / row.leads) * 100 : 0,
        avancouIcpPercent: row.leads > 0 ? (row.avancouIcp / row.leads) * 100 : 0,
        reuniaoIcpPercent: row.avancouIcp > 0 ? (row.reuniaoIcp / row.avancouIcp) * 100 : 0,
        avancouTotalPercent: row.leads > 0 ? (row.avancouTotal / row.leads) * 100 : 0,
        fezReuniaoTotalPercent: row.avancouTotal > 0 ? (row.fezReuniaoTotal / row.avancouTotal) * 100 : 0,
        vendasPercent: row.fezReuniaoTotal > 0 ? (row.vendas / row.fezReuniaoTotal) * 100 : 0,
      };
    })
    .sort((a, b) => b.leads - a.leads);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  try {
    const raw = await fs.readFile(INPUT_FILE, "utf-8");
    const leads = JSON.parse(raw);

    // Processa flags de funil em cada lead
    const processedLeads = leads.map((lead) => {
      const is_icp = isICP(lead);
      const sale = hasSale(lead);
      // Se vendeu, considera que fez reunião e avançou também
      const meeting = sale ? true : hasMeeting(lead);
      const advanced_funnel = meeting ? true : advancedInFunnel(lead);

      return {
        ...lead,
        platform_grouped: getPlatform(lead),
        campaign_grouped: getCampaignLabel(lead),
        is_icp,
        advanced_funnel,
        meeting,
        sale,
      };
    });

    const totals = {
      totalLeads: processedLeads.length,
      leadsICP: processedLeads.filter((l) => l.is_icp).length,
      leadsNoICP: processedLeads.filter((l) => !l.is_icp).length,
      avancouFunil: processedLeads.filter((l) => l.advanced_funnel).length,
      reunioes: processedLeads.filter((l) => l.meeting).length,
      vendas: processedLeads.filter((l) => l.sale).length,
    };

    // Série temporal por dia
    const byDayMap = new Map();
    for (const lead of processedLeads) {
      const day = new Date(extractLeadDate(lead)).toISOString().slice(0, 10);

      if (!byDayMap.has(day)) {
        byDayMap.set(day, { date: day, leads: 0, icp: 0, avancouFunil: 0, reunioes: 0, vendas: 0 });
      }

      const row = byDayMap.get(day);
      row.leads += 1;
      if (lead.is_icp) row.icp += 1;
      if (lead.advanced_funnel) row.avancouFunil += 1;
      if (lead.meeting) row.reunioes += 1;
      if (lead.sale) row.vendas += 1;
    }

    const chartData = Array.from(byDayMap.values()).sort((a, b) =>
      a.date.localeCompare(b.date)
    );

    const campaignPerformance = buildCampaignMap(processedLeads, totals.leadsICP);
    const platformData = buildPlatformData(processedLeads, totals.vendas);

    // Diagnóstico de plataformas
    const platformCounts = processedLeads.reduce((acc, l) => {
      acc[l.platform_grouped] = (acc[l.platform_grouped] || 0) + 1;
      return acc;
    }, {});
    await logInfo(`Distribuição de plataformas: ${JSON.stringify(platformCounts)}`);

    const withAdName = processedLeads.filter((l) => l.ad_name).length;
    await logInfo(`Leads com ad_name preenchido: ${withAdName} / ${processedLeads.length}`);

    const dashboardData = {
      generatedAt: new Date().toISOString(),
      totals,
      chartData,
      campaignPerformance,
      platformData,
      platformConfig: PLATFORM_CONFIG,
      leads: processedLeads,
    };

    await fs.writeFile(OUTPUT_FILE, JSON.stringify(dashboardData, null, 2), "utf-8");
    await logInfo(`Dashboard data gerado: ${OUTPUT_FILE}`);
    await logInfo(`Leads: ${totals.totalLeads} | ICP: ${totals.leadsICP} | Reuniões: ${totals.reunioes} | Vendas: ${totals.vendas}`);
  } catch (error) {
    await logError("Erro ao gerar dashboard-data.json", error);
    throw error;
  }
}

main();
