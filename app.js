const DEFAULT_API_ENDPOINT = "https://www.sintesiserp.com/ApiGestionCarteERP/api/ObtenerCarteraFianncieraIden";
const STORAGE_KEYS = {
  identificacion: "collectai.identificacion",
  apiUrl: "collectai.apiUrl",
  token: "collectai.token",
  useProxy: "collectai.useProxy",
  scheduleEnabled: "collectai.schedule.enabled",
  scheduleTime: "collectai.schedule.time",
  scheduleLastRunDay: "collectai.schedule.lastRunDay",
  scheduleLastRunAt: "collectai.schedule.lastRunAt",
  scheduleLastRunCount: "collectai.schedule.lastRunCount",
  scheduleLastRunError: "collectai.schedule.lastRunError",
  lcToken: "collectai.lc.token",
  lcLocationId: "collectai.lc.locationId",
  lcPhoneField: "collectai.lc.phoneField",
  lcTagPorVencer: "collectai.lc.tagPorVencer",
  lcTagCobro: "collectai.lc.tagCobro",
  lcFromNumber: "collectai.lc.fromNumber",
  lcTplPorVencer: "collectai.lc.tplPorVencer",
  lcTplCobro: "collectai.lc.tplCobro",
  lcTestToNumber: "collectai.lc.testToNumber",
  lcUseTemplate: "collectai.lc.useTemplate",
  lcTemplateName: "collectai.lc.template.name",
  lcTemplateLang: "collectai.lc.template.lang",
  lcTemplateBody: "collectai.lc.template.body",
  lcAuto: "collectai.lc.auto",
  lcMax: "collectai.lc.max",
};

const formatLastSync = (date) => {
  return new Intl.DateTimeFormat("es-CO", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
};

const formatCOP = (value) => {
  return new Intl.NumberFormat("es-CO", {
    style: "currency",
    currency: "COP",
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(value);
};

const setText = (id, value) => {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value;
};

const setHidden = (el, hidden) => {
  if (!el) return;
  if (hidden) {
    el.setAttribute("hidden", "true");
  } else {
    el.removeAttribute("hidden");
  }
};

const toNumber = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const raw = value.trim().replace(/\s/g, "");
    if (raw.length === 0) return null;

    const normalized = (() => {
      if (/^-?\d+(\.\d+)?$/.test(raw)) return raw;
      if (/^-?\d+(,\d+)?$/.test(raw)) return raw.replace(",", ".");
      if (raw.includes(".") && raw.includes(",")) return raw.replace(/\./g, "").replace(",", ".");
      return raw.replace(/[^\d,.\-]/g, "");
    })();

    const n = Number.parseFloat(normalized);
    return Number.isFinite(n) ? n : null;
  }
  return null;
};

const pickString = (obj, keys) => {
  for (const k of keys) {
    const v = obj?.[k];
    if (typeof v === "string" && v.trim().length > 0) return v.trim();
    if (typeof v === "number") return String(v);
  }
  return "";
};

const pickNumber = (obj, keys) => {
  for (const k of keys) {
    const n = toNumber(obj?.[k]);
    if (n !== null) return n;
  }
  return null;
};

const getDias = (item) => {
  const n = pickNumber(item, ["dias", "Dias", "DIAS"]);
  if (n === null) return null;
  const i = Math.trunc(n);
  return Number.isFinite(i) ? i : null;
};

const getMonto = (item) => {
  const direct = pickNumber(item, [
    "saldo",
    "Saldo",
    "SALDO",
    "saldo_cuota",
    "Saldo_Cuota",
    "valor",
    "Valor",
    "VALOR",
    "valor_cuota",
    "Valor_Cuota",
    "sactFac",
    "SACTFAC",
    "total",
    "Total",
  ]);

  if (direct !== null) return direct;

  const hasAgingBuckets =
    item &&
    (Object.prototype.hasOwnProperty.call(item, "PorVencer") ||
      Object.prototype.hasOwnProperty.call(item, "Treinta_Dias") ||
      Object.prototype.hasOwnProperty.call(item, "sesenta_Dias") ||
      Object.prototype.hasOwnProperty.call(item, "noventa_Dias") ||
      Object.prototype.hasOwnProperty.call(item, "Mas_de_Noventa"));

  if (hasAgingBuckets) {
    const porVencer = pickNumber(item, ["PorVencer", "porvencer", "Por_Vencer", "por_vencer"]) ?? 0;
    const treinta = pickNumber(item, ["Treinta_Dias", "treinta_dias", "TreintaDias", "treintadias"]) ?? 0;
    const sesenta = pickNumber(item, ["sesenta_Dias", "Sesenta_Dias", "sesenta_dias", "SesentaDias", "sesentadias"]) ?? 0;
    const noventa = pickNumber(item, ["noventa_Dias", "Noventa_Dias", "noventa_dias", "NoventaDias", "noventadias"]) ?? 0;
    const masNoventa =
      pickNumber(item, ["Mas_de_Noventa", "mas_de_noventa", "MasDeNoventa", "Mas_De_Noventa", "masdenoventa"]) ?? 0;
    return porVencer + treinta + sesenta + noventa + masNoventa;
  }

  const bucketCandidates = Object.entries(item || {})
    .filter(([key, value]) => {
      if (!/(vencer|treinta|sesenta|noventa|ciento)/i.test(key)) return false;
      return toNumber(value) !== null;
    })
    .map(([, value]) => toNumber(value))
    .filter((v) => v !== null);

  if (bucketCandidates.length === 0) return 0;
  return bucketCandidates.reduce((acc, v) => acc + v, 0);
};

const getEstado = (dias, range) => {
  if (dias === null) return { label: "Sin datos", className: "status--slate" };

  const r = typeof range === "number" && Number.isFinite(range) ? range : 30;

  if (dias <= 0) {
    const daysToDue = Math.abs(dias);
    if (daysToDue <= r) return { label: "Por vencer", className: "status--green" };
    return { label: "Al día", className: "status--blue" };
  }

  if (dias <= 30) return { label: "Vencida 30", className: "status--amber" };
  if (dias <= 60) return { label: "Vencida 60", className: "status--orange" };
  return { label: "Vencida 90+", className: "status--red" };
};

const getBucket = (dias, range) => {
  if (dias === null) return "sinDatos";

  const r = typeof range === "number" && Number.isFinite(range) ? range : 30;

  if (dias <= 0) {
    const daysToDue = Math.abs(dias);
    return daysToDue <= r ? "porVencer" : "alDia";
  }

  if (dias <= 30) return "v30";
  if (dias <= 60) return "v60";
  return "v90";
};

const setBarHeight = (id, percent) => {
  const el = document.getElementById(id);
  if (!el) return;
  const p = Math.max(4, Math.min(100, Math.round(percent)));
  el.style.height = `${p}%`;
};

const showError = (title, message) => {
  const box = document.getElementById("errorBox");
  const t = document.getElementById("errorTitle");
  const m = document.getElementById("errorMessage");
  if (!box || !t || !m) return;

  t.textContent = title;
  m.textContent = message;
  box.classList.remove("banner--hidden");
};

const hideError = () => {
  const box = document.getElementById("errorBox");
  if (!box) return;
  box.classList.add("banner--hidden");
};

const getTodayISO = () => {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
};

const init = () => {
  setText("year", String(new Date().getFullYear()));
  setText("lastSync", "—");

  const pageTitleEl = document.getElementById("pageTitle");
  const pageSubtitleEl = document.getElementById("pageSubtitle");
  const viewSections = Array.from(document.querySelectorAll(".view[data-view]"));
  const viewLinks = Array.from(document.querySelectorAll("[data-view]"));

  const form = document.getElementById("queryForm");
  const identificacionInput = document.getElementById("identificacion");
  const fechaInput = document.getElementById("fecha");
  const fetchBtn = document.getElementById("fetchNow");

  const configForm = document.getElementById("configForm");
  const configApiUrlInput = document.getElementById("configApiUrl");
  const configTokenInput = document.getElementById("configToken");
  const configStatusEl = document.getElementById("configStatus");
  const configUseProxyInput = document.getElementById("configUseProxy");
  const configScheduleEnabledInput = document.getElementById("configScheduleEnabled");
  const configScheduleTimeInput = document.getElementById("configScheduleTime");
  const configLcTokenInput = document.getElementById("configLcToken");
  const configLcLocationIdInput = document.getElementById("configLcLocationId");
  const configLcPhoneFieldInput = document.getElementById("configLcPhoneField");
  const configLcFromNumberInput = document.getElementById("configLcFromNumber");
  const configLcTagPorVencerInput = document.getElementById("configLcTagPorVencer");
  const configLcTagCobroInput = document.getElementById("configLcTagCobro");
  const configLcTplPorVencerInput = document.getElementById("configLcTplPorVencer");
  const configLcTplCobroInput = document.getElementById("configLcTplCobro");
  const configLcUseTemplateInput = document.getElementById("configLcUseTemplate");
  const configLcTemplateNameInput = document.getElementById("configLcTemplateName");
  const configLcTemplateLangInput = document.getElementById("configLcTemplateLang");
  const configLcTemplateBodyInput = document.getElementById("configLcTemplateBody");
  const configLcTestToNumberInput = document.getElementById("configLcTestToNumber");
  const configLcAutoInput = document.getElementById("configLcAuto");
  const configLcMaxInput = document.getElementById("configLcMax");
  const configDebugEl = document.getElementById("configDebug");
  const scheduleNextRunEl = document.getElementById("scheduleNextRun");
  const scheduleLastRunEl = document.getElementById("scheduleLastRun");
  const runNowBtn = document.getElementById("runNow");
  const testWhatsAppBtn = document.getElementById("testWhatsApp");
  const testWhatsAppToNumberBtn = document.getElementById("testWhatsAppToNumber");
  const sendRemindersBtn = document.getElementById("sendReminders");
  const sendCollectionsBtn = document.getElementById("sendCollections");

  const totalCarteraEl = document.getElementById("totalCartera");
  const facturasTotalesEl = document.getElementById("facturasTotales");
  const porVencerEl = document.getElementById("porVencer");
  const v30El = document.getElementById("v30");
  const v60El = document.getElementById("v60");
  const v90El = document.getElementById("v90");

  const insightTopClientEl = document.getElementById("insightTopClient");
  const insightTopClientAmountEl = document.getElementById("insightTopClientAmount");
  const insightNextInvoiceEl = document.getElementById("insightNextInvoice");
  const insightNextMetaEl = document.getElementById("insightNextMeta");
  const insightMostOverdueInvoiceEl = document.getElementById("insightMostOverdueInvoice");
  const insightMostOverdueMetaEl = document.getElementById("insightMostOverdueMeta");

  const chartNote = document.getElementById("chartNote");
  const rowsEl = document.getElementById("rows");
  const filterInput = document.getElementById("filterInput");
  const rowsCountEl = document.getElementById("rowsCount");
  const toggleAllBtn = document.getElementById("toggleAll");
  const statusChips = document.getElementById("statusChips");

  if (!form || !identificacionInput || !fechaInput || !fetchBtn) return;
  if (!rowsEl || !filterInput || !rowsCountEl || !toggleAllBtn) return;

  const storedIdentificacion = window.localStorage.getItem(STORAGE_KEYS.identificacion) || "";
  const storedApiUrl = window.localStorage.getItem(STORAGE_KEYS.apiUrl) || DEFAULT_API_ENDPOINT;
  const storedToken = window.localStorage.getItem(STORAGE_KEYS.token) || "";
  const storedUseProxyRaw = window.localStorage.getItem(STORAGE_KEYS.useProxy);
  const inferredProxy = /netlify/i.test(window.location.hostname);
  const storedUseProxy = storedUseProxyRaw === null ? inferredProxy : storedUseProxyRaw === "1";
  const storedScheduleEnabled = window.localStorage.getItem(STORAGE_KEYS.scheduleEnabled) === "1";
  const storedScheduleTime = window.localStorage.getItem(STORAGE_KEYS.scheduleTime) || "02:00";
  const storedLcToken = window.localStorage.getItem(STORAGE_KEYS.lcToken) || "";
  const storedLcLocationId = window.localStorage.getItem(STORAGE_KEYS.lcLocationId) || "";
  const storedLcPhoneField = window.localStorage.getItem(STORAGE_KEYS.lcPhoneField) || "telefono";
  const storedLcTagPorVencer = window.localStorage.getItem(STORAGE_KEYS.lcTagPorVencer) || "recarIA_por_vencer";
  const storedLcTagCobro = window.localStorage.getItem(STORAGE_KEYS.lcTagCobro) || "recarIA_cobro";
  const storedLcFromNumber = window.localStorage.getItem(STORAGE_KEYS.lcFromNumber) || "";
  const storedLcTplPorVencer =
    window.localStorage.getItem(STORAGE_KEYS.lcTplPorVencer) ||
    "Hola {nombre}, te recordamos que tienes un vencimiento el {vencimiento} por {saldo} (Factura {factura}).";
  const storedLcTplCobro =
    window.localStorage.getItem(STORAGE_KEYS.lcTplCobro) ||
    "Hola {nombre}, tu factura {factura} venció el {vencimiento} hace {dias} días. Saldo pendiente: {saldo}.";
  const storedLcTestToNumber = window.localStorage.getItem(STORAGE_KEYS.lcTestToNumber) || "";
  const storedLcUseTemplate = window.localStorage.getItem(STORAGE_KEYS.lcUseTemplate) === "1";
  const storedLcTemplateName = window.localStorage.getItem(STORAGE_KEYS.lcTemplateName) || "";
  const storedLcTemplateLang = window.localStorage.getItem(STORAGE_KEYS.lcTemplateLang) || "es_MX";
  const storedLcTemplateBody = window.localStorage.getItem(STORAGE_KEYS.lcTemplateBody) || "";
  const storedLcAuto = window.localStorage.getItem(STORAGE_KEYS.lcAuto) === "1";
  const storedLcMaxRaw = window.localStorage.getItem(STORAGE_KEYS.lcMax) || "50";
  const storedLcMax = Number.isFinite(Number(storedLcMaxRaw)) ? Math.trunc(Number(storedLcMaxRaw)) : 50;

  identificacionInput.value = storedIdentificacion;
  fechaInput.value = getTodayISO();

  let currentItems = [];
  let showAll = false;
  let statusFilter = "all";
  let selectedRange = 30;
  const pageSize = 12;
  let inFlight = null;

  let configApiUrl = storedApiUrl;
  let configToken = storedToken;
  let useProxy = storedUseProxy;
  let scheduleEnabled = storedScheduleEnabled;
  let scheduleTime = storedScheduleTime;
  let scheduleIntervalId = null;
  let lcToken = storedLcToken;
  let lcLocationId = storedLcLocationId;
  let lcPhoneField = storedLcPhoneField;
  let lcTagPorVencer = storedLcTagPorVencer;
  let lcTagCobro = storedLcTagCobro;
  let lcFromNumber = storedLcFromNumber;
  let lcTplPorVencer = storedLcTplPorVencer;
  let lcTplCobro = storedLcTplCobro;
  let lcTestToNumber = storedLcTestToNumber;
  let lcUseTemplate = storedLcUseTemplate;
  let lcTemplateName = storedLcTemplateName;
  let lcTemplateLang = storedLcTemplateLang;
  let lcTemplateBody = storedLcTemplateBody;
  let lcAuto = storedLcAuto;
  let lcMax = storedLcMax;

  const isConfigured = () => {
    return String(configApiUrl || "").trim().length > 0 && String(configToken || "").trim().length > 0;
  };

  const isWhatsAppConfigured = () => {
    return String(lcToken || "").trim().length > 0 && String(lcLocationId || "").trim().length > 0;
  };

  const setLoading = (loading) => {
    fetchBtn.disabled = loading || !isConfigured();
    fetchBtn.textContent = loading ? "Consultando…" : "Consultar";
  };

  const setView = (view) => {
    const viewName = String(view || "").trim() || "dashboard";

    for (const section of viewSections) {
      const isActive = section.getAttribute("data-view") === viewName;
      section.classList.toggle("view--hidden", !isActive);
    }

    const navItems = Array.from(document.querySelectorAll(".nav__item[data-view]"));
    for (const a of navItems) {
      const isActive = a.getAttribute("data-view") === viewName;
      a.classList.toggle("nav__item--active", isActive);
      if (isActive) a.setAttribute("aria-current", "page");
      else a.removeAttribute("aria-current");
    }

    if (pageTitleEl && pageSubtitleEl) {
      if (viewName === "cartera") {
        pageTitleEl.textContent = "Cartera";
        pageSubtitleEl.textContent = "Detalle y filtros de cartera";
      } else if (viewName === "config") {
        pageTitleEl.textContent = "Configuración";
        pageSubtitleEl.textContent = "URL y token de conexión";
      } else {
        pageTitleEl.textContent = "Panel de Control";
        pageSubtitleEl.textContent = "Monitoreo en tiempo real de la cartera";
      }
    }

    if (viewName === "cartera") {
      window.setTimeout(() => filterInput.focus(), 0);
    }

    if (viewName === "config") {
      setHidden(form, true);
      window.setTimeout(() => {
        const active = document.activeElement;
        if (active && configForm && configForm.contains(active)) return;
        (configApiUrlInput || configTokenInput)?.focus();
      }, 0);
    } else {
      setHidden(form, false);
    }
  };

  const setConfigStatus = (text) => {
    if (!configStatusEl) return;
    configStatusEl.textContent = text;
  };

  const setConfigDebug = (value) => {
    if (!configDebugEl) return;
    if (value === null || value === undefined) {
      configDebugEl.textContent = "—";
      return;
    }
    if (typeof value === "string") {
      configDebugEl.textContent = value || "—";
      return;
    }
    try {
      configDebugEl.textContent = JSON.stringify(value, null, 2);
    } catch {
      configDebugEl.textContent = String(value);
    }
  };

  const parseTimeMinutes = (hhmm) => {
    const raw = String(hhmm || "").trim();
    if (!/^\d{2}:\d{2}$/.test(raw)) return null;
    const [h, m] = raw.split(":").map((v) => Number.parseInt(v, 10));
    if (!Number.isFinite(h) || !Number.isFinite(m)) return null;
    if (h < 0 || h > 23) return null;
    if (m < 0 || m > 59) return null;
    return h * 60 + m;
  };

  const formatRunAt = (iso) => {
    if (!iso) return "";
    const d = new Date(iso);
    return Number.isFinite(d.getTime()) ? formatLastSync(d) : String(iso);
  };

  const computeNextRun = () => {
    if (!scheduleEnabled) return null;
    const mins = parseTimeMinutes(scheduleTime);
    if (mins === null) return null;
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
    const run = new Date(today.getTime() + mins * 60_000);
    if (run.getTime() <= now.getTime()) {
      run.setDate(run.getDate() + 1);
    }
    return run;
  };

  const syncLcFromInputs = () => {
    if (configLcTokenInput) lcToken = String(configLcTokenInput.value || "").trim();
    if (configLcLocationIdInput) lcLocationId = String(configLcLocationIdInput.value || "").trim();
    if (configLcFromNumberInput) lcFromNumber = String(configLcFromNumberInput.value || "").trim();
    if (configLcTagPorVencerInput) lcTagPorVencer = String(configLcTagPorVencerInput.value || "").trim() || "recarIA_por_vencer";
    if (configLcTagCobroInput) lcTagCobro = String(configLcTagCobroInput.value || "").trim() || "recarIA_cobro";
    if (configLcTplPorVencerInput) lcTplPorVencer = String(configLcTplPorVencerInput.value || "").trim() || storedLcTplPorVencer;
    if (configLcTplCobroInput) lcTplCobro = String(configLcTplCobroInput.value || "").trim() || storedLcTplCobro;
    if (configLcTestToNumberInput) lcTestToNumber = String(configLcTestToNumberInput.value || "").trim();
    if (configLcUseTemplateInput) lcUseTemplate = Boolean(configLcUseTemplateInput.checked);
    if (configLcTemplateNameInput) lcTemplateName = String(configLcTemplateNameInput.value || "").trim();
    if (configLcTemplateLangInput) lcTemplateLang = String(configLcTemplateLangInput.value || "").trim() || "es_MX";
    if (configLcTemplateBodyInput) lcTemplateBody = String(configLcTemplateBodyInput.value || "");
  };

  const refreshScheduleUi = () => {
    syncLcFromInputs();
    if (scheduleNextRunEl) {
      if (!scheduleEnabled) {
        scheduleNextRunEl.textContent = "Programación desactivada";
      } else {
        const next = computeNextRun();
        scheduleNextRunEl.textContent = next ? `Próxima ejecución: ${formatLastSync(next)}` : "Hora inválida";
      }
    }

    if (scheduleLastRunEl) {
      const at = window.localStorage.getItem(STORAGE_KEYS.scheduleLastRunAt) || "";
      const count = window.localStorage.getItem(STORAGE_KEYS.scheduleLastRunCount) || "";
      const err = window.localStorage.getItem(STORAGE_KEYS.scheduleLastRunError) || "";
      if (!at) {
        scheduleLastRunEl.textContent = "Aún sin ejecuciones";
      } else if (err) {
        scheduleLastRunEl.textContent = `Última ejecución: ${formatRunAt(at)} • Error: ${err}`;
      } else if (count) {
        scheduleLastRunEl.textContent = `Última ejecución: ${formatRunAt(at)} • ${count} registros`;
      } else {
        scheduleLastRunEl.textContent = `Última ejecución: ${formatRunAt(at)}`;
      }
    }

    if (runNowBtn) runNowBtn.disabled = !isConfigured();
    const waOk = isWhatsAppConfigured();
    if (testWhatsAppBtn) testWhatsAppBtn.disabled = !waOk;
    if (testWhatsAppToNumberBtn) testWhatsAppToNumberBtn.disabled = !waOk;
    if (sendRemindersBtn) sendRemindersBtn.disabled = !waOk;
    if (sendCollectionsBtn) sendCollectionsBtn.disabled = !waOk;
  };

  const refreshConfigUi = () => {
    const ok = isConfigured();
    if (!ok) {
      setConfigStatus("Configura URL y token para poder consultar.");
      setLoading(false);
      refreshScheduleUi();
      return;
    }

    try {
      const host = new URL(String(configApiUrl)).host;
      setConfigStatus(`Configurado • ${host}`);
    } catch {
      setConfigStatus("Configurado");
    }

    setLoading(false);
    refreshScheduleUi();
  };

  if (configApiUrlInput) configApiUrlInput.value = configApiUrl;
  if (configTokenInput) configTokenInput.value = configToken;
  if (configUseProxyInput) configUseProxyInput.checked = useProxy;
  if (configScheduleEnabledInput) configScheduleEnabledInput.checked = scheduleEnabled;
  if (configScheduleTimeInput) configScheduleTimeInput.value = scheduleTime;
  if (configLcTokenInput) configLcTokenInput.value = lcToken;
  if (configLcLocationIdInput) configLcLocationIdInput.value = lcLocationId;
  if (configLcPhoneFieldInput) configLcPhoneFieldInput.value = lcPhoneField;
  if (configLcFromNumberInput) configLcFromNumberInput.value = lcFromNumber;
  if (configLcTagPorVencerInput) configLcTagPorVencerInput.value = lcTagPorVencer;
  if (configLcTagCobroInput) configLcTagCobroInput.value = lcTagCobro;
  if (configLcTplPorVencerInput) configLcTplPorVencerInput.value = lcTplPorVencer;
  if (configLcTplCobroInput) configLcTplCobroInput.value = lcTplCobro;
  if (configLcUseTemplateInput) configLcUseTemplateInput.checked = lcUseTemplate;
  if (configLcTemplateNameInput) configLcTemplateNameInput.value = lcTemplateName;
  if (configLcTemplateLangInput) configLcTemplateLangInput.value = lcTemplateLang;
  if (configLcTemplateBodyInput) configLcTemplateBodyInput.value = lcTemplateBody;
  if (configLcTestToNumberInput) configLcTestToNumberInput.value = lcTestToNumber;
  if (configLcAutoInput) configLcAutoInput.checked = lcAuto;
  if (configLcMaxInput) configLcMaxInput.value = String(lcMax || 50);
  refreshConfigUi();

  const wireRefresh = (el) => {
    if (!el) return;
    el.addEventListener("input", () => refreshScheduleUi());
    el.addEventListener("change", () => refreshScheduleUi());
  };

  wireRefresh(configLcTokenInput);
  wireRefresh(configLcLocationIdInput);
  wireRefresh(configLcFromNumberInput);
  wireRefresh(configLcTestToNumberInput);
  wireRefresh(configLcUseTemplateInput);
  wireRefresh(configLcTemplateNameInput);
  wireRefresh(configLcTemplateLangInput);
  wireRefresh(configLcTemplateBodyInput);

  const normalizePhone = (raw) => {
    const s = String(raw || "").trim();
    if (!s) return "";
    if (s.startsWith("+")) return s.replace(/\s+/g, "");
    const digits = s.replace(/[^\d]/g, "");
    if (!digits) return "";
    if (digits.length === 12 && digits.startsWith("57")) return `+${digits}`;
    if (digits.length === 10) return `+57${digits}`;
    if (digits.length >= 11 && digits.length <= 15) return `+${digits}`;
    return s;
  };

  const getPhone = (item) => {
    const preferred = String(lcPhoneField || "").trim();
    const preferredValue = preferred ? pickString(item, [preferred]) : "";
    const v =
      preferredValue ||
      pickString(item, ["telefono", "Telefono", "tel", "Tel", "celular", "Celular", "movil", "Movil", "whatsapp", "WhatsApp", "phone", "Phone"]);
    return normalizePhone(v);
  };

  const getEmail = (item) => {
    return pickString(item, ["email", "Email", "correo", "Correo", "mail", "Mail"]) || "";
  };

  const renderTemplate = (tpl, vars) => {
    const s = String(tpl || "");
    return s.replace(/\{(\w+)\}/g, (_, key) => (vars && vars[key] !== undefined && vars[key] !== null ? String(vars[key]) : ""));
  };

  const templateIsEnabled = () => {
    return Boolean(lcUseTemplate && String(lcTemplateName || "").trim());
  };

  const parseLines = (raw) => {
    return String(raw || "")
      .split(/\r?\n/g)
      .map((s) => s.trim())
      .filter(Boolean);
  };

  const buildTemplatePayload = ({ vars }) => {
    if (!templateIsEnabled()) return null;
    const name = String(lcTemplateName || "").trim();
    const lang = String(lcTemplateLang || "").trim() || "es_MX";
    const body = parseLines(lcTemplateBody).map((line) => renderTemplate(line, vars));
    return {
      MessageType: 19,
      whatsapp: {
        type: "template",
        template: { name, lang },
        placeholders: { header: [], body, buttons: [] },
      },
    };
  };

  const buildMessages = ({ mode }) => {
    const maxLocal = Math.max(1, Math.min(200, Number.isFinite(lcMax) ? lcMax : 50));
    const tag = mode === "porVencer" ? String(lcTagPorVencer || "").trim() : String(lcTagCobro || "").trim();
    const wanted = [];
    const seen = new Set();
    let missingPhone = 0;

    for (const it of currentItems) {
      const dias = getDias(it);
      if (dias === null) continue;
      if (mode === "porVencer") {
        if (dias > 0) continue;
        if (Math.abs(dias) > selectedRange) continue;
      } else {
        if (dias <= 0) continue;
      }

      const phone = getPhone(it);
      if (!phone) {
        missingPhone += 1;
        continue;
      }

      if (seen.has(phone)) continue;
      seen.add(phone);

      const name = pickString(it, ["cliente", "Cliente", "CLIENTE"]) || "Cliente";
      const email = getEmail(it);
      const factura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "";
      const vencimiento = pickString(it, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"]) || "";
      const saldo = getMonto(it);
      const tpl = mode === "porVencer" ? lcTplPorVencer : lcTplCobro;
      const vars = { nombre: name, factura, vencimiento, dias: String(Math.abs(dias)), saldo: formatCOP(saldo) };
      const msg = renderTemplate(tpl, vars).trim();

      const payload = {
        name,
        toNumber: phone,
        message: msg,
      };
      if (email) payload.email = email;
      if (tag) payload.tags = [tag];
      const tplPayload = buildTemplatePayload({ vars });
      if (tplPayload) Object.assign(payload, tplPayload);
      wanted.push(payload);
      if (wanted.length >= maxLocal) break;
    }

    return { messages: wanted, missingPhone };
  };

  const sendToLeadConnector = async ({ mode, test }) => {
    try {
      if (!isWhatsAppConfigured()) {
        setConfigStatus("Configura Token WhatsApp y LocationId para enviar.");
        return { ok: false, error: "no_config" };
      }

      const maxLocal = Math.max(1, Math.min(200, Number.isFinite(lcMax) ? lcMax : 50));

      if (lcUseTemplate && !templateIsEnabled()) {
        setConfigStatus("Activa Template name o desmarca Enviar como plantilla.");
        return { ok: false, error: "no_template" };
      }

      if (!String(lcFromNumber || "").trim()) {
        setConfigStatus("Configura FromNumber (WhatsApp) para enviar.");
        return { ok: false, error: "no_from" };
      }

      let messages = [];
      let missingPhone = 0;
      if (test) {
        const first = currentItems[0];
        if (!first) {
          setConfigStatus("No hay datos cargados para probar.");
          return { ok: false, error: "no_data" };
        }
        const phone = getPhone(first);
        if (!phone) {
          setConfigStatus("No se encontró teléfono en el primer registro.");
          return { ok: false, error: "no_phone" };
        }
        const name = pickString(first, ["cliente", "Cliente", "CLIENTE"]) || "Cliente";
        const email = getEmail(first);
        const factura = pickString(first, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "";
        const vencimiento = pickString(first, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"]) || "";
        const dias = getDias(first) ?? 0;
        const saldo = getMonto(first);
        const tpl = mode === "porVencer" ? lcTplPorVencer : lcTplCobro;
        const vars = { nombre: name, factura, vencimiento, dias: String(Math.abs(dias)), saldo: formatCOP(saldo) };
        const msg = renderTemplate(tpl, vars).trim();

        const payload = { name, toNumber: phone, message: msg };
        if (email) payload.email = email;
        const tplPayload = buildTemplatePayload({ vars });
        if (tplPayload) Object.assign(payload, tplPayload);
        messages = [payload];
      } else {
        const built = buildMessages({ mode });
        messages = built.messages;
        missingPhone = built.missingPhone;
      }

      if (messages.length === 0) {
        setConfigStatus(missingPhone ? `Sin teléfonos para enviar (${missingPhone} sin teléfono).` : "No hay contactos para enviar.");
        return { ok: true, sent: 0, failed: 0 };
      }

      setConfigStatus(`Enviando WhatsApp (${messages.length}${missingPhone ? ` • ${missingPhone} sin teléfono` : ""})…`);
      setConfigDebug("—");

      const resp = await fetch("/.netlify/functions/leadconnector", {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify({
          action: "sendWhatsApp",
          token: lcToken,
          locationId: lcLocationId,
          fromNumber: lcFromNumber,
          versionMsg: "2021-04-15",
          messages,
          max: maxLocal,
        }),
      });

      const text = await resp.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = null;
      }

      if (!resp.ok) {
        setConfigStatus(`Error WhatsApp: HTTP ${resp.status}`);
        setConfigDebug(text || `HTTP ${resp.status}`);
        return { ok: false, error: text || `HTTP ${resp.status}` };
      }

      const sent = Number(json?.sent ?? 0);
      const failed = Number(json?.failed ?? 0);
      const trunc = Boolean(json?.truncated);
      const contactId = Array.isArray(json?.results) ? String(json.results.find((r) => r && r.ok && r.contactId)?.contactId || "") : "";
      setConfigStatus(`WhatsApp: ${sent} ok • ${failed} error${trunc ? " • truncado" : ""}${contactId ? ` • contactId ${contactId}` : ""}`);
      setConfigDebug(json || text);
      return { ok: failed === 0, sent, failed };
    } catch (err) {
      setConfigStatus("Error WhatsApp: no se pudo conectar.");
      setConfigDebug(String(err?.message || err || "Error"));
      return { ok: false, error: String(err?.message || err || "Error") };
    }
  };

  const sendTestToNumber = async () => {
    try {
      if (!isWhatsAppConfigured()) {
        setConfigStatus("Configura Token WhatsApp y LocationId para enviar.");
        return;
      }

      const to = normalizePhone(configLcTestToNumberInput?.value || lcTestToNumber || "");
      if (!to) {
        setConfigStatus("Ingresa un número de prueba válido.");
        return;
      }

      if (lcUseTemplate && !templateIsEnabled()) {
        setConfigStatus("Activa Template name o desmarca Enviar como plantilla.");
        return;
      }

      if (!String(lcFromNumber || "").trim()) {
        setConfigStatus("Configura FromNumber (WhatsApp) para enviar.");
        return;
      }

      setConfigStatus(`Enviando WhatsApp a ${to}…`);
      setConfigDebug("—");

      const vars = { nombre: "Prueba", factura: "", vencimiento: "", dias: "0", saldo: "" };
      const msgPayload = { name: "Prueba", toNumber: to, message: lcTplPorVencer || "Prueba" };
      const tplPayload = buildTemplatePayload({ vars });
      if (tplPayload) Object.assign(msgPayload, tplPayload);

      const resp = await fetch("/.netlify/functions/leadconnector", {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify({
          action: "sendWhatsApp",
          token: lcToken,
          locationId: lcLocationId,
          fromNumber: lcFromNumber,
          versionMsg: "2021-04-15",
          messages: [msgPayload],
          max: 1,
        }),
      });

      const text = await resp.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = null;
      }

      if (!resp.ok) {
        setConfigStatus(`Error WhatsApp: HTTP ${resp.status}`);
        setConfigDebug(text || `HTTP ${resp.status}`);
        return;
      }

      const sent = Number(json?.sent ?? 0);
      const failed = Number(json?.failed ?? 0);
      const contactId = Array.isArray(json?.results) ? String(json.results.find((r) => r && r.ok && r.contactId)?.contactId || "") : "";
      setConfigStatus(`WhatsApp: ${sent} ok • ${failed} error${contactId ? ` • contactId ${contactId}` : ""}`);
      setConfigDebug(json || text);
    } catch (err) {
      setConfigStatus("Error WhatsApp: no se pudo conectar.");
      setConfigDebug(String(err?.message || err || "Error"));
    }
  };

  const render = () => {
    const q = String(filterInput.value || "").trim().toLowerCase();
    const filteredByStatus = statusFilter === "all"
      ? currentItems
      : currentItems.filter((it) => getBucket(getDias(it), selectedRange) === statusFilter);

    const filtered = q.length
      ? filteredByStatus.filter((it) => {
          const factura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]).toLowerCase();
          const cliente = pickString(it, ["cliente", "Cliente", "nombre", "Nombre"]).toLowerCase();
          const iden = pickString(it, ["iden", "Identificacion", "identificacion"]).toLowerCase();
          return factura.includes(q) || cliente.includes(q) || iden.includes(q);
        })
      : filteredByStatus.slice();

    const visible = showAll ? filtered : filtered.slice(0, pageSize);
    rowsEl.innerHTML = "";

    if (visible.length === 0) {
      rowsEl.innerHTML =
        '<tr><td class="mono">—</td><td>—</td><td>—</td><td class="t-right mono">—</td><td class="t-right">$0</td><td class="t-center"><span class="status status--slate">Sin datos</span></td></tr>';
      rowsCountEl.textContent = "0";
      setHidden(toggleAllBtn, true);
      return;
    }

    for (const it of visible) {
      const factura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "—";
      const cliente = pickString(it, ["cliente", "Cliente", "CLIENTE"]) || "—";
      const vencimiento = pickString(it, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"]) || "—";
      const dias = getDias(it);
      const monto = getMonto(it);
      const estado = getEstado(dias, selectedRange);

      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="mono">${escapeHtml(factura)}</td>
        <td>${escapeHtml(cliente)}</td>
        <td>${escapeHtml(vencimiento)}</td>
        <td class="t-right mono">${dias === null ? "—" : escapeHtml(String(dias))}</td>
        <td class="t-right">${escapeHtml(formatCOP(monto))}</td>
        <td class="t-center"><span class="status ${estado.className}">${escapeHtml(estado.label)}</span></td>
      `;
      rowsEl.appendChild(tr);
    }

    rowsCountEl.textContent = String(visible.length);
    setHidden(toggleAllBtn, filtered.length <= pageSize);
    toggleAllBtn.textContent = showAll ? "Ver menos" : "Ver todo";
  };

  const computeAndRenderKpis = () => {
    const total = currentItems.reduce((acc, it) => acc + getMonto(it), 0);
    if (totalCarteraEl) totalCarteraEl.textContent = formatCOP(total);
    if (facturasTotalesEl) facturasTotalesEl.textContent = String(currentItems.length);

    let alDia = 0;
    let porVencer = 0;
    let v30 = 0;
    let v60 = 0;
    let v90 = 0;

    for (const it of currentItems) {
      const dias = getDias(it);
      if (dias === null) continue;
      const bucket = getBucket(dias, selectedRange);
      if (bucket === "alDia") alDia += 1;
      else if (bucket === "porVencer") porVencer += 1;
      else if (bucket === "v30") v30 += 1;
      else if (bucket === "v60") v60 += 1;
      else if (bucket === "v90") v90 += 1;
    }

    if (porVencerEl) porVencerEl.textContent = String(porVencer);
    if (v30El) v30El.textContent = String(v30);
    if (v60El) v60El.textContent = String(v60);
    if (v90El) v90El.textContent = String(v90);

    const denom = Math.max(1, alDia + porVencer + v30 + v60 + v90);
    setBarHeight("barAlDia", (alDia / denom) * 100);
    setBarHeight("barPorVencer", (porVencer / denom) * 100);
    setBarHeight("bar30", (v30 / denom) * 100);
    setBarHeight("bar60", (v60 / denom) * 100);
    setBarHeight("bar90", (v90 / denom) * 100);

    setText("countAlDia", String(alDia));
    setText("countPorVencer", String(porVencer));
    setText("count30", String(v30));
    setText("count60", String(v60));
    setText("count90", String(v90));

    if (chartNote) {
      chartNote.textContent = currentItems.length ? `${currentItems.length} registros • horizonte ${selectedRange} días` : "Sin datos";
    }
  };

  const computeAndRenderInsights = () => {
    const setEmpty = () => {
      if (insightTopClientEl) insightTopClientEl.textContent = "—";
      if (insightTopClientAmountEl) insightTopClientAmountEl.textContent = "—";
      if (insightNextInvoiceEl) insightNextInvoiceEl.textContent = "—";
      if (insightNextMetaEl) insightNextMetaEl.textContent = "—";
      if (insightMostOverdueInvoiceEl) insightMostOverdueInvoiceEl.textContent = "—";
      if (insightMostOverdueMetaEl) insightMostOverdueMetaEl.textContent = "—";
    };

    if (!Array.isArray(currentItems) || currentItems.length === 0) {
      setEmpty();
      return;
    }

    const byClient = new Map();
    for (const it of currentItems) {
      const cliente = pickString(it, ["cliente", "Cliente", "CLIENTE"]);
      const iden = pickString(it, ["iden", "Identificacion", "identificacion"]);
      const key = [iden, cliente].filter(Boolean).join(" • ") || "—";
      const prev = byClient.get(key) || 0;
      byClient.set(key, prev + getMonto(it));
    }

    let topClientKey = "";
    let topClientTotal = -Infinity;
    for (const [key, total] of byClient.entries()) {
      if (total > topClientTotal) {
        topClientTotal = total;
        topClientKey = key;
      }
    }

    if (insightTopClientEl) insightTopClientEl.textContent = topClientKey || "—";
    if (insightTopClientAmountEl) {
      insightTopClientAmountEl.textContent = topClientTotal > -Infinity ? formatCOP(topClientTotal) : "—";
    }

    let nextDue = null;
    let nextDueDias = -Infinity;
    for (const it of currentItems) {
      const dias = getDias(it);
      if (dias === null) continue;
      if (dias > 0) continue;
      const within = Math.abs(dias) <= selectedRange;
      if (!within) continue;
      if (dias > nextDueDias) {
        nextDueDias = dias;
        nextDue = it;
      }
    }

    if (!nextDue) {
      for (const it of currentItems) {
        const dias = getDias(it);
        if (dias === null) continue;
        if (dias > 0) continue;
        if (dias > nextDueDias) {
          nextDueDias = dias;
          nextDue = it;
        }
      }
    }

    if (nextDue && insightNextInvoiceEl && insightNextMetaEl) {
      const factura = pickString(nextDue, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "—";
      const venc = pickString(nextDue, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"]) || "—";
      const monto = getMonto(nextDue);
      insightNextInvoiceEl.textContent = factura;
      insightNextMetaEl.textContent = `${venc} • ${formatCOP(monto)} • ${Math.abs(nextDueDias)} días`;
    } else {
      if (insightNextInvoiceEl) insightNextInvoiceEl.textContent = "—";
      if (insightNextMetaEl) insightNextMetaEl.textContent = "—";
    }

    let mostOverdue = null;
    let mostOverdueDias = -Infinity;
    for (const it of currentItems) {
      const dias = getDias(it);
      if (dias === null) continue;
      if (dias <= 0) continue;
      if (dias > mostOverdueDias) {
        mostOverdueDias = dias;
        mostOverdue = it;
      }
    }

    if (mostOverdue && insightMostOverdueInvoiceEl && insightMostOverdueMetaEl) {
      const factura = pickString(mostOverdue, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "—";
      const cliente = pickString(mostOverdue, ["cliente", "Cliente", "CLIENTE"]) || "—";
      const monto = getMonto(mostOverdue);
      insightMostOverdueInvoiceEl.textContent = factura;
      insightMostOverdueMetaEl.textContent = `${cliente} • ${formatCOP(monto)} • ${mostOverdueDias} días`;
    } else {
      if (insightMostOverdueInvoiceEl) insightMostOverdueInvoiceEl.textContent = "—";
      if (insightMostOverdueMetaEl) insightMostOverdueMetaEl.textContent = "—";
    }
  };

  const fetchCartera = async ({ identificacion, fecha }) => {
    hideError();

    if (!isConfigured()) {
      showError("Configuración requerida", "Configura la URL y el token antes de consultar.");
      setView("config");
      return { ok: false, error: "Configuración requerida" };
    }

    if (!fecha) {
      showError("Datos incompletos", "Fecha es obligatoria. Identificación es opcional.");
      return { ok: false, error: "Fecha requerida" };
    }

    window.localStorage.setItem(STORAGE_KEYS.identificacion, identificacion || "");

    if (inFlight) inFlight.abort();
    inFlight = new AbortController();

    setLoading(true);
    try {
      const endpoint = useProxy
        ? "/.netlify/functions/sync"
        : (() => {
            const url = new URL(String(configApiUrl));
            url.searchParams.set("token", String(configToken));
            return url.toString();
          })();

      const payload = useProxy
        ? {
            apiUrl: String(configApiUrl),
            token: String(configToken),
            Identificacion: identificacion || "",
            Fecha: fecha,
          }
        : {
            Identificacion: identificacion || "",
            Fecha: fecha,
          };

      const resp = await fetch(endpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "application/json",
        },
        body: JSON.stringify(payload),
        signal: inFlight.signal,
      });

      if (!resp.ok) {
        const text = await resp.text().catch(() => "");
        let message = `HTTP ${resp.status} ${resp.statusText}`;
        try {
          const parsed = text ? JSON.parse(text) : null;
          if (parsed?.Mensaje) message = String(parsed.Mensaje);
          else if (parsed?.message) message = String(parsed.message);
          else if (text) message = String(text).slice(0, 500);
        } catch {
          if (text) message = String(text).slice(0, 500);
        }
        throw new Error(message);
      }

      const data = await resp.json();
      if (data?.Error) {
        throw new Error(String(data?.Mensaje || "La API devolvió un error."));
      }

      const result = Array.isArray(data?.Resultado) ? data.Resultado : [];
      currentItems = result;
      showAll = false;
      statusFilter = "all";
      setText("lastSync", formatLastSync(new Date()));
      computeAndRenderKpis();
      computeAndRenderInsights();
      render();
      if (statusChips) {
        for (const btn of Array.from(statusChips.querySelectorAll("[data-status]"))) {
          btn.classList.toggle("btn--ghost-active", btn.getAttribute("data-status") === "all");
        }
      }
      return { ok: true, count: result.length };
    } catch (err) {
      if (err?.name === "AbortError") return;

      const msg = String(err?.message || err || "Error desconocido");
      if (/failed to fetch/i.test(msg)) {
        showError(
          "No se pudo consultar",
          "El navegador bloqueó la petición (CORS o red). Si ves esto desde archivo (file://), prueba abrirlo desde un servidor local y verifica que la API permita CORS."
        );
      } else {
        showError("No se pudo consultar", msg);
      }
      return { ok: false, error: msg };
    } finally {
      setLoading(false);
    }
  };

  const runScheduled = async () => {
    const now = new Date();
    const todayKey = getTodayISO();
    window.localStorage.setItem(STORAGE_KEYS.scheduleLastRunDay, todayKey);
    window.localStorage.setItem(STORAGE_KEYS.scheduleLastRunAt, now.toISOString());
    window.localStorage.setItem(STORAGE_KEYS.scheduleLastRunError, "");
    window.localStorage.setItem(STORAGE_KEYS.scheduleLastRunCount, "");
    refreshScheduleUi();

    const res = await fetchCartera({ identificacion: "", fecha: todayKey });
    if (res?.ok) {
      window.localStorage.setItem(STORAGE_KEYS.scheduleLastRunError, "");
      window.localStorage.setItem(STORAGE_KEYS.scheduleLastRunCount, String(res.count ?? 0));
      if (lcAuto && isWhatsAppConfigured()) {
        await sendToLeadConnector({ mode: "porVencer", test: false });
        await sendToLeadConnector({ mode: "cobro", test: false });
      }
    } else if (res?.error) {
      window.localStorage.setItem(STORAGE_KEYS.scheduleLastRunError, String(res.error));
    }
    refreshScheduleUi();
  };

  const startScheduler = () => {
    if (scheduleIntervalId) {
      window.clearInterval(scheduleIntervalId);
      scheduleIntervalId = null;
    }

    if (!scheduleEnabled) return;
    scheduleIntervalId = window.setInterval(() => {
      if (!scheduleEnabled) return;
      if (!isConfigured()) return;

      const scheduleMinutes = parseTimeMinutes(scheduleTime);
      if (scheduleMinutes === null) return;

      const now = new Date();
      const target = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
      target.setTime(target.getTime() + scheduleMinutes * 60_000);
      if (now.getTime() < target.getTime()) return;

      const lastAtRaw = window.localStorage.getItem(STORAGE_KEYS.scheduleLastRunAt) || "";
      const lastAt = lastAtRaw ? new Date(lastAtRaw) : null;
      const lastAtMs = lastAt && Number.isFinite(lastAt.getTime()) ? lastAt.getTime() : -Infinity;
      if (lastAtMs >= target.getTime()) return;

      runScheduled();
    }, 30_000);
  };

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    fetchCartera({
      identificacion: String(identificacionInput.value || "").trim(),
      fecha: String(fechaInput.value || "").trim(),
    });
  });

  if (configForm && configApiUrlInput && configTokenInput) {
    configForm.addEventListener("submit", (e) => {
      e.preventDefault();
      hideError();

      const nextUrl = String(configApiUrlInput.value || "").trim();
      const nextToken = String(configTokenInput.value || "").trim();

      if (!nextUrl || !nextToken) {
        showError("Configuración incompleta", "URL y token son obligatorios.");
        setConfigStatus("Configura URL y token para poder consultar.");
        refreshConfigUi();
        return;
      }

      try {
        new URL(nextUrl);
      } catch {
        showError("URL inválida", "La URL API no es válida.");
        setConfigStatus("La URL ingresada no es válida.");
        refreshConfigUi();
        return;
      }

      configApiUrl = nextUrl;
      configToken = nextToken;
      window.localStorage.setItem(STORAGE_KEYS.apiUrl, configApiUrl);
      window.localStorage.setItem(STORAGE_KEYS.token, configToken);

      if (configUseProxyInput) useProxy = Boolean(configUseProxyInput.checked);
      window.localStorage.setItem(STORAGE_KEYS.useProxy, useProxy ? "1" : "0");

      if (configScheduleEnabledInput) scheduleEnabled = Boolean(configScheduleEnabledInput.checked);
      if (configScheduleTimeInput) scheduleTime = String(configScheduleTimeInput.value || "").trim() || "02:00";

      if (scheduleEnabled && parseTimeMinutes(scheduleTime) === null) {
        showError("Hora inválida", "Selecciona una hora válida para la programación.");
        refreshConfigUi();
        return;
      }

      window.localStorage.setItem(STORAGE_KEYS.scheduleEnabled, scheduleEnabled ? "1" : "0");
      window.localStorage.setItem(STORAGE_KEYS.scheduleTime, scheduleTime);

      if (configLcTokenInput) lcToken = String(configLcTokenInput.value || "").trim();
      if (configLcLocationIdInput) lcLocationId = String(configLcLocationIdInput.value || "").trim();
      if (configLcPhoneFieldInput) lcPhoneField = String(configLcPhoneFieldInput.value || "").trim() || "telefono";
      if (configLcFromNumberInput) lcFromNumber = String(configLcFromNumberInput.value || "").trim();
      if (configLcTagPorVencerInput) lcTagPorVencer = String(configLcTagPorVencerInput.value || "").trim() || "recarIA_por_vencer";
      if (configLcTagCobroInput) lcTagCobro = String(configLcTagCobroInput.value || "").trim() || "recarIA_cobro";
      if (configLcTplPorVencerInput) lcTplPorVencer = String(configLcTplPorVencerInput.value || "").trim() || storedLcTplPorVencer;
      if (configLcTplCobroInput) lcTplCobro = String(configLcTplCobroInput.value || "").trim() || storedLcTplCobro;
      if (configLcUseTemplateInput) lcUseTemplate = Boolean(configLcUseTemplateInput.checked);
      if (configLcTemplateNameInput) lcTemplateName = String(configLcTemplateNameInput.value || "").trim();
      if (configLcTemplateLangInput) lcTemplateLang = String(configLcTemplateLangInput.value || "").trim() || "es_MX";
      if (configLcTemplateBodyInput) lcTemplateBody = String(configLcTemplateBodyInput.value || "");
      if (configLcTestToNumberInput) lcTestToNumber = String(configLcTestToNumberInput.value || "").trim();
      if (configLcAutoInput) lcAuto = Boolean(configLcAutoInput.checked);
      if (configLcMaxInput) {
        const n = Number(configLcMaxInput.value);
        lcMax = Number.isFinite(n) ? Math.max(1, Math.min(200, Math.trunc(n))) : 50;
      }

      window.localStorage.setItem(STORAGE_KEYS.lcToken, lcToken);
      window.localStorage.setItem(STORAGE_KEYS.lcLocationId, lcLocationId);
      window.localStorage.setItem(STORAGE_KEYS.lcPhoneField, lcPhoneField);
      window.localStorage.setItem(STORAGE_KEYS.lcFromNumber, lcFromNumber);
      window.localStorage.setItem(STORAGE_KEYS.lcTagPorVencer, lcTagPorVencer);
      window.localStorage.setItem(STORAGE_KEYS.lcTagCobro, lcTagCobro);
      window.localStorage.setItem(STORAGE_KEYS.lcTplPorVencer, lcTplPorVencer);
      window.localStorage.setItem(STORAGE_KEYS.lcTplCobro, lcTplCobro);
      window.localStorage.setItem(STORAGE_KEYS.lcTestToNumber, lcTestToNumber);
      window.localStorage.setItem(STORAGE_KEYS.lcUseTemplate, lcUseTemplate ? "1" : "0");
      window.localStorage.setItem(STORAGE_KEYS.lcTemplateName, lcTemplateName);
      window.localStorage.setItem(STORAGE_KEYS.lcTemplateLang, lcTemplateLang);
      window.localStorage.setItem(STORAGE_KEYS.lcTemplateBody, lcTemplateBody);
      window.localStorage.setItem(STORAGE_KEYS.lcAuto, lcAuto ? "1" : "0");
      window.localStorage.setItem(STORAGE_KEYS.lcMax, String(lcMax));

      setConfigStatus("Guardado");
      refreshConfigUi();
      startScheduler();
    });
  }

  if (runNowBtn) {
    runNowBtn.addEventListener("click", () => {
      runScheduled();
    });
  }

  if (testWhatsAppBtn) {
    testWhatsAppBtn.addEventListener("click", () => {
      setConfigStatus("Enviando WhatsApp…");
      setConfigDebug("—");
      sendToLeadConnector({ mode: "porVencer", test: true });
    });
  }

  if (testWhatsAppToNumberBtn) {
    testWhatsAppToNumberBtn.addEventListener("click", () => {
      setConfigStatus("Enviando WhatsApp…");
      setConfigDebug("—");
      sendTestToNumber();
    });
  }

  if (sendRemindersBtn) {
    sendRemindersBtn.addEventListener("click", () => {
      sendToLeadConnector({ mode: "porVencer", test: false });
    });
  }

  if (sendCollectionsBtn) {
    sendCollectionsBtn.addEventListener("click", () => {
      sendToLeadConnector({ mode: "cobro", test: false });
    });
  }

  filterInput.addEventListener("input", () => {
    render();
  });

  toggleAllBtn.addEventListener("click", () => {
    showAll = !showAll;
    render();
  });

  if (statusChips) {
    const chips = Array.from(statusChips.querySelectorAll("[data-status]"));
    for (const btn of chips) {
      btn.addEventListener("click", () => {
        const next = btn.getAttribute("data-status") || "all";
        statusFilter = next;
        showAll = false;
        for (const other of chips) other.classList.remove("btn--ghost-active");
        btn.classList.add("btn--ghost-active");
        render();
      });
    }
  }

  if (viewSections.length > 0) {
    for (const a of viewLinks) {
      a.addEventListener("click", (e) => {
        const view = a.getAttribute("data-view");
        if (!view) return;
        if (a.tagName.toLowerCase() === "a") e.preventDefault();
        if (window.location.hash !== `#${view}`) window.location.hash = view;
        setView(view);
      });
    }

    window.addEventListener("hashchange", () => {
      const raw = window.location.hash.replace("#", "").trim();
      setView(raw || "dashboard");
    });
  }

  const rangeButtons = Array.from(document.querySelectorAll("[data-range]"));
  for (const btn of rangeButtons) {
    btn.addEventListener("click", () => {
      for (const other of rangeButtons) other.classList.remove("btn--ghost-active");
      btn.classList.add("btn--ghost-active");
      const n = Number.parseInt(btn.getAttribute("data-range") || "30", 10);
      selectedRange = Number.isFinite(n) ? n : 30;
      computeAndRenderKpis();
      computeAndRenderInsights();
      render();
    });
  }

  computeAndRenderKpis();
  computeAndRenderInsights();
  render();
  const initialView = window.location.hash.replace("#", "").trim();
  if (!initialView && !isConfigured()) {
    window.location.hash = "config";
    setView("config");
  } else {
    setView(initialView || "dashboard");
  }

  startScheduler();
  refreshScheduleUi();
};

const escapeHtml = (s) => {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
