import sql from "mssql";
import { handler as syncHandler } from "./sync.js";
import { handler as leadconnectorHandler } from "./leadconnector.js";

export const config = { schedule: "*/5 * * * *" };

let poolPromise = null;

const getEnv = (name) => {
  const v = process.env[name];
  return v === undefined ? "" : String(v);
};

const getSqlConfig = () => {
  const encryptRaw = getEnv("SQL_SERVER_ENCRYPT").trim().toLowerCase();
  const trustRaw = getEnv("SQL_SERVER_TRUST_CERT").trim().toLowerCase();
  return {
    server: getEnv("SQL_SERVER_HOST").trim(),
    user: getEnv("SQL_SERVER_USER").trim(),
    password: getEnv("SQL_SERVER_PASSWORD"),
    database: getEnv("SQL_SERVER_DB").trim(),
    port: Number.parseInt(getEnv("SQL_SERVER_PORT") || "1433", 10),
    options: {
      encrypt: encryptRaw ? encryptRaw !== "false" : true,
      trustServerCertificate: trustRaw ? trustRaw === "true" : false,
    },
    pool: {
      max: 5,
      min: 0,
      idleTimeoutMillis: 30_000,
    },
  };
};

const getPool = async () => {
  if (!poolPromise) {
    const cfg = getSqlConfig();
    if (!cfg.server || !cfg.user || !cfg.password || !cfg.database) {
      throw new Error("Faltan variables de entorno SQL_SERVER_* para conectar a la base.");
    }
    poolPromise = sql.connect(cfg).catch((err) => {
      poolPromise = null;
      throw err;
    });
  }
  return poolPromise;
};

const ensureSchedulerConfigTable = async () => {
  const pool = await getPool();
  await pool
    .request()
    .query(`IF OBJECT_ID('dbo.CarteraSchedulerConfig','U') IS NULL
BEGIN
  CREATE TABLE dbo.CarteraSchedulerConfig(
    EmpresaId INT NOT NULL PRIMARY KEY,
    ApiUrl NVARCHAR(2048) NOT NULL,
    ApiToken NVARCHAR(512) NOT NULL,
    ScheduleEnabled BIT NOT NULL,
    ScheduleTime CHAR(5) NOT NULL,
    LcFromNumber NVARCHAR(32) NULL,
    LcUseTemplate BIT NOT NULL,
    LcAuto BIT NOT NULL,
    LcMax INT NOT NULL,
    LastRunDay CHAR(10) NULL,
    LastRunAt DATETIME2(0) NULL,
    UpdatedAt DATETIME2(0) NOT NULL
  );
END`);
};

const pad2 = (n) => String(n).padStart(2, "0");

const getBogotaParts = (now = new Date()) => {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Bogota",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = fmt.formatToParts(now);
  const map = {};
  for (const p of parts) map[p.type] = p.value;
  const dayKey = `${map.year}-${map.month}-${map.day}`;
  const hhmm = `${map.hour}:${map.minute}`;
  return { dayKey, hhmm, hour: Number(map.hour), minute: Number(map.minute) };
};

const parseTimeMinutes = (time) => {
  const s = String(time || "").trim();
  const m = s.match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return null;
  const hh = Number.parseInt(m[1], 10);
  const mm = Number.parseInt(m[2], 10);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  if (hh < 0 || hh > 23) return null;
  if (mm < 0 || mm > 59) return null;
  return hh * 60 + mm;
};

const shouldRunNow = ({ nowMin, scheduleMin, windowMin }) => {
  if (nowMin < scheduleMin) return false;
  if (nowMin >= scheduleMin + windowMin) return false;
  return true;
};

const pickString = (obj, keys) => {
  for (const k of keys) {
    const v = obj?.[k];
    if (v === null || v === undefined) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return "";
};

const parseNumber = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;

  let s = String(value).trim();
  if (!s) return null;
  s = s.replace(/\s+/g, "");
  s = s.replace(/[^\d.,-]/g, "");

  const hasDot = s.includes(".");
  const hasComma = s.includes(",");

  if (hasDot && hasComma) {
    s = s.replace(/\./g, "");
    s = s.replace(/,/g, ".");
  } else if (hasComma && !hasDot) {
    s = s.replace(/,/g, ".");
  } else if (hasDot && !hasComma) {
    const parts = s.split(".");
    const last = parts[parts.length - 1] || "";
    if (parts.length >= 2 && last.length === 3) {
      s = parts.join("");
    } else if (parts.length > 2) {
      const decimal = parts.pop();
      const integer = parts.join("");
      s = `${integer}.${decimal}`;
    }
  }

  const n = Number.parseFloat(s);
  return Number.isFinite(n) ? n : null;
};

const pickNumber = (obj, keys) => {
  for (const k of keys) {
    const v = obj?.[k];
    if (v === null || v === undefined) continue;
    const n = parseNumber(v);
    if (n !== null) return n;
  }
  return null;
};

const normalizePhone = (raw) => {
  let s = String(raw || "").trim();
  if (!s) return "";
  s = s.replace(/[^\d+]/g, "");
  if (s.startsWith("00")) s = `+${s.slice(2)}`;
  if (!s.startsWith("+")) {
    if (s.length === 10) s = `+57${s}`;
    else if (s.length === 12 && s.startsWith("57")) s = `+${s}`;
  }
  if (!/^\+\d{8,15}$/.test(s)) return "";
  return s;
};

const getPhone = (it) => {
  const raw =
    pickString(it, ["telefono", "Telefono", "tel", "Tel", "celular", "Celular", "movil", "Movil", "whatsapp", "WhatsApp", "phone", "Phone"]) ||
    "";
  return normalizePhone(raw);
};

const getEmail = (it) => {
  const email = pickString(it, ["email", "Email", "correo", "Correo", "mail", "Mail"]);
  return email || "";
};

const getDias = (item) => {
  const n = pickNumber(item, ["dias", "Dias", "DIAS", "dias_mora", "DiasMora"]);
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

  const porVencer = pickNumber(item, ["PorVencer", "porvencer", "Por_Vencer", "por_vencer"]) ?? 0;
  const treinta = pickNumber(item, ["Treinta_Dias", "treinta_dias", "TreintaDias", "treintadias"]) ?? 0;
  const sesenta = pickNumber(item, ["sesenta_Dias", "Sesenta_Dias", "sesenta_dias", "SesentaDias", "sesentadias"]) ?? 0;
  const noventa = pickNumber(item, ["noventa_Dias", "Noventa_Dias", "noventa_dias", "NoventaDias", "noventadias"]) ?? 0;
  const masNoventa =
    pickNumber(item, ["Mas_de_Noventa", "mas_de_noventa", "MasDeNoventa", "Mas_De_Noventa", "masdenoventa"]) ?? 0;
  return porVencer + treinta + sesenta + noventa + masNoventa;
};

const formatCOP = (value) => {
  const n = Number.isFinite(Number(value)) ? Number(value) : 0;
  return new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(n);
};

const renderTemplate = (tpl, vars) => {
  return String(tpl || "").replace(/\{(\w+)\}/g, (_, key) => {
    return vars && Object.prototype.hasOwnProperty.call(vars, key) ? String(vars[key] ?? "") : "";
  });
};

const MSG_TPL_POR_VENCER = "Hola {nombre}, te recordamos que tienes un vencimiento el {vencimiento} por {saldo} (Factura {factura}).";
const MSG_TPL_COBRO = "Hola {nombre}, tu factura {factura} venció el {vencimiento} hace {dias} días. Saldo pendiente: {saldo}.";

const buildTemplatePayload = ({ vars, mode }) => {
  const name = mode === "porVencer" ? "por_vencer" : "cobro";
  const lang = "es";
  const body = [
    vars?.nombre || "",
    vars?.factura || "",
    vars?.vencimiento || "",
    mode === "porVencer" ? vars?.saldo || "" : vars?.dias || "",
    mode === "porVencer" ? "" : vars?.saldo || "",
  ];

  if (name === "por_vencer") {
    body.splice(4);
    while (body.length < 4) body.push("");
  } else if (name === "cobro") {
    body.splice(5);
    while (body.length < 5) body.push("");
  }

  return {
    MessageType: 19,
    whatsapp: {
      type: "template",
      template: { name, lang },
      placeholders: { header: [], body, buttons: [] },
      components: [
        {
          type: "BODY",
          parameters: body.map((text) => ({ type: "text", text: String(text ?? "") })),
        },
      ],
    },
  };
};

const buildMessages = ({ items, mode, max, useTemplate }) => {
  const wanted = [];
  const seen = new Set();
  for (const it of items) {
    const dias = getDias(it);
    if (dias === null) continue;
    if (mode === "porVencer") {
      if (dias > 0) continue;
      if (Math.abs(dias) > 30) continue;
    } else {
      if (dias <= 0) continue;
    }

    const phone = getPhone(it);
    if (!phone) continue;
    if (seen.has(phone)) continue;
    seen.add(phone);

    const name = pickString(it, ["cliente", "Cliente", "CLIENTE"]) || "Cliente";
    const email = getEmail(it);
    const factura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "";
    const vencimiento = pickString(it, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"]) || "";
    const saldo = getMonto(it);
    const tpl = mode === "porVencer" ? MSG_TPL_POR_VENCER : MSG_TPL_COBRO;
    const vars = { nombre: name, factura, vencimiento, dias: String(Math.abs(dias)), saldo: formatCOP(saldo), saldoNumero: saldo };
    const msg = renderTemplate(tpl, vars).trim();

    const payload = { name, toNumber: phone, message: msg };
    if (email) payload.email = email;
    if (useTemplate) {
      payload.message = "Mensaje";
      Object.assign(payload, buildTemplatePayload({ vars, mode }));
    }

    wanted.push(payload);
    if (wanted.length >= max) break;
  }
  return wanted;
};

export async function handler(event) {
  const headers = { "content-type": "application/json; charset=utf-8" };
  const startedAt = Date.now();
  try {
    await ensureSchedulerConfigTable();
    const pool = await getPool();
    const res = await pool.request().query(
      "SELECT EmpresaId, ApiUrl, ApiToken, ScheduleEnabled, ScheduleTime, LcFromNumber, LcUseTemplate, LcAuto, LcMax, LastRunDay FROM dbo.CarteraSchedulerConfig WITH (NOLOCK) WHERE ScheduleEnabled = 1"
    );
    const rows = Array.isArray(res?.recordset) ? res.recordset : [];
    if (!rows.length) {
      return { statusCode: 200, headers, body: JSON.stringify({ ok: true, ran: 0 }) };
    }

    const bogota = getBogotaParts(new Date());
    const nowMin = bogota.hour * 60 + bogota.minute;
    const nowStamp = `${bogota.dayKey} ${pad2(bogota.hour)}:${pad2(bogota.minute)}:00`;
    let ran = 0;
    const results = [];

    for (const row of rows) {
      const empresaId = Number.parseInt(row.EmpresaId, 10);
      const scheduleTime = String(row.ScheduleTime || "").trim();
      const scheduleMin = parseTimeMinutes(scheduleTime);
      if (scheduleMin === null) continue;
      if (!shouldRunNow({ nowMin, scheduleMin, windowMin: 5 })) continue;
      if (String(row.LastRunDay || "").trim() === bogota.dayKey) continue;

      const apiUrl = String(row.ApiUrl || "").trim();
      const apiToken = String(row.ApiToken || "").trim();
      if (!apiUrl || !apiToken) continue;

      const syncResp = await syncHandler({
        httpMethod: "POST",
        body: JSON.stringify({ apiUrl, token: apiToken, Identificacion: "", Fecha: bogota.dayKey }),
      });

      let syncOk = false;
      let syncCount = 0;
      let apiItems = [];
      try {
        const json = syncResp?.body ? JSON.parse(syncResp.body) : {};
        const arr = Array.isArray(json?.Resultado) ? json.Resultado : [];
        apiItems = arr;
        syncOk = Boolean(syncResp && Number(syncResp.statusCode) >= 200 && Number(syncResp.statusCode) < 300);
        syncCount = arr.length;
      } catch {
        syncOk = false;
      }

      if (syncOk && Boolean(row.LcAuto) && String(row.LcFromNumber || "").trim()) {
        const max = Math.max(1, Math.min(200, Number.isFinite(Number(row.LcMax)) ? Math.trunc(Number(row.LcMax)) : 50));
        const useTemplate = Boolean(row.LcUseTemplate);
        const fromNumber = String(row.LcFromNumber || "").trim();
        const messages = [
          ...buildMessages({ items: apiItems, mode: "porVencer", max, useTemplate }),
          ...buildMessages({ items: apiItems, mode: "cobro", max, useTemplate }),
        ];

        if (messages.length) {
          await leadconnectorHandler({
            httpMethod: "POST",
            body: JSON.stringify({
              action: "sendWhatsApp",
              fromNumber,
              versionMsg: "2021-04-15",
              max,
              messages,
            }),
          });
        }
      }

      const req = pool.request();
      req.input("EmpresaId", sql.Int, empresaId);
      req.input("LastRunDay", sql.Char(10), bogota.dayKey);
      req.input("LastRunAt", sql.DateTime2(0), nowStamp);
      await req.query("UPDATE dbo.CarteraSchedulerConfig SET LastRunDay=@LastRunDay, LastRunAt=@LastRunAt WHERE EmpresaId=@EmpresaId");

      ran += 1;
      results.push({ empresaId, ok: syncOk, count: syncCount });
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ ok: true, ran, results, elapsedMs: Date.now() - startedAt, scheduledTime: event?.scheduledTime || null }),
    };
  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(err?.message || err || "Error") }) };
  }
}

