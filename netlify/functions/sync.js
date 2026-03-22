import crypto from "crypto";
import sql from "mssql";

let poolPromise = null;
let waTokenPoolPromise = null;
let cachedWaToken = {
  value: "",
  fetchedAtMs: 0,
  expiresAtMs: 0,
};

const getEnv = (name) => {
  const v = process.env[name];
  return v === undefined ? "" : String(v);
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

const stripOuterQuotes = (s) => {
  const v = String(s || "").trim();
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) return v.slice(1, -1).trim();
  return v;
};

const looksLikeToken = (raw) => {
  const s = stripOuterQuotes(raw);
  if (!s) return false;
  const tokenOnly = s.replace(/^bearer\s+/i, "").trim();
  const dotCount = (tokenOnly.match(/\./g) || []).length;
  if (dotCount !== 2) return false;
  return /^[A-Za-z0-9\-_]+=*\.[A-Za-z0-9\-_]+=*\.[A-Za-z0-9\-_+=/]+$/.test(tokenOnly);
};

const computeExpiresAtMs = (fetchedAtMs) => {
  const d = new Date(fetchedAtMs);
  const nextMidnight = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1, 0, 0, 0, 0).getTime();
  const atMost24h = fetchedAtMs + 24 * 60 * 60 * 1000;
  return Math.min(atMost24h, nextMidnight + 10 * 60 * 1000);
};

const getWaTokenSqlConfig = () => {
  const encryptRaw = getEnv("WA_TOKEN_SQL_SERVER_ENCRYPT").trim().toLowerCase();
  const trustRaw = getEnv("WA_TOKEN_SQL_SERVER_TRUST_CERT").trim().toLowerCase();
  return {
    server: getEnv("WA_TOKEN_SQL_SERVER_HOST").trim(),
    user: getEnv("WA_TOKEN_SQL_SERVER_USER").trim(),
    password: getEnv("WA_TOKEN_SQL_SERVER_PASSWORD"),
    database: getEnv("WA_TOKEN_SQL_SERVER_DB").trim(),
    port: Number.parseInt(getEnv("WA_TOKEN_SQL_SERVER_PORT") || "1433", 10),
    options: {
      encrypt: encryptRaw ? encryptRaw !== "false" : true,
      trustServerCertificate: trustRaw ? trustRaw === "true" : false,
    },
    pool: {
      max: 2,
      min: 0,
      idleTimeoutMillis: 30_000,
    },
  };
};

const getWaTokenPool = async () => {
  if (!waTokenPoolPromise) {
    const cfg = getWaTokenSqlConfig();
    if (!cfg.server || !cfg.user || !cfg.password || !cfg.database) {
      throw new Error("Faltan variables de entorno WA_TOKEN_SQL_SERVER_* para leer el token WhatsApp.");
    }
    waTokenPoolPromise = new sql.ConnectionPool(cfg).connect().catch((err) => {
      waTokenPoolPromise = null;
      throw err;
    });
  }
  return waTokenPoolPromise;
};

const fetchWhatsAppTokenWithRetry = async ({ maxAttempts }) => {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const startedAt = Date.now();
    try {
      console.log(JSON.stringify({ scope: "sync", step: "wa_token_fetch_start", attempt }));
      const pool = await getWaTokenPool();
      const res = await pool.request().query("select top 1 access_token from Empresas");
      const token = stripOuterQuotes(String(res?.recordset?.[0]?.access_token || "").trim());
      const elapsedMs = Date.now() - startedAt;

      if (!token) {
        console.log(JSON.stringify({ scope: "sync", step: "wa_token_fetch_empty", attempt, elapsedMs }));
        throw new Error("Token no disponible (access_token vacío).");
      }
      if (!looksLikeToken(token)) {
        const tokenOnly = token.replace(/^bearer\s+/i, "").trim();
        const dotCount = (tokenOnly.match(/\./g) || []).length;
        console.log(
          JSON.stringify({ scope: "sync", step: "wa_token_fetch_invalid_format", attempt, elapsedMs, length: token.length, dotCount })
        );
        throw new Error("Token con formato inválido.");
      }

      console.log(JSON.stringify({ scope: "sync", step: "wa_token_fetch_ok", attempt, elapsedMs, length: token.length }));
      return token;
    } catch (err) {
      const elapsedMs = Date.now() - startedAt;
      console.log(
        JSON.stringify({
          scope: "sync",
          step: "wa_token_fetch_error",
          attempt,
          elapsedMs,
          name: err?.name || null,
          code: err?.code || null,
          message: String(err?.message || err || "Error"),
        })
      );
      if (attempt >= maxAttempts) throw err;
      await sleep(250 * attempt);
    }
  }
  throw new Error("No se pudo obtener el token.");
};

const ensureWhatsAppTokenFresh = async ({ force }) => {
  const now = Date.now();
  if (!force && cachedWaToken.value && cachedWaToken.expiresAtMs > now) {
    console.log(JSON.stringify({ scope: "sync", step: "wa_token_cache_hit", expiresInSec: Math.round((cachedWaToken.expiresAtMs - now) / 1000) }));
    return cachedWaToken.value;
  }

  console.log(JSON.stringify({ scope: "sync", step: force ? "wa_token_cache_refresh" : "wa_token_cache_miss" }));
  const token = await fetchWhatsAppTokenWithRetry({ maxAttempts: 3 });
  const fetchedAtMs = Date.now();
  cachedWaToken = {
    value: token,
    fetchedAtMs,
    expiresAtMs: computeExpiresAtMs(fetchedAtMs),
  };
  console.log(JSON.stringify({ scope: "sync", step: "wa_token_cache_set", expiresInSec: Math.round((cachedWaToken.expiresAtMs - fetchedAtMs) / 1000) }));
  return token;
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

const parseDate = (value) => {
  const s = String(value || "").trim();
  if (!s) return null;
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  const d = new Date(s);
  if (!Number.isFinite(d.getTime())) return null;
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
};

const parseDateTime2 = (value) => {
  const s = String(value || "").trim();
  if (!s) return null;
  const d = new Date(s);
  if (!Number.isFinite(d.getTime())) return null;
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}`;
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

const getOrCreateEmpresaId = async ({ tokenHash }) => {
  const pool = await getPool();
  const tx = new sql.Transaction(pool);
  await tx.begin();
  try {
    const reqEmpresa = new sql.Request(tx);
    reqEmpresa.input("TokenHash", sql.Char(64), tokenHash);
    const empresaRes = await reqEmpresa.query(
      "SELECT EmpresaId FROM dbo.Empresa WITH (UPDLOCK, HOLDLOCK) WHERE TokenHash = @TokenHash"
    );
    if (empresaRes.recordset.length) {
      await tx.commit();
      return Number.parseInt(empresaRes.recordset[0].EmpresaId, 10);
    }
    const ins = await reqEmpresa.query("INSERT INTO dbo.Empresa(TokenHash) VALUES (@TokenHash); SELECT SCOPE_IDENTITY() AS EmpresaId;");
    const empresaId = Number.parseInt(ins.recordset?.[0]?.EmpresaId, 10);
    await tx.commit();
    return empresaId;
  } catch (err) {
    try {
      await tx.rollback();
    } catch {}
    throw err;
  }
};

export async function handler(event) {
  const headers = {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type, accept",
    "access-control-allow-methods": "POST, OPTIONS",
  };

  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers, body: "" };
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers, body: JSON.stringify({ Error: true, Mensaje: "Método no permitido." }) };
  }

  let body = {};
  try {
    body = event.body ? JSON.parse(event.body) : {};
  } catch {
    return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "JSON inválido." }) };
  }

  const apiUrl = String(body.apiUrl || "").trim();
  const token = String(body.token || "").trim();
  const Identificacion = String(body.Identificacion || "");
  const Fecha = String(body.Fecha || "").trim();
  const action = String(body.action || "").trim();

  if (action === "saveSchedulerConfig") {
    if (!apiUrl || !token) {
      return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "apiUrl y token son obligatorios." }) };
    }

    let url;
    try {
      url = new URL(apiUrl);
    } catch {
      return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "apiUrl inválida." }) };
    }

    const scheduleEnabled = Boolean(body.scheduleEnabled);
    const scheduleTime = String(body.scheduleTime || "").trim() || "02:00";
    if (scheduleEnabled && parseTimeMinutes(scheduleTime) === null) {
      return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "Hora inválida." }) };
    }

    const lcFromNumber = String(body.lcFromNumber || "").trim();
    const lcUseTemplate = Boolean(body.lcUseTemplate);
    const lcAuto = Boolean(body.lcAuto);
    const rawMax = Number(body.lcMax);
    const lcMax = Number.isFinite(rawMax) ? Math.max(1, Math.min(200, Math.trunc(rawMax))) : 50;

    const tokenHash = crypto.createHash("sha256").update(token, "utf8").digest("hex");
    try {
      await ensureSchedulerConfigTable();
      const empresaId = await getOrCreateEmpresaId({ tokenHash });
      const pool = await getPool();
      const req = pool.request();
      req.input("EmpresaId", sql.Int, empresaId);
      req.input("ApiUrl", sql.NVarChar(2048), url.toString());
      req.input("ApiToken", sql.NVarChar(512), token);
      req.input("ScheduleEnabled", sql.Bit, scheduleEnabled ? 1 : 0);
      req.input("ScheduleTime", sql.Char(5), scheduleTime);
      req.input("LcFromNumber", sql.NVarChar(32), lcFromNumber || null);
      req.input("LcUseTemplate", sql.Bit, lcUseTemplate ? 1 : 0);
      req.input("LcAuto", sql.Bit, lcAuto ? 1 : 0);
      req.input("LcMax", sql.Int, lcMax);
      req.input("UpdatedAt", sql.DateTime2(0), new Date().toISOString().slice(0, 19).replace("T", " "));
      await req.query(`MERGE dbo.CarteraSchedulerConfig AS t
USING (SELECT @EmpresaId AS EmpresaId) AS s
ON (t.EmpresaId = s.EmpresaId)
WHEN MATCHED THEN
  UPDATE SET ApiUrl=@ApiUrl, ApiToken=@ApiToken, ScheduleEnabled=@ScheduleEnabled, ScheduleTime=@ScheduleTime,
    LcFromNumber=@LcFromNumber, LcUseTemplate=@LcUseTemplate, LcAuto=@LcAuto, LcMax=@LcMax, UpdatedAt=@UpdatedAt
WHEN NOT MATCHED THEN
  INSERT (EmpresaId, ApiUrl, ApiToken, ScheduleEnabled, ScheduleTime, LcFromNumber, LcUseTemplate, LcAuto, LcMax, UpdatedAt)
  VALUES (@EmpresaId, @ApiUrl, @ApiToken, @ScheduleEnabled, @ScheduleTime, @LcFromNumber, @LcUseTemplate, @LcAuto, @LcMax, @UpdatedAt);`);

      return { statusCode: 200, headers, body: JSON.stringify({ Ok: true }) };
    } catch (err) {
      return { statusCode: 500, headers, body: JSON.stringify({ Error: true, Mensaje: String(err?.message || err || "Error guardando configuración") }) };
    }
  }

  if (!apiUrl || !token || !Fecha) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ Error: true, Mensaje: "apiUrl, token y Fecha son obligatorios." }),
    };
  }

  let url;
  try {
    url = new URL(apiUrl);
  } catch {
    return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "apiUrl inválida." }) };
  }

  url.searchParams.set("token", token);

  const tokenHash = crypto.createHash("sha256").update(token, "utf8").digest("hex");
  const syncId = typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  console.log(JSON.stringify({ scope: "sync", step: "start", syncId, fecha: Fecha, hasIdentificacion: Boolean(String(Identificacion || "").trim()) }));

  try {
    await ensureWhatsAppTokenFresh({ force: false });
  } catch (err) {
    console.log(
      JSON.stringify({
        scope: "sync",
        step: "wa_token_refresh_failed",
        syncId,
        message: String(err?.message || err || "Error"),
      })
    );
  }

  let apiText = "";
  let apiJson = null;
  try {
    const resp = await fetch(url.toString(), {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify({ Identificacion, Fecha }),
    });

    apiText = await resp.text();
    if (!resp.ok) {
      return { statusCode: resp.status, headers, body: apiText || JSON.stringify({ Error: true, Mensaje: "Error API" }) };
    }

    apiJson = apiText ? JSON.parse(apiText) : {};
  } catch (err) {
    console.log(JSON.stringify({ scope: "sync", step: "api_error", syncId, message: String(err?.message || err || "Error") }));
    return {
      statusCode: 502,
      headers,
      body: JSON.stringify({ Error: true, Mensaje: String(err?.message || err || "Error consultando API") }),
    };
  }

  const itemsRaw = Array.isArray(apiJson?.Resultado) ? apiJson.Resultado : [];
  console.log(JSON.stringify({ scope: "sync", step: "api_ok", syncId, itemsRaw: itemsRaw.length }));
  const uniq = new Map();
  for (const it of itemsRaw) {
    const numFactura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "—";
    const iden = pickString(it, ["iden", "Identificacion", "identificacion"]) || "—";
    const vencKey = pickString(it, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"]) || "—";
    uniq.set(`${numFactura}||${iden}||${vencKey}`, it);
  }
  const items = Array.from(uniq.values());

  const getPorVencer = (it) => pickNumber(it, ["PorVencer", "porvencer", "Por_Vencer", "por_vencer"]) ?? 0;
  const getTreinta = (it) => pickNumber(it, ["Treinta_Dias", "treinta_dias", "TreintaDias", "treintadias"]) ?? 0;
  const getSesenta = (it) => pickNumber(it, ["sesenta_Dias", "Sesenta_Dias", "sesenta_dias", "SesentaDias", "sesentadias"]) ?? 0;
  const getNoventa = (it) => pickNumber(it, ["noventa_Dias", "Noventa_Dias", "noventa_dias", "NoventaDias", "noventadias"]) ?? 0;
  const getMasNoventa = (it) =>
    pickNumber(it, ["Mas_de_Noventa", "mas_de_noventa", "MasDeNoventa", "Mas_De_Noventa", "masdenoventa"]) ?? 0;

  const getSaldo = (it) => getPorVencer(it) + getTreinta(it) + getSesenta(it) + getNoventa(it) + getMasNoventa(it);

  const totalMonto = items.reduce((acc, it) => {
    return acc + getSaldo(it);
  }, 0);

  let empresaId = null;
  const now = new Date();
  const lastSyncAt = now.toISOString().slice(0, 19).replace("T", " ");

  try {
    const pool = await getPool();
    const tx = new sql.Transaction(pool);
    await tx.begin();

    const reqEmpresa = new sql.Request(tx);
    reqEmpresa.input("TokenHash", sql.Char(64), tokenHash);
    const empresaRes = await reqEmpresa.query(
      "SELECT EmpresaId FROM dbo.Empresa WITH (UPDLOCK, HOLDLOCK) WHERE TokenHash = @TokenHash"
    );

    if (empresaRes.recordset.length) {
      empresaId = empresaRes.recordset[0].EmpresaId;
    } else {
      const ins = await reqEmpresa.query("INSERT INTO dbo.Empresa(TokenHash) VALUES (@TokenHash); SELECT SCOPE_IDENTITY() AS EmpresaId;");
      empresaId = Number.parseInt(ins.recordset?.[0]?.EmpresaId, 10);
      const reqMetaInit = new sql.Request(tx);
      reqMetaInit.input("EmpresaId", sql.Int, empresaId);
      await reqMetaInit.query("INSERT INTO dbo.CarteraLatestMeta(EmpresaId, LastSyncOk) VALUES (@EmpresaId, 0);");
    }

    const reqDelete = new sql.Request(tx);
    reqDelete.input("EmpresaId", sql.Int, empresaId);
    await reqDelete.query("DELETE FROM dbo.CarteraFacturaLatest WHERE EmpresaId = @EmpresaId;");

    if (items.length) {
      const table = new sql.Table("dbo.CarteraFacturaLatest");
      table.create = false;
      table.columns.add("EmpresaId", sql.Int, { nullable: false });
      table.columns.add("NumFactura", sql.NVarChar(64), { nullable: false });
      table.columns.add("Identificacion", sql.NVarChar(64), { nullable: false });
      table.columns.add("Cliente", sql.NVarChar(256), { nullable: true });
      table.columns.add("FechaFac", sql.DateTime2(0), { nullable: true });
      table.columns.add("AnoMes", sql.NVarChar(20), { nullable: true });
      table.columns.add("Cuota", sql.Int, { nullable: true });
      table.columns.add("Vencimiento", sql.Date, { nullable: false });
      table.columns.add("Dias", sql.Int, { nullable: true });
      table.columns.add("PorVencer", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("Treinta_Dias", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("Sesenta_Dias", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("Noventa_Dias", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("Mas_de_Noventa", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("UpdatedAt", sql.DateTime2(0), { nullable: false });

      for (const it of items) {
        const numFactura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "—";
        const iden = pickString(it, ["iden", "Identificacion", "identificacion"]) || "—";
        const cliente = pickString(it, ["cliente", "Cliente", "CLIENTE"]) || null;
        const fechafac =
          parseDateTime2(pickString(it, ["fechafac", "FechaFac", "fecha_fac", "Fecha_Fac"])) || null;
        const anomes = pickString(it, ["anomes", "AnoMes", "ANO MES", "ano_mes", "Ano_Mes"]) || null;
        const cuota = pickNumber(it, ["cuota", "Cuota"]) ?? null;
        const venc = parseDate(pickString(it, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"])) || "1900-01-01";
        const diasRaw = pickString(it, ["dias", "Dias", "dias_mora", "DiasMora"]);
        const dias = diasRaw ? Number.parseInt(diasRaw, 10) : null;
        const porVencer = getPorVencer(it);
        const treinta = getTreinta(it);
        const sesenta = getSesenta(it);
        const noventa = getNoventa(it);
        const masNoventa = getMasNoventa(it);
        table.rows.add(
          empresaId,
          numFactura,
          iden,
          cliente,
          fechafac,
          anomes,
          Number.isFinite(cuota) ? Math.trunc(cuota) : null,
          venc,
          Number.isFinite(dias) ? dias : null,
          porVencer,
          treinta,
          sesenta,
          noventa,
          masNoventa,
          lastSyncAt
        );
      }

      const reqBulk = new sql.Request(tx);
      await reqBulk.bulk(table);
    }

    const reqMeta = new sql.Request(tx);
    reqMeta.input("EmpresaId", sql.Int, empresaId);
    reqMeta.input("LastSyncAt", sql.DateTime2(0), lastSyncAt);
    reqMeta.input("LastSyncOk", sql.Bit, 1);
    reqMeta.input("LastSyncCount", sql.Int, items.length);
    reqMeta.input("LastSyncTotalMonto", sql.Decimal(18, 2), totalMonto);
    reqMeta.input("LastError", sql.NVarChar(4000), null);
    reqMeta.input("LastPayloadJson", sql.NVarChar(sql.MAX), apiText || JSON.stringify(apiJson));
    await reqMeta.query(
      "UPDATE dbo.CarteraLatestMeta SET LastSyncAt=@LastSyncAt, LastSyncOk=@LastSyncOk, LastSyncCount=@LastSyncCount, LastSyncTotalMonto=@LastSyncTotalMonto, LastError=@LastError, LastPayloadJson=@LastPayloadJson WHERE EmpresaId=@EmpresaId"
    );

    await tx.commit();
    console.log(JSON.stringify({ scope: "sync", step: "sql_ok", syncId, empresaId, count: items.length }));
  } catch (err) {
    const msg = String(err?.message || err || "Error SQL");
    const detalle = {
      name: err?.name || null,
      code: err?.code || null,
      number: err?.number || null,
    };
    console.log(JSON.stringify({ scope: "sync", step: "sql_error", syncId, message: msg, detalle }));
    try {
      const pool = await getPool();
      const req = pool.request();
      req.input("TokenHash", sql.Char(64), tokenHash);
      const empresaRes = await req.query("SELECT EmpresaId FROM dbo.Empresa WHERE TokenHash = @TokenHash");
      if (empresaRes.recordset.length) {
        empresaId = empresaRes.recordset[0].EmpresaId;
        const reqMeta = pool.request();
        reqMeta.input("EmpresaId", sql.Int, empresaId);
        reqMeta.input("LastSyncAt", sql.DateTime2(0), lastSyncAt);
        reqMeta.input("LastSyncOk", sql.Bit, 0);
        reqMeta.input("LastError", sql.NVarChar(4000), msg);
        await reqMeta.query(
          "UPDATE dbo.CarteraLatestMeta SET LastSyncAt=@LastSyncAt, LastSyncOk=@LastSyncOk, LastError=@LastError WHERE EmpresaId=@EmpresaId"
        );
      }
    } catch {}

    return { statusCode: 500, headers, body: JSON.stringify({ Error: true, Mensaje: msg, Detalle: detalle }) };
  }

  const persisted = {
    ok: true,
    empresaId,
    count: items.length,
    totalMonto,
    syncedAt: lastSyncAt,
  };

  if (apiJson && typeof apiJson === "object" && !Array.isArray(apiJson)) {
    return { statusCode: 200, headers, body: JSON.stringify({ ...apiJson, _persist: persisted }) };
  }

  return { statusCode: 200, headers, body: apiText || JSON.stringify({ Resultado: itemsRaw, _persist: persisted }) };
}
