import sql from "mssql";

let tokenDbPoolPromise = null;
let cachedLcToken = {
  value: "",
  fetchedAtMs: 0,
  expiresAtMs: 0,
};

const getEnv = (name) => {
  const v = process.env[name];
  return v === undefined ? "" : String(v);
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const getTokenDbConfig = () => {
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

const getTokenDbPool = async () => {
  if (!tokenDbPoolPromise) {
    const cfg = getTokenDbConfig();
    if (!cfg.server || !cfg.user || !cfg.password || !cfg.database) {
      throw new Error("Faltan variables de entorno WA_TOKEN_SQL_SERVER_* para leer el token WhatsApp.");
    }
    tokenDbPoolPromise = new sql.ConnectionPool(cfg).connect().catch((err) => {
      tokenDbPoolPromise = null;
      throw err;
    });
  }
  return tokenDbPoolPromise;
};

const stripOuterQuotes = (s) => {
  const v = String(s || "").trim();
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) return v.slice(1, -1).trim();
  return v;
};

const jwtLike = (raw) => {
  const s = stripOuterQuotes(raw);
  if (!s) return false;
  const tokenOnly = s.replace(/^bearer\s+/i, "").trim();
  const dotCount = (tokenOnly.match(/\./g) || []).length;
  if (dotCount !== 2) return false;
  return /^[A-Za-z0-9\-_]+=*\.[A-Za-z0-9\-_]+=*\.[A-Za-z0-9\-_+=/]+$/.test(tokenOnly);
};

const looksLikeToken = (raw) => jwtLike(raw);

const normalizeBearer = (raw) => {
  const s = stripOuterQuotes(String(raw || "").trim());
  if (!s) return "";
  const tokenOnly = stripOuterQuotes(s.replace(/^bearer\s+/i, "").trim());
  return `Bearer ${tokenOnly}`;
};

const computeExpiresAtMs = (fetchedAtMs) => {
  const d = new Date(fetchedAtMs);
  const nextMidnight = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1, 0, 0, 0, 0).getTime();
  const atMost24h = fetchedAtMs + 24 * 60 * 60 * 1000;
  return Math.min(atMost24h, nextMidnight + 10 * 60 * 1000);
};

const fetchTokenFromDbWithRetry = async ({ maxAttempts }) => {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const startedAt = Date.now();
    try {
      console.log(JSON.stringify({ scope: "wa_token", step: "fetch_start", attempt }));
      const pool = await getTokenDbPool();
      const res = await pool.request().query("select top 1 access_token from Empresas");
      const token = stripOuterQuotes(String(res?.recordset?.[0]?.access_token || "").trim());
      const elapsedMs = Date.now() - startedAt;

      if (!token) {
        console.log(JSON.stringify({ scope: "wa_token", step: "fetch_empty", attempt, elapsedMs }));
        throw new Error("Token no disponible (access_token vacío).");
      }
      if (!looksLikeToken(token)) {
        const tokenOnly = token.replace(/^bearer\s+/i, "").trim();
        const dotCount = (tokenOnly.match(/\./g) || []).length;
        console.log(
          JSON.stringify({ scope: "wa_token", step: "fetch_invalid_format", attempt, elapsedMs, length: token.length, dotCount })
        );
        throw new Error("Token con formato inválido.");
      }

      console.log(JSON.stringify({ scope: "wa_token", step: "fetch_ok", attempt, elapsedMs, length: token.length }));
      return token;
    } catch (err) {
      const elapsedMs = Date.now() - startedAt;
      console.log(
        JSON.stringify({
          scope: "wa_token",
          step: "fetch_error",
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

const getCachedOrFetchLcToken = async ({ force }) => {
  const now = Date.now();
  if (!force && cachedLcToken.value && cachedLcToken.expiresAtMs > now) {
    console.log(JSON.stringify({ scope: "wa_token", step: "cache_hit", expiresInSec: Math.round((cachedLcToken.expiresAtMs - now) / 1000) }));
    return cachedLcToken.value;
  }

  console.log(JSON.stringify({ scope: "wa_token", step: force ? "cache_refresh" : "cache_miss" }));
  const token = await fetchTokenFromDbWithRetry({ maxAttempts: 3 });
  const fetchedAtMs = Date.now();
  cachedLcToken = {
    value: token,
    fetchedAtMs,
    expiresAtMs: computeExpiresAtMs(fetchedAtMs),
  };
  console.log(
    JSON.stringify({
      scope: "wa_token",
      step: "cache_set",
      expiresInSec: Math.round((cachedLcToken.expiresAtMs - fetchedAtMs) / 1000),
    })
  );
  return token;
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

  const tokenRaw = String(body.token || "").trim();
  const locationId = String(body.locationId || "").trim();
  const action = String(body.action || "upsertContacts").trim() || "upsertContacts";
  const max = Number.isFinite(Number(body.max)) ? Math.trunc(Number(body.max)) : 50;

  let token = "";
  try {
    if (tokenRaw) {
      if (!looksLikeToken(tokenRaw)) {
        return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "token con formato inválido." }) };
      }
      token = normalizeBearer(tokenRaw);
    } else {
      const dbToken = await getCachedOrFetchLcToken({ force: false });
      token = normalizeBearer(dbToken);
    }
  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ Error: true, Mensaje: String(err?.message || err || "Error leyendo token WhatsApp") }) };
  }

  const limit = Math.max(1, Math.min(200, max));

  const pickContactId = (json) => {
    const v =
      json?.contact?.id ??
      json?.contact?.contactId ??
      json?.contactId ??
      json?.id ??
      json?._id ??
      null;
    return v ? String(v) : "";
  };

  const postJson = async ({ url, version, payload }) => {
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/json",
        Version: version,
        Authorization: token,
      },
      body: JSON.stringify(payload),
    });
    const text = await resp.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    return { ok: resp.ok, status: resp.status, text, json };
  };

  const getJson = async ({ url, version }) => {
    const resp = await fetch(url, {
      method: "GET",
      headers: {
        accept: "application/json",
        Version: version,
        Authorization: token,
      },
    });
    const text = await resp.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    return { ok: resp.ok, status: resp.status, text, json };
  };

  if (action === "refreshToken") {
    try {
      await getCachedOrFetchLcToken({ force: true });
      return { statusCode: 200, headers, body: JSON.stringify({ ok: true }) };
    } catch (err) {
      return {
        statusCode: 500,
        headers,
        body: JSON.stringify({ Error: true, Mensaje: String(err?.message || err || "Error refrescando token") }),
      };
    }
  }

  if (action === "upsertContacts") {
    const version = String(body.version || "2021-07-28").trim() || "2021-07-28";
    const contacts = Array.isArray(body.contacts) ? body.contacts : [];
    if (!locationId) {
      return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "locationId es obligatorio." }) };
    }
    if (contacts.length === 0) {
      return { statusCode: 200, headers, body: JSON.stringify({ ok: true, sent: 0, failed: 0, results: [] }) };
    }

    const slice = contacts.slice(0, limit);
    const results = [];
    let sent = 0;
    let failed = 0;

    for (const c of slice) {
      const payload = {
        locationId,
        ...(c && typeof c === "object" ? c : {}),
      };

      try {
        const r = await postJson({
          url: "https://services.leadconnectorhq.com/contacts/upsert",
          version,
          payload,
        });

        if (!r.ok) {
          failed += 1;
          results.push({ ok: false, status: r.status, body: r.text });
          continue;
        }

        sent += 1;
        results.push({ ok: true, status: r.status, body: r.text, contactId: pickContactId(r.json) });
      } catch (err) {
        failed += 1;
        results.push({ ok: false, status: 0, body: String(err?.message || err || "Error enviando") });
      }
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ ok: failed === 0, sent, failed, results, truncated: contacts.length > slice.length }),
    };
  }

  if (action === "sendWhatsApp") {
    const versionUpsert = "2021-07-28";
    const versionMsg = String(body.versionMsg || "2021-04-15").trim() || "2021-04-15";
    const fromNumber = String(body.fromNumber || "").trim();
    const messages = Array.isArray(body.messages) ? body.messages : [];

    if (!locationId) {
      return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "locationId es obligatorio." }) };
    }
    if (!fromNumber) {
      return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "fromNumber es obligatorio." }) };
    }
    if (messages.length === 0) {
      return { statusCode: 200, headers, body: JSON.stringify({ ok: true, sent: 0, failed: 0, results: [] }) };
    }

    const slice = messages.slice(0, limit);
    const results = [];
    let sent = 0;
    let failed = 0;

    for (const m of slice) {
      const name = String(m?.name || "").trim() || "Cliente";
      const email = String(m?.email || "").trim();
      const toNumber = String(m?.toNumber || "").trim();
      const message = String(m?.message || "").trim();
      const tags = Array.isArray(m?.tags) ? m.tags : [];

      if (!toNumber || !message) {
        failed += 1;
        results.push({ ok: false, status: 0, body: "toNumber y message son obligatorios." });
        continue;
      }

      try {
        const upsertPayload = { name, phone: toNumber, locationId };
        if (email) upsertPayload.email = email;
        if (tags.length) upsertPayload.tags = tags;

        const upsertRes = await postJson({
          url: "https://services.leadconnectorhq.com/contacts/upsert",
          version: versionUpsert,
          payload: upsertPayload,
        });

        if (!upsertRes.ok) {
          failed += 1;
          results.push({ ok: false, status: upsertRes.status, body: upsertRes.text, step: "upsert" });
          continue;
        }

        const contactId = pickContactId(upsertRes.json);
        if (!contactId) {
          failed += 1;
          results.push({ ok: false, status: 0, body: upsertRes.text, step: "upsert_no_contactId" });
          continue;
        }

        const msgPayload = {
          type: "WhatsApp",
          contactId,
          message,
          fromNumber,
          toNumber,
        };
        const messageTypeRaw = m?.MessageType ?? m?.messageType ?? null;
        const messageTypeNum = Number.isFinite(Number(messageTypeRaw)) ? Math.trunc(Number(messageTypeRaw)) : null;
        if (messageTypeNum !== null) msgPayload.MessageType = messageTypeNum;
        if (m?.whatsapp && typeof m.whatsapp === "object") msgPayload.whatsapp = m.whatsapp;

        const msgRes = await postJson({
          url: "https://services.leadconnectorhq.com/conversations/messages",
          version: versionMsg,
          payload: msgPayload,
        });

        if (!msgRes.ok) {
          failed += 1;
          results.push({
            ok: false,
            status: msgRes.status,
            body: msgRes.text,
            step: "message",
            contactId,
          });
          continue;
        }

        sent += 1;
        results.push({
          ok: true,
          status: msgRes.status,
          body: msgRes.text,
          contactId,
          toNumber,
          fromNumber,
        });
      } catch (err) {
        failed += 1;
        results.push({ ok: false, status: 0, body: String(err?.message || err || "Error enviando") });
      }
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ ok: failed === 0, sent, failed, results, truncated: messages.length > slice.length }),
    };
  }

  if (action === "getConversationMessages") {
    const versionMsg = String(body.versionMsg || "2021-04-15").trim() || "2021-04-15";
    const conversationId = String(body.conversationId || "").trim();
    const msgLimit = Math.max(1, Math.min(100, Number.isFinite(Number(body.limit)) ? Math.trunc(Number(body.limit)) : 20));

    if (!conversationId) {
      return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "conversationId es obligatorio." }) };
    }

    const url = `https://services.leadconnectorhq.com/conversations/${encodeURIComponent(conversationId)}/messages?limit=${msgLimit}`;
    try {
      const r = await getJson({ url, version: versionMsg });
      return { statusCode: r.ok ? 200 : r.status || 500, headers, body: r.text || JSON.stringify(r.json || {}) };
    } catch (err) {
      return { statusCode: 500, headers, body: JSON.stringify({ Error: true, Mensaje: String(err?.message || err || "Error") }) };
    }
  }

  return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "action inválida." }) };
}
