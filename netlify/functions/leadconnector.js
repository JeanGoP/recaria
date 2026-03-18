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
  const version = String(body.version || "2021-07-28").trim() || "2021-07-28";
  const contacts = Array.isArray(body.contacts) ? body.contacts : [];
  const max = Number.isFinite(Number(body.max)) ? Math.trunc(Number(body.max)) : 50;

  if (!tokenRaw || !locationId) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ Error: true, Mensaje: "token y locationId son obligatorios." }),
    };
  }

  if (contacts.length === 0) {
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, sent: 0, failed: 0, results: [] }) };
  }

  const token = /^bearer\s+/i.test(tokenRaw) ? tokenRaw : `Bearer ${tokenRaw}`;
  const url = "https://services.leadconnectorhq.com/contacts/upsert";

  const limit = Math.max(1, Math.min(200, max));
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
      if (!resp.ok) {
        failed += 1;
        results.push({ ok: false, status: resp.status, body: text });
        continue;
      }

      sent += 1;
      results.push({ ok: true, status: resp.status, body: text });
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

