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

  if (!tokenRaw) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ Error: true, Mensaje: "token es obligatorio." }),
    };
  }

  const token = /^bearer\s+/i.test(tokenRaw) ? tokenRaw : `Bearer ${tokenRaw}`;

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
        results.push({ ok: true, status: msgRes.status, body: msgRes.text, contactId });
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

  return { statusCode: 400, headers, body: JSON.stringify({ Error: true, Mensaje: "action inválida." }) };
}
