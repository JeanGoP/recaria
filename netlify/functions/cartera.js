export async function handler(event) {
  const baseHeaders = {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type, accept",
    "access-control-allow-methods": "POST, OPTIONS",
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: baseHeaders, body: "" };
  }

  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers: baseHeaders,
      body: JSON.stringify({ Error: true, Mensaje: "Método no permitido." }),
    };
  }

  let body = {};
  try {
    body = event.body ? JSON.parse(event.body) : {};
  } catch {
    return {
      statusCode: 400,
      headers: baseHeaders,
      body: JSON.stringify({ Error: true, Mensaje: "JSON inválido." }),
    };
  }

  const apiUrl = String(body.apiUrl || "").trim();
  const token = String(body.token || "").trim();
  const Identificacion = String(body.Identificacion || "");
  const Fecha = String(body.Fecha || "").trim();

  if (!apiUrl || !token || !Fecha) {
    return {
      statusCode: 400,
      headers: baseHeaders,
      body: JSON.stringify({ Error: true, Mensaje: "apiUrl, token y Fecha son obligatorios." }),
    };
  }

  let url;
  try {
    url = new URL(apiUrl);
  } catch {
    return {
      statusCode: 400,
      headers: baseHeaders,
      body: JSON.stringify({ Error: true, Mensaje: "apiUrl inválida." }),
    };
  }

  url.searchParams.set("token", token);

  try {
    const resp = await fetch(url.toString(), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/json",
      },
      body: JSON.stringify({ Identificacion, Fecha }),
    });

    const text = await resp.text();
    return {
      statusCode: resp.status,
      headers: baseHeaders,
      body: text,
    };
  } catch (err) {
    return {
      statusCode: 502,
      headers: baseHeaders,
      body: JSON.stringify({ Error: true, Mensaje: String(err?.message || err || "Error de red") }),
    };
  }
}
