import crypto from "crypto";
import sql from "mssql";

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
    return {
      statusCode: 502,
      headers,
      body: JSON.stringify({ Error: true, Mensaje: String(err?.message || err || "Error consultando API") }),
    };
  }

  const itemsRaw = Array.isArray(apiJson?.Resultado) ? apiJson.Resultado : [];
  const uniq = new Map();
  for (const it of itemsRaw) {
    const numFactura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "—";
    const iden = pickString(it, ["iden", "Identificacion", "identificacion"]) || "—";
    uniq.set(`${numFactura}||${iden}`, it);
  }
  const items = Array.from(uniq.values());

  const getSaldo = (it) => {
    return (
      pickNumber(it, ["saldo", "Saldo", "saldo_cuota", "Saldo_Cuota", "saldoCuota", "SaldoCuota", "SaldoFactura", "Saldo_Factura"]) ??
      null
    );
  };

  const getCuota = (it) => {
    return (
      pickNumber(it, ["cuota", "Cuota", "valor_cuota", "Valor_Cuota", "valorCuota", "ValorCuota", "valorcuota"]) ?? null
    );
  };

  const getMonto = (it) => {
    return pickNumber(it, ["monto", "Monto", "valor", "Valor", "monto_cuota", "Monto_Cuota"]) ?? null;
  };

  const totalMonto = items.reduce((acc, it) => {
    const saldo = getSaldo(it);
    const monto = getMonto(it);
    return acc + (saldo ?? monto ?? 0);
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
      table.columns.add("Vencimiento", sql.Date, { nullable: true });
      table.columns.add("Dias", sql.Int, { nullable: true });
      table.columns.add("Saldo", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("Cuota", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("Monto", sql.Decimal(18, 2), { nullable: false });
      table.columns.add("UpdatedAt", sql.DateTime2(0), { nullable: false });

      for (const it of items) {
        const numFactura = pickString(it, ["numfactura", "NumFactura", "numeFac", "NumeFac"]) || "—";
        const iden = pickString(it, ["iden", "Identificacion", "identificacion"]) || "—";
        const cliente = pickString(it, ["cliente", "Cliente", "CLIENTE"]) || null;
        const venc = parseDate(pickString(it, ["vencimiento_cuota", "Vencimiento_Cuota", "vencFac", "VencFac"])) || null;
        const diasRaw = pickString(it, ["dias", "Dias", "dias_mora", "DiasMora"]);
        const dias = diasRaw ? Number.parseInt(diasRaw, 10) : null;
        const saldo = getSaldo(it) ?? 0;
        const cuota = getCuota(it) ?? 0;
        const monto = getMonto(it) ?? 0;
        table.rows.add(
          empresaId,
          numFactura,
          iden,
          cliente,
          venc,
          Number.isFinite(dias) ? dias : null,
          saldo,
          cuota,
          monto,
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
  } catch (err) {
    const msg = String(err?.message || err || "Error SQL");
    const detalle = {
      name: err?.name || null,
      code: err?.code || null,
      number: err?.number || null,
    };
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
