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
      max: 2,
      min: 0,
      idleTimeoutMillis: 10_000,
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

export async function handler(event) {
  const headers = {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type, accept",
    "access-control-allow-methods": "GET, OPTIONS",
  };

  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers, body: "" };
  if (event.httpMethod !== "GET") {
    return { statusCode: 405, headers, body: JSON.stringify({ Error: true, Mensaje: "Método no permitido." }) };
  }

  try {
    const pool = await getPool();
    const info = await pool
      .request()
      .query("SELECT DB_NAME() AS DbName, @@SERVERNAME AS ServerName, SYSTEM_USER AS SystemUser, @@VERSION AS Version;");

    const tables = await pool.request().query(`
      SELECT
        OBJECT_ID('dbo.Empresa') AS Empresa,
        OBJECT_ID('dbo.CarteraLatestMeta') AS CarteraLatestMeta,
        OBJECT_ID('dbo.CarteraFacturaLatest') AS CarteraFacturaLatest
    `);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        ok: true,
        db: info.recordset?.[0]?.DbName || null,
        server: info.recordset?.[0]?.ServerName || null,
        user: info.recordset?.[0]?.SystemUser || null,
        tables: tables.recordset?.[0] || {},
      }),
    };
  } catch (err) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        Error: true,
        Mensaje: String(err?.message || err || "Error conectando a SQL"),
        Detalle: {
          name: err?.name || null,
          code: err?.code || null,
          number: err?.number || null,
        },
      }),
    };
  }
}

