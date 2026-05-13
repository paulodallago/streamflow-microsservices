/**
 * StreamFlow — Streaming Service
 *
 * Gerencia sessões de reprodução de conteúdo.
 *
 * ⚠️  ANTIPATTERN: Cadeia síncrona obrigatória
 *     Quando o usuário clica "Play", este serviço faz chamadas
 *     síncronas em cadeia:
 *       1. catalog-service → verificar licença
 *       2. recommendation-service → registrar visualização
 *       3. notification-service → notificar início de sessão
 *     Todas são bloqueantes — se qualquer uma falhar ou atrasar,
 *     o usuário fica esperando.
 */

const Fastify = require("fastify");
const Database = require("better-sqlite3");
const { ServiceBroker } = require("moleculer");

const app = Fastify({ logger: true });

const broker = new ServiceBroker({
  logger: console,
  transporter: "TCP",
});

const PORT = process.env.PORT || 3003;
const DB_PATH = process.env.DB_PATH || "./data/streaming.db";

const CATALOG_URL = process.env.CATALOG_SERVICE_URL || "http://localhost:3002";
const RECOMMENDATION_URL =
  process.env.RECOMMENDATION_SERVICE_URL || "http://localhost:3004";
const NOTIFICATION_URL =
  process.env.NOTIFICATION_SERVICE_URL || "http://localhost:3005";

// ── Banco de dados ──────────────────────────────────────────
const db = new Database(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    movie_id INTEGER NOT NULL,
    started_at TEXT DEFAULT (datetime('now')),
    duration_seconds INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active'
  )
`);

// ── Rotas ───────────────────────────────────────────────────

// POST /streaming/play — iniciar reprodução
// Este é o fluxo que demonstra a cadeia síncrona (antipattern)
app.post("/streaming/play", async (request, reply) => {
  const { movieId } = request.body || {};
  const userId = request.headers["x-user-id"] || "anonymous";

  if (!movieId) {
    return reply.code(400).send({ error: "movieId é obrigatório." });
  }

  app.log.info({ movieId, userId }, "Iniciando fluxo de play...");

  // ── PASSO 1: Verificar licença no catalog-service (SÍNCRONO) ──
  let licenseData;
  try {
    const licenseRes = await fetch(
      `${CATALOG_URL}/catalog/${movieId}/license`,
      {
        signal: AbortSignal.timeout(5000),
      },
    );
    if (!licenseRes.ok) {
      return reply
        .code(404)
        .send({ error: "Título não encontrado no catálogo." });
    }
    licenseData = await licenseRes.json();

    if (!licenseData.licensed) {
      return reply
        .code(403)
        .send({ error: "Licença expirada para este título." });
    }
  } catch (err) {
    app.log.error({ err }, "Falha ao verificar licença no catalog-service");
    return reply.code(503).send({ error: "Serviço de catálogo indisponível." });
  }

  // ── PASSO 2 & 3: Gera evento para registrar recomendação e notificar início da sessão - Assíncrono ──

  broker.emit("playback.started", {
    userId,
    type: "session_started",
    message: `Reprodução iniciada: ${licenseData.title}`,
    movieId,
  });

  app.log.info("Evento playback.started emitido");

  // ── PASSO 4: Criar sessão local ──
  const info = db
    .prepare("INSERT INTO sessions (user_id, movie_id) VALUES (?, ?)")
    .run(userId, movieId);

  return reply.code(201).send({
    sessionId: info.lastInsertRowid,
    movie: licenseData.title,
    status: "playing",
    message: "Reprodução iniciada.",
  });
});

// GET /streaming/sessions — listar sessões do usuário
app.get("/streaming/sessions", async (request) => {
  const userId = request.headers["x-user-id"] || "anonymous";
  return db
    .prepare(
      "SELECT * FROM sessions WHERE user_id = ? ORDER BY started_at DESC",
    )
    .all(userId);
});

// GET /health
app.get("/health", async () => ({
  status: "ok",
  service: "streaming-service",
  uptime: process.uptime(),
  timestamp: new Date().toISOString(),
}));

// ── Inicialização ───────────────────────────────────────────
app.listen({ port: PORT, host: "0.0.0.0" }, async (err) => {
  await broker.start();

  if (err) {
    app.log.error(err);
    process.exit(1);
  }
  app.log.info(`streaming-service rodando na porta ${PORT}`);
});
