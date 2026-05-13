/**
 * StreamFlow — Recommendation Service
 *
 * Gerencia perfis de preferência e histórico de visualizações.
 * Gera recomendações simples baseadas em gêneros assistidos.
 * Banco de dados próprio (recommendation.db).
 */

const Fastify = require("fastify");
const Database = require("better-sqlite3");
const { ServiceBroker } = require("moleculer");

const app = Fastify({ logger: true });

const broker = new ServiceBroker({
  logger: console,
  transporter: "TCP",
});

const PORT = process.env.PORT || 3004;
const DB_PATH = process.env.DB_PATH || "./data/recommendation.db";

// ── Banco de dados ──────────────────────────────────────────
const db = new Database(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS view_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    movie_id INTEGER NOT NULL,
    viewed_at TEXT DEFAULT (datetime('now'))
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS user_preferences (
    user_id TEXT PRIMARY KEY,
    preferred_genres TEXT DEFAULT '[]',
    updated_at TEXT DEFAULT (datetime('now'))
  )
`);

// Seed de preferências
const prefCount = db
  .prepare("SELECT COUNT(*) as total FROM user_preferences")
  .get();
if (prefCount.total === 0) {
  const insert = db.prepare(
    "INSERT INTO user_preferences (user_id, preferred_genres) VALUES (?, ?)",
  );
  insert.run("user_1", JSON.stringify(["Drama", "Ação", "Ficção Científica"]));
  insert.run("user_2", JSON.stringify(["Ação", "Thriller"]));
  insert.run("user_3", JSON.stringify(["Drama", "Mistério", "Biografia"]));
  console.log("Seed: preferências de 3 usuários criadas");
}

// ── Rotas ───────────────────────────────────────────────────

// GET /recommendations/:userId — recomendações para o usuário
app.get("/recommendations/:userId", async (request, reply) => {
  const { userId } = request.params;

  const prefs = db
    .prepare("SELECT * FROM user_preferences WHERE user_id = ?")
    .get(userId);
  const history = db
    .prepare(
      "SELECT movie_id FROM view_history WHERE user_id = ? ORDER BY viewed_at DESC LIMIT 20",
    )
    .all(userId);

  return {
    userId,
    preferences: prefs ? JSON.parse(prefs.preferred_genres) : [],
    recentlyViewed: history.map((h) => h.movie_id),
    // Recomendação simplificada: retorna gêneros preferidos
    suggestedGenres: prefs
      ? JSON.parse(prefs.preferred_genres)
      : ["Drama", "Ação"],
  };
});

// POST /recommendations/viewed — registrar que o usuário assistiu algo
// (chamado sincronamente pelo streaming-service — parte da cadeia)
broker.createService({
  name: "viewed",

  events: {
    "playback.started": async (ctx) => {
      const { userId, movieId } = ctx.params || {};

      if (!userId || !movieId) {
        app.log.warn("userId e message são obrigatórios.");
        return;
      }

      db.prepare(
        "INSERT INTO view_history (user_id, movie_id) VALUES (?, ?)",
      ).run(userId, movieId);

      app.log.info("Recomendação enviada");

      return { registered: true, userId, movieId };
    },
  },
});

// GET /health
app.get("/health", async () => ({
  status: "ok",
  service: "recommendation-service",
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
  app.log.info(`recommendation-service rodando na porta ${PORT}`);
});
