/**
 * StreamFlow — Notification Service
 *
 * ⚠️  ANTIPATTERN: Microsserviço síncrono no caminho crítico
 *     Este serviço é chamado de forma síncrona pelo streaming-service
 *     durante o fluxo de "Play". Ele simula o envio de uma notificação
 *     (push, e-mail, etc.) e bloqueia a resposta ao usuário enquanto
 *     "processa".
 *
 *     Em uma arquitetura bem projetada, notificações seriam disparadas
 *     de forma assíncrona via fila de mensagens (RabbitMQ, Redis Pub/Sub,
 *     Kafka), sem bloquear o fluxo principal.
 *
 *     Este serviço também NÃO possui banco de dados próprio — ele não
 *     persiste nada, apenas simula o envio.
 */

const Fastify = require("fastify");
const { ServiceBroker } = require("moleculer");

const app = Fastify({ logger: true });

const broker = new ServiceBroker({
  logger: console,
  transporter: "TCP",
});

const PORT = process.env.PORT || 3005;

// ── Rotas ───────────────────────────────────────────────────

broker.createService({
  name: "notify",

  events: {
    "playback.started": async (ctx) => {
      const { userId, type, message } = ctx.params || {};

      if (!userId || !message) {
        app.log.warn("userId e message são obrigatórios.");
        return;
      }

      // Simula latência de processamento de notificação (100-300ms)
      const delay = 100 + Math.floor(Math.random() * 200);
      await new Promise((resolve) => setTimeout(resolve, delay));

      app.log.info(
        { userId, type, delay },
        `Notificação enviada (simulada em ${delay}ms)`,
      );

      return {
        sent: true,
        userId,
        type: type || "generic",
        message,
        processingTime: `${delay}ms`,
      };
    },
  },
});

// GET /health
app.get("/health", async () => ({
  status: "ok",
  service: "notification-service",
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
  app.log.info(`notification-service rodando na porta ${PORT}`);
});
