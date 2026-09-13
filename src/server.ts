import "dotenv/config";

import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type Server as HttpServer,
} from "node:http";
import { pathToFileURL } from "node:url";

import { WebSocket, WebSocketServer, type RawData } from "ws";

import { createSupabaseAuthenticator, type Authenticate } from "./auth.js";
import { readEnvironment } from "./config.js";
import { createDatabase, type Database } from "./db.js";
import { TtlDedupe } from "./dedupe.js";
import { DeliveryHandler, noOpPushNotifier } from "./delivery.js";
import { startPresenceHeartbeat, type Heartbeat } from "./heartbeat.js";
import {
  createConfluentConsumer,
  createMessageConsumer,
  readKafkaSettings,
  type MessageConsumer,
} from "./kafka.js";
import { Presence } from "./presence.js";
import {
  DEFAULT_MAX_TRANSPORT_FRAME_BYTES,
  errorFrame,
  parseClientFrame,
  type ServerFrame,
} from "./protocol.js";

const SHUTDOWN_TIMEOUT_MS = 5_000;

export interface Logger {
  info(event: Record<string, unknown>): void;
  warn(event: Record<string, unknown>): void;
  error(event: Record<string, unknown>): void;
}

export interface DeliveryServiceOptions {
  port?: number;
  httpServer?: HttpServer;
  logger?: Logger;
  authenticate: Authenticate;
  presence?: Presence;
  database?: Database;
  consumer?: MessageConsumer;
  heartbeatIntervalMs?: number;
  connectionStaleMs?: number;
  wsMaxPayloadBytes?: number;
  wsAuthTimeoutMs?: number;
}

export interface DeliveryService {
  httpServer: HttpServer;
  webSocketServer: WebSocketServer;
  presence: Presence;
  start(): Promise<void>;
  close(): Promise<void>;
}

function normalizeRawData(data: RawData): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (Array.isArray(data)) return Buffer.concat(data);
  return Buffer.from(data);
}

function sendJson(socket: WebSocket, frame: ServerFrame): void {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(frame));
  }
}

function requestPath(request: IncomingMessage): string {
  try {
    return new URL(request.url ?? "/", "http://localhost").pathname;
  } catch {
    return "/";
  }
}

function createHealthServer(): HttpServer {
  return createServer((request, response) => {
    if (request.method === "GET" && requestPath(request) === "/health") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ status: "ok" }));
      return;
    }

    response.writeHead(404, { "content-type": "application/json" });
    response.end(
      JSON.stringify({
        error: { code: "NOT_FOUND", message: "Route not found" },
      }),
    );
  });
}

export function createDeliveryService(
  options: DeliveryServiceOptions,
): DeliveryService {
  const logger = options.logger ?? console;
  const presence = options.presence ?? new Presence();
  const port = options.port ?? 0;
  const httpServer = options.httpServer ?? createHealthServer();
  const maxPayload =
    options.wsMaxPayloadBytes ?? DEFAULT_MAX_TRANSPORT_FRAME_BYTES;
  const authTimeoutMs = options.wsAuthTimeoutMs ?? 10_000;
  const heartbeatIntervalMs = options.heartbeatIntervalMs ?? 30_000;
  const connectionStaleMs = options.connectionStaleMs ?? 60_000;

  const webSocketServer = new WebSocketServer({
    server: httpServer,
    maxPayload,
  });
  const socketUsers = new WeakMap<WebSocket, string>();
  let heartbeat: Heartbeat | undefined;
  let started = false;

  webSocketServer.on("connection", (socket) => {
    const connectionId = randomUUID();
    let authenticated = false;
    let processing = Promise.resolve();
    logger.info({ event: "connection_opened", connectionId });

    const authTimer = setTimeout(() => {
      if (authenticated || socket.readyState !== WebSocket.OPEN) return;
      sendJson(
        socket,
        errorFrame("AUTH_TIMEOUT", "Authentication was not completed in time"),
      );
      socket.close(1008, "Authentication timeout");
    }, authTimeoutMs);
    authTimer.unref();

    socket.on("message", (data, isBinary) => {
      processing = processing
        .then(async () => {
          const parsed = parseClientFrame(normalizeRawData(data), isBinary);
          if (!parsed.ok) {
            logger.warn({
              event: "frame_rejected",
              connectionId,
              code: parsed.error.code,
            });
            sendJson(socket, parsed.error);
            return;
          }

          if (authenticated) {
            sendJson(
              socket,
              errorFrame(
                "UNEXPECTED_FRAME",
                "Authenticated connections are receive-only",
              ),
            );
            return;
          }

          const user = await options.authenticate(parsed.frame.accessToken);
          if (user === null) {
            logger.warn({ event: "auth_failed", connectionId });
            sendJson(
              socket,
              errorFrame("AUTH_FAILED", "Access token is invalid or expired"),
            );
            return;
          }

          authenticated = true;
          clearTimeout(authTimer);
          socketUsers.set(socket, user.id);
          presence.setConnection(user.id, socket);
          sendJson(socket, { type: "auth_ack" });
          logger.info({
            event: "connection_authenticated",
            connectionId,
            userId: user.id,
          });
        })
        .catch((error: unknown) => {
          logger.error({
            event: "frame_processing_failed",
            connectionId,
            error: error instanceof Error ? error.message : "Unknown error",
          });
          sendJson(
            socket,
            errorFrame("INTERNAL_ERROR", "Internal server error"),
          );
        });
    });

    socket.on("pong", () => {
      const userId = socketUsers.get(socket);
      if (userId !== undefined) {
        presence.touchConnection(userId, socket);
      }
    });

    socket.on("error", (error) => {
      logger.warn({
        event: "connection_error",
        connectionId,
        error: error.message,
      });
    });

    socket.on("close", (code) => {
      clearTimeout(authTimer);
      const userId = socketUsers.get(socket);
      if (userId !== undefined) {
        presence.deleteConnection(userId, socket);
      }
      logger.info({ event: "connection_closed", connectionId, code });
    });
  });

  webSocketServer.on("error", (error) => {
    logger.error({ event: "websocket_server_error", error: error.message });
  });

  return {
    httpServer,
    webSocketServer,
    presence,
    async start() {
      if (started) return;
      started = true;

      if (!httpServer.listening) {
        await new Promise<void>((resolve, reject) => {
          const onError = (error: Error): void => {
            reject(error);
          };
          httpServer.once("error", onError);
          httpServer.listen(port, () => {
            httpServer.off("error", onError);
            resolve();
          });
        });
      }

      heartbeat = startPresenceHeartbeat(presence, {
        intervalMs: heartbeatIntervalMs,
        staleMs: connectionStaleMs,
      });
      if (options.consumer !== undefined) {
        await options.consumer.start();
      }

      const address = httpServer.address();
      logger.info({
        event: "server_listening",
        ...(typeof address === "object" && address !== null
          ? { port: address.port }
          : {}),
      });
    },
    async close() {
      heartbeat?.stop();

      for (const client of webSocketServer.clients) {
        client.close(1001, "Server shutting down");
      }

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          for (const client of webSocketServer.clients) {
            client.terminate();
          }
        }, SHUTDOWN_TIMEOUT_MS);
        timeout.unref();

        webSocketServer.close((error) => {
          clearTimeout(timeout);
          if (error === undefined) resolve();
          else reject(error);
        });
      });

      if (httpServer.listening) {
        await new Promise<void>((resolve, reject) => {
          httpServer.close((error) => {
            if (error === undefined) resolve();
            else reject(error);
          });
        });
      }

      if (options.consumer !== undefined) {
        await options.consumer.disconnect();
      }
      if (options.database !== undefined) {
        await options.database.close();
      }
    },
  };
}

function run(): void {
  const logger: Logger = console;
  const env = readEnvironment();
  const database = createDatabase(env.DATABASE_URL);
  const presence = new Presence();
  const handler = new DeliveryHandler(
    database.members,
    presence,
    noOpPushNotifier,
    new TtlDedupe(env.DEDUPE_TTL_MS, env.DEDUPE_MAX_ENTRIES),
    {
      info(event) {
        logger.info(event);
      },
      error(message, details) {
        logger.error({ event: "delivery_failed", message, details });
      },
    },
  );
  const consumer = createMessageConsumer(
    createConfluentConsumer(readKafkaSettings()),
    (event) => handler.handle(event),
    logger,
  );
  const service = createDeliveryService({
    port: env.PORT,
    logger,
    authenticate: createSupabaseAuthenticator(
      env.SUPABASE_URL,
      env.SUPABASE_ANON_KEY,
    ),
    presence,
    database,
    consumer,
    heartbeatIntervalMs: env.HEARTBEAT_INTERVAL_MS,
    connectionStaleMs: env.CONNECTION_STALE_MS,
    wsMaxPayloadBytes: env.WS_MAX_PAYLOAD_BYTES,
    wsAuthTimeoutMs: env.WS_AUTH_TIMEOUT_MS,
  });

  let shuttingDown = false;
  const shutdown = (signal: NodeJS.Signals): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ event: "server_shutdown_started", signal });
    void service
      .close()
      .then(() => logger.info({ event: "server_shutdown_complete" }))
      .catch((error: unknown) => {
        logger.error({
          event: "server_shutdown_failed",
          error: error instanceof Error ? error.message : "Unknown error",
        });
        process.exitCode = 1;
      });
  };

  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
  void service.start().catch((error: unknown) => {
    logger.error({
      event: "server_start_failed",
      error: error instanceof Error ? error.message : "Unknown error",
    });
    process.exitCode = 1;
  });
}

const entrypoint = process.argv[1];
if (
  entrypoint !== undefined &&
  import.meta.url === pathToFileURL(entrypoint).href
) {
  run();
}
