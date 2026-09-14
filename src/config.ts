import { z } from "zod";

const positiveInteger = z.coerce.number().int().positive();

const environmentSchema = z.object({
  PORT: z.coerce.number().int().min(0).max(65_535).default(8_082),
  DATABASE_URL: z.string().min(1),
  SUPABASE_URL: z.url(),
  SUPABASE_ANON_KEY: z.string().min(1),
  KAFKA_BROKERS: z.string().min(1),
  KAFKA_CLIENT_ID: z.string().min(1).default("chitchat-delivery-service"),
  KAFKA_GROUP_ID: z.string().min(1).default("chitchat-delivery-service"),
  KAFKA_SSL: z.enum(["true", "false"]).default("false"),
  KAFKA_API_KEY: z.string().optional(),
  KAFKA_API_SECRET: z.string().optional(),
  WS_MAX_PAYLOAD_BYTES: positiveInteger.default(65_536),
  WS_AUTH_TIMEOUT_MS: positiveInteger.default(10_000),
  HEARTBEAT_INTERVAL_MS: positiveInteger.default(30_000),
  CONNECTION_STALE_MS: positiveInteger.default(60_000),
  DEDUPE_TTL_MS: positiveInteger.default(300_000),
  DEDUPE_MAX_ENTRIES: positiveInteger.default(10_000),
  TYPING_TIMEOUT_MS: positiveInteger.default(5_000),
});

export type Environment = z.infer<typeof environmentSchema>;

export function readEnvironment(
  env: NodeJS.ProcessEnv = process.env,
): Environment {
  const parsed = environmentSchema.parse(env);
  if (parsed.KAFKA_SSL === "true") {
    if (!parsed.KAFKA_API_KEY || !parsed.KAFKA_API_SECRET) {
      throw new Error(
        "KAFKA_API_KEY and KAFKA_API_SECRET are required when KAFKA_SSL=true",
      );
    }
  }
  return parsed;
}
