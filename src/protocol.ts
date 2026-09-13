import { z } from "zod";

import type { MessageCreatedEvent } from "./events.js";

export const DEFAULT_MAX_TRANSPORT_FRAME_BYTES = 65_536;

export const authRequestSchema = z
  .object({
    type: z.literal("auth"),
    accessToken: z.string().min(1),
  })
  .strict();

export type AuthRequest = z.infer<typeof authRequestSchema>;

export interface AuthAck {
  type: "auth_ack";
}

export interface ServerErrorResponse {
  type: "error";
  code: string;
  message: string;
}

export interface MessageCreatedFrame {
  type: "message_created";
  message: MessageCreatedEvent;
}

export type ServerFrame = AuthAck | ServerErrorResponse | MessageCreatedFrame;

export function errorFrame(code: string, message: string): ServerErrorResponse {
  return { type: "error", code, message };
}

export function parseClientFrame(
  raw: Buffer,
  isBinary: boolean,
):
  { ok: true; frame: AuthRequest } | { ok: false; error: ServerErrorResponse } {
  if (isBinary) {
    return {
      ok: false,
      error: errorFrame("BINARY_FRAME", "Binary frames are not supported"),
    };
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(raw.toString("utf8")) as unknown;
  } catch {
    return {
      ok: false,
      error: errorFrame("MALFORMED_JSON", "Message must be valid JSON"),
    };
  }

  const result = authRequestSchema.safeParse(decoded);
  if (!result.success) {
    return {
      ok: false,
      error: errorFrame(
        "INVALID_FRAME",
        "First frame must be a valid auth request",
      ),
    };
  }

  return { ok: true, frame: result.data };
}
