import { z } from "zod";

import type { MessageCreatedEvent } from "./events.js";

export const DEFAULT_MAX_TRANSPORT_FRAME_BYTES = 65_536;

export const authRequestSchema = z
  .object({
    type: z.literal("auth"),
    accessToken: z.string().min(1),
  })
  .strict();

export const activityRequestSchema = z
  .object({
    type: z.literal("activity"),
    action: z.enum(["set", "delete"]),
  })
  .strict();

export const typingRequestSchema = z
  .object({
    type: z.literal("typing"),
    conversationId: z.uuid(),
    isTyping: z.boolean(),
  })
  .strict();

export const authenticatedClientFrameSchema = z.discriminatedUnion("type", [
  activityRequestSchema,
  typingRequestSchema,
]);

export type AuthRequest = z.infer<typeof authRequestSchema>;
export type ActivityRequest = z.infer<typeof activityRequestSchema>;
export type TypingRequest = z.infer<typeof typingRequestSchema>;
export type ClientFrame = AuthRequest | ActivityRequest | TypingRequest;
export type ActivityAction = ActivityRequest["action"];
export type ActivityBroadcastAction = ActivityAction | "timeout";

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

export interface ActivityChangedFrame {
  type: "activity_changed";
  userId: string;
  action: ActivityBroadcastAction;
}

export interface ActivitySnapshotEntry {
  userId: string;
  action: "set";
}

export interface ActivitySnapshotFrame {
  type: "activity_snapshot";
  users: ActivitySnapshotEntry[];
}

export interface TypingFrame {
  type: "typing";
  userId: string;
  conversationId: string;
  isTyping: boolean;
}

export type ServerFrame =
  | AuthAck
  | ServerErrorResponse
  | MessageCreatedFrame
  | ActivityChangedFrame
  | ActivitySnapshotFrame
  | TypingFrame;

export function errorFrame(code: string, message: string): ServerErrorResponse {
  return { type: "error", code, message };
}

export function parseClientFrame(
  raw: Buffer,
  isBinary: boolean,
  authenticated = false,
):
  { ok: true; frame: ClientFrame } | { ok: false; error: ServerErrorResponse } {
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

  if (!authenticated) {
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

  const result = authenticatedClientFrameSchema.safeParse(decoded);
  if (!result.success) {
    return {
      ok: false,
      error: errorFrame(
        "INVALID_FRAME",
        "Frame must be a valid activity or typing request",
      ),
    };
  }
  return { ok: true, frame: result.data };
}
