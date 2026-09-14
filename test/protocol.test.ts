import { randomUUID } from "node:crypto";

import { describe, expect, it } from "vitest";

import { parseClientFrame } from "../src/protocol.js";

function frame(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value));
}

describe("parseClientFrame", () => {
  it("parses the first-frame auth request", () => {
    expect(
      parseClientFrame(frame({ type: "auth", accessToken: "token" }), false),
    ).toEqual({
      ok: true,
      frame: { type: "auth", accessToken: "token" },
    });
  });

  it("rejects binary, malformed JSON, and non-auth frames before authentication", () => {
    expect(parseClientFrame(Buffer.from("x"), true)).toEqual({
      ok: false,
      error: {
        type: "error",
        code: "BINARY_FRAME",
        message: "Binary frames are not supported",
      },
    });
    expect(parseClientFrame(Buffer.from("{"), false)).toEqual({
      ok: false,
      error: {
        type: "error",
        code: "MALFORMED_JSON",
        message: "Message must be valid JSON",
      },
    });
    expect(
      parseClientFrame(frame({ type: "activity", action: "set" }), false),
    ).toEqual({
      ok: false,
      error: {
        type: "error",
        code: "INVALID_FRAME",
        message: "First frame must be a valid auth request",
      },
    });
  });

  it("parses activity and typing frames after authentication", () => {
    const conversationId = randomUUID();
    expect(
      parseClientFrame(
        frame({ type: "activity", action: "delete" }),
        false,
        true,
      ),
    ).toEqual({
      ok: true,
      frame: { type: "activity", action: "delete" },
    });
    expect(
      parseClientFrame(
        frame({ type: "typing", conversationId, isTyping: true }),
        false,
        true,
      ),
    ).toEqual({
      ok: true,
      frame: { type: "typing", conversationId, isTyping: true },
    });
  });

  it("rejects auth and invalid frames after authentication", () => {
    expect(
      parseClientFrame(
        frame({ type: "auth", accessToken: "token" }),
        false,
        true,
      ),
    ).toEqual({
      ok: false,
      error: {
        type: "error",
        code: "INVALID_FRAME",
        message: "Frame must be a valid activity or typing request",
      },
    });
    expect(
      parseClientFrame(
        frame({ type: "activity", action: "timeout" }),
        false,
        true,
      ),
    ).toMatchObject({
      ok: false,
      error: { type: "error", code: "INVALID_FRAME" },
    });
    expect(
      parseClientFrame(
        frame({ type: "typing", conversationId: "nope", isTyping: true }),
        false,
        true,
      ),
    ).toMatchObject({
      ok: false,
      error: { type: "error", code: "INVALID_FRAME" },
    });
  });
});
