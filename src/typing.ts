import type { MemberRepository } from "./db.js";
import { sendToOpenSockets, type FanoutLogger } from "./fanout.js";
import type { Presence } from "./presence.js";

export const DEFAULT_TYPING_TIMEOUT_MS = 5_000;

export interface TypingLogger extends FanoutLogger {
  info?(event: Record<string, unknown>): void;
}

export type TypingHandleResult = "ok" | "forbidden";

export class TypingHub {
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();

  public constructor(
    private readonly members: MemberRepository,
    private readonly presence: Presence,
    private readonly timeoutMs: number = DEFAULT_TYPING_TIMEOUT_MS,
    private readonly logger: TypingLogger = console,
  ) {}

  public async handle(
    userId: string,
    conversationId: string,
    isTyping: boolean,
  ): Promise<TypingHandleResult> {
    const memberIds = await this.members.getUserIds(conversationId);
    if (!memberIds.includes(userId)) return "forbidden";

    const key = typingKey(userId, conversationId);
    const alreadyTyping = this.timers.has(key);

    if (isTyping) {
      this.resetTimer(userId, conversationId, memberIds);
      if (!alreadyTyping) {
        this.broadcast(userId, conversationId, memberIds, true);
      }
      return "ok";
    }

    if (!alreadyTyping) return "ok";
    this.clearTimer(key);
    this.broadcast(userId, conversationId, memberIds, false);
    return "ok";
  }

  public stop(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
  }

  private resetTimer(
    userId: string,
    conversationId: string,
    memberIds: string[],
  ): void {
    const key = typingKey(userId, conversationId);
    this.clearTimer(key);
    const timer = setTimeout(() => {
      this.timers.delete(key);
      this.broadcast(userId, conversationId, memberIds, false);
    }, this.timeoutMs);
    timer.unref();
    this.timers.set(key, timer);
  }

  private clearTimer(key: string): void {
    const timer = this.timers.get(key);
    if (timer === undefined) return;
    clearTimeout(timer);
    this.timers.delete(key);
  }

  private broadcast(
    userId: string,
    conversationId: string,
    memberIds: string[],
    isTyping: boolean,
  ): void {
    const payload = JSON.stringify({
      type: "typing",
      userId,
      conversationId,
      isTyping,
    });
    const recipients = memberIds.filter((memberId) => memberId !== userId);
    let websocketSends = 0;
    for (const memberId of recipients) {
      websocketSends += sendToOpenSockets(
        this.presence,
        memberId,
        payload,
        this.logger,
      );
    }
    this.logger.info?.({
      event: "typing_fanout_completed",
      userId,
      conversationId,
      isTyping,
      recipientCount: recipients.length,
      websocketSends,
    });
  }
}

function typingKey(userId: string, conversationId: string): string {
  return `${userId}:${conversationId}`;
}
