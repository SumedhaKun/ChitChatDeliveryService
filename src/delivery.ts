import WebSocket from "ws";

import type { MemberRepository } from "./db.js";
import type { TtlDedupe } from "./dedupe.js";
import type { MessageCreatedEvent } from "./events.js";
import type { Presence, SocketLike } from "./presence.js";

export interface PushNotifier {
  notifyMessageCreated(
    userId: string,
    event: MessageCreatedEvent,
  ): Promise<void>;
}

export const noOpPushNotifier: PushNotifier = {
  notifyMessageCreated() {
    return Promise.resolve();
  },
};

export interface DeliveryLogger {
  info?(event: Record<string, unknown>): void;
  error(message: string, details?: unknown): void;
}

export class DeliveryHandler {
  public constructor(
    private readonly members: MemberRepository,
    private readonly presence: Presence,
    private readonly pushNotifier: PushNotifier,
    private readonly dedupe: TtlDedupe,
    private readonly logger: DeliveryLogger = console,
  ) {}

  public async handle(event: MessageCreatedEvent): Promise<void> {
    if (this.dedupe.has(event.messageId)) {
      this.logger.info?.({
        event: "delivery_skipped_duplicate",
        messageId: event.messageId,
      });
      return;
    }

    const memberIds = await this.members.getUserIds(event.conversationId);
    const recipients = new Set(
      memberIds.filter((userId) => userId !== event.senderId),
    );
    const payload = JSON.stringify({ type: "message_created", message: event });
    const pushTasks: Promise<void>[] = [];
    let websocketSends = 0;

    for (const userId of recipients) {
      const openSockets = [
        ...this.presence.getConnections(userId).keys(),
      ].filter((socket) => socket.readyState === WebSocket.OPEN);
      if (openSockets.length === 0) {
        pushTasks.push(this.notifyBestEffort(userId, event));
        continue;
      }
      websocketSends += openSockets.length;
      for (const socket of openSockets)
        this.sendBestEffort(userId, socket, payload);
    }

    await Promise.all(pushTasks);
    this.dedupe.add(event.messageId);
    this.logger.info?.({
      event: "delivery_completed",
      messageId: event.messageId,
      conversationId: event.conversationId,
      recipientCount: recipients.size,
      websocketSends,
      pushNotifications: pushTasks.length,
    });
  }

  private sendBestEffort(
    userId: string,
    socket: SocketLike,
    payload: string,
  ): void {
    try {
      socket.send(payload, (error) => {
        if (!error) return;
        this.presence.deleteConnection(userId, socket);
        this.logger.error("WebSocket delivery failed", {
          userId,
          error: error.message,
        });
      });
    } catch (error) {
      this.presence.deleteConnection(userId, socket);
      this.logger.error("WebSocket delivery failed", { userId, error });
    }
  }

  private async notifyBestEffort(
    userId: string,
    event: MessageCreatedEvent,
  ): Promise<void> {
    try {
      await this.pushNotifier.notifyMessageCreated(userId, event);
    } catch (error) {
      this.logger.error("Push notification failed", { userId, error });
    }
  }
}
