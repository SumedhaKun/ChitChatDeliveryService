import type { MemberRepository } from "./db.js";
import {
  sendBestEffort,
  sendToOpenSockets,
  type FanoutLogger,
} from "./fanout.js";
import type { Presence, SocketLike } from "./presence.js";
import type {
  ActivityAction,
  ActivityBroadcastAction,
  ActivitySnapshotEntry,
} from "./protocol.js";

export interface ActivityLogger extends FanoutLogger {
  info?(event: Record<string, unknown>): void;
}

export class ActivityHub {
  private readonly states = new Map<string, ActivityAction>();

  public constructor(
    private readonly members: MemberRepository,
    private readonly presence: Presence,
    private readonly logger: ActivityLogger = console,
  ) {}

  public async afterAuthenticated(
    userId: string,
    socket: SocketLike,
    isFirstConnection: boolean,
  ): Promise<void> {
    if (isFirstConnection) {
      await this.apply(userId, "set");
    }
    await this.sendSnapshot(userId, socket);
  }

  public async handleClientAction(
    userId: string,
    action: ActivityAction,
  ): Promise<void> {
    await this.apply(userId, action);
  }

  public async onBecameInactive(userId: string): Promise<void> {
    await this.apply(userId, "timeout");
  }

  public onlineUsers(): ActivitySnapshotEntry[] {
    return [...this.states.entries()]
      .filter(([, action]) => action === "set")
      .map(([id]) => ({ userId: id, action: "set" as const }))
      .sort((left, right) => left.userId.localeCompare(right.userId));
  }

  private async apply(
    userId: string,
    action: ActivityBroadcastAction,
  ): Promise<void> {
    const current = this.states.get(userId);
    if (action === "timeout") {
      this.states.delete(userId);
      if (current === "set") {
        await this.broadcast(userId, "timeout");
      }
      return;
    }
    if (current === action) return;
    this.states.set(userId, action);
    await this.broadcast(userId, action);
  }

  private async sendSnapshot(
    userId: string,
    socket: SocketLike,
  ): Promise<void> {
    const contactIds = new Set(await this.members.getContactIds(userId));
    const users = this.onlineUsers().filter((entry) =>
      contactIds.has(entry.userId),
    );
    sendBestEffort(
      this.presence,
      userId,
      socket,
      JSON.stringify({ type: "activity_snapshot", users }),
      this.logger,
    );
  }

  private async broadcast(
    userId: string,
    action: ActivityBroadcastAction,
  ): Promise<void> {
    const contactIds = await this.members.getContactIds(userId);
    const payload = JSON.stringify({
      type: "activity_changed",
      userId,
      action,
    });
    let websocketSends = 0;
    for (const contactId of contactIds) {
      websocketSends += sendToOpenSockets(
        this.presence,
        contactId,
        payload,
        this.logger,
      );
    }
    this.logger.info?.({
      event: "activity_fanout_completed",
      userId,
      action,
      recipientCount: contactIds.length,
      websocketSends,
    });
  }
}
