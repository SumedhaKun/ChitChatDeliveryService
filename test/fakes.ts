import { WebSocket } from "ws";

import type { SocketLike } from "../src/presence.js";

export class FakeSocket implements SocketLike {
  public readyState: number = WebSocket.OPEN;
  public sent: string[] = [];
  public pingCount = 0;
  public terminated = false;
  public sendError: Error | undefined;
  public throwOnSend = false;

  public send(data: string, callback?: (error?: Error) => void): void {
    if (this.throwOnSend) {
      throw new Error("send failed");
    }
    if (this.sendError !== undefined) {
      callback?.(this.sendError);
      return;
    }
    this.sent.push(data);
    callback?.();
  }

  public ping(): void {
    this.pingCount += 1;
  }

  public terminate(): void {
    this.terminated = true;
    this.readyState = WebSocket.CLOSED;
  }
}
