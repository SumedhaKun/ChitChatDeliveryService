import WebSocket from "ws";

export interface Connection {
  lastSeen: number;
}

export interface SocketLike {
  readonly readyState: number;
  send(data: string, callback?: (error?: Error) => void): void;
  ping?(): void;
  terminate?(): void;
}

export class Presence {
  private readonly users = new Map<string, Map<SocketLike, Connection>>();

  public getConnections(userId: string): ReadonlyMap<SocketLike, Connection> {
    return this.users.get(userId) ?? new Map();
  }

  public setConnection(
    userId: string,
    socket: SocketLike,
    now = Date.now(),
  ): void {
    let connections = this.users.get(userId);
    if (!connections) {
      connections = new Map();
      this.users.set(userId, connections);
    }
    connections.set(socket, { lastSeen: now });
  }

  public isActive(userId: string): boolean {
    return (this.users.get(userId)?.size ?? 0) > 0;
  }

  public touchConnection(
    userId: string,
    socket: SocketLike,
    now = Date.now(),
  ): boolean {
    const connection = this.users.get(userId)?.get(socket);
    if (!connection) return false;
    connection.lastSeen = now;
    return true;
  }

  public deleteConnection(userId: string, socket: SocketLike): boolean {
    const connections = this.users.get(userId);
    if (!connections) return false;
    const deleted = connections.delete(socket);
    if (connections.size === 0) this.users.delete(userId);
    return deleted;
  }

  public removeStale(
    staleBefore: number,
    onStale: (socket: SocketLike, userId: string) => void = (socket) => {
      socket.terminate?.();
    },
  ): number {
    let removed = 0;
    for (const [userId, connections] of this.users) {
      for (const [socket, connection] of connections) {
        if (connection.lastSeen < staleBefore) {
          connections.delete(socket);
          removed += 1;
          onStale(socket, userId);
        }
      }
      if (connections.size === 0) this.users.delete(userId);
    }
    return removed;
  }

  public pingAll(): void {
    for (const connections of this.users.values()) {
      for (const socket of connections.keys()) {
        if (socket.readyState === WebSocket.OPEN) socket.ping?.();
      }
    }
  }
}
