export interface ClientOptions {
  host?: string;
  port?: number;
}

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
}

export class MagicQueueClient {
  private host: string;
  private port: number;
  private socket: any = null;
  private buffer: string = "";
  private pendingRequests: PendingRequest[] = [];
  private connected: boolean = false;

  constructor(options: ClientOptions = {}) {
    this.host = options.host || "localhost";
    this.port = options.port || 6789;
  }

  async connect(): Promise<void> {
    if (this.connected) return;

    return new Promise((resolve, reject) => {
      const client = this;

      Bun.connect({
        hostname: this.host,
        port: this.port,
        socket: {
          data(socket, data) {
            client.handleData(data);
          },
          open(socket) {
            client.socket = socket;
            client.connected = true;
            resolve();
          },
          close() {
            client.connected = false;
            client.socket = null;
          },
          error(socket, error) {
            if (!client.connected) {
              reject(error);
            }
          },
        },
      });
    });
  }

  private handleData(data: Buffer): void {
    this.buffer += data.toString();

    // Processa linee complete
    let newlineIndex: number;
    while ((newlineIndex = this.buffer.indexOf("\n")) !== -1) {
      const line = this.buffer.slice(0, newlineIndex);
      this.buffer = this.buffer.slice(newlineIndex + 1);

      try {
        const response = JSON.parse(line);
        const pending = this.pendingRequests.shift();
        if (pending) {
          if (response.ok === false) {
            pending.reject(new Error(response.error || "Unknown error"));
          } else {
            pending.resolve(response);
          }
        }
      } catch (e) {
        console.error("Failed to parse response:", line);
      }
    }
  }

  async send<T = any>(command: object): Promise<T> {
    if (!this.socket || !this.connected) {
      throw new Error("Not connected");
    }

    return new Promise((resolve, reject) => {
      this.pendingRequests.push({ resolve, reject });
      const message = JSON.stringify(command) + "\n";
      this.socket!.write(message);
    });
  }

  async close(): Promise<void> {
    if (this.socket) {
      this.socket.end();
      this.socket = null;
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }
}
