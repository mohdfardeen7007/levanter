import WebSocket from 'ws';
import { AbstractSocketClient } from './types.js';
/**
 * whatsmeow 07e92ecd: a browser WebSocket upgrade carries Cache-Control/Pragma no-cache.
 * Caller headers (any HeadersInit form) are merged on top; names are lower-cased so a caller
 * override replaces the default instead of duplicating it.
 */
export declare const buildWebSocketHeaders: (custom?: HeadersInit) => Record<string, string>;
export declare class WebSocketClient extends AbstractSocketClient {
    protected socket: WebSocket | null;
    get isOpen(): boolean;
    get isClosed(): boolean;
    get isClosing(): boolean;
    get isConnecting(): boolean;
    connect(): void;
    close(): void;
    send(str: string | Uint8Array, cb?: (err?: Error) => void): boolean;
}
//# sourceMappingURL=websocket.d.ts.map