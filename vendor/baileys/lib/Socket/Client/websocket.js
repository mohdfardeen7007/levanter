import WebSocket from 'ws';
import { DEFAULT_ORIGIN } from '../../Defaults/index.js';
import { AbstractSocketClient } from './types.js';
/**
 * whatsmeow 07e92ecd: a browser WebSocket upgrade carries Cache-Control/Pragma no-cache.
 * Caller headers (any HeadersInit form) are merged on top; names are lower-cased so a caller
 * override replaces the default instead of duplicating it.
 */
export const buildWebSocketHeaders = (custom) => {
    const headers = { 'cache-control': 'no-cache', pragma: 'no-cache' };
    if (custom) {
        new Headers(custom).forEach((value, key) => {
            headers[key] = value;
        });
    }
    return headers;
};
export class WebSocketClient extends AbstractSocketClient {
    constructor() {
        super(...arguments);
        this.socket = null;
    }
    get isOpen() {
        return this.socket?.readyState === WebSocket.OPEN;
    }
    get isClosed() {
        return this.socket === null || this.socket?.readyState === WebSocket.CLOSED;
    }
    get isClosing() {
        return this.socket === null || this.socket?.readyState === WebSocket.CLOSING;
    }
    get isConnecting() {
        return this.socket?.readyState === WebSocket.CONNECTING;
    }
    connect() {
        if (this.socket) {
            return;
        }
        this.socket = new WebSocket(this.url, {
            origin: DEFAULT_ORIGIN,
            headers: buildWebSocketHeaders(this.config.options?.headers),
            handshakeTimeout: this.config.connectTimeoutMs,
            timeout: this.config.connectTimeoutMs,
            agent: this.config.agent
        });
        this.socket.setMaxListeners(0);
        const events = ['close', 'error', 'upgrade', 'message', 'open', 'ping', 'pong', 'unexpected-response'];
        for (const event of events) {
            this.socket?.on(event, (...args) => this.emit(event, ...args));
        }
    }
    close() {
        if (!this.socket) {
            return;
        }
        this.socket.removeAllListeners();
        this.socket.on('error', () => { }); // Ignore errors during closure
        this.socket.close();
        this.socket = null;
    }
    send(str, cb) {
        this.socket?.send(str, cb);
        return Boolean(this.socket);
    }
}
//# sourceMappingURL=websocket.js.map