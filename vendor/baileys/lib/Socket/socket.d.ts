import { Boom } from '@hapi/boom';
import type { SocketConfig } from '../Types/index.js';
import { type BinaryNode } from '../WABinary/index.js';
import { BinaryInfo } from '../WAM/BinaryInfo.js';
import { type UsernameLookupResult, USyncQuery } from '../WAUSync/index.js';
import { WebSocketClient } from './Client/index.js';
export declare const makeSocket: (config: SocketConfig) => {
    type: "md";
    ws: WebSocketClient;
    ev: any;
    authState: {
        creds: import("../Types/index.js").AuthenticationCreds;
        keys: import("../Types/index.js").SignalKeyStoreWithTransaction;
    };
    signalRepository: import("../Types/index.js").SignalRepositoryWithLIDStore;
    logger: import("../Utils/logger.js").ILogger;
    readonly user: import("../Types/index.js").Contact | undefined;
    generateMessageTag: () => string;
    query: (node: BinaryNode, timeoutMs?: number) => Promise<any>;
    waitForMessage: <T>(msgId: string, timeoutMs?: number | undefined) => Promise<T | undefined>;
    waitForSocketOpen: () => Promise<void>;
    sendRawMessage: (data: Uint8Array | Buffer) => Promise<void>;
    sendNode: (frame: BinaryNode) => Promise<void>;
    /** offline delivery pull loop (WA Web WAWebOfflineHandler) */
    offlineBatch: {
        start(): void;
        onOfflineStanza(): void;
        onBacklogProcessed(): void;
        setBacklogCounter(counter: () => number): void;
        stop: () => void;
        isActive: () => boolean;
    };
    logout: (msg?: string) => Promise<void>;
    end: (error: Error | undefined) => void;
    onUnexpectedError: (err: Error | Boom, msg: string) => void;
    uploadPreKeys: (count?: number, retryCount?: number) => Promise<void>;
    uploadPreKeysToServerIfRequired: () => Promise<void>;
    digestKeyBundle: () => Promise<void>;
    rotateSignedPreKey: () => Promise<void>;
    requestPairingCode: (phoneNumber: string, customPairingCode?: string) => Promise<string>;
    wamBuffer: BinaryInfo;
    /** Waits for the connection to WA to reach a state */
    waitForConnectionUpdate: (check: (u: Partial<import("../Types/index.js").ConnectionState>) => Promise<boolean | undefined>, timeoutMs?: number) => Promise<void>;
    sendWAMBuffer: (wamBuffer: Buffer) => Promise<any>;
    executeUSyncQuery: (usyncQuery: USyncQuery) => Promise<import("../index.js").USyncQueryResult | undefined>;
    onWhatsApp: (...phoneNumber: string[]) => Promise<{
        jid: string;
        exists: boolean;
        lid?: string;
    }[] | undefined>;
    resolveUsername: (handle: string, usernameKey?: string) => Promise<UsernameLookupResult>;
    updateServerTimeOffset: ({ attrs }: BinaryNode) => void;
    /** Server-corrected wall clock (ms): Date.now() adjusted by the measured server time offset. */
    getServerTime: () => number;
    getUnifiedSessionId: () => string;
    disposables: (() => void)[];
};
//# sourceMappingURL=socket.d.ts.map