import type { Agent } from 'https';
import type { URL } from 'url';
import { proto } from '../../WAProto/index.js';
import type { ILogger } from '../Utils/logger.js';
import type { ShortcakeAssertionSigner } from '../Utils/shortcake.js';
import type { AuthenticationState, LIDMapping, SignalAuthState, TransactionCapabilityOptions } from './Auth.js';
import type { BaileysEventEmitter } from './Events.js';
import type { GroupMetadata } from './GroupMetadata.js';
import { type MediaConnInfo, type WAMessageKey } from './Message.js';
import type { SignalRepositoryWithLIDStore } from './Signal.js';
export type WAVersion = [number, number, number];
export type WABrowserDescription = [string, string, string];
export type CacheStore = {
    /** get a cached key and change the stats */
    get<T>(key: string): Promise<T> | T | undefined;
    /** set a key in the cache */
    set<T>(key: string, value: T): Promise<void> | void | number | boolean;
    /** delete a key from the cache */
    del(key: string): void | Promise<void> | number | boolean;
    /** flush all data */
    flushAll(): void | Promise<void>;
};
export type PossiblyExtendedCacheStore = CacheStore & {
    mget?: <T>(keys: string[]) => Promise<Record<string, T | undefined>>;
    mset?: <T>(entries: {
        key: string;
        value: T;
    }[]) => Promise<void> | void | number | boolean;
    mdel?: (keys: string[]) => void | Promise<void> | number | boolean;
};
export type PatchedMessageWithRecipientJID = proto.IMessage & {
    recipientJid?: string;
};
export type SocketConfig = {
    /** the WS url to connect to WA */
    waWebSocketUrl: string | URL;
    /** Fails the connection if the socket times out in this interval */
    connectTimeoutMs: number;
    /** Default timeout for queries, undefined for no timeout */
    defaultQueryTimeoutMs: number | undefined;
    /** ping-pong interval for WS connection */
    keepAliveIntervalMs: number;
    /** should baileys use the mobile api instead of the multi device api
     * @deprecated This feature has been removed
     */
    mobile?: boolean;
    /** proxy agent */
    agent?: Agent;
    /** logger */
    logger: ILogger;
    /** version to connect with */
    version: WAVersion;
    /** override browser config */
    browser: WABrowserDescription;
    /** pushName to attach to payload */
    pushName?: string;
    /** agent used for fetch requests -- uploading/downloading media */
    fetchAgent?: Agent;
    /** should the QR be printed in the terminal
     * @deprecated This feature has been removed
     */
    printQRInTerminal?: boolean;
    /** should events be emitted for actions done by this socket connection */
    emitOwnEvents: boolean;
    /** custom upload hosts to upload media to */
    customUploadHosts: MediaConnInfo['hosts'];
    /** time to wait between sending new retry requests */
    retryRequestDelayMs: number;
    /** max retry count */
    maxMsgRetryCount: number;
    /** time to wait for the generation of the next QR in ms */
    qrTimeout?: number;
    /** test-only: trust anchor for the inbound Noise cert chain. Defaults to WA_CERT_DETAILS.
     * Used by the in-process fake-server cross-check harness; leave unset in production. */
    certTrustAnchor?: {
        publicKey: Uint8Array;
        serial: number;
    };
    /** provide an auth state object to maintain the auth state */
    auth: AuthenticationState;
    /** manage history processing with this control; by default will sync up everything */
    shouldSyncHistoryMessage: (msg: proto.Message.IHistorySyncNotification) => boolean;
    /**
     * fork-only, default false: when true, the statuses posted before pairing (history sync INITIAL_STATUS_V3, WA Web
     * WAWebHistorySyncHandleStatusMessages) are emitted in `messaging-history.set`: oldest first, as `status@broadcast`
     * messages with the poster in `key.participant`, no chats or contacts, expired ones kept (WA Web leaves expiry to
     * the display). Only for the chunks `shouldSyncHistoryMessage` lets through.
     * Off by default because the event buffer folds a live 'notify' upsert into a buffered history set that holds the
     * same key (event-buffer.ts, locked by its 'folds the live upsert into the buffered history set' test), so a status
     * posted around pairing time could reach the app only in `messaging-history.set` and miss `messages.upsert`.
     * The buffer also overwrites `syncType` per chunk: pick these out by `key.remoteJid === 'status@broadcast'`.
     */
    processHistoryStatuses?: boolean;
    /** transaction capability options for SignalKeyStore */
    transactionOpts: TransactionCapabilityOptions;
    /** marks the client as online whenever the socket successfully connects */
    markOnlineOnConnect: boolean;
    /** alphanumeric country code (USA -> US) for the number used */
    countryCode: string;
    /** provide a cache to store media, so does not have to be re-uploaded */
    mediaCache?: CacheStore;
    /**
     * map to store the retry counts for failed messages;
     * used to determine whether to retry a message or not */
    msgRetryCounterCache?: CacheStore;
    /** provide a cache to store a user's device list */
    userDevicesCache?: PossiblyExtendedCacheStore;
    /** cache to store call offers */
    callOfferCache?: CacheStore;
    /** cache to track placeholder resends */
    placeholderResendCache?: CacheStore;
    /** width for link preview images */
    linkPreviewImageThumbnailWidth: number;
    /** Should Baileys ask the phone for full history, will be received async */
    syncFullHistory: boolean;
    /** Should baileys fire init queries automatically, default true */
    fireInitQueries: boolean;
    /**
     * generate a high quality link preview,
     * entails uploading the jpegThumbnail to WA
     * */
    generateHighQualityLinkPreview: boolean;
    /** Enable automatic session recreation for failed messages */
    enableAutoSessionRecreation: boolean;
    /** Enable recent message caching for retry handling */
    enableRecentMessageCache: boolean;
    /**
     * Returns if a jid should be ignored,
     * no event for that jid will be triggered.
     * Messages from that jid will also not be decrypted.
     * `category="peer"` messages from our own account (app-state key shares, history sync, PDO answers)
     * are never ignored: they are protocol traffic between our own devices (zapo 408d48c)
     * */
    shouldIgnoreJid: (jid: string) => boolean | undefined;
    /**
     * skip the app-level processing of offline messages (default: true): a message received while we were
     * disconnected (and the resend of one that failed to decrypt, and a channel post inside an offline
     * newsletter notification) emits no `messages.upsert` and runs no message side effects. It is still
     * decrypted, receipted and acked, so the Signal sessions and sender keys it carries are kept.
     * Peer messages from our own devices, notifications, receipts and calls always run, offline or not:
     * offline notifications emit their events (`group-participants.update`, `groups.update`,
     * `contacts.update` for pictures, ...) and their stub messages (group participant add/remove, subject,
     * icon, ...) as `messages.upsert` of type `append`, so e.g. a welcome/goodbye bot sees group changes
     * made while it was offline; offline calls emit `call` and their call stubs (missed call, group call: `append`).
     */
    ignoreOfflineMessages: boolean;
    /**
     * Optionally patch the message before sending out
     * */
    patchMessageBeforeSending: (msg: proto.IMessage, recipientJids?: string[]) => Promise<PatchedMessageWithRecipientJID[] | PatchedMessageWithRecipientJID> | PatchedMessageWithRecipientJID[] | PatchedMessageWithRecipientJID;
    /** verify app state MACs */
    appStateMacVerification: {
        patch: boolean;
        snapshot: boolean;
    };
    /** options for HTTP fetch requests */
    options: RequestInit;
    /**
     * fetch a message from your store
     * implement this so that messages failed to send
     * (solves the "this message can take a while" issue) can be retried
     * */
    getMessage: (key: WAMessageKey) => Promise<proto.IMessage | undefined>;
    /** cached group metadata, use to prevent redundant requests to WA & speed up msg sending */
    cachedGroupMetadata: (jid: string) => Promise<GroupMetadata | undefined>;
    makeSignalRepository: (auth: SignalAuthState, logger: ILogger, pnToLIDFunc?: (jids: string[]) => Promise<LIDMapping[] | undefined>) => SignalRepositoryWithLIDStore;
    /**
     * handler concurrency (default: 1)
     * */
    handlerConcurrency?: number;
    /**
     * handler queue size (default: 2048)
     * */
    handlerQueueSize?: number;
    /** provide an event emitter to listen to events */
    ev?: BaileysEventEmitter;
    /**
     * External WebAuthn signer; enables handling a server-forced passkey ("Shortcake") prologue
     * (e.g. right after a pairing-code `companion_finish`). The credential source stays outside the library.
     */
    signPasskeyAssertion?: ShortcakeAssertionSigner;
};
//# sourceMappingURL=Socket.d.ts.map