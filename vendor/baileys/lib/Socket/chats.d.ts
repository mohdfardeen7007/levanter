import { Boom } from '@hapi/boom';
import { proto } from '../../WAProto/index.js';
import type { BlocklistResult, BotListInfo, ChatModification, SocketConfig, WABusinessProfile, WAMediaUpload, WAPatchCreate, WAPresence, WAPrivacyCallValue, WAPrivacyGroupAddValue, WAPrivacyMessagesValue, WAPrivacyOnlineValue, WAPrivacyValue, WAReadReceiptsValue } from '../Types/index.js';
import type { QuickReplyAction } from '../Types/Bussines.js';
import type { LabelActionBody } from '../Types/Label.js';
import { type BlocklistIdentity } from '../Utils/username.js';
import { type BinaryNode } from '../WABinary/index.js';
import { USyncQuery } from '../WAUSync/index.js';
export declare const buildProfilePictureQueryContent: (type: "preview" | "image", tcTokenContent?: BinaryNode[]) => BinaryNode[];
/** structured status input, mirrors WA's SetStatusInput for the mex mutation */
export type StatusUpdateInput = {
    /** the status/about text */
    text?: string;
    /** a single emoji to attach to the status */
    emoji?: string;
    /** ephemeral duration in seconds; 0 = permanent */
    ephemeralDurationSec?: number;
};
export declare const makeChatsSocket: (config: SocketConfig) => {
    serverProps: {
        /** AB prop 10518: gate tctoken on 1:1 messages. Default true (safe: avoids server error 463). */
        privacyTokenOn1to1: boolean;
        /** AB prop 9666: gate tctoken on profile picture IQs. WA Web default: true. */
        profilePicPrivacyToken: boolean;
        /** AB prop 14303: issue tctokens to LID instead of PN. WA Web default: false. */
        lidTrustedTokenIssueToLid: boolean;
        /** AB prop 19696: name the poster's PN in peer_participant_pn on LID status read receipts. WA Web default: true. */
        lidStatusNonSoakedClientSupport: boolean;
    };
    setHistorySyncBlobProcessedHandler: (handler: (notification: proto.Message.IHistorySyncNotification) => void) => void;
    fetchAccountReachoutTimelock: () => Promise<{
        isActive: boolean;
        timeEnforcementEnds: number;
        enforcementType: string;
    }>;
    createCallLink: (type: "audio" | "video", event?: {
        startTime: number;
    }, timeoutMs?: number) => Promise<string | undefined>;
    getBotListV2: () => Promise<BotListInfo[]>;
    messageMutex: {
        mutex<T>(code: () => Promise<T> | T): Promise<T>;
    };
    receiptMutex: {
        mutex<T>(code: () => Promise<T> | T): Promise<T>;
    };
    appStatePatchMutex: {
        mutex<T>(code: () => Promise<T> | T): Promise<T>;
    };
    notificationMutex: {
        mutex<T>(code: () => Promise<T> | T): Promise<T>;
    };
    fetchPrivacySettings: (force?: boolean) => Promise<{
        [_: string]: string;
    }>;
    upsertMessage: any;
    appPatch: (patchCreate: WAPatchCreate) => Promise<void>;
    sendPresenceUpdate: (type: WAPresence, toJid?: string) => Promise<void>;
    presenceSubscribe: (toJid: string) => Promise<void>;
    profilePictureUrl: (jid: string, type?: "preview" | "image", timeoutMs?: number) => Promise<string | undefined>;
    getBlocklist: ({ dhash }?: {
        dhash?: string;
    }) => Promise<BlocklistResult | undefined>;
    fetchBlocklist: () => Promise<string[]>;
    fetchStatus: (...jids: string[]) => Promise<import("../WAUSync/index.js").USyncQueryResultList[] | undefined>;
    fetchDisappearingDuration: (...jids: string[]) => Promise<import("../WAUSync/index.js").USyncQueryResultList[] | undefined>;
    updateProfilePicture: (jid: string, content: WAMediaUpload, dimensions?: {
        width: number;
        height: number;
    }) => Promise<void>;
    removeProfilePicture: (jid: string) => Promise<void>;
    updateProfileStatus: (status: string | StatusUpdateInput) => Promise<void>;
    updateProfileName: (name: string) => Promise<void>;
    updateBlockStatus: (jid: string, action: "block" | "unblock", identity?: BlocklistIdentity, opts?: {
        dhash?: string;
    }) => Promise<void>;
    updateDisableLinkPreviewsPrivacy: (isPreviewsDisabled: boolean) => Promise<void>;
    updateCallPrivacy: (value: WAPrivacyCallValue) => Promise<void>;
    updateMessagesPrivacy: (value: WAPrivacyMessagesValue) => Promise<void>;
    updateLastSeenPrivacy: (value: WAPrivacyValue) => Promise<void>;
    updateOnlinePrivacy: (value: WAPrivacyOnlineValue) => Promise<void>;
    updateProfilePicturePrivacy: (value: WAPrivacyValue) => Promise<void>;
    updateStatusPrivacy: (value: WAPrivacyValue) => Promise<void>;
    updateReadReceiptsPrivacy: (value: WAReadReceiptsValue) => Promise<void>;
    updateGroupsAddPrivacy: (value: WAPrivacyGroupAddValue) => Promise<void>;
    updateDefaultDisappearingMode: (duration: number) => Promise<void>;
    getBusinessProfile: (jid: string) => Promise<WABusinessProfile | void>;
    resyncAppState: any;
    chatModify: (mod: ChatModification, jid: string) => Promise<void>;
    cleanDirtyBits: (type: "account_sync" | "groups", fromTimestamp?: number | string) => Promise<void>;
    addOrEditContact: (jid: string, contact: proto.SyncActionValue.IContactAction) => Promise<void>;
    removeContact: (jid: string) => Promise<void>;
    addLabel: (jid: string, labels: LabelActionBody) => Promise<void>;
    addChatLabel: (jid: string, labelId: string) => Promise<void>;
    removeChatLabel: (jid: string, labelId: string) => Promise<void>;
    addMessageLabel: (jid: string, messageId: string, labelId: string) => Promise<void>;
    removeMessageLabel: (jid: string, messageId: string, labelId: string) => Promise<void>;
    star: (jid: string, messages: {
        id: string;
        fromMe?: boolean;
    }[], star: boolean) => Promise<void>;
    addOrEditQuickReply: (quickReply: QuickReplyAction) => Promise<void>;
    removeQuickReply: (timestamp: string) => Promise<void>;
    type: "md";
    ws: import("./Client/websocket.js").WebSocketClient;
    ev: any;
    authState: {
        creds: import("../Types/index.js").AuthenticationCreds;
        keys: import("../Types/index.js").SignalKeyStoreWithTransaction;
    };
    signalRepository: import("../Types/index.js").SignalRepositoryWithLIDStore;
    logger: import("../Utils/logger.js").ILogger;
    user: import("../Types/index.js").Contact | undefined;
    generateMessageTag: () => string;
    query: (node: BinaryNode, timeoutMs?: number) => Promise<any>;
    waitForMessage: <T>(msgId: string, timeoutMs?: number | undefined) => Promise<T | undefined>;
    waitForSocketOpen: () => Promise<void>;
    sendRawMessage: (data: Uint8Array | Buffer) => Promise<void>;
    sendNode: (frame: BinaryNode) => Promise<void>;
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
    wamBuffer: import("../index.js").BinaryInfo;
    waitForConnectionUpdate: (check: (u: Partial<import("../Types/index.js").ConnectionState>) => Promise<boolean | undefined>, timeoutMs?: number) => Promise<void>;
    sendWAMBuffer: (wamBuffer: Buffer) => Promise<any>;
    executeUSyncQuery: (usyncQuery: USyncQuery) => Promise<import("../WAUSync/index.js").USyncQueryResult | undefined>;
    onWhatsApp: (...phoneNumber: string[]) => Promise<{
        jid: string;
        exists: boolean;
        lid?: string;
    }[] | undefined>;
    resolveUsername: (handle: string, usernameKey?: string) => Promise<import("../WAUSync/index.js").UsernameLookupResult>;
    updateServerTimeOffset: ({ attrs }: BinaryNode) => void;
    getServerTime: () => number;
    getUnifiedSessionId: () => string;
    disposables: (() => void)[];
};
//# sourceMappingURL=chats.d.ts.map