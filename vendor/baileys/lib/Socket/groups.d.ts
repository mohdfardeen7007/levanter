import { Boom } from '@hapi/boom';
import { proto } from '../../WAProto/index.js';
import type { GroupMetadata, ParticipantAction, SignalKeyStoreWithTransaction, SignalRepositoryWithLIDStore, SocketConfig } from '../Types/index.js';
import type { ILogger } from '../Utils/logger.js';
import { type BinaryNode } from '../WABinary/index.js';
export type GroupCreateParticipant = {
    jid: string;
    phoneNumber?: string;
    privacyToken?: Uint8Array;
};
/**
 * `<create>` payload as WA Web builds it (WASmaxOutGroupsCreateRequest / WAWebGroupCreateJob;
 * whatsmeow 8b4a8ba0 + 39b719ba + e277b766 + 0dcf1f50): no dedup `key`, LID participants
 * carry `phone_number`, explicit ephemeral / approval / member-add defaults.
 */
export declare const buildGroupCreateNode: (subject: string, participants: GroupCreateParticipant[], extraContent?: BinaryNode[]) => BinaryNode;
/** Normalizes participants, looks up the phone number of LID participants and their privacy tokens. */
export declare const resolveGroupCreateParticipants: (jids: string[], { authState, lidMapping }: {
    authState: {
        keys: SignalKeyStoreWithTransaction;
    };
    lidMapping: SignalRepositoryWithLIDStore["lidMapping"];
}) => Promise<GroupCreateParticipant[]>;
export declare const makeGroupsSocket: (config: SocketConfig) => {
    groupMetadata: (jid: string) => Promise<GroupMetadata>;
    groupCreate: (subject: string, participants: string[]) => Promise<GroupMetadata>;
    groupLeave: (id: string) => Promise<void>;
    groupUpdateSubject: (jid: string, subject: string) => Promise<void>;
    groupRequestParticipantsList: (jid: string) => Promise<{
        [key: string]: string;
    }[]>;
    groupRequestParticipantsUpdate: (jid: string, participants: string[], action: "approve" | "reject") => Promise<{
        status: string;
        jid: string | undefined;
    }[]>;
    groupParticipantsUpdate: (jid: string, participants: string[], action: ParticipantAction) => Promise<{
        status: string;
        jid: string | undefined;
        content: BinaryNode;
    }[]>;
    groupUpdateDescription: (jid: string, description?: string) => Promise<void>;
    groupInviteCode: (jid: string) => Promise<string | undefined>;
    groupRevokeInvite: (jid: string) => Promise<string | undefined>;
    groupAcceptInvite: (code: string) => Promise<string | undefined>;
    /**
     * revoke a v4 invite for someone
     * @param groupJid group jid
     * @param invitedJid jid of person you invited
     * @returns true if successful
     */
    groupRevokeInviteV4: (groupJid: string, invitedJid: string) => Promise<boolean>;
    /**
     * accept a GroupInviteMessage
     * @param key the key of the invite message, or optionally only provide the jid of the person who sent the invite
     * @param inviteMessage the message to accept
     */
    groupAcceptInviteV4: any;
    groupGetInviteInfo: (code: string) => Promise<GroupMetadata>;
    groupToggleEphemeral: (jid: string, ephemeralExpiration: number) => Promise<void>;
    groupSettingUpdate: (jid: string, setting: "announcement" | "not_announcement" | "locked" | "unlocked") => Promise<void>;
    groupMemberAddMode: (jid: string, mode: "admin_add" | "all_member_add") => Promise<void>;
    groupJoinApprovalMode: (jid: string, mode: "on" | "off") => Promise<void>;
    groupFetchAllParticipating: () => Promise<{
        [_: string]: GroupMetadata;
    }>;
    serverProps: {
        privacyTokenOn1to1: boolean;
        profilePicPrivacyToken: boolean;
        lidTrustedTokenIssueToLid: boolean;
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
    getBotListV2: () => Promise<import("../Types/index.js").BotListInfo[]>;
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
    appPatch: (patchCreate: import("../Types/index.js").WAPatchCreate) => Promise<void>;
    sendPresenceUpdate: (type: import("../Types/index.js").WAPresence, toJid?: string) => Promise<void>;
    presenceSubscribe: (toJid: string) => Promise<void>;
    profilePictureUrl: (jid: string, type?: "preview" | "image", timeoutMs?: number) => Promise<string | undefined>;
    getBlocklist: ({ dhash }?: {
        dhash?: string;
    }) => Promise<import("../Types/index.js").BlocklistResult | undefined>;
    fetchBlocklist: () => Promise<string[]>;
    fetchStatus: (...jids: string[]) => Promise<import("../index.js").USyncQueryResultList[] | undefined>;
    fetchDisappearingDuration: (...jids: string[]) => Promise<import("../index.js").USyncQueryResultList[] | undefined>;
    updateProfilePicture: (jid: string, content: import("../Types/index.js").WAMediaUpload, dimensions?: {
        width: number;
        height: number;
    }) => Promise<void>;
    removeProfilePicture: (jid: string) => Promise<void>;
    updateProfileStatus: (status: string | import("./chats.js").StatusUpdateInput) => Promise<void>;
    updateProfileName: (name: string) => Promise<void>;
    updateBlockStatus: (jid: string, action: "block" | "unblock", identity?: import("../Utils/index.js").BlocklistIdentity, opts?: {
        dhash?: string;
    }) => Promise<void>;
    updateDisableLinkPreviewsPrivacy: (isPreviewsDisabled: boolean) => Promise<void>;
    updateCallPrivacy: (value: import("../Types/index.js").WAPrivacyCallValue) => Promise<void>;
    updateMessagesPrivacy: (value: import("../Types/index.js").WAPrivacyMessagesValue) => Promise<void>;
    updateLastSeenPrivacy: (value: import("../Types/index.js").WAPrivacyValue) => Promise<void>;
    updateOnlinePrivacy: (value: import("../Types/index.js").WAPrivacyOnlineValue) => Promise<void>;
    updateProfilePicturePrivacy: (value: import("../Types/index.js").WAPrivacyValue) => Promise<void>;
    updateStatusPrivacy: (value: import("../Types/index.js").WAPrivacyValue) => Promise<void>;
    updateReadReceiptsPrivacy: (value: import("../Types/index.js").WAReadReceiptsValue) => Promise<void>;
    updateGroupsAddPrivacy: (value: import("../Types/index.js").WAPrivacyGroupAddValue) => Promise<void>;
    updateDefaultDisappearingMode: (duration: number) => Promise<void>;
    getBusinessProfile: (jid: string) => Promise<import("../Types/index.js").WABusinessProfile | void>;
    resyncAppState: any;
    chatModify: (mod: import("../Types/index.js").ChatModification, jid: string) => Promise<void>;
    cleanDirtyBits: (type: "account_sync" | "groups", fromTimestamp?: number | string) => Promise<void>;
    addOrEditContact: (jid: string, contact: proto.SyncActionValue.IContactAction) => Promise<void>;
    removeContact: (jid: string) => Promise<void>;
    addLabel: (jid: string, labels: import("../Types/Label.js").LabelActionBody) => Promise<void>;
    addChatLabel: (jid: string, labelId: string) => Promise<void>;
    removeChatLabel: (jid: string, labelId: string) => Promise<void>;
    addMessageLabel: (jid: string, messageId: string, labelId: string) => Promise<void>;
    removeMessageLabel: (jid: string, messageId: string, labelId: string) => Promise<void>;
    star: (jid: string, messages: {
        id: string;
        fromMe?: boolean;
    }[], star: boolean) => Promise<void>;
    addOrEditQuickReply: (quickReply: import("../Types/Bussines.js").QuickReplyAction) => Promise<void>;
    removeQuickReply: (timestamp: string) => Promise<void>;
    type: "md";
    ws: import("./Client/websocket.js").WebSocketClient;
    ev: any;
    authState: {
        creds: import("../Types/index.js").AuthenticationCreds;
        keys: SignalKeyStoreWithTransaction;
    };
    signalRepository: SignalRepositoryWithLIDStore;
    logger: ILogger;
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
    executeUSyncQuery: (usyncQuery: import("../index.js").USyncQuery) => Promise<import("../index.js").USyncQueryResult | undefined>;
    onWhatsApp: (...phoneNumber: string[]) => Promise<{
        jid: string;
        exists: boolean;
        lid?: string;
    }[] | undefined>;
    resolveUsername: (handle: string, usernameKey?: string) => Promise<import("../index.js").UsernameLookupResult>;
    updateServerTimeOffset: ({ attrs }: BinaryNode) => void;
    getServerTime: () => number;
    getUnifiedSessionId: () => string;
    disposables: (() => void)[];
};
export declare const extractGroupMetadata: (result: BinaryNode, sock?: {
    signalRepository: SignalRepositoryWithLIDStore;
    logger: ILogger;
}) => GroupMetadata;
export type GroupsSocket = ReturnType<typeof makeGroupsSocket>;
//# sourceMappingURL=groups.d.ts.map