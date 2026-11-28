import { proto } from '../../WAProto/index.js';
import type { AuthenticationCreds, BaileysEventEmitter, CacheStore, SignalKeyStoreWithTransaction, SignalRepositoryWithLIDStore, SocketConfig, WAMessage, WAMessageKey } from '../Types/index.js';
import type { ILogger } from './logger.js';
type ProcessMessageContext = {
    shouldProcessHistoryMsg: boolean;
    /** emit the statuses of an INITIAL_STATUS_V3 history chunk (SocketConfig.processHistoryStatuses) */
    processHistoryStatuses?: boolean;
    placeholderResendCache?: CacheStore;
    creds: AuthenticationCreds;
    keyStore: SignalKeyStoreWithTransaction;
    ev: BaileysEventEmitter;
    logger?: ILogger;
    options: RequestInit;
    signalRepository: SignalRepositoryWithLIDStore;
    getMessage: SocketConfig['getMessage'];
    /** called once a downloaded history chunk has been processed (so its blob can be deleted) */
    onHistorySyncBlobProcessed?: (notification: proto.Message.IHistorySyncNotification) => void;
};
/** Cleans a received message to further processing */
export declare const cleanMessage: (message: WAMessage, meId: string, meLid: string) => void;
export declare const isRealMessage: (message: WAMessage) => boolean;
export declare const shouldIncrementChatUnread: (message: WAMessage) => boolean;
/**
 * Get the ID of the chat from the given key.
 * Typically -- that'll be the remoteJid, but for broadcasts, it'll be the participant
 */
export declare const getChatId: ({ remoteJid, participant, fromMe }: WAMessageKey) => string;
type PollContext = {
    /** normalised jid of the person that created the poll */
    pollCreatorJid: string;
    /** ID of the poll creation message */
    pollMsgId: string;
    /** poll creation message enc key */
    pollEncKey: Uint8Array;
    /** jid of the person that voted */
    voterJid: string;
};
type EventContext = {
    /** normalised jid of the person that created the event */
    eventCreatorJid: string;
    /** ID of the event creation message */
    eventMsgId: string;
    /** event creation message enc key */
    eventEncKey: Uint8Array;
    /** jid of the person that responded */
    responderJid: string;
};
/**
 * Decrypt a poll vote
 * @param vote encrypted vote
 * @param ctx additional info about the poll required for decryption
 * @returns list of SHA256 options
 */
export declare function decryptPollVote({ encPayload, encIv }: proto.Message.IPollEncValue, { pollCreatorJid, pollMsgId, pollEncKey, voterJid }: PollContext): proto.Message.PollVoteMessage;
/**
 * Decrypt an event response
 * @param response encrypted event response
 * @param ctx additional info about the event required for decryption
 * @returns event response message
 */
export declare function decryptEventResponse({ encPayload, encIv }: proto.Message.IPollEncValue, { eventCreatorJid, eventMsgId, eventEncKey, responderJid }: EventContext): proto.Message.EventResponseMessage;
type AddonLidMapping = {
    getLIDForPN(pn: string): Promise<string | null | undefined>;
    getPNForLID(lid: string): Promise<string | null | undefined>;
};
/**
 * WA Web WAWebAddonEncryption.decryptAddOn (zapo 0e921e3f): an addon (poll vote, event
 * response, …) may be sealed with either identity of the original author and the addon
 * sender — try both as LIDs, then both as PNs, then exactly as received.
 */
export declare const decryptAddonWithFallback: <T>(originalSender: string, addonSender: string, lidMapping: AddonLidMapping, tryDecrypt: (originalSenderJid: string, addonSenderJid: string) => T) => Promise<T>;
type SecretEncMsgContext = {
    getMessage: SocketConfig['getMessage'];
    signalRepository: SignalRepositoryWithLIDStore;
    /** our own pn jid */
    meId: string;
    /** our own lid jid */
    meLid?: string;
    /** key of the incoming secret encrypted message */
    msgKey: WAMessageKey;
    logger?: ILogger;
};
/**
 * Decrypt a SecretEncryptedMessage (e.g. a MESSAGE_EDIT / EVENT_EDIT) into the underlying message.
 * Fetches the original (target) message via getMessage to obtain its messageSecret.
 * Returns undefined (and logs) when the type is unsupported or the original secret is unavailable.
 */
export declare function decryptSecretEncryptedMessage(secretMsg: proto.Message.ISecretEncryptedMessage, { getMessage, signalRepository, meId, meLid, msgKey, logger }: SecretEncMsgContext): Promise<proto.IMessage | undefined>;
/**
 * Rewrite a decrypted MESSAGE_EDIT payload into the plaintext `protocolMessage`
 * MESSAGE_EDIT shape, so it flows through the existing edit path and emits
 * `messages.update` (updating the original) instead of surfacing as a new message.
 * Non-edit payloads (e.g. EVENT_EDIT) and already-wrapped ones pass through unchanged.
 */
export declare const rewriteSecretEditToProtocolMessage: (decrypted: proto.IMessage, secretMsg: proto.Message.ISecretEncryptedMessage) => proto.IMessage;
declare const processMessage: (message: WAMessage, { shouldProcessHistoryMsg, processHistoryStatuses, placeholderResendCache, ev, creds, signalRepository, keyStore, logger, options, getMessage, onHistorySyncBlobProcessed }: ProcessMessageContext) => Promise<void>;
export default processMessage;
//# sourceMappingURL=process-message.d.ts.map