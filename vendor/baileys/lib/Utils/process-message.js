import { Boom } from '@hapi/boom';
import { proto } from '../../WAProto/index.js';
import { WAMessageStubType } from '../Types/index.js';
import { getContentType, normalizeMessageContent } from '../Utils/messages.js';
import { areJidsSameUser, isHostedLidUser, isHostedPnUser, isJidBroadcast, isJidStatusBroadcast, isLidUser, jidDecode, jidEncode, jidNormalizedUser } from '../WABinary/index.js';
import { aesDecryptGCM, hmacSign, toMessageSecretBytes } from './crypto.js';
import { getKeyAuthor, toNumber } from './generics.js';
import { downloadAndProcessHistorySyncNotification, shouldDeleteHistorySyncBlob } from './history.js';
/**
 * The participants a group stub names: each parameter is the JSON the group notification handler writes
 * (messages-recv handleGroupNotification). A bare JID, as a number change got before or a stub from elsewhere may
 * carry, is taken as the participant's id: JSON.parse threw on it and took the rest of the notification down.
 */
const stubParticipants = (params) => (params || []).map(param => {
    try {
        const parsed = JSON.parse(param);
        if (parsed && typeof parsed === 'object' && typeof parsed.id === 'string') {
            return parsed;
        }
    }
    catch {
        // a bare JID
    }
    return { id: param };
});
/** the requester a join-request stub names: the JSON handleGroupNotification writes, or a bare LID */
const stubJoinRequester = (param) => {
    if (!param) {
        return undefined;
    }
    try {
        const parsed = JSON.parse(param);
        if (parsed && typeof parsed === 'object' && typeof parsed.lid === 'string') {
            return parsed;
        }
    }
    catch {
        // a bare JID
    }
    return { lid: param, pn: undefined };
};
const REAL_MSG_STUB_TYPES = new Set([
    WAMessageStubType.CALL_MISSED_GROUP_VIDEO,
    WAMessageStubType.CALL_MISSED_GROUP_VOICE,
    WAMessageStubType.CALL_MISSED_VIDEO,
    WAMessageStubType.CALL_MISSED_VOICE
]);
const REAL_MSG_REQ_ME_STUB_TYPES = new Set([WAMessageStubType.GROUP_PARTICIPANT_ADD]);
/** Cleans a received message to further processing */
export const cleanMessage = (message, meId, meLid) => {
    // ensure remoteJid and participant doesn't have device or agent in it
    if (isHostedPnUser(message.key.remoteJid) || isHostedLidUser(message.key.remoteJid)) {
        message.key.remoteJid = jidEncode(jidDecode(message.key?.remoteJid)?.user, isHostedPnUser(message.key.remoteJid) ? 's.whatsapp.net' : 'lid');
    }
    else {
        message.key.remoteJid = jidNormalizedUser(message.key.remoteJid);
    }
    if (isHostedPnUser(message.key.participant) || isHostedLidUser(message.key.participant)) {
        message.key.participant = jidEncode(jidDecode(message.key.participant)?.user, isHostedPnUser(message.key.participant) ? 's.whatsapp.net' : 'lid');
    }
    else {
        message.key.participant = jidNormalizedUser(message.key.participant);
    }
    const content = normalizeMessageContent(message.message);
    // if the message has a reaction, ensure fromMe & remoteJid are from our perspective
    if (content?.reactionMessage) {
        normaliseKey(content.reactionMessage.key);
    }
    if (content?.pollUpdateMessage) {
        normaliseKey(content.pollUpdateMessage.pollCreationMessageKey);
    }
    function normaliseKey(msgKey) {
        // if the reaction is from another user
        // we've to correctly map the key to this user's perspective
        if (!message.key.fromMe) {
            // if the sender believed the message being reacted to is not from them
            // we've to correct the key to be from them, or some other participant
            msgKey.fromMe = !msgKey.fromMe
                ? areJidsSameUser(msgKey.participant || msgKey.remoteJid, meId) ||
                    areJidsSameUser(msgKey.participant || msgKey.remoteJid, meLid)
                : // if the message being reacted to, was from them
                    // fromMe automatically becomes false
                    false;
            // set the remoteJid to being the same as the chat the message came from
            // TODO: investigate inconsistencies
            msgKey.remoteJid = message.key.remoteJid;
            // set participant of the message
            msgKey.participant = msgKey.participant || message.key.participant;
        }
    }
};
// TODO: target:audit AUDIT THIS FUNCTION AGAIN
export const isRealMessage = (message) => {
    const normalizedContent = normalizeMessageContent(message.message);
    const hasSomeContent = !!getContentType(normalizedContent);
    return ((!!normalizedContent ||
        REAL_MSG_STUB_TYPES.has(message.messageStubType) ||
        REAL_MSG_REQ_ME_STUB_TYPES.has(message.messageStubType)) &&
        hasSomeContent &&
        !normalizedContent?.protocolMessage &&
        !normalizedContent?.reactionMessage &&
        !normalizedContent?.pollUpdateMessage &&
        // an undecryptable secret envelope (e.g. an edit whose original we couldn't find)
        // must not surface as a phantom empty message
        !normalizedContent?.secretEncryptedMessage);
};
export const shouldIncrementChatUnread = (message) => !message.key.fromMe && !message.messageStubType;
/**
 * Get the ID of the chat from the given key.
 * Typically -- that'll be the remoteJid, but for broadcasts, it'll be the participant
 */
export const getChatId = ({ remoteJid, participant, fromMe }) => {
    if (!remoteJid) {
        throw new Boom('Cannot derive chat id: message key is missing remoteJid', {
            data: { remoteJid, participant, fromMe }
        });
    }
    if (isJidBroadcast(remoteJid) && !isJidStatusBroadcast(remoteJid) && !fromMe) {
        if (!participant) {
            throw new Boom('Cannot derive chat id: broadcast message key is missing participant', {
                data: { remoteJid, fromMe }
            });
        }
        return participant;
    }
    return remoteJid;
};
/**
 * Decrypt a poll vote
 * @param vote encrypted vote
 * @param ctx additional info about the poll required for decryption
 * @returns list of SHA256 options
 */
export function decryptPollVote({ encPayload, encIv }, { pollCreatorJid, pollMsgId, pollEncKey, voterJid }) {
    const sign = Buffer.concat([
        toBinary(pollMsgId),
        toBinary(pollCreatorJid),
        toBinary(voterJid),
        toBinary('Poll Vote'),
        new Uint8Array([1])
    ]);
    const key0 = hmacSign(pollEncKey, new Uint8Array(32), 'sha256');
    const decKey = hmacSign(sign, key0, 'sha256');
    const aad = toBinary(`${pollMsgId}\u0000${voterJid}`);
    const decrypted = aesDecryptGCM(encPayload, decKey, encIv, aad);
    return proto.Message.PollVoteMessage.decode(decrypted);
    function toBinary(txt) {
        return Buffer.from(txt);
    }
}
/**
 * Decrypt an event response
 * @param response encrypted event response
 * @param ctx additional info about the event required for decryption
 * @returns event response message
 */
export function decryptEventResponse({ encPayload, encIv }, { eventCreatorJid, eventMsgId, eventEncKey, responderJid }) {
    const sign = Buffer.concat([
        toBinary(eventMsgId),
        toBinary(eventCreatorJid),
        toBinary(responderJid),
        toBinary('Event Response'),
        new Uint8Array([1])
    ]);
    const key0 = hmacSign(eventEncKey, new Uint8Array(32), 'sha256');
    const decKey = hmacSign(sign, key0, 'sha256');
    const aad = toBinary(`${eventMsgId}\u0000${responderJid}`);
    const decrypted = aesDecryptGCM(encPayload, decKey, encIv, aad);
    return proto.Message.EventResponseMessage.decode(decrypted);
    function toBinary(txt) {
        return Buffer.from(txt);
    }
}
/**
 * WA Web WAWebAddonEncryption.decryptAddOn (zapo 0e921e3f): an addon (poll vote, event
 * response, …) may be sealed with either identity of the original author and the addon
 * sender — try both as LIDs, then both as PNs, then exactly as received.
 */
export const decryptAddonWithFallback = async (originalSender, addonSender, lidMapping, tryDecrypt) => {
    const original = jidNormalizedUser(originalSender);
    const addon = jidNormalizedUser(addonSender);
    // a failed lookup (e.g. a timed-out usync) only skips its rung; the pair as received is still tried
    const lookup = async (resolve) => {
        try {
            return await resolve();
        }
        catch {
            return undefined;
        }
    };
    const toLid = async (jid) => (isLidUser(jid) ? jid : await lookup(() => lidMapping.getLIDForPN(jid)));
    const toPn = async (jid) => (isLidUser(jid) ? await lookup(() => lidMapping.getPNForLID(jid)) : jid);
    const [originalLid, addonLid, originalPn, addonPn] = await Promise.all([
        toLid(original),
        toLid(addon),
        toPn(original),
        toPn(addon)
    ]);
    const attempts = [];
    if (originalLid && addonLid) {
        attempts.push([jidNormalizedUser(originalLid), jidNormalizedUser(addonLid)]);
    }
    if (originalPn && addonPn) {
        attempts.push([jidNormalizedUser(originalPn), jidNormalizedUser(addonPn)]);
    }
    attempts.push([original, addon]);
    let lastError;
    for (const [originalJid, addonJid] of attempts) {
        try {
            return await tryDecrypt(originalJid, addonJid);
        }
        catch (error) {
            lastError = error;
        }
    }
    throw lastError;
};
/** maps the secret enc type to whatsmeow's modification-type string used in key derivation */
function getSecretEncModType(type) {
    switch (type) {
        case proto.Message.SecretEncryptedMessage.SecretEncType.MESSAGE_EDIT:
            return 'Message Edit';
        case proto.Message.SecretEncryptedMessage.SecretEncType.EVENT_EDIT:
            return 'Event Edit';
        default:
            return undefined;
    }
}
/**
 * Decrypt the payload of a SecretEncryptedMessage given the derivation inputs.
 * Mirrors whatsmeow's generateMsgSecretKey (HKDF-SHA256) with an empty AAD.
 */
function decryptSecretPayload({ encPayload, encIv }, origMsgId, origMsgSender, modSender, modType, messageSecret) {
    // info = origMsgId + origMsgSender + modSender + modType, then the HKDF counter byte
    const sign = Buffer.concat([
        Buffer.from(origMsgId),
        Buffer.from(origMsgSender),
        Buffer.from(modSender),
        Buffer.from(modType),
        new Uint8Array([1])
    ]);
    // HKDF-SHA256(messageSecret, salt=zeros, info=sign) — extract then expand
    const key0 = hmacSign(messageSecret, new Uint8Array(32), 'sha256');
    const decKey = hmacSign(sign, key0, 'sha256');
    // edit types use an empty additionalData (unlike poll vote / event response)
    const decrypted = aesDecryptGCM(encPayload, decKey, encIv, Buffer.alloc(0));
    return proto.Message.decode(decrypted);
}
/**
 * Decrypt a SecretEncryptedMessage (e.g. a MESSAGE_EDIT / EVENT_EDIT) into the underlying message.
 * Fetches the original (target) message via getMessage to obtain its messageSecret.
 * Returns undefined (and logs) when the type is unsupported or the original secret is unavailable.
 */
export async function decryptSecretEncryptedMessage(secretMsg, { getMessage, signalRepository, meId, meLid, msgKey, logger }) {
    const modType = getSecretEncModType(secretMsg.secretEncType);
    if (!modType) {
        logger?.warn({ secretEncType: secretMsg.secretEncType }, 'unsupported secretEncType, cannot decrypt secret message');
        return undefined;
    }
    const targetKey = secretMsg.targetMessageKey;
    if (!targetKey?.id) {
        logger?.warn('secret message missing targetMessageKey, cannot decrypt');
        return undefined;
    }
    // look up the original message by id + chat, matching how getMessage is called elsewhere
    const origMsg = await getMessage({ id: targetKey.id, remoteJid: targetKey.remoteJid || msgKey.remoteJid });
    const messageSecret = toMessageSecretBytes(origMsg?.messageContextInfo?.messageSecret);
    if (!messageSecret) {
        logger?.warn({ targetKey, foundOrigMsg: !!origMsg, hasSecret: !!origMsg?.messageContextInfo?.messageSecret }, 'original message secret not found, cannot decrypt secret message');
        return undefined;
    }
    const meIdNormalised = jidNormalizedUser(meId);
    const meLidNormalised = meLid ? jidNormalizedUser(meLid) : undefined;
    // returns the jid plus its LID/PN counterpart (so we can tolerate the ongoing LID migration,
    // where the sender used to derive the key may be addressed as either form)
    const withAlt = async (jid) => {
        if (!jid) {
            return [];
        }
        const norm = jidNormalizedUser(jid);
        const out = [norm];
        // self: use the known own LID rather than a mapping lookup (which usually doesn't map self)
        if (norm === meIdNormalised && meLidNormalised) {
            out.push(meLidNormalised);
        }
        else if (norm === meLidNormalised) {
            out.push(meIdNormalised);
        }
        else {
            try {
                const alt = isLidUser(norm)
                    ? await signalRepository.lidMapping.getPNForLID(norm)
                    : await signalRepository.lidMapping.getLIDForPN(norm);
                if (alt) {
                    out.push(jidNormalizedUser(alt));
                }
            }
            catch { }
        }
        return [...new Set(out)];
    };
    const modSenders = await withAlt(getKeyAuthor(msgKey, meIdNormalised));
    // original message sender (mirrors whatsmeow getOrigSenderFromKey):
    // fromMe => same user that sent the edit; group => participant; 1:1 => remoteJid
    const origSenders = targetKey.fromMe
        ? modSenders
        : await withAlt(targetKey.participant || targetKey.remoteJid);
    // try every LID/PN combination of (origSender, modSender) until one authenticates
    for (const origSender of origSenders) {
        for (const modSender of modSenders) {
            try {
                const decoded = decryptSecretPayload(secretMsg, targetKey.id, origSender, modSender, modType, messageSecret);
                // Edits (message + event) are always sealed with the ORIGINAL message's secret,
                // keyed by the original id. Editing an edit targets the same original again, so the
                // secret must remain retrievable via getMessage(originalId) for the whole chain.
                // Consumer stores commonly overwrite the original's content on edit (losing the
                // secret), so stamp it back onto the decrypted payload — whatever gets persisted
                // under the original id then still carries the secret for the next edit.
                if (!decoded.messageContextInfo?.messageSecret) {
                    decoded.messageContextInfo = { ...decoded.messageContextInfo, messageSecret };
                }
                return decoded;
            }
            catch { }
        }
    }
    logger?.warn({ targetKey, origSenders, modSenders, secretLen: messageSecret.length }, 'failed to decrypt secret message');
    return undefined;
}
/**
 * Rewrite a decrypted MESSAGE_EDIT payload into the plaintext `protocolMessage`
 * MESSAGE_EDIT shape, so it flows through the existing edit path and emits
 * `messages.update` (updating the original) instead of surfacing as a new message.
 * Non-edit payloads (e.g. EVENT_EDIT) and already-wrapped ones pass through unchanged.
 */
export const rewriteSecretEditToProtocolMessage = (decrypted, secretMsg) => {
    if (secretMsg.secretEncType !== proto.Message.SecretEncryptedMessage.SecretEncType.MESSAGE_EDIT ||
        decrypted.protocolMessage ||
        !secretMsg.targetMessageKey) {
        return decrypted;
    }
    const wrapped = proto.Message.fromObject({
        protocolMessage: {
            key: secretMsg.targetMessageKey,
            type: proto.Message.ProtocolMessage.Type.MESSAGE_EDIT,
            editedMessage: decrypted
        }
    });
    // keep the secret reachable on the wrapper too (stores that persist the whole
    // message object under the original id round-trip it for the next edit)
    if (decrypted.messageContextInfo && !wrapped.messageContextInfo) {
        wrapped.messageContextInfo = decrypted.messageContextInfo;
    }
    return wrapped;
};
const processMessage = async (message, { shouldProcessHistoryMsg, processHistoryStatuses, placeholderResendCache, ev, creds, signalRepository, keyStore, logger, options, getMessage, onHistorySyncBlobProcessed }) => {
    const meId = creds.me.id;
    const { accountSettings } = creds;
    const chat = { id: jidNormalizedUser(getChatId(message.key)) };
    const isRealMsg = isRealMessage(message);
    if (isRealMsg) {
        chat.messages = [{ message }];
        chat.conversationTimestamp = toNumber(message.messageTimestamp);
        // only increment unread count if not CIPHERTEXT and from another person
        if (shouldIncrementChatUnread(message)) {
            chat.unreadCount = (chat.unreadCount || 0) + 1;
        }
    }
    const content = normalizeMessageContent(message.message);
    // unarchive chat if it's a real message, or someone reacted to our message
    // and we've the unarchive chats setting on
    if ((isRealMsg || content?.reactionMessage?.key?.fromMe) && accountSettings?.unarchiveChats) {
        chat.archived = false;
        chat.readOnly = false;
    }
    const protocolMsg = content?.protocolMessage;
    if (protocolMsg) {
        // Mirror whatsmeow's `handleProtocolMessage` guard, but applied only to
        // the protocol message types that originate from our own device — an
        // attacker could otherwise spoof any of these to manipulate local state.
        //
        // Self-only types (drop if `!fromMe`):
        //   - HISTORY_SYNC_NOTIFICATION                 (our phone driving history sync)
        //   - APP_STATE_SYNC_KEY_SHARE                  (key share between our devices)
        //   - LID_MIGRATION_MAPPING_SYNC                (server-initiated via our phone)
        //   - PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE (response from our phone to our PDO request)
        //
        // Cross-user types (must NOT be dropped — legitimately arrive from others):
        //   - REVOKE
        //   - MESSAGE_EDIT
        //   - EPHEMERAL_SETTING
        //   - GROUP_MEMBER_LABEL_CHANGE
        //
        // See https://github.com/tulir/whatsmeow/blob/8d3700152a/message.go#L842-L845
        // for the reference architecture — whatsmeow's `handleProtocolMessage`
        // only contains self-only types because edits are unwrapped from
        // `EditedMessage` BEFORE this dispatch and revokes aren't routed here.
        const SELF_ONLY_TYPES = new Set([
            proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
            proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE,
            proto.Message.ProtocolMessage.Type.LID_MIGRATION_MAPPING_SYNC,
            proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE
        ]);
        if (protocolMsg.type !== null &&
            protocolMsg.type !== undefined &&
            SELF_ONLY_TYPES.has(protocolMsg.type) &&
            !message.key.fromMe) {
            logger?.warn({ msgId: message.key.id, type: protocolMsg.type, from: message.key.participant || message.key.remoteJid }, 'dropping spoofed self-only protocolMessage from non-self origin');
            return;
        }
        switch (protocolMsg.type) {
            case proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION:
                const histNotification = protocolMsg.historySyncNotification;
                const process = shouldProcessHistoryMsg;
                const isLatest = !creds.processedHistoryMessages?.length;
                logger?.info({
                    histNotification,
                    process,
                    id: message.key.id,
                    isLatest
                }, 'got history notification');
                if (process) {
                    // TODO: investigate
                    if (histNotification.syncType !== proto.HistorySync.HistorySyncType.ON_DEMAND) {
                        ev.emit('creds.update', {
                            processedHistoryMessages: [
                                ...(creds.processedHistoryMessages || []),
                                { key: message.key, messageTimestamp: message.messageTimestamp }
                            ]
                        });
                    }
                    const data = await downloadAndProcessHistorySyncNotification(histNotification, options, logger, {
                        includeStatuses: processHistoryStatuses
                    });
                    logger?.info({ syncType: data.syncType, hasNctSalt: !!data.nctSalt?.length, chats: data.chats?.length }, 'processed history sync');
                    // Capture the account NCT salt from the bootstrap (WA Web storeNctSaltFromHistorySync).
                    // Persist only when it actually changes; it gates cstoken and the 463 reach-out lock.
                    const currentSalt = creds.nctSalt;
                    const saltChanged = !!data.nctSalt?.length &&
                        (!currentSalt?.length || Buffer.compare(Buffer.from(currentSalt), Buffer.from(data.nctSalt)) !== 0);
                    if (saltChanged) {
                        logger?.info({ saltLen: data.nctSalt.length }, 'captured NCT salt from history sync');
                        ev.emit('creds.update', { nctSalt: data.nctSalt });
                    }
                    // Capture the companion meta nonce from the bootstrap (whatsmeow storeCompanionMetaNonce).
                    // Persist only when it actually changes; it is sent as Companion_User_Secret on media delete.
                    if (data.companionMetaNonce && data.companionMetaNonce !== creds.companionMetaNonce) {
                        logger?.info('captured companion meta nonce from history sync');
                        ev.emit('creds.update', { companionMetaNonce: data.companionMetaNonce });
                    }
                    if (data.lidPnMappings?.length) {
                        logger?.debug({ count: data.lidPnMappings.length }, 'processing LID-PN mappings from history sync');
                        await signalRepository.lidMapping
                            .storeLIDPNMappings(data.lidPnMappings)
                            .catch(err => logger?.warn({ err }, 'failed to store LID-PN mappings from history sync'));
                    }
                    if (data.tcTokens?.length) {
                        logger?.debug({ count: data.tcTokens.length }, 'processing tcTokens from history sync');
                        for (const token of data.tcTokens) {
                            let storageJid = token.jid;
                            if (!isLidUser(storageJid)) {
                                const lid = await signalRepository.lidMapping.getLIDForPN(storageJid);
                                if (lid)
                                    storageJid = lid;
                            }
                            const existingTcData = await keyStore.get('tctoken', [storageJid]);
                            const existingEntry = existingTcData[storageJid];
                            const existingTs = existingEntry?.timestamp ? Number(existingEntry.timestamp) : 0;
                            const incomingTs = token.timestamp;
                            if (existingTs > 0 && incomingTs > 0 && existingTs > incomingTs) {
                                continue;
                            }
                            await keyStore.set({
                                tctoken: {
                                    [storageJid]: {
                                        ...existingEntry,
                                        token: Buffer.from(token.token),
                                        timestamp: token.timestamp.toString(),
                                        senderTimestamp: toNumber(token.senderTimestamp)
                                    }
                                }
                            });
                        }
                    }
                    ev.emit('messaging-history.set', {
                        ...data,
                        isLatest: histNotification.syncType !== proto.HistorySync.HistorySyncType.ON_DEMAND ? isLatest : undefined,
                        chunkOrder: histNotification.chunkOrder,
                        peerDataRequestSessionId: histNotification.peerDataRequestSessionId
                    });
                    if (shouldDeleteHistorySyncBlob(histNotification)) {
                        onHistorySyncBlobProcessed?.(histNotification);
                    }
                }
                break;
            case proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE:
                const keys = protocolMsg.appStateSyncKeyShare.keys;
                if (keys?.length) {
                    let newAppStateSyncKeyId = '';
                    await keyStore.transaction(async () => {
                        const newKeys = [];
                        for (const { keyData, keyId } of keys) {
                            const strKeyId = Buffer.from(keyId.keyId).toString('base64');
                            newKeys.push(strKeyId);
                            await keyStore.set({ 'app-state-sync-key': { [strKeyId]: keyData } });
                            newAppStateSyncKeyId = strKeyId;
                        }
                        logger?.info({ newAppStateSyncKeyId, newKeys }, 'injecting new app state sync keys');
                    }, meId);
                    ev.emit('creds.update', { myAppStateKeyId: newAppStateSyncKeyId });
                }
                else {
                    logger?.info({ protocolMsg }, 'recv app state sync with 0 keys');
                }
                break;
            case proto.Message.ProtocolMessage.Type.REVOKE:
                ev.emit('messages.update', [
                    {
                        key: {
                            ...message.key,
                            id: protocolMsg.key.id
                        },
                        update: { message: null, messageStubType: WAMessageStubType.REVOKE, key: message.key }
                    }
                ]);
                break;
            case proto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING:
                Object.assign(chat, {
                    ephemeralSettingTimestamp: toNumber(message.messageTimestamp),
                    ephemeralExpiration: protocolMsg.ephemeralExpiration || null
                });
                break;
            case proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE:
                const response = protocolMsg.peerDataOperationRequestResponseMessage;
                if (response) {
                    // TODO: IMPLEMENT HISTORY SYNC ETC (sticker uploads etc.).
                    const peerDataOperationResult = response.peerDataOperationResult || [];
                    for (const result of peerDataOperationResult) {
                        const retryResponse = result?.placeholderMessageResendResponse;
                        //eslint-disable-next-line max-depth
                        if (!retryResponse?.webMessageInfoBytes) {
                            continue;
                        }
                        //eslint-disable-next-line max-depth
                        try {
                            const webMessageInfo = proto.WebMessageInfo.decode(retryResponse.webMessageInfoBytes);
                            const msgId = webMessageInfo.key?.id;
                            // Retrieve cached original message data (preserves LID details,
                            // timestamps, etc. that the phone may omit in its PDO response)
                            const cachedData = msgId
                                ? await placeholderResendCache?.get(msgId)
                                : undefined;
                            //eslint-disable-next-line max-depth
                            if (msgId) {
                                await placeholderResendCache?.del(msgId);
                            }
                            let finalMsg;
                            //eslint-disable-next-line max-depth
                            if (cachedData && typeof cachedData === 'object') {
                                // Apply decoded message content onto cached metadata (preserves LID etc.)
                                cachedData.message = webMessageInfo.message;
                                //eslint-disable-next-line max-depth
                                if (webMessageInfo.messageTimestamp) {
                                    cachedData.messageTimestamp = webMessageInfo.messageTimestamp;
                                }
                                finalMsg = cachedData;
                            }
                            else {
                                finalMsg = webMessageInfo;
                            }
                            logger?.debug({ msgId, requestId: response.stanzaId }, 'received placeholder resend');
                            ev.emit('messages.upsert', {
                                messages: [finalMsg],
                                type: 'notify',
                                requestId: response.stanzaId
                            });
                        }
                        catch (err) {
                            logger?.warn({ err, stanzaId: response.stanzaId }, 'failed to decode placeholder resend response');
                        }
                    }
                }
                break;
            case proto.Message.ProtocolMessage.Type.MESSAGE_EDIT:
                ev.emit('messages.update', [
                    {
                        // flip the sender / fromMe properties because they're in the perspective of the sender
                        key: { ...message.key, id: protocolMsg.key?.id },
                        update: {
                            message: {
                                editedMessage: {
                                    message: protocolMsg.editedMessage
                                }
                            },
                            messageTimestamp: protocolMsg.timestampMs
                                ? Math.floor(toNumber(protocolMsg.timestampMs) / 1000)
                                : message.messageTimestamp
                        }
                    }
                ]);
                break;
            case proto.Message.ProtocolMessage.Type.GROUP_MEMBER_LABEL_CHANGE:
                const labelAssociationMsg = protocolMsg.memberLabel;
                if (labelAssociationMsg?.label) {
                    ev.emit('group.member-tag.update', {
                        groupId: chat.id,
                        label: labelAssociationMsg.label,
                        participant: message.key.participant,
                        participantAlt: message.key.participantAlt,
                        messageTimestamp: Number(message.messageTimestamp)
                    });
                }
                break;
            case proto.Message.ProtocolMessage.Type.LID_MIGRATION_MAPPING_SYNC:
                const encodedPayload = protocolMsg.lidMigrationMappingSyncMessage?.encodedMappingPayload;
                const { pnToLidMappings, chatDbMigrationTimestamp } = proto.LIDMigrationMappingSyncPayload.decode(encodedPayload);
                logger?.debug({ pnToLidMappings, chatDbMigrationTimestamp }, 'got lid mappings and chat db migration timestamp');
                if (pnToLidMappings) {
                    for (const { pn, latestLid, assignedLid } of pnToLidMappings) {
                        const lid = latestLid || assignedLid;
                        if (lid) {
                            ev.emit('lid-mapping.update', { pn: pn.toString(), lid: lid.toString() });
                        }
                    }
                }
                if (chatDbMigrationTimestamp) {
                    ev.emit('creds.update', { lidMigrationTimestamp: toNumber(chatDbMigrationTimestamp) });
                }
                const pairs = [];
                for (const { pn, latestLid, assignedLid } of pnToLidMappings) {
                    const lid = latestLid || assignedLid;
                    pairs.push({ lid: `${lid}@lid`, pn: `${pn}@s.whatsapp.net` });
                }
                await signalRepository.lidMapping.storeLIDPNMappings(pairs);
                if (pairs.length) {
                    for (const { pn, lid } of pairs) {
                        await signalRepository.migrateSession(pn, lid);
                    }
                }
        }
    }
    else if (content?.reactionMessage) {
        const reaction = {
            ...content.reactionMessage,
            key: message.key
        };
        ev.emit('messages.reaction', [
            {
                reaction,
                key: content.reactionMessage?.key
            }
        ]);
    }
    else if (content?.encEventResponseMessage) {
        const encEventResponse = content.encEventResponseMessage;
        const creationMsgKey = encEventResponse.eventCreationMessageKey;
        // we need to fetch the event creation message to get the event enc key
        const eventMsg = await getMessage(creationMsgKey);
        if (eventMsg) {
            try {
                const meIdNormalised = jidNormalizedUser(meId);
                const eventCreatorJid = creationMsgKey.fromMe
                    ? meIdNormalised
                    : creationMsgKey.participant || creationMsgKey.remoteJid;
                const responderJid = getKeyAuthor(message.key, meIdNormalised);
                const eventEncKey = toMessageSecretBytes(eventMsg?.messageContextInfo?.messageSecret);
                if (!eventEncKey) {
                    logger?.warn({ creationMsgKey }, 'event response: missing messageSecret for decryption');
                }
                else {
                    // WA Web decryptAddOn ladder: LID pair, PN pair, then as received (zapo 0e921e3f)
                    const responseMsg = await decryptAddonWithFallback(eventCreatorJid, responderJid, signalRepository.lidMapping, (creator, responder) => decryptEventResponse(encEventResponse, {
                        eventEncKey,
                        eventCreatorJid: creator,
                        eventMsgId: creationMsgKey.id,
                        responderJid: responder
                    }));
                    const eventResponse = {
                        eventResponseMessageKey: message.key,
                        senderTimestampMs: responseMsg.timestampMs,
                        response: responseMsg
                    };
                    ev.emit('messages.update', [
                        {
                            key: creationMsgKey,
                            update: {
                                eventResponses: [eventResponse]
                            }
                        }
                    ]);
                }
            }
            catch (err) {
                logger?.warn({ err, creationMsgKey }, 'failed to decrypt event response');
            }
        }
        else {
            logger?.warn({ creationMsgKey }, 'event creation message not found, cannot decrypt response');
        }
    }
    else if (message.messageStubType) {
        const jid = message.key?.remoteJid;
        //let actor = whatsappID (message.participant)
        let participants;
        const emitParticipantsUpdate = (action) => ev.emit('group-participants.update', {
            id: jid,
            author: message.key.participant,
            authorPn: message.key.participantAlt,
            authorUsername: message.key.participantUsername,
            participants,
            action
        });
        const emitGroupUpdate = (update) => {
            ev.emit('groups.update', [
                {
                    id: jid,
                    ...update,
                    author: message.key.participant ?? undefined,
                    authorPn: message.key.participantAlt,
                    authorUsername: message.key.participantUsername
                }
            ]);
        };
        const emitGroupRequestJoin = (participant, action, method) => {
            ev.emit('group.join-request', {
                id: jid,
                author: message.key.participant,
                authorPn: message.key.participantAlt,
                authorUsername: message.key.participantUsername,
                participant: participant.lid,
                participantPn: participant.pn,
                action,
                method: method
            });
        };
        const participantsIncludesMe = () => participants.find(jid => areJidsSameUser(meId, jid.phoneNumber)); // ADD SUPPORT FOR LID
        switch (message.messageStubType) {
            case WAMessageStubType.GROUP_PARTICIPANT_CHANGE_NUMBER:
                participants = stubParticipants(message.messageStubParameters);
                emitParticipantsUpdate('modify');
                break;
            case WAMessageStubType.GROUP_PARTICIPANT_LEAVE:
            case WAMessageStubType.GROUP_PARTICIPANT_REMOVE:
                participants = stubParticipants(message.messageStubParameters);
                emitParticipantsUpdate('remove');
                // mark the chat read only if you left the group
                if (participantsIncludesMe()) {
                    chat.readOnly = true;
                }
                break;
            case WAMessageStubType.GROUP_PARTICIPANT_ADD:
            case WAMessageStubType.GROUP_PARTICIPANT_INVITE:
            case WAMessageStubType.GROUP_PARTICIPANT_ADD_REQUEST_JOIN:
                participants = stubParticipants(message.messageStubParameters);
                if (participantsIncludesMe()) {
                    chat.readOnly = false;
                }
                emitParticipantsUpdate('add');
                break;
            case WAMessageStubType.GROUP_PARTICIPANT_DEMOTE:
                participants = stubParticipants(message.messageStubParameters);
                emitParticipantsUpdate('demote');
                break;
            case WAMessageStubType.GROUP_PARTICIPANT_PROMOTE:
                participants = stubParticipants(message.messageStubParameters);
                emitParticipantsUpdate('promote');
                break;
            case WAMessageStubType.GROUP_CHANGE_ANNOUNCE:
                const announceValue = message.messageStubParameters?.[0];
                emitGroupUpdate({ announce: announceValue === 'true' || announceValue === 'on' });
                break;
            case WAMessageStubType.GROUP_CHANGE_RESTRICT:
                const restrictValue = message.messageStubParameters?.[0];
                emitGroupUpdate({ restrict: restrictValue === 'true' || restrictValue === 'on' });
                break;
            case WAMessageStubType.GROUP_CHANGE_SUBJECT:
                const name = message.messageStubParameters?.[0];
                chat.name = name;
                emitGroupUpdate({ subject: name });
                break;
            case WAMessageStubType.GROUP_CHANGE_DESCRIPTION:
                const description = message.messageStubParameters?.[0];
                chat.description = description;
                emitGroupUpdate({ desc: description });
                break;
            case WAMessageStubType.GROUP_CHANGE_INVITE_LINK:
                const code = message.messageStubParameters?.[0];
                emitGroupUpdate({ inviteCode: code });
                break;
            case WAMessageStubType.GROUP_MEMBER_ADD_MODE:
                const memberAddValue = message.messageStubParameters?.[0];
                emitGroupUpdate({ memberAddMode: memberAddValue === 'all_member_add' });
                break;
            case WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_MODE:
                const approvalMode = message.messageStubParameters?.[0];
                emitGroupUpdate({ joinApprovalMode: approvalMode === 'on' });
                break;
            case WAMessageStubType.GROUP_MEMBERSHIP_JOIN_APPROVAL_REQUEST_NON_ADMIN_ADD: // TODO: Add other events
                const participant = stubJoinRequester(message.messageStubParameters?.[0]);
                const action = message.messageStubParameters?.[1];
                const method = message.messageStubParameters?.[2];
                if (participant) {
                    emitGroupRequestJoin(participant, action, method);
                }
                break;
        }
    } /*  else if(content?.pollUpdateMessage) {
        const creationMsgKey = content.pollUpdateMessage.pollCreationMessageKey!
        // we need to fetch the poll creation message to get the poll enc key
        // TODO: make standalone, remove getMessage reference
        // TODO: Remove entirely
        const pollMsg = await getMessage(creationMsgKey)
        if(pollMsg) {
            const meIdNormalised = jidNormalizedUser(meId)
            const pollCreatorJid = getKeyAuthor(creationMsgKey, meIdNormalised)
            const voterJid = getKeyAuthor(message.key, meIdNormalised)
            const pollEncKey = pollMsg.messageContextInfo?.messageSecret!

            try {
                const voteMsg = decryptPollVote(
                    content.pollUpdateMessage.vote!,
                    {
                        pollEncKey,
                        pollCreatorJid,
                        pollMsgId: creationMsgKey.id!,
                        voterJid,
                    }
                )
                ev.emit('messages.update', [
                    {
                        key: creationMsgKey,
                        update: {
                            pollUpdates: [
                                {
                                    pollUpdateMessageKey: message.key,
                                    vote: voteMsg,
                                    senderTimestampMs: (content.pollUpdateMessage.senderTimestampMs! as Long).toNumber(),
                                }
                            ]
                        }
                    }
                ])
            } catch(err) {
                logger?.warn(
                    { err, creationMsgKey },
                    'failed to decrypt poll vote'
                )
            }
        } else {
            logger?.warn(
                { creationMsgKey },
                'poll creation message not found, cannot decrypt update'
            )
        }
        } */
    if (Object.keys(chat).length > 1) {
        ev.emit('chats.update', [chat]);
    }
};
export default processMessage;
//# sourceMappingURL=process-message.js.map