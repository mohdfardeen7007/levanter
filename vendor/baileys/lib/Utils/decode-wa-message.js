import { Boom } from '@hapi/boom';
import { proto } from '../../WAProto/index.js';
import { areJidsSameUser, getBinaryNodeChild, isHostedLidUser, isHostedPnUser, isJidBroadcast, isJidGroup, isJidMetaAI, isJidNewsletter, isJidStatusBroadcast, isLidUser, isPnBot, isPnUser
//	transferDevice
 } from '../WABinary/index.js';
import { unpadRandomMax16 } from './generics.js';
import { getContentType } from './messages.js';
import { aesDecryptGCM, hkdf, toMessageSecretBytes } from './crypto.js';
import { normalizeUsername } from './username.js';
export const getDecryptionJid = async (sender, repository) => {
    if (isLidUser(sender) || isHostedLidUser(sender)) {
        return sender;
    }
    const mapped = await repository.lidMapping.getLIDForPN(sender);
    return mapped || sender;
};
const storeMappingFromEnvelope = async (stanza, sender, repository, decryptionJid, logger) => {
    // TODO: Handle hosted IDs
    const { senderAlt } = extractAddressingContext(stanza);
    if (senderAlt && isLidUser(senderAlt) && isPnUser(sender) && decryptionJid === sender) {
        try {
            await repository.lidMapping.storeLIDPNMappings([{ lid: senderAlt, pn: sender }]);
            await repository.migrateSession(sender, senderAlt);
            logger.debug({ sender, senderAlt }, 'Stored LID mapping from envelope');
        }
        catch (error) {
            logger.warn({ sender, senderAlt, error }, 'Failed to store LID mapping');
        }
    }
};
export const NO_MESSAGE_FOUND_ERROR_TEXT = 'Message absent from node';
export const MISSING_KEYS_ERROR_TEXT = 'Key used already or never filled';
// Retry configuration for failed decryption
export const DECRYPTION_RETRY_CONFIG = {
    maxRetries: 3,
    baseDelayMs: 100,
    sessionRecordErrors: ['No session record', 'SessionError: No session record']
};
export const NACK_REASONS = {
    ParsingError: 487,
    UnrecognizedStanza: 488,
    UnrecognizedStanzaClass: 489,
    UnrecognizedStanzaType: 490,
    InvalidProtobuf: 491,
    InvalidHostedCompanionStanza: 493,
    MissingMessageSecret: 495,
    SignalErrorOldCounter: 496,
    MessageDeletedOnPeer: 499,
    UnhandledError: 500,
    UnsupportedAdminRevoke: 550,
    UnsupportedLIDGroup: 551,
    DBOperationFailed: 552,
    NackInvalidHostedCompanionStanza: 493,
    NackUnhandledError: 500,
    NackUnsupportedAdminRevoke: 550,
    NackUnsupportedLIDGroup: 551,
    NackDBOperationFailed: 552
};
/**
 * Server-side error codes returned in ack stanzas (server → client).
 * These are distinct from the client-side NackReason enum
 * (WAWebCreateNackFromStanza) which covers client→server nack codes.
 * 421 and 475 happen to overlap numerically, but 463 and 479 are
 * server-specific codes not present in the client enum.
 */
export const SERVER_ERROR_CODES = {
    /** Group addressing mode is stale — re-query group metadata */
    StaleGroupAddressingMode: '421',
    /**
     * 1:1 message missing privacy token (tctoken). Usually means the account is
     * restricted: WhatsApp blocks starting new chats but preserves existing ones,
     * since established chats already carry a tctoken.
     */
    MessageAccountRestriction: '463',
    /** New chat messages rate limited */
    NewChatMessagesCapped: '475',
    /** Stanza validation failure (SMAX_INVALID) — likely stale device session */
    SmaxInvalid: '479'
};
export const extractAddressingContext = (stanza) => {
    let senderAlt;
    let recipientAlt;
    const sender = stanza.attrs.participant || stanza.attrs.from;
    const addressingMode = stanza.attrs.addressing_mode || (sender?.endsWith('lid') ? 'lid' : 'pn');
    if (addressingMode === 'lid') {
        // Message is LID-addressed: sender is LID, extract corresponding PN
        // without device data
        senderAlt = stanza.attrs.participant_pn || stanza.attrs.sender_pn || stanza.attrs.peer_recipient_pn;
        recipientAlt = stanza.attrs.recipient_pn;
        // with device data
        //if (sender && senderAlt) senderAlt = transferDevice(sender, senderAlt)
    }
    else {
        // Message is PN-addressed: sender is PN, extract corresponding LID
        // without device data
        senderAlt = stanza.attrs.participant_lid || stanza.attrs.sender_lid || stanza.attrs.peer_recipient_lid;
        recipientAlt = stanza.attrs.recipient_lid;
        //with device data
        //if (sender && senderAlt) senderAlt = transferDevice(sender, senderAlt)
    }
    return {
        addressingMode,
        senderAlt,
        recipientAlt
    };
};
/**
 * Decode the received node as a message.
 * @note this will only parse the message, not decrypt it
 */
export function decodeMessageNode(stanza, meId, meLid) {
    let msgType;
    let chatId;
    let author;
    let fromMe = false;
    const msgId = stanza.attrs.id;
    const from = stanza.attrs.from;
    const participant = stanza.attrs.participant;
    const recipient = stanza.attrs.recipient;
    if (!msgId) {
        throw new Boom('Invalid message stanza: missing id attribute', { data: stanza });
    }
    if (!from) {
        throw new Boom('Invalid message stanza: missing from attribute', { data: stanza });
    }
    const addressingContext = extractAddressingContext(stanza);
    const isMe = (jid) => areJidsSameUser(jid, meId);
    const isMeLid = (jid) => areJidsSameUser(jid, meLid);
    if (isPnUser(from) || isLidUser(from) || isHostedLidUser(from) || isHostedPnUser(from)) {
        if (recipient && !isJidMetaAI(recipient)) {
            if (!isMe(from) && !isMeLid(from)) {
                throw new Boom('receipient present, but msg not from me', { data: stanza });
            }
            if (isMe(from) || isMeLid(from)) {
                fromMe = true;
            }
            chatId = recipient;
        }
        else {
            // Peer-routed self stanzas (history sync, app-state sync, etc.) arrive
            // with `from` set to our own device but no `recipient` attribute —
            // still mark as fromMe so self-only protocolMessage handlers run.
            if (isMe(from) || isMeLid(from)) {
                fromMe = true;
            }
            chatId = from;
        }
        msgType = 'chat';
        author = from;
    }
    else if (isJidGroup(from)) {
        if (!participant) {
            throw new Boom('No participant in group message');
        }
        if (isMe(participant) || isMeLid(participant)) {
            fromMe = true;
        }
        msgType = 'group';
        author = participant;
        chatId = from;
    }
    else if (isJidBroadcast(from)) {
        if (!participant) {
            throw new Boom('No participant in group message');
        }
        // WA Web WAWebMsgProcessingApiUtils keys a status or peer broadcast fromMe by isMeAccount(author), which matches
        // our PN or our LID (WAWebUserPrefsMeUser); WAWebHandleMsgParser also checks it for DIRECT_PEER_STATUS
        const isParticipantMe = isMe(participant) || isMeLid(participant);
        if (isJidStatusBroadcast(from)) {
            msgType = isParticipantMe ? 'direct_peer_status' : 'other_status';
        }
        else {
            msgType = isParticipantMe ? 'peer_broadcast' : 'other_broadcast';
        }
        fromMe = isParticipantMe;
        chatId = from;
        author = participant;
    }
    else if (isJidNewsletter(from)) {
        msgType = 'newsletter';
        chatId = from;
        author = from;
        if (isMe(from) || isMeLid(from)) {
            fromMe = true;
        }
    }
    else {
        throw new Boom('Unknown message type', { data: stanza });
    }
    const pushname = stanza?.attrs?.notify;
    // whatsmeow parseMessageSource / zapo 432d5fb5: our own 1:1 messages name the chat in
    // peer_recipient_* (sender_* describes us, and would store our own PN as the peer's)
    const selfChatAlt = isLidUser(chatId) || isHostedLidUser(chatId) ? stanza.attrs.peer_recipient_pn : stanza.attrs.peer_recipient_lid;
    const remoteJidAlt = isJidGroup(chatId)
        ? undefined
        : fromMe && msgType === 'chat'
            ? selfChatAlt
            : addressingContext.senderAlt;
    // WA Web WAWebHandleMsg (zapo 471ca18a): a 1:1 message names its author in `username` and the
    // counterpart of our own DMs in `peer_recipient_username`; statuses name their author in
    // `username`; groups and other broadcasts use `participant_username`. `recipient_username`
    // is kept as a fallback.
    const isStatusMessage = msgType === 'direct_peer_status' || msgType === 'other_status';
    const remoteJidUsername = msgType === 'chat'
        ? (normalizeUsername(fromMe ? stanza.attrs.peer_recipient_username : stanza.attrs.username) ??
            normalizeUsername(stanza.attrs.peer_recipient_username || stanza.attrs.recipient_username))
        : undefined;
    const participantUsername = participant
        ? normalizeUsername(isStatusMessage ? stanza.attrs.username || stanza.attrs.participant_username : stanza.attrs.participant_username)
        : undefined;
    const metaNode = getBinaryNodeChild(stanza, 'meta');
    // WA Web marks a group status on the stanza: a <status> from a group (WAWebCommsHandleMessagingStanza)
    // or <meta is_group_status="true"> (WAWebHandleMsgParser). Add-ons on a group status carry it too.
    const isGroupStatus = msgType === 'group' && (stanza.tag === 'status' || metaNode?.attrs.is_group_status === 'true');
    const key = {
        remoteJid: chatId,
        remoteJidAlt,
        remoteJidUsername,
        fromMe,
        id: msgId,
        participant,
        // WA Web WAWebHandleMsgParser reads participant_pn / participant_lid for every message type: a status or broadcast
        // names its author's other JID like a group does (remoteJidAlt carries it too for broadcasts)
        participantAlt: isJidGroup(chatId) || isJidBroadcast(chatId) ? addressingContext.senderAlt : undefined,
        participantUsername,
        addressingMode: addressingContext.addressingMode,
        ...(msgType === 'newsletter' && stanza.attrs.server_id ? { server_id: stanza.attrs.server_id } : {}),
        ...(isGroupStatus ? { isGroupStatus: true } : {})
    };
    const botNode = getBinaryNodeChild(stanza, 'bot');
    const fullMessage = {
        key,
        category: stanza.attrs.category,
        messageTimestamp: +stanza.attrs.t,
        pushName: pushname,
        broadcast: isJidBroadcast(from),
        botInfo: botNode ? {
            editType: botNode.attrs.edit,
            editTargetId: botNode.attrs.edit_target_id,
            editSenderTimestampMs: +botNode.attrs.sender_timestamp_ms
        } : undefined,
        metaInfo: metaNode ? {
            targetId: metaNode.attrs.target_id,
            targetSenderJid: metaNode.attrs.target_sender_jid,
            targetChatJid: metaNode.attrs.target_chat_jid
        } : undefined
    };
    if (key.fromMe) {
        fullMessage.status = proto.WebMessageInfo.Status.SERVER_ACK;
    }
    // WA Web WAWebHandleMsgParser reads <meta status_mentioned="true"> into statusMentioned, which WAWebStatusModel
    // notifies as a private mention of us in someone's status
    if (isStatusMessage && metaNode?.attrs.status_mentioned === 'true') {
        fullMessage.isMentionedInStatus = true;
    }
    return {
        fullMessage,
        author,
        sender: msgType === 'chat' ? author : chatId
    };
}
export const decryptMessageNode = (stanza, meId, meLid, repository, config) => {
    const { logger } = config;
    const { fullMessage, author, sender } = decodeMessageNode(stanza, meId, meLid);
    // WA Web WAWebHandleMsg (WAWebStatusSessionGatingUtils): a status, from status@broadcast or a group status, whose
    // sender marks <meta session_scope="status"> is decrypted with the status-scoped session; only pkmsg/msg, sender
    // keys have no scope (WAWebMsgProcessingDecryptEnc). WAWebSignalAddress gives hosted devices and bots (isPnBot,
    // isFbidBot: @bot) no status address, so theirs stays the default session
    const sessionScope = getBinaryNodeChild(stanza, 'meta')?.attrs.session_scope === 'status' &&
        (isJidStatusBroadcast(fullMessage.key.remoteJid) || !!fullMessage.key.isGroupStatus) &&
        !isHostedPnUser(author) &&
        !isHostedLidUser(author) &&
        !isPnBot(author) &&
        !isJidMetaAI(author)
        ? 'status'
        : undefined;
    return {
        fullMessage,
        category: stanza.attrs.category,
        author,
        sessionScope,
        async decrypt() {
            let decryptables = 0;
            if (Array.isArray(stanza.content)) {
                for (const { tag, attrs, content } of stanza.content) {
                    if (tag === 'verified_name' && content instanceof Uint8Array) {
                        const cert = proto.VerifiedNameCertificate.decode(content);
                        const details = proto.VerifiedNameCertificate.Details.decode(cert.details);
                        fullMessage.verifiedBizName = details.verifiedName;
                    }
                    if (tag === 'unavailable' && attrs.type === 'view_once') {
                        fullMessage.key.isViewOnce = true; // TODO: remove from here and add a STUB TYPE
                    }
                    if (attrs.count && tag === 'enc') {
                        fullMessage.retryCount = Number(attrs.count);
                    }
                    if (tag !== 'enc' && tag !== 'plaintext') {
                        continue;
                    }
                    if (!(content instanceof Uint8Array)) {
                        continue;
                    }
                    decryptables += 1;
                    let msgBuffer;
                    const decryptionJid = await getDecryptionJid(author, repository);
                    if (tag !== 'plaintext') {
                        // TODO: Handle hosted devices
                        await storeMappingFromEnvelope(stanza, author, repository, decryptionJid, logger);
                    }
                    try {
                        const e2eType = tag === 'plaintext' ? 'plaintext' : attrs.type;
                        switch (e2eType) {
                            case 'skmsg':
                                msgBuffer = await repository.decryptGroupMessage({
                                    group: sender,
                                    authorJid: decryptionJid,
                                    msg: content
                                });
                                break;
                            case 'pkmsg':
                            case 'msg':
                                msgBuffer = await repository.decryptMessage({
                                    jid: decryptionJid,
                                    type: e2eType,
                                    ciphertext: content,
                                    // whatsmeow f376da26: <enc type="pkmsg" state="false">
                                    stateless: e2eType === 'pkmsg' && attrs.state === 'false',
                                    ...(sessionScope ? { scope: sessionScope } : {})
                                });
                                break;
                            case 'msmsg':
                                if (fullMessage.metaInfo?.targetId) {
                                    const targetMsg = await config.getMessage({ id: fullMessage.metaInfo.targetId, remoteJid: fullMessage.key.remoteJid });
                                    const messageSecret = toMessageSecretBytes(targetMsg?.messageContextInfo?.messageSecret);
                                    if (messageSecret) {
                                        const targetSenderJid = fullMessage.metaInfo.targetSenderJid || (fullMessage.key.fromMe ? meLid || meId : author);
                                        const botMsgSecret = await hkdf(messageSecret, 32, { info: 'Bot Message' });
                                        const msgID = fullMessage.botInfo?.editTargetId || fullMessage.key.id;
                                        const useCaseSecret = Buffer.concat([
                                            Buffer.from(msgID),
                                            Buffer.from(targetSenderJid),
                                            Buffer.from(author),
                                            Buffer.from('')
                                        ]);
                                        const botMsgKey = await hkdf(botMsgSecret, 32, { info: useCaseSecret.toString('latin1') });
                                        const additionalData = Buffer.concat([
                                            Buffer.from(msgID),
                                            Buffer.from('\x00'),
                                            Buffer.from(author)
                                        ]);
                                        const msMsg = proto.MessageSecretMessage.decode(content);
                                        msgBuffer = aesDecryptGCM(msMsg.encPayload, botMsgKey, msMsg.encIv, additionalData);
                                    }
                                    else {
                                        throw new Error('Message secret not found for msmsg');
                                    }
                                }
                                else {
                                    throw new Error('targetId missing for msmsg');
                                }
                                break;
                            case 'plaintext':
                                msgBuffer = content;
                                break;
                            default:
                                throw new Error(`Unknown e2e type: ${e2eType}`);
                        }
                        let msg = proto.Message.decode(e2eType !== 'plaintext' ? unpadRandomMax16(msgBuffer) : msgBuffer);
                        msg = msg.deviceSentMessage?.message || msg;
                        if (msg.senderKeyDistributionMessage) {
                            //eslint-disable-next-line max-depth
                            try {
                                await repository.processSenderKeyDistributionMessage({
                                    authorJid: author,
                                    item: msg.senderKeyDistributionMessage
                                });
                            }
                            catch (err) {
                                logger.error({ key: fullMessage.key, err }, 'failed to process sender key distribution message');
                            }
                        }
                        if (fullMessage.message) {
                            Object.assign(fullMessage.message, msg);
                        }
                        else {
                            fullMessage.message = msg;
                        }
                    }
                    catch (err) {
                        const errorContext = {
                            key: fullMessage.key,
                            err,
                            messageType: tag === 'plaintext' ? 'plaintext' : attrs.type,
                            sender,
                            author,
                            isSessionRecordError: isSessionRecordError(err)
                        };
                        logger.error(errorContext, 'failed to decrypt message');
                        fullMessage.messageStubType = proto.WebMessageInfo.StubType.CIPHERTEXT;
                        fullMessage.messageStubParameters = [err.message.toString()];
                    }
                }
            }
            if (!decryptables && !fullMessage.key?.isViewOnce) {
                fullMessage.messageStubType = proto.WebMessageInfo.StubType.CIPHERTEXT;
                fullMessage.messageStubParameters = [NO_MESSAGE_FOUND_ERROR_TEXT];
            }
        }
    };
};
/**
 * Utility function to check if an error is related to missing session record
 */
function isSessionRecordError(error) {
    const errorMessage = error?.message || error?.toString() || '';
    return DECRYPTION_RETRY_CONFIG.sessionRecordErrors.some(errorPattern => errorMessage.includes(errorPattern));
}
//# sourceMappingURL=decode-wa-message.js.map