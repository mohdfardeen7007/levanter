import { pipeline } from 'stream/promises';
import { promisify } from 'util';
import { createInflate, inflate } from 'zlib';
import { proto } from '../../WAProto/index.js';
import { WAMessageStubType } from '../Types/index.js';
import { isHostedLidUser, isHostedPnUser, isJidGroup, isJidStatusBroadcast, isLidUser, isPnUser, STORIES_JID } from '../WABinary/index.js';
import { toNumber } from './generics.js';
import { normalizeMessageContent } from './messages.js';
import { downloadContentFromMessage } from './messages-media.js';
const inflatePromise = promisify(inflate);
const extractPnFromMessages = (messages) => {
    for (const msgItem of messages) {
        const message = msgItem.message;
        // Only extract from outgoing messages (fromMe: true) in 1:1 chats
        // because userReceipt.userJid is the recipient's JID
        if (!message?.key?.fromMe || !message.userReceipt?.length) {
            continue;
        }
        const userJid = message.userReceipt[0]?.userJid;
        if (userJid && (isPnUser(userJid) || isHostedPnUser(userJid))) {
            return userJid;
        }
    }
    return undefined;
};
export const downloadHistory = async (msg, options) => {
    const stream = await downloadContentFromMessage(msg, 'md-msg-hist', { options });
    // Pipe decrypted stream directly through zlib inflate
    // This avoids allocating an intermediate buffer for the compressed data
    const inflater = createInflate();
    const chunks = [];
    inflater.on('data', (chunk) => chunks.push(chunk));
    await pipeline(stream, inflater);
    const buffer = Buffer.concat(chunks);
    const syncData = proto.HistorySync.decode(buffer);
    return syncData;
};
export const processHistoryMessage = (item, logger, { includeStatuses = false } = {}) => {
    const messages = [];
    const contacts = [];
    const chats = [];
    const lidPnMappings = [];
    const tcTokens = [];
    logger?.trace({ progress: item.progress }, 'processing history of type ' + item.syncType?.toString());
    // Extract LID-PN mappings for all sync types
    for (const m of item.phoneNumberToLidMappings || []) {
        if (m.lidJid && m.pnJid) {
            lidPnMappings.push({ lid: m.lidJid, pn: m.pnJid });
        }
    }
    switch (item.syncType) {
        case proto.HistorySync.HistorySyncType.INITIAL_BOOTSTRAP:
        case proto.HistorySync.HistorySyncType.RECENT:
        case proto.HistorySync.HistorySyncType.FULL:
        case proto.HistorySync.HistorySyncType.ON_DEMAND:
            for (const chat of item.conversations) {
                contacts.push({
                    id: chat.id,
                    name: chat.displayName || chat.name || chat.username || undefined,
                    username: chat.username || undefined,
                    lid: chat.lidJid || chat.accountLid || undefined,
                    phoneNumber: chat.pnJid || undefined
                });
                const chatId = chat.id;
                const isLid = isLidUser(chatId) || isHostedLidUser(chatId);
                const isPn = isPnUser(chatId) || isHostedPnUser(chatId);
                if (isLid && chat.pnJid) {
                    lidPnMappings.push({ lid: chatId, pn: chat.pnJid });
                }
                else if (isPn && chat.lidJid) {
                    lidPnMappings.push({ lid: chat.lidJid, pn: chatId });
                }
                else if (isLid && !chat.pnJid) {
                    // Fallback: extract PN from userReceipt in messages when pnJid is missing
                    const pnFromReceipt = extractPnFromMessages(chat.messages || []);
                    if (pnFromReceipt) {
                        lidPnMappings.push({ lid: chatId, pn: pnFromReceipt });
                    }
                }
                if (chat.tcToken) {
                    tcTokens.push({
                        jid: chatId,
                        token: chat.tcToken,
                        timestamp: toNumber(chat.tcTokenTimestamp || 0),
                        senderTimestamp: toNumber(chat.tcTokenSenderTimestamp || 0)
                    });
                }
                const msgs = chat.messages || [];
                delete chat.messages;
                for (const item of msgs) {
                    const message = item.message;
                    // WA Web buildMsgKey / whatsmeow ParseWebMessage / zapo 82758041: group & status history rows keep
                    // the author in the top-level participant; key.participant is usually empty. WA Web gates this on
                    // isGroup || isStatus (not broadcast lists) and never overrides an existing key.participant
                    if (!message.key.participant && message.participant && (isJidGroup(chatId) || isJidStatusBroadcast(chatId))) {
                        message.key.participant = message.participant;
                    }
                    if (message.key.fromMe && message.originalSelfAuthorUserJidString) {
                        message.key.participant = message.originalSelfAuthorUserJidString;
                    }
                    messages.push(message);
                    if (!chat.messages?.length) {
                        // keep only the most recent message in the chat array
                        chat.messages = [{ message }];
                    }
                    if (!message.key.fromMe && !chat.lastMessageRecvTimestamp) {
                        chat.lastMessageRecvTimestamp = toNumber(message.messageTimestamp);
                    }
                    if ((message.messageStubType === WAMessageStubType.BIZ_PRIVACY_MODE_TO_BSP ||
                        message.messageStubType === WAMessageStubType.BIZ_PRIVACY_MODE_TO_FB) &&
                        message.messageStubParameters?.[0]) {
                        contacts.push({
                            id: message.key.participant || message.key.remoteJid,
                            verifiedName: message.messageStubParameters?.[0]
                        });
                    }
                }
                chats.push(chat);
            }
            break;
        case proto.HistorySync.HistorySyncType.PUSH_NAME:
            for (const c of item.pushnames) {
                contacts.push({ id: c.id, notify: c.pushname });
            }
            break;
        case proto.HistorySync.HistorySyncType.INITIAL_STATUS_V3: {
            // fork opt-in (SocketConfig.processHistoryStatuses): off, the chunk yields no messages, as it always has
            if (!includeStatuses) {
                break;
            }
            // WA Web WAWebHistorySyncHandleStatusMessages, which WAWebHandleHistorySyncChunk runs for this sync type:
            // the statuses posted before pairing, oldest first; no 24 h cut-off here (expiry is a display concern)
            const statuses = [...(item.statusV3Messages || [])].sort((a, b) => toNumber(a.messageTimestamp) - toNumber(b.messageTimestamp));
            for (const status of statuses) {
                // WA Web skips a row without a message; one without key.id (nullthrows) or holding a deviceSentMessage
                // (its OTHER_STATUS parse in WAWebMsgProcessingApiUtils throws DeviceSentMessageError) fails the whole
                // chunk there, so skip just that row
                if (!status.key?.id || !status.message || status.message.deviceSentMessage) {
                    continue;
                }
                const message = status;
                if (message.key.fromMe) {
                    // our own status keeps our own author (WA Web getMeUserOrThrow), the self author as on conversation rows
                    if (message.originalSelfAuthorUserJidString) {
                        message.key.participant = message.originalSelfAuthorUserJidString;
                    }
                }
                else {
                    // anyone else's is authored by the top-level participant (an existing key.participant kept, as on
                    // conversation rows); WA Web skips the row without one, and the PSA rows (WAJids PSA_JID)
                    const participant = message.key.participant || message.participant;
                    if (!participant || participant === '0@s.whatsapp.net') {
                        continue;
                    }
                    message.key.participant = participant;
                }
                message.key.remoteJid = STORIES_JID;
                messages.push(message);
            }
            break;
        }
    }
    return {
        chats,
        contacts,
        messages,
        lidPnMappings,
        tcTokens,
        // Account-level NCT salt (HistorySync field 19). WA Web reads this same field on the
        // initial bootstrap (storeNctSaltFromHistorySync) to derive cstokens; without it the
        // client sends token-less 1:1 messages and accrues the reach-out rate-limit (463).
        nctSalt: item.nctSalt || undefined,
        // Companion meta nonce (HistorySync field 16). Captured on bootstrap and later
        // sent as the `Companion_User_Secret` header when deleting companion media.
        companionMetaNonce: item.companionMetaNonce || undefined,
        pastParticipants: item.pastParticipants,
        syncType: item.syncType,
        progress: item.progress
    };
};
export const downloadAndProcessHistorySyncNotification = async (msg, options, logger, processOptions) => {
    let historyMsg;
    if (msg.initialHistBootstrapInlinePayload) {
        historyMsg = proto.HistorySync.decode(await inflatePromise(msg.initialHistBootstrapInlinePayload));
    }
    else {
        historyMsg = await downloadHistory(msg, options);
    }
    return processHistoryMessage(historyMsg, logger, processOptions);
};
export const getHistoryMsg = (message) => {
    const normalizedContent = !!message ? normalizeMessageContent(message) : undefined;
    const anyHistoryMsg = normalizedContent?.protocolMessage?.historySyncNotification;
    return anyHistoryMsg;
};
/**
 * A downloaded (non-inline) history chunk leaves a blob on the media server that WA Web
 * (WAWebHandleHistorySyncChunk) and whatsmeow (6723e556) delete once the chunk is processed.
 */
export const shouldDeleteHistorySyncBlob = (notification) => !notification.initialHistBootstrapInlinePayload?.length &&
    !!notification.directPath &&
    !!notification.fileEncSha256?.length;
//# sourceMappingURL=history.js.map