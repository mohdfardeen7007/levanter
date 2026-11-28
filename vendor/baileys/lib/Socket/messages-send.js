import NodeCache from '@cacheable/node-cache';
import { Boom } from '@hapi/boom';
import { proto } from '../../WAProto/index.js';
import { DEFAULT_CACHE_TTLS, DEFAULT_ORIGIN, MEDIA_PATH_MAP, WA_DEFAULT_EPHEMERAL } from '../Defaults/index.js';
import { aggregateMessageKeysNotFromMe, assertMediaContent, assertMeId, bindWaitForEvent, buildTcTokenFromJid, resolveIssuanceJid, decryptMediaRetryData, downloadContentFromMessage, encodeNewsletterMessage, encodeSignedDeviceIdentity, encodeWAMessage, encryptMediaRetryRequest, extractDeviceJids, generateCsToken, generateMessageIDV2, generateParticipantHashV2, generateWAMessage, getMediaType, getRawMediaUploadData, getStatusCodeForMediaRetry, getUrlFromDirectPath, getWANewsletterUploadToServer, getWAUploadToServer, isTcTokenExpired, makeRecentStatusCache, MessageRetryManager, normalizeMessageContent, parseAndInjectE2ESessions, prepareNewsletterMedia, resolveTcTokenJid, shouldSendNewTcToken, toBuffer, toPhashDeviceString, unixTimestampSeconds } from '../Utils/index.js';
import { getUrlInfo } from '../Utils/link-preview.js';
import { makeKeyedMutex, makeMutex } from '../Utils/make-mutex.js';
import { getMessageReportingToken, shouldIncludeReportingToken } from '../Utils/reporting-utils.js';
import { areJidsSameUser, getBinaryNodeChild, getBinaryNodeChildren, isHostedLidUser, isHostedPnUser, isJidGroup, isJidStatusBroadcast, isLidUser, isPnUser, jidDecode, jidEncode, jidNormalizedUser, S_WHATSAPP_NET } from '../WABinary/index.js';
import { USyncQuery, USyncUser } from '../WAUSync/index.js';
import { makeNewsletterSocket } from './newsletter.js';
/** sent statuses per account (keyed by its creds, like socket.ts connectAttemptCounts), so they outlive a reconnect */
const recentStatusCaches = new WeakMap();
const recentStatusCacheOf = (creds) => {
    let cache = recentStatusCaches.get(creds);
    if (!cache) {
        cache = makeRecentStatusCache();
        recentStatusCaches.set(creds, cache);
    }
    return cache;
};
/** `nodes` with <meta status_setting>: merged into a copy of their first <meta>, or as a <meta> of its own */
const withStatusSetting = (nodes, setting) => {
    const at = nodes.findIndex(node => node.tag === 'meta');
    if (at < 0) {
        return [...nodes, { tag: 'meta', attrs: { status_setting: setting } }];
    }
    const meta = nodes[at];
    return [...nodes.slice(0, at), { ...meta, attrs: { ...meta.attrs, status_setting: setting } }, ...nodes.slice(at + 1)];
};
/**
 * WA Web WAWebSendMsgCommonApi.editAttribute of a resend, which keeps none of its first send's attrs: what sendMessage
 * names a revoke (7, or 8 for someone else's group message: an admin revoke), an edit (1) and a pin (2)
 */
const editAttributeOf = (message) => {
    const content = normalizeMessageContent(message);
    const protocol = content?.protocolMessage;
    if (protocol?.type === proto.Message.ProtocolMessage.Type.REVOKE) {
        return isJidGroup(protocol.key?.remoteJid || undefined) && protocol.key?.fromMe === false ? '8' : '7';
    }
    if (protocol?.type === proto.Message.ProtocolMessage.Type.MESSAGE_EDIT) {
        return '1';
    }
    if (content?.pinInChatMessage) {
        return '2';
    }
    return undefined;
};
export const makeMessagesSocket = (config) => {
    const { logger, linkPreviewImageThumbnailWidth, generateHighQualityLinkPreview, options: httpRequestOptions, patchMessageBeforeSending, cachedGroupMetadata, enableRecentMessageCache, maxMsgRetryCount } = config;
    const sock = makeNewsletterSocket(config);
    const { ev, authState, messageMutex, signalRepository, upsertMessage, query, fetchPrivacySettings, sendNode, groupMetadata, groupToggleEphemeral } = sock;
    const userDevicesCache = config.userDevicesCache ||
        new NodeCache({
            stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES, // 5 minutes
            useClones: false
        });
    /** Serializes writes to userDevicesCache across USync refresh and device-notification handling. */
    const devicesMutex = makeMutex();
    const peerSessionsCache = new NodeCache({
        stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES,
        useClones: false
    });
    // In-memory throttle for OUR tctoken issuance (one per 7-day bucket per contact).
    // MUST be in-memory: issuance is fire-and-forget AFTER the send transaction commits,
    // so a key-store write there inherits the committed transaction's async context and
    // is silently dropped (never flushed). Mirrors whatsmeow tcTokenSenderTS.
    const tcTokenSenderTsCache = new NodeCache({
        stdTTL: 4 * 604800, // ~28 days (4 tctoken buckets)
        useClones: false
    });
    // Initialize message retry manager if enabled
    const messageRetryManager = enableRecentMessageCache
        ? new MessageRetryManager(logger, maxMsgRetryCount, recentStatusCacheOf(authState.creds))
        : null;
    // Prevent race conditions in Signal session encryption by user
    const encryptionMutex = makeKeyedMutex();
    let mediaConn;
    const refreshMediaConn = async (forceGet = false) => {
        // a failed media_conn query must not poison every later media request on this socket
        const media = await mediaConn?.catch(() => undefined);
        if (!media || forceGet || new Date().getTime() - media.fetchDate.getTime() > media.ttl * 1000) {
            mediaConn = (async () => {
                const result = await query({
                    tag: 'iq',
                    attrs: {
                        type: 'set',
                        xmlns: 'w:m',
                        to: S_WHATSAPP_NET
                    },
                    content: [{ tag: 'media_conn', attrs: {} }]
                });
                const mediaConnNode = getBinaryNodeChild(result, 'media_conn');
                // TODO: explore full length of data that whatsapp provides
                const node = {
                    hosts: getBinaryNodeChildren(mediaConnNode, 'host').map(({ attrs }) => ({
                        hostname: attrs.hostname,
                        maxContentLengthBytes: +attrs.maxContentLengthBytes
                    })),
                    auth: mediaConnNode.attrs.auth,
                    ttl: +mediaConnNode.attrs.ttl,
                    fetchDate: new Date()
                };
                logger.debug('fetched media conn');
                return node;
            })();
        }
        return mediaConn;
    };
    /**
     * generic send receipt function
     * used for receipts of phone call, read, delivery etc.
     * @param stanzaClass `class` attr: `status` on the receipts of a group status. Receipts to status@broadcast get
     * it without asking (WA Web sends class=status on every receipt of a status)
     * */
    const sendReceipt = async (jid, participant, messageIds, type, stanzaClass) => {
        if (!messageIds || messageIds.length === 0) {
            throw new Boom('missing ids in receipt');
        }
        const node = {
            tag: 'receipt',
            attrs: {
                id: messageIds[0]
            }
        };
        const isReadReceipt = type === 'read' || type === 'read-self';
        if (isReadReceipt) {
            node.attrs.t = unixTimestampSeconds().toString();
        }
        if (type === 'sender' && (isPnUser(jid) || isLidUser(jid))) {
            node.attrs.recipient = jid;
            node.attrs.to = participant;
        }
        else {
            node.attrs.to = jid;
            if (participant) {
                node.attrs.participant = participant;
            }
        }
        // WA Web WAWebSendReceiptJobCommon: a read receipt for a status posted by a LID keeps the LID in
        // `participant` and names the poster's phone number in `peer_participant_pn`. Without that attr the
        // view never registers for the poster. AB prop 19696 gates it.
        if (type === 'read' &&
            isJidStatusBroadcast(jid) &&
            participant &&
            isLidUser(participant) &&
            sock.serverProps.lidStatusNonSoakedClientSupport) {
            const pn = await signalRepository.lidMapping.getPNForLID(jidNormalizedUser(participant));
            if (pn) {
                node.attrs.peer_participant_pn = jidNormalizedUser(pn);
            }
            else {
                logger.debug({ participant }, 'no PN known for status LID; receipt sent without peer_participant_pn');
            }
        }
        // WA Web: a delivery receipt of a status is class=status whatever its type (WAWebSendDeliveryReceiptJob,
        // getDeliveryReceiptClass), as is any receipt to status@broadcast (WAWebSendReceiptJobCommon
        // sendAggregateReceipts). Retry receipts are built in sendRetryRequest and keep none, like WA Web's
        const receiptClass = stanzaClass ?? (isJidStatusBroadcast(jid) ? 'status' : undefined);
        if (receiptClass) {
            node.attrs.class = receiptClass;
        }
        if (type) {
            node.attrs.type = type;
        }
        const remainingMessageIds = messageIds.slice(1);
        if (remainingMessageIds.length) {
            node.content = [
                {
                    tag: 'list',
                    attrs: {},
                    content: remainingMessageIds.map(id => ({
                        tag: 'item',
                        attrs: { id }
                    }))
                }
            ];
        }
        logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages');
        await sendNode(node);
    };
    /** Correctly bulk send receipts to multiple chats, participants */
    const sendReceipts = async (keys, type) => {
        // WA Web markStatusRead / sendAggregateReceipts: a group status is receipted apart from the group's messages,
        // with class=status
        const groupStatusKeys = keys.filter(key => key.isGroupStatus);
        const otherKeys = groupStatusKeys.length ? keys.filter(key => !key.isGroupStatus) : keys;
        for (const { jid, participant, messageIds } of aggregateMessageKeysNotFromMe(otherKeys)) {
            await sendReceipt(jid, participant, messageIds, type);
        }
        for (const { jid, participant, messageIds } of aggregateMessageKeysNotFromMe(groupStatusKeys)) {
            await sendReceipt(jid, participant, messageIds, type, 'status');
        }
    };
    /** Bulk read messages. Keys can be from different chats & participants */
    const readMessages = async (keys) => {
        const privacySettings = await fetchPrivacySettings();
        // based on privacy settings, we have to change the read type
        const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self';
        // WA Web markStatusRead reads a group status with type=read whatever the setting (its `to` is a group)
        const groupStatusKeys = keys.filter(key => key.isGroupStatus);
        await sendReceipts(groupStatusKeys.length ? keys.filter(key => !key.isGroupStatus) : keys, readType);
        if (groupStatusKeys.length) {
            await sendReceipts(groupStatusKeys, 'read');
        }
    };
    /** Fetch all the devices we've to send a message to */
    const getUSyncDevices = async (jids, useCache, ignoreZeroDevices) => {
        const deviceResults = [];
        if (!useCache) {
            logger.debug('not using cache for devices');
        }
        const toFetch = [];
        const jidsWithUser = jids
            .map(jid => {
            const decoded = jidDecode(jid);
            const user = decoded?.user;
            const device = decoded?.device;
            const isExplicitDevice = typeof device === 'number' && device >= 0;
            if (isExplicitDevice && user) {
                deviceResults.push({
                    user,
                    device,
                    jid
                });
                return null;
            }
            jid = jidNormalizedUser(jid);
            return { jid, user };
        })
            .filter(jid => jid !== null);
        let mgetDevices;
        if (useCache && userDevicesCache.mget) {
            const usersToFetch = jidsWithUser.map(j => j?.user).filter(Boolean);
            mgetDevices = await userDevicesCache.mget(usersToFetch);
        }
        const results = await Promise.all(jidsWithUser.map(async ({ jid, user }) => {
            if (useCache) {
                const devices = mgetDevices?.[user] || (userDevicesCache.mget ? undefined : await userDevicesCache.get(user));
                if (devices) {
                    const devicesWithJid = devices.map(d => ({
                        ...d,
                        jid: jidEncode(d.user, d.server, d.device)
                    }));
                    logger.trace({ user }, 'using cache for devices');
                    return devicesWithJid;
                }
            }
            return jid;
        }));
        for (const item of results) {
            if (typeof item === 'string') {
                toFetch.push(item);
            }
            else {
                deviceResults.push(...item);
            }
        }
        if (!toFetch.length) {
            return deviceResults;
        }
        const requestedLidUsers = new Set();
        for (const jid of toFetch) {
            if (isLidUser(jid) || isHostedLidUser(jid)) {
                const user = jidDecode(jid)?.user;
                if (user)
                    requestedLidUsers.add(user);
            }
        }
        const query = new USyncQuery().withContext('message').withDeviceProtocol().withLIDProtocol();
        for (const jid of toFetch) {
            query.withUser(new USyncUser().withId(jid)); // todo: investigate - the idea here is that <user> should have an inline lid field with the lid being the pn equivalent
        }
        const result = await sock.executeUSyncQuery(query);
        if (result) {
            // TODO: LID MAP this stuff (lid protocol will now return lid with devices)
            const lidResults = result.list.filter(a => !!a.lid);
            if (lidResults.length > 0) {
                logger.trace('Storing LID maps from device call');
                await signalRepository.lidMapping.storeLIDPNMappings(lidResults.map(a => ({ lid: a.lid, pn: a.id })));
                // Force-refresh sessions for newly mapped LIDs to align identity addressing
                try {
                    const lids = lidResults.map(a => a.lid);
                    if (lids.length) {
                        await assertSessions(lids);
                    }
                }
                catch (e) {
                    logger.warn({ e, count: lidResults.length }, 'failed to assert sessions for newly mapped LIDs');
                }
            }
            const extracted = extractDeviceJids(result?.list, authState.creds.me.id, authState.creds.me.lid, ignoreZeroDevices);
            const deviceMap = {};
            for (const item of extracted) {
                deviceMap[item.user] = deviceMap[item.user] || [];
                deviceMap[item.user]?.push(item);
            }
            // Process each user's devices as a group for bulk LID migration
            for (const [user, userDevices] of Object.entries(deviceMap)) {
                const isLidUser = requestedLidUsers.has(user);
                // Process all devices for this user
                for (const item of userDevices) {
                    const finalJid = isLidUser
                        ? jidEncode(user, item.server, item.device)
                        : jidEncode(item.user, item.server, item.device);
                    deviceResults.push({
                        ...item,
                        jid: finalJid
                    });
                    logger.debug({
                        user: item.user,
                        device: item.device,
                        finalJid,
                        usedLid: isLidUser
                    }, 'Processed device with LID priority');
                }
            }
            await devicesMutex.mutex(async () => {
                if (userDevicesCache.mset) {
                    // if the cache supports mset, we can set all devices in one go
                    await userDevicesCache.mset(Object.entries(deviceMap).map(([key, value]) => ({ key, value })));
                }
                else {
                    for (const key in deviceMap) {
                        if (deviceMap[key])
                            await userDevicesCache.set(key, deviceMap[key]);
                    }
                }
            });
            const userDeviceUpdates = {};
            for (const [userId, devices] of Object.entries(deviceMap)) {
                if (devices && devices.length > 0) {
                    userDeviceUpdates[userId] = devices.map(d => d.device?.toString() || '0');
                }
            }
            if (Object.keys(userDeviceUpdates).length > 0) {
                try {
                    await authState.keys.set({ 'device-list': userDeviceUpdates });
                    logger.debug({ userCount: Object.keys(userDeviceUpdates).length }, 'stored user device lists for bulk migration');
                }
                catch (error) {
                    logger.warn({ error }, 'failed to store user device lists');
                }
            }
        }
        return deviceResults;
    };
    /**
     * Update Member Label
     */
    const updateMemberLabel = (jid, memberLabel) => {
        return relayMessage(jid, {
            protocolMessage: {
                type: proto.Message.ProtocolMessage.Type.GROUP_MEMBER_LABEL_CHANGE,
                memberLabel: {
                    label: memberLabel?.slice(0, 30),
                    labelTimestamp: unixTimestampSeconds()
                }
            }
        }, {
            additionalNodes: [
                {
                    tag: 'meta',
                    attrs: {
                        tag_reason: 'user_update',
                        appdata: 'member_tag'
                    },
                    content: undefined
                }
            ]
        });
    };
    const assertSessions = async (jids, force) => {
        let didFetchNewSession = false;
        const uniqueJids = [...new Set(jids)]; // Deduplicate JIDs
        const jidsRequiringFetch = [];
        logger.debug({ jids }, 'assertSessions call with jids');
        const sessionsToValidate = [];
        for (const jid of uniqueJids) {
            const signalId = signalRepository.jidToSignalProtocolAddress(jid);
            const cachedSession = peerSessionsCache.get(signalId);
            if (cachedSession !== undefined) {
                if (cachedSession && !force) {
                    continue; // Session exists in cache
                }
            }
            sessionsToValidate.push(jid);
        }
        if (sessionsToValidate.length > 0) {
            const validationResults = await signalRepository.validateSessions(sessionsToValidate);
            for (const jid of sessionsToValidate) {
                const result = validationResults[jid];
                const hasSession = !!result?.exists;
                const signalId = signalRepository.jidToSignalProtocolAddress(jid);
                peerSessionsCache.set(signalId, hasSession);
                if (hasSession && !force) {
                    continue;
                }
                jidsRequiringFetch.push(jid);
            }
        }
        if (jidsRequiringFetch.length) {
            // LID if mapped, otherwise original
            const wireJids = [
                ...jidsRequiringFetch.filter(jid => !!isLidUser(jid) || !!isHostedLidUser(jid)),
                ...((await signalRepository.lidMapping.getLIDsForPNs(jidsRequiringFetch.filter(jid => !!isPnUser(jid) || !!isHostedPnUser(jid)))) || []).map(a => a.lid)
            ];
            logger.debug({ jidsRequiringFetch, wireJids }, 'fetching sessions');
            const result = await query({
                tag: 'iq',
                attrs: {
                    xmlns: 'encrypt',
                    type: 'get',
                    to: S_WHATSAPP_NET
                },
                content: [
                    {
                        tag: 'key',
                        attrs: {},
                        content: wireJids.map(jid => {
                            const attrs = { jid };
                            if (force)
                                attrs.reason = 'identity';
                            return { tag: 'user', attrs };
                        })
                    }
                ]
            });
            await parseAndInjectE2ESessions(result, signalRepository);
            didFetchNewSession = true;
            // Cache fetched sessions using wire JIDs
            for (const wireJid of wireJids) {
                const signalId = signalRepository.jidToSignalProtocolAddress(wireJid);
                peerSessionsCache.set(signalId, true);
            }
        }
        return didFetchNewSession;
    };
    /**
     * Send an arbitrary message to your own account (all your other devices), with
     * the `peer` category. Mirrors whatsmeow's SendMessage(ownID, msg, {Peer:true}).
     * Useful for app-state key resync, on-demand history sync, and other self-directed
     * protocol messages.
     */
    const sendPeerMessage = async (message, opts = {}) => {
        if (!authState.creds.me?.id) {
            throw new Boom('Not authenticated');
        }
        const meJid = jidNormalizedUser(authState.creds.me.id);
        return relayMessage(meJid, message, {
            additionalAttributes: {
                category: 'peer',
                push_priority: 'high_force',
                ...opts.additionalAttributes
            },
            additionalNodes: opts.additionalNodes
        });
    };
    const sendPeerDataOperationMessage = async (pdoMessage) => {
        const protocolMessage = {
            protocolMessage: {
                peerDataOperationRequestMessage: pdoMessage,
                type: proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
            }
        };
        return sendPeerMessage(protocolMessage, {
            // Marks history-sync on-demand peer messages so the server skips push-notification
            // delivery for them; mirrors whatsmeow's send.preparePeerMessageNode (5465030).
            additionalAttributes: { privacy_sensitive: '1' },
            additionalNodes: [{ tag: 'meta', attrs: { appdata: 'default' } }]
        });
    };
    const createParticipantNodes = async (recipientJids, message, extraAttrs, dsmMessage) => {
        if (!recipientJids.length) {
            return { nodes: [], shouldIncludeDeviceIdentity: false };
        }
        const patched = await patchMessageBeforeSending(message, recipientJids);
        const patchedMessages = Array.isArray(patched)
            ? patched
            : recipientJids.map(jid => ({ recipientJid: jid, message: patched }));
        let shouldIncludeDeviceIdentity = false;
        const meId = authState.creds.me.id;
        const meLid = authState.creds.me?.lid;
        const meLidUser = meLid ? jidDecode(meLid)?.user : null;
        const encryptionPromises = patchedMessages.map(async ({ recipientJid: jid, message: patchedMessage }) => {
            try {
                if (!jid)
                    return null;
                let msgToEncrypt = patchedMessage;
                const { user: targetUser } = jidDecode(jid);
                const { user: ownPnUser } = jidDecode(meId);
                const ownLidUser = meLidUser;
                const isOwnUser = targetUser === ownPnUser || (ownLidUser && targetUser === ownLidUser);
                const isExactSenderDevice = jid === meId || (meLid && jid === meLid);
                if (isExactSenderDevice) {
                    return null;
                }
                if (dsmMessage) {
                    if (isOwnUser) {
                        msgToEncrypt = dsmMessage;
                        logger.debug({ jid, targetUser }, 'Using DSM for own device');
                    }
                }
                const bytes = encodeWAMessage(msgToEncrypt);
                const mutexKey = jid;
                const node = await encryptionMutex.mutex(mutexKey, async () => {
                    const { type, ciphertext } = await signalRepository.encryptMessage({ jid, data: bytes });
                    if (type === 'pkmsg') {
                        shouldIncludeDeviceIdentity = true;
                    }
                    return {
                        tag: 'to',
                        attrs: { jid },
                        content: [
                            {
                                tag: 'enc',
                                attrs: { v: '2', type, ...(extraAttrs || {}) },
                                content: ciphertext
                            }
                        ]
                    };
                });
                return node;
            }
            catch (err) {
                logger.error({ jid, err }, 'Failed to encrypt for recipient');
                return null;
            }
        });
        const nodes = (await Promise.all(encryptionPromises)).filter(node => node !== null);
        if (recipientJids.length > 0 && nodes.length === 0) {
            throw new Boom('All encryptions failed', { statusCode: 500 });
        }
        return { nodes, shouldIncludeDeviceIdentity };
    };
    const getPrivacyTokens = async (jids, timestamp) => {
        const t = (timestamp ?? unixTimestampSeconds()).toString();
        const result = await query({
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                type: 'set',
                xmlns: 'privacy'
            },
            content: [
                {
                    tag: 'tokens',
                    attrs: {},
                    content: jids.map(jid => ({
                        tag: 'token',
                        attrs: {
                            jid: jidNormalizedUser(jid),
                            t,
                            type: 'trusted_contact'
                        }
                    }))
                }
            ]
        });
        return result;
    };
    /**
     * The users a status goes to, fanned out like WA Web WAWebEncryptAndSendStatusMsg: the audience as LIDs, the LID we
     * know of a PN (WAWebLidMigrationUtils.toUserLid, a local lookup; an unknown PN stays a PN, and the device query
     * that follows stores its LID for the next post), plus our own LID, so our other devices get the post as well. A
     * reaction keeps its targets as given, a shape WA Web does not send, and only gains our own LID.
     */
    const statusFanout = async (audience, keepTargets) => {
        const { me } = authState.creds;
        const ownUsers = [me?.id, me?.lid].filter(jid => !!jid).map(jid => jidNormalizedUser(jid));
        const others = audience.filter(jid => !ownUsers.includes(jidNormalizedUser(jid)));
        if (!others.length) {
            logger.warn({ audience: audience.length }, 'status sent to none but our own devices');
        }
        let targets = others;
        if (!keepTargets) {
            try {
                const pns = others.filter(jid => isPnUser(jid) || isHostedPnUser(jid));
                const known = await signalRepository.lidMapping.getKnownLIDsForPNs(pns);
                const lidOf = new Map(known.map(({ pn, lid }) => [pn, lid]));
                targets = others.map(jid => lidOf.get(jid) ?? jid);
            }
            catch (error) {
                logger.warn({ error }, 'could not map the status audience to LIDs, sending to it as given');
            }
        }
        return [...new Set([...targets, jidNormalizedUser(me?.lid || me?.id)])];
    };
    const relayMessage = async (jid, message, { messageId: msgId, participant, additionalAttributes, additionalNodes, useUserDevicesCache, useCachedGroupMetadata, statusJidList, statusSetting }) => {
        const meId = assertMeId(authState.creds);
        const meLid = authState.creds.me?.lid;
        msgId = msgId || generateMessageIDV2(meId);
        // Logging infrastructure for tracking relayMessage performance
        const shouldLog = process.env.BAILEYS_RELAY_LOGGING === 'true' || process.env.BAILEYS_RELAY_LOGGING === '1';
        const startMs = Date.now();
        const logSteps = [];
        const addLogStep = (name) => {
            if (shouldLog)
                logSteps.push({ name, time: Date.now() - startMs });
        };
        let isCompleted = false;
        const logDetailedCycle = (status) => {
            if (!shouldLog || isCompleted)
                return;
            const totalTime = Date.now() - startMs;
            logger.error({ msgId, jid, status, totalTime, steps: logSteps }, `relayMessage ${status}`);
        };
        const timeout = shouldLog ? setTimeout(() => logDetailedCycle('timeout'), 30000) : undefined;
        addLogStep('start');
        try {
            const isRetryResend = Boolean(participant?.jid);
            const statusJid = 'status@broadcast';
            // WA Web WAWebResendStatusMsg: a status resend carries device-identity only with a pkmsg sender key, which
            // createParticipantNodes reports below
            let shouldIncludeDeviceIdentity = isRetryResend && jid !== statusJid;
            let { user, server } = jidDecode(jid);
            const isPn = server === 's.whatsapp.net';
            // WA Web WAWebSendMsgCreateFanoutStanza (whatsmeow 4f8f64e0): a LID-migrated client sends 1:1
            // messages to the LID and names the phone number in peer_recipient_pn; PN chats stay PN.
            // Peer messages always go to our own LID (WA Web October WAWebUserPrefsMeUser
            // .getMyPrimaryForOutgoingPeerMessage = meLid device 0; WAWebStanzaValidation flags a PN peer stanza
            // as a leak) — the July AB prop 24184 gate is gone. They never carry peer_recipient_pn.
            const isPeerCategory = additionalAttributes?.category === 'peer';
            let peerRecipientPn;
            if (isPn && !isRetryResend && !isJidGroup(jid) && jid !== statusJid) {
                // a peer message targets our own account: WA Web reads meLid from the creds (never the mapping
                // store); without it the message stays on our PN, so no bogus `<pnUser>@lid` device is built
                const lid = isPeerCategory
                    ? meLid && areJidsSameUser(jid, meId)
                        ? jidEncode(jidDecode(meLid).user, 'lid')
                        : undefined
                    : await signalRepository.lidMapping.getLIDForPN(jid);
                if (lid) {
                    const decoded = jidDecode(lid);
                    if (decoded) {
                        logger.debug({ pn: jid, lid }, 'translated PN to LID for message');
                        if (!isPeerCategory) {
                            peerRecipientPn = jidNormalizedUser(jid);
                        }
                        user = decoded.user;
                        server = decoded.server;
                        jid = lid;
                    }
                }
            }
            else if (server === 'lid' && !isPeerCategory && !isRetryResend) {
                const pn = await signalRepository.lidMapping.getPNForLID(jid);
                if (pn) {
                    peerRecipientPn = jidNormalizedUser(pn);
                }
            }
            addLogStep('jid_decode_and_lid_mapping');
            const isGroup = server === 'g.us';
            const isStatus = jid === statusJid;
            const isLid = server === 'lid';
            const isNewsletter = server === 'newsletter';
            const isGroupOrStatus = isGroup || isStatus;
            const finalJid = jid;
            msgId = msgId || generateMessageIDV2(meId);
            useUserDevicesCache = useUserDevicesCache !== false;
            useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus;
            const participants = [];
            const destinationJid = !isStatus ? finalJid : statusJid;
            const binaryNodeContent = [];
            const devices = [];
            let reportingMessage;
            /** the identity the message is sent under: the sender its reporting token names */
            let reportingSender;
            /** a status post: the users it goes to, kept with it for its viewers' retries */
            let statusAudience;
            const meMsg = {
                deviceSentMessage: {
                    destinationJid,
                    message
                },
                messageContextInfo: message.messageContextInfo
            };
            const extraAttrs = {};
            if (participant) {
                if (!isGroup && !isStatus) {
                    additionalAttributes = { ...additionalAttributes, device_fanout: 'false' };
                }
                // a retry keeps none of the first send's attrs: WA Web names the edit of a resend from its content
                // (WAWebSendMsgCreateDeviceStanza, WAWebResendStatusMsg)
                const resendEdit = editAttributeOf(message);
                if (resendEdit) {
                    additionalAttributes = { ...additionalAttributes, edit: resendEdit };
                }
                const { user: decodedUser, device } = jidDecode(participant.jid);
                devices.push({
                    user: decodedUser,
                    device,
                    jid: participant.jid
                });
            }
            await authState.keys.transaction(async () => {
                // WA Web WAWebBackendJobsCommon.mediaTypeFromProtobuf (zapo 27ed73cd): unwrap view-once /
                // ephemeral / document-with-caption first, and send `gif` for gif-playback video
                const mediaType = getMediaType(getStanzaClassificationContent(message) ?? message);
                if (mediaType && ['image', 'video', 'gif', 'audio', 'ptt', 'document', 'sticker'].includes(mediaType)) {
                    extraAttrs['mediatype'] = mediaType;
                }
                if (isNewsletter) {
                    let patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message;
                    if (Array.isArray(patched)) {
                        patched = patched[0] || message;
                    }
                    // newsletter media messages require some specific cleaning to render correctly
                    const normalizedContent = normalizeMessageContent(patched);
                    const mediaType = normalizedContent ? getMediaType(normalizedContent) : '';
                    let mediaHandle;
                    if (mediaType) {
                        const mediaMessage = normalizedContent[`${mediaType}Message`];
                        if (mediaMessage && typeof mediaMessage === 'object') {
                            // if it's an encrypted media message, we need to convert it
                            if (mediaMessage.mediaKey) {
                                const stream = await downloadContentFromMessage(mediaMessage, mediaType);
                                const buffer = await toBuffer(stream);
                                const { message: converted, handle } = await prepareNewsletterMedia(buffer, {
                                    upload: getWANewsletterUploadToServer(config, refreshMediaConn),
                                    mediaTypeOverride: mediaType,
                                    logger
                                });
                                patched = converted;
                                mediaHandle = handle;
                                // copy over any caption/contextInfo/metadata from the original message
                                const newMediaMessage = patched[`${mediaType}Message`];
                                newMediaMessage.caption = mediaMessage.caption;
                                newMediaMessage.contextInfo = mediaMessage.contextInfo;
                                newMediaMessage.mimetype = mediaMessage.mimetype;
                                newMediaMessage.jpegThumbnail = mediaMessage.jpegThumbnail;
                                if (mediaType === 'audio' || mediaType === 'ptt') {
                                    newMediaMessage.ptt = !!mediaMessage.ptt;
                                    newMediaMessage.seconds = mediaMessage.seconds;
                                    newMediaMessage.waveform = mediaMessage.waveform;
                                }
                            }
                            // newsletter media often fails if sidecar or other metadata is present
                            delete mediaMessage.scansSidecar;
                            delete mediaMessage.scanLengths;
                            delete mediaMessage.streamingSidecar;
                            delete mediaMessage.qrUrl;
                            if (mediaMessage.mediaKey && !mediaHandle) {
                                throw new Boom('Encrypted media not supported in newsletters without conversion', { statusCode: 400 });
                            }
                        }
                    }
                    const bytes = encodeNewsletterMessage(patched);
                    const newsletterMediaTypes = ['image', 'video', 'audio', 'ptt', 'gif', 'sticker', 'document', 'url'];
                    const plaintextAttrs = (mediaType && newsletterMediaTypes.includes(mediaType)) ? { mediatype: mediaType } : {};
                    if (mediaHandle) {
                        additionalAttributes = { ...additionalAttributes, 'media_id': mediaHandle };
                    }
                    if (mediaType === 'reaction' && patched.reactionMessage) {
                        const reaction = patched.reactionMessage;
                        binaryNodeContent.push({
                            tag: 'reaction',
                            attrs: {
                                code: reaction.text || ''
                            }
                        });
                        // reactions also require the server_id in the message attribute
                        const serverId = reaction.key?.serverId;
                        if (serverId) {
                            additionalAttributes = { ...additionalAttributes, 'server_id': serverId };
                        }
                    }
                    else {
                        binaryNodeContent.push({
                            tag: 'plaintext',
                            attrs: plaintextAttrs,
                            content: bytes
                        });
                    }
                    const stanza = {
                        tag: 'message',
                        attrs: {
                            to: jid,
                            id: msgId,
                            type: getMessageType(patched),
                            ...(additionalAttributes || {})
                        },
                        content: binaryNodeContent
                    };
                    await sendNode(stanza);
                    return;
                }
                const msgContent = normalizeMessageContent(message);
                const isPoll = !!msgContent?.pollCreationMessage ||
                    !!msgContent?.pollCreationMessageV2 ||
                    !!msgContent?.pollCreationMessageV3 ||
                    !!msgContent?.pollUpdateMessage;
                if (isPoll || !!msgContent?.reactionMessage || !!msgContent?.pinInChatMessage || !!msgContent?.keepInChatMessage) {
                    extraAttrs['decrypt-fail'] = 'hide';
                }
                // WA Web WAWebResendStatusMsg: a status resend goes the sender-key way too, to the device that asked only
                // (in `devices` already): a fresh SKDM for it and the post re-encrypted as skmsg under the same sender key
                const isStatusResend = isStatus && isRetryResend;
                if (isGroupOrStatus && (!isRetryResend || isStatusResend)) {
                    const groupData = await (async () => {
                        let groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined;
                        if (groupData && Array.isArray(groupData?.participants)) {
                            logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata');
                        }
                        else if (!isStatus) {
                            groupData = await groupMetadata(jid);
                        }
                        addLogStep('group_metadata_resolve');
                        return groupData;
                    })();
                    if (!isStatusResend) {
                        const participantsList = groupData ? groupData.participants.map(p => p.id) : [];
                        if (isStatus) {
                            participantsList.push(...(await statusFanout(statusJidList || [], !!msgContent?.reactionMessage)));
                            statusAudience = participantsList.map(jidNormalizedUser);
                        }
                        const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false);
                        devices.push(...additionalDevices);
                        addLogStep('usync_devices_fetch');
                        logger.debug({ jid, participantsList: participantsList.length, deviceCount: devices.length }, 'group devices resolved');
                    }
                    if (groupData?.ephemeralDuration && groupData.ephemeralDuration > 0) {
                        additionalAttributes = {
                            ...additionalAttributes,
                            expiration: groupData.ephemeralDuration.toString()
                        };
                    }
                    const groupAddressingMode = additionalAttributes?.['addressing_mode'] || groupData?.addressingMode || 'lid';
                    const groupSenderIdentity = groupAddressingMode === 'lid' && meLid ? meLid : meId;
                    const senderKeyMemoryId = `${jid}:${groupSenderIdentity}`;
                    const phashList = devices.map(d => toPhashDeviceString(d.jid));
                    if (isGroup) {
                        additionalAttributes = {
                            ...additionalAttributes,
                            addressing_mode: groupAddressingMode,
                            phash: generateParticipantHashV2(phashList)
                        };
                        logger.debug({ jid, deviceCount: devices.length, phash: additionalAttributes.phash }, 'generated phash for group');
                    }
                    const senderKeyMap = await (async () => {
                        if (!participant && !isStatus) {
                            const result = await authState.keys.get('sender-key-memory', [senderKeyMemoryId]);
                            return result[senderKeyMemoryId] || {};
                        }
                        return {};
                    })();
                    const senderKeyRecipients = [];
                    const seenRecipients = new Set();
                    // our sending device under our LID even when creds.me.lid lacks its device number
                    const meDevice = jidDecode(meId)?.device;
                    const meLidDevice = meLid && meDevice ? jidEncode(jidDecode(meLid).user, 'lid', meDevice) : undefined;
                    for (const device of devices) {
                        const deviceJid = device.jid;
                        const hasKey = !!senderKeyMap[deviceJid];
                        const isExactSenderDevice = deviceJid === meId || (meLid && deviceJid === meLid) || deviceJid === meLidDevice;
                        if ((!hasKey || !!participant) &&
                            !isExactSenderDevice &&
                            !isHostedLidUser(deviceJid) &&
                            !isHostedPnUser(deviceJid) &&
                            device.device !== 99 &&
                            !seenRecipients.has(deviceJid)) {
                            seenRecipients.add(deviceJid);
                            senderKeyRecipients.push(deviceJid);
                        }
                    }
                    // the asking device is one we never hand the key to (hosted): a bare skmsg would help no one
                    if (isStatusResend && !senderKeyRecipients.length) {
                        logger.warn({ msgId, participant: participant.jid }, 'not resending a status to this device');
                        return;
                    }
                    if (senderKeyRecipients.length) {
                        await assertSessions(senderKeyRecipients);
                    }
                    await authState.keys.transaction(async () => {
                        const patched = await patchMessageBeforeSending(message);
                        if (Array.isArray(patched)) {
                            throw new Boom('Per-jid patching is not supported in groups');
                        }
                        const bytes = encodeWAMessage(patched);
                        reportingMessage = patched;
                        reportingSender = groupSenderIdentity;
                        logger.debug({ destinationJid, groupAddressingMode, groupSenderIdentity }, 'encrypting group message');
                        const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
                            group: destinationJid,
                            data: bytes,
                            meId: groupSenderIdentity
                        });
                        if (senderKeyRecipients.length) {
                            logger.debug({ senderKeyJids: senderKeyRecipients }, 'sending new sender key');
                            const senderKeyMsg = {
                                senderKeyDistributionMessage: {
                                    axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
                                    groupId: destinationJid
                                }
                            };
                            const result = await createParticipantNodes(senderKeyRecipients, senderKeyMsg, extraAttrs);
                            shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity;
                            participants.push(...result.nodes);
                        }
                        binaryNodeContent.push({
                            tag: 'enc',
                            attrs: { v: '2', type: 'skmsg', ...extraAttrs },
                            content: ciphertext
                        });
                        // Update sender key memory with newly sent keys (a status resend leaves it as the post left it)
                        if (!isStatusResend) {
                            for (const recipient of senderKeyRecipients) {
                                senderKeyMap[recipient] = true;
                            }
                            await authState.keys.set({ 'sender-key-memory': { [senderKeyMemoryId]: senderKeyMap } });
                        }
                    }, jid);
                }
                else {
                    // ADDRESSING CONSISTENCY: Match own identity to conversation context
                    // TODO: investigate if this is true
                    let ownId = meId;
                    if (isLid && meLid) {
                        ownId = meLid;
                        logger.debug({ to: jid, ownId }, 'Using LID identity for @lid conversation');
                    }
                    else {
                        logger.debug({ to: jid, ownId }, 'Using PN identity for @s.whatsapp.net conversation');
                    }
                    const { user: ownUser } = jidDecode(ownId);
                    if (!participant) {
                        const patchedForReporting = await patchMessageBeforeSending(message, [jid]);
                        reportingMessage = Array.isArray(patchedForReporting)
                            ? patchedForReporting.find(item => item.recipientJid === jid) || patchedForReporting[0]
                            : patchedForReporting;
                        reportingSender = ownId;
                    }
                    else if (!areJidsSameUser(participant.jid, meId) && !(meLid && areJidsSameUser(participant.jid, meLid))) {
                        // a resend carries the message's token (WA Web WAWebSendMsgCreateDeviceStanza; none to our own devices),
                        // named for the identity it went under: in a group our LID for a LID device, the group's sender key
                        reportingMessage = message;
                        reportingSender = isGroup ? (meLid && isLidUser(participant.jid) ? meLid : meId) : ownId;
                    }
                    if (!isRetryResend) {
                        const targetUserServer = isLid ? 'lid' : 's.whatsapp.net';
                        devices.push({
                            user: jidDecode(jid).user,
                            device: 0,
                            jid: jidEncode(jidDecode(jid).user, targetUserServer, 0)
                        });
                        if (user !== ownUser) {
                            const ownUserServer = isLid ? 'lid' : 's.whatsapp.net';
                            const ownUserForAddressing = isLid && meLid ? jidDecode(meLid).user : jidDecode(meId).user;
                            devices.push({
                                user: ownUserForAddressing,
                                device: 0,
                                jid: jidEncode(ownUserForAddressing, ownUserServer, 0)
                            });
                        }
                        if (additionalAttributes?.['category'] !== 'peer') {
                            // Clear placeholders and enumerate actual devices
                            devices.length = 0;
                            // Use conversation-appropriate sender identity
                            const senderIdentity = isLid && meLid
                                ? jidEncode(jidDecode(meLid)?.user, 'lid', undefined)
                                : jidEncode(jidDecode(meId)?.user, 's.whatsapp.net', undefined);
                            // Enumerate devices for sender and target with consistent addressing
                            const sessionDevices = await getUSyncDevices([senderIdentity, jid], !!useUserDevicesCache, false);
                            devices.push(...sessionDevices);
                            addLogStep('usync_devices_fetch');
                            logger.debug({
                                deviceCount: devices.length,
                                devices: devices.map(d => `${d.user}:${d.device}@${jidDecode(d.jid)?.server}`)
                            }, 'Device enumeration complete with unified addressing');
                        }
                    }
                    // a resend is encrypted once, below, for the device that asked: WA Web WAWebSendMsgCreateDeviceStanza
                    // sends one <enc> and no <participants>, which here handed that device a second copy
                    if (!isRetryResend) {
                        const allRecipients = [];
                        const meRecipients = [];
                        const otherRecipients = [];
                        const { user: mePnUser } = jidDecode(meId);
                        const { user: meLidUser } = meLid ? jidDecode(meLid) : { user: null };
                        for (const { user: deviceUser, jid: deviceJid } of devices) {
                            const isExactSenderDevice = deviceJid === meId || (meLid && deviceJid === meLid);
                            if (isExactSenderDevice) {
                                logger.debug({ jid: deviceJid, meId, meLid }, 'Skipping exact sender device (whatsmeow pattern)');
                                continue;
                            }
                            // Check if this is our device (could match either PN or LID user)
                            const isMe = deviceUser === mePnUser || deviceUser === meLidUser;
                            if (isMe) {
                                meRecipients.push(deviceJid);
                            }
                            else {
                                otherRecipients.push(deviceJid);
                            }
                            allRecipients.push(deviceJid);
                        }
                        await assertSessions(allRecipients);
                        const [{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 }, { nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }] = await Promise.all([
                            // For own devices: use DSM if available (1:1 chats only)
                            createParticipantNodes(meRecipients, meMsg || message, extraAttrs),
                            createParticipantNodes(otherRecipients, message, extraAttrs, meMsg)
                        ]);
                        participants.push(...meNodes);
                        participants.push(...otherNodes);
                        shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2;
                        addLogStep('encryption');
                    }
                }
                // a status resend got its sender key and skmsg above (WA Web WAWebResendStatusMsg)
                if (isRetryResend && !isStatus) {
                    const isParticipantLid = isLidUser(participant.jid);
                    const isMe = areJidsSameUser(participant.jid, isParticipantLid ? meLid : meId);
                    // On a group retry resend, re-embed the sender-key distribution
                    // message so a requester who lost our sender key can decrypt (#2506).
                    let messageToSend = message;
                    if (isGroupOrStatus) {
                        let groupSenderIdentity;
                        if (meLid && (await signalRepository.hasSenderKey({ group: destinationJid, meId: meLid }))) {
                            groupSenderIdentity = meLid;
                        }
                        else if (await signalRepository.hasSenderKey({ group: destinationJid, meId })) {
                            groupSenderIdentity = meId;
                        }
                        if (groupSenderIdentity) {
                            try {
                                const skdm = await signalRepository.getSenderKeyDistributionMessage({
                                    group: destinationJid,
                                    meId: groupSenderIdentity
                                });
                                messageToSend = {
                                    ...message,
                                    senderKeyDistributionMessage: {
                                        groupId: destinationJid,
                                        axolotlSenderKeyDistributionMessage: skdm
                                    }
                                };
                            }
                            catch (err) {
                                logger.warn({ err, jid: destinationJid }, 'failed to build SKDM for retry, sending without it');
                            }
                        }
                    }
                    const encodedMessageToSend = isMe
                        ? encodeWAMessage({
                            deviceSentMessage: {
                                destinationJid,
                                message: messageToSend
                            }
                        })
                        : encodeWAMessage(messageToSend);
                    await assertSessions([participant.jid]);
                    const { type, ciphertext: encryptedContent } = await signalRepository.encryptMessage({
                        data: encodedMessageToSend,
                        jid: participant.jid
                    });
                    // WA Web WAWebSendMsgCreateDeviceStanza: the resend's <enc> names its count, mediatype and decrypt-fail
                    binaryNodeContent.push({
                        tag: 'enc',
                        attrs: {
                            v: '2',
                            type,
                            count: participant.count.toString(),
                            ...extraAttrs
                        },
                        content: encryptedContent
                    });
                }
                if (participants.length) {
                    if (additionalAttributes?.['category'] === 'peer') {
                        const peerNode = participants[0]?.content?.[0];
                        if (peerNode) {
                            binaryNodeContent.push(peerNode); // push only enc
                        }
                    }
                    else {
                        binaryNodeContent.push({
                            tag: 'participants',
                            attrs: {},
                            content: participants
                        });
                    }
                }
                const stanza = {
                    tag: 'message',
                    attrs: {
                        id: msgId,
                        to: jid,
                        type: getMessageType(message),
                        ...(peerRecipientPn ? { peer_recipient_pn: peerRecipientPn } : {}),
                        ...(additionalAttributes || {})
                    },
                    content: binaryNodeContent
                };
                // if the participant to send to is explicitly specified (generally retry recp)
                // ensure the message is only sent to that person
                // if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
                // (a status resend stays to status@broadcast; its <participants> name the device, WA Web WAWebResendStatusMsg)
                if (participant && !isStatus) {
                    if (isJidGroup(jid)) {
                        stanza.attrs.to = jid;
                        stanza.attrs.participant = participant.jid;
                    }
                    else if (areJidsSameUser(participant.jid, meId)) {
                        stanza.attrs.to = participant.jid;
                        stanza.attrs.recipient = jid;
                    }
                    else {
                        stanza.attrs.to = participant.jid;
                    }
                }
                if (shouldIncludeDeviceIdentity) {
                    ;
                    stanza.content.push({
                        tag: 'device-identity',
                        attrs: {},
                        content: encodeSignedDeviceIdentity(authState.creds.account, true)
                    });
                    logger.debug({ jid }, 'adding device identity');
                }
                // WA Web WAWebSendMsgCreateFanoutStanza / WAWebSendGroupSkmsgJob / WAWebEncryptAndSendStatusMsg: the <reporting>
                // of WAWebReportingTokenUtils.genReportingTokenBody, for the protobuf as encrypted minus its random padding,
                // named for the identity we send under (WA Web msg.from) and the chat; none for a peer message
                // (WAWebSendMsgCreateDeviceStanza). Resends carry it too (WAWebSendMsgCreateDeviceStanza, not to our own
                // devices; WAWebResendStatusMsg). A group-status post goes without: WA Web never posts one, and the post shape
                // phones were seen to show (2026-10-30) has none
                if (!isNewsletter &&
                    !isPeerCategory &&
                    reportingSender &&
                    reportingMessage?.messageContextInfo?.messageSecret &&
                    !normalizeMessageContent(reportingMessage)?.groupStatusMessageV2 &&
                    shouldIncludeReportingToken(reportingMessage)) {
                    try {
                        const reportingKey = {
                            id: msgId,
                            fromMe: true,
                            remoteJid: destinationJid,
                            participant: reportingSender
                        };
                        const reportingNode = await getMessageReportingToken(proto.Message.encode(reportingMessage).finish(), reportingMessage, reportingKey);
                        if (reportingNode) {
                            ;
                            stanza.content.push(reportingNode);
                            logger.trace({ jid }, 'added reporting token to message');
                        }
                    }
                    catch (error) {
                        logger.warn({ jid, trace: error?.stack }, 'failed to attach reporting token');
                    }
                }
                const is1on1Send = !isGroup && !isRetryResend && !isStatus && !isNewsletter;
                // Resolve destination to LID for tctoken storage — matches Signal session key pattern
                const tcTokenJid = is1on1Send
                    ? await resolveTcTokenJid(destinationJid, signalRepository.lidMapping.getLIDForPN.bind(signalRepository.lidMapping))
                    : destinationJid;
                const contactTcTokenData = is1on1Send ? await authState.keys.get('tctoken', [tcTokenJid]) : {};
                const existingTokenEntry = contactTcTokenData[tcTokenJid];
                let tcTokenBuffer = existingTokenEntry?.token;
                // Treat expired tokens the same as missing — clear the stored token; our
                // token is re-issued by the throttled fire-and-forget below when the bucket rolls.
                if (tcTokenBuffer?.length && isTcTokenExpired(existingTokenEntry?.timestamp)) {
                    logger.debug({ jid: destinationJid, timestamp: existingTokenEntry?.timestamp }, 'tctoken expired, will re-fetch');
                    tcTokenBuffer = undefined;
                    // Opportunistic cleanup: drop the expired token but KEEP senderTimestamp so the
                    // durable issuance throttle survives (matches buildTcTokenFromJid; else this contact
                    // re-issues once after the token expires).
                    try {
                        await authState.keys.set({
                            tctoken: {
                                [tcTokenJid]: existingTokenEntry?.senderTimestamp
                                    ? { senderTimestamp: existingTokenEntry.senderTimestamp }
                                    : null
                            }
                        });
                    }
                    catch {
                        /* ignore cleanup errors */
                    }
                }
                // AB prop 10518 (privacy_token_sending_on_all_1_on_1_messages) gates whether
                // we attach the tctoken to outbound 1:1 messages. Default true to avoid
                // server error 463 on accounts where the prop hasn't yet been pushed.
                if (tcTokenBuffer?.length && sock.serverProps.privacyTokenOn1to1) {
                    ;
                    stanza.content.push({
                        tag: 'tctoken',
                        attrs: {},
                        content: tcTokenBuffer
                    });
                }
                else if (is1on1Send && authState.creds.nctSalt?.length) {
                    // Fallback when no tctoken is available: attach a locally-derived
                    // cstoken so the send still carries a privacy token and doesn't count
                    // as a "reach out" (issue #2441). Mirrors whatsmeow's send path.
                    const csToken = await generateCsToken({
                        jid: destinationJid,
                        nctSalt: authState.creds.nctSalt,
                        getLIDForPN: signalRepository.lidMapping.getLIDForPN.bind(signalRepository.lidMapping)
                    });
                    if (csToken?.length) {
                        ;
                        stanza.content.push({
                            tag: 'cstoken',
                            attrs: {},
                            content: csToken
                        });
                    }
                }
                // WA Web buildStatusMetaNode: a post names its privacy setting in <meta status_setting>, merged into the meta
                // the post has (WAWebEncryptAndSendStatusMsg re()); a resend, a revoke and a reaction name none
                const isStatusPost = isStatus &&
                    !isRetryResend &&
                    !msgContent?.reactionMessage &&
                    msgContent?.protocolMessage?.type !== proto.Message.ProtocolMessage.Type.REVOKE;
                const nodesToAdd = statusSetting && isStatusPost ? withStatusSetting(additionalNodes || [], statusSetting) : additionalNodes;
                if (nodesToAdd && nodesToAdd.length > 0) {
                    ;
                    stanza.content.push(...nodesToAdd);
                }
                logger.debug({ msgId, destinationJid, participants: participants.length, devices: devices.length, meId, meLid }, 'sending message to devices');
                await sendNode(stanza);
                addLogStep('send_node');
                // Fire-and-forget: issue OUR token to the contact after send (WA Web sendTcToken).
                // Throttle to one issuance per 7-day bucket (WA Web tctoken_duration_sender=604800,
                // tctoken_num_buckets_sender=4). The last-issued time comes from a race-safe in-memory
                // stamp, falling back to the DURABLE senderTimestamp on the tctoken entry — WA Web
                // persists tcTokenSenderTimestamp on the Conversation record, so the throttle survives a
                // restart. The in-memory stamp sits on top and also prevents a rapid double-issue.
                const lastIssuedTs = tcTokenSenderTsCache.get(tcTokenJid) ?? existingTokenEntry?.senderTimestamp;
                // Server-corrected time: WA Web keys the tctoken issuance buckets to server time,
                // so a skewed local clock must not shift which 7-day bucket we consider current.
                const nowSeconds = Math.floor((sock.getServerTime?.() ?? Date.now()) / 1000);
                if (is1on1Send && shouldSendNewTcToken(lastIssuedTs, nowSeconds)) {
                    const issueTimestamp = nowSeconds;
                    // Stamp the in-memory cache SYNCHRONOUSLY (before the async IQ) so the very next send,
                    // even in the same tick, is suppressed. AB prop 14303 controls LID-vs-PN issuance target.
                    tcTokenSenderTsCache.set(tcTokenJid, issueTimestamp);
                    const getLIDForPN = signalRepository.lidMapping.getLIDForPN.bind(signalRepository.lidMapping);
                    const getPNForLID = signalRepository.lidMapping.getPNForLID.bind(signalRepository.lidMapping);
                    resolveIssuanceJid(destinationJid, sock.serverProps.lidTrustedTokenIssueToLid, getLIDForPN, getPNForLID)
                        .then(issueJid => getPrivacyTokens([issueJid], issueTimestamp))
                        .then(() => {
                        // Persist senderTimestamp DURABLY after a successful issue (WA Web merges
                        // tcTokenSenderTimestamp onto the chat record, regardless of token presence). MUST run
                        // OUTSIDE the committed send transaction, else the write lands in it and is dropped.
                        return authState.keys.exitTransaction?.(async () => {
                            const cur = (await authState.keys.get('tctoken', [tcTokenJid]))[tcTokenJid];
                            await authState.keys.set({
                                tctoken: { [tcTokenJid]: { ...cur, senderTimestamp: issueTimestamp } }
                            });
                        })?.catch((err) => logger.debug({ jid: destinationJid, err: err?.message }, 'failed to persist tctoken senderTimestamp'));
                    })
                        .catch(err => {
                        // Issuance IQ failed - drop the optimistic in-memory stamp so the next send retries
                        // instead of staying blocked for the whole bucket.
                        if (tcTokenSenderTsCache.get(tcTokenJid) === issueTimestamp) {
                            tcTokenSenderTsCache.del(tcTokenJid);
                        }
                        logger.debug({ jid: destinationJid, err: err?.message }, 'fire-and-forget tctoken issuance failed');
                    });
                }
                // Add message to retry cache if enabled. A status post goes to the status cache with its audience, so a
                // viewer's retry finds it all its life; a reaction to a status is no post and stays in the general cache
                if (messageRetryManager && !participant) {
                    if (statusAudience && !msgContent?.reactionMessage) {
                        messageRetryManager.addRecentStatus(msgId, message, statusAudience);
                    }
                    else {
                        messageRetryManager.addRecentMessage(destinationJid, msgId, message);
                    }
                }
            }, meId);
            isCompleted = true;
            clearTimeout(timeout);
            return msgId;
        }
        catch (error) {
            logDetailedCycle('failed');
            clearTimeout(timeout);
            throw error;
        }
    };
    const waUploadToServer = getWAUploadToServer(config, refreshMediaConn);
    const waitForMsgMediaUpdate = bindWaitForEvent(ev, 'messages.media-update');
    /**
     * In-flight media re-upload requests by message id. WA Web WAWebRequestMediaReuploadManager keys them by
     * msg.id.id and hands a second request the same promise without sending another server-error receipt.
     */
    const mediaReuploadRequests = new Map();
    /** sends the server-error receipt and resolves with the primary's decrypted, successful answer */
    const requestMediaReupload = async (node, msgId, mediaKey) => {
        let reply = undefined;
        let error = undefined;
        await Promise.all([
            sendNode(node),
            waitForMsgMediaUpdate(async (update) => {
                const result = update.find(c => c.key.id === msgId);
                if (!result) {
                    return false;
                }
                if (result.error) {
                    error = result.error;
                }
                else {
                    try {
                        // a reply sealed for another message id rejects. Deviation: WA Web nacks it and keeps the
                        // request pending, but nothing here times out (only a closed connection ends the wait)
                        const media = decryptMediaRetryData(result.media, mediaKey, msgId);
                        if (media.result !== proto.MediaRetryNotification.ResultType.SUCCESS) {
                            const resultStr = proto.MediaRetryNotification.ResultType[media.result];
                            throw new Boom(`Media re-upload failed by device (${resultStr})`, {
                                data: media,
                                statusCode: getStatusCodeForMediaRetry(media.result) || 404
                            });
                        }
                        reply = media;
                    }
                    catch (err) {
                        error = err;
                    }
                }
                return true;
            })
        ]);
        if (error) {
            throw error;
        }
        return reply;
    };
    /**
     * Delete media from WhatsApp servers at the given direct path.
     * Used for things like history-sync attachments that should be cleaned up
     * after processing. Mirrors whatsmeow's Client.DeleteMedia (02ec817).
     */
    const deleteMedia = async (appInfo, directPath, encFileHash, encHandle) => {
        // whatsmeow 6723e556: inline payloads leave nothing on the CDN
        if (!directPath) {
            return;
        }
        const mediaConn = await refreshMediaConn(false);
        const host = mediaConn.hosts[0]?.hostname;
        if (!host) {
            throw new Boom('No media host available');
        }
        // Strip any query string from the direct path before encoding
        const queryStart = directPath.indexOf('?');
        const cleanPath = queryStart > 0 ? directPath.slice(0, queryStart) : directPath;
        // WA Web WABase64UrlSafe.urlSafeBase64 / whatsmeow base64.URLEncoding: URL-safe alphabet, padding kept
        const token = Buffer.from(encFileHash).toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
        const params = {
            token,
            d_md: Buffer.from(cleanPath).toString('base64url'),
            auth: mediaConn.auth
        };
        if (encHandle) {
            params.e_handle = encHandle;
        }
        // history/app-state blobs are deleted from their own /mms/<type> path (WA Web
        // WAWebMmsClientFormatHashUrl, whatsmeow mediaTypeToMMSType), not the upload path in MEDIA_PATH_MAP
        const mmsSegment = appInfo === 'md-msg-hist' || appInfo === 'md-app-state'
            ? appInfo
            : (MEDIA_PATH_MAP[appInfo] || `/mms/${appInfo}`).replace(/^\/mms\//, '');
        const url = `https://${host}/mms/${mmsSegment}/${token}?${new URLSearchParams(params).toString()}`;
        // Send the companion meta nonce for companion-uploaded media, matching whatsmeow (6723e55):
        // only when both the nonce and an encryption handle are present.
        const companionMetaNonce = authState.creds.companionMetaNonce;
        const headers = {
            Origin: DEFAULT_ORIGIN,
            Referer: `${DEFAULT_ORIGIN}/`
        };
        if (companionMetaNonce && encHandle) {
            headers.Companion_User_Secret = companionMetaNonce;
        }
        // the same proxy as media downloads (options.dispatcher) / uploads (undici-style fetchAgent), and bounded:
        // history blobs are deleted automatically, so a proxied or offline host must not leak or hang here
        const agentDispatcher = typeof config.fetchAgent?.dispatch === 'function' ? config.fetchAgent : undefined;
        const dispatcher = httpRequestOptions?.dispatcher ?? agentDispatcher;
        const response = await fetch(url, {
            ...(dispatcher ? { dispatcher } : {}),
            method: 'DELETE',
            headers,
            signal: AbortSignal.timeout(config.defaultQueryTimeoutMs ?? 60000)
        });
        if (!response.ok) {
            throw new Boom(`media delete failed: ${response.status} ${response.statusText}`, {
                statusCode: response.status
            });
        }
    };
    // WA Web (WAWebHandleHistorySyncChunk) / whatsmeow: delete a downloaded history blob once processed
    sock.setHistorySyncBlobProcessedHandler(notification => {
        deleteMedia('md-msg-hist', notification.directPath, notification.fileEncSha256, notification.encHandle || undefined).catch(err => logger.warn({ err }, 'failed to delete history sync media from server'));
    });
    return {
        ...sock,
        userDevicesCache,
        devicesMutex,
        assertSessions,
        getPrivacyTokens,
        relayMessage,
        sendReceipt,
        sendReceipts,
        readMessages,
        refreshMediaConn,
        waUploadToServer,
        deleteMedia,
        fetchPrivacySettings,
        sendPeerMessage,
        sendPeerDataOperationMessage,
        createParticipantNodes,
        getUSyncDevices,
        messageRetryManager,
        updateMemberLabel,
        updateMediaMessage: async (message) => {
            const content = assertMediaContent(message.message);
            const mediaKey = content.mediaKey;
            const meId = authState.creds.me.id;
            // a message without a media key fails here (no retry key), before the in-flight lookup and before any
            // receipt is sent: where WA Web WAWebRequestMediaReuploadManager throws RMRWithoutMediaKeyError
            const node = encryptMediaRetryRequest(message.key, mediaKey, meId);
            const msgId = message.key.id;
            let request = mediaReuploadRequests.get(msgId);
            if (!request) {
                request = requestMediaReupload(node, msgId, mediaKey).finally(() => mediaReuploadRequests.delete(msgId));
                mediaReuploadRequests.set(msgId, request);
            }
            // every caller applies the answer to its own message (WA Web WAWebMediaObject swaps only directPath/url)
            const media = await request;
            content.directPath = media.directPath;
            content.url = getUrlFromDirectPath(content.directPath);
            logger.debug({ directPath: media.directPath, key: message.key }, 'media update successful');
            ev.emit('messages.update', [{ key: message.key, update: { message: message.message } }]);
            return message;
        },
        sendMessage: async (jid, content, options = {}) => {
            const userJid = authState.creds.me.id;
            if (typeof content === 'object' &&
                'disappearingMessagesInChat' in content &&
                typeof content['disappearingMessagesInChat'] !== 'undefined' &&
                isJidGroup(jid)) {
                const { disappearingMessagesInChat } = content;
                const value = typeof disappearingMessagesInChat === 'boolean'
                    ? disappearingMessagesInChat
                        ? WA_DEFAULT_EPHEMERAL
                        : 0
                    : disappearingMessagesInChat;
                await groupToggleEphemeral(jid, value);
            }
            else {
                const fullMsg = await generateWAMessage(jid, content, {
                    logger,
                    userJid,
                    getUrlInfo: text => getUrlInfo(text, {
                        thumbnailWidth: linkPreviewImageThumbnailWidth,
                        fetchOpts: {
                            timeout: 3000,
                            ...(httpRequestOptions || {})
                        },
                        logger,
                        uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
                    }),
                    //TODO: CACHE
                    getProfilePicUrl: sock.profilePictureUrl,
                    getCallLink: sock.createCallLink,
                    upload: waUploadToServer,
                    mediaCache: config.mediaCache,
                    options: config.options,
                    messageId: generateMessageIDV2(sock.user?.id),
                    ...options
                });
                const isEventMsg = 'event' in content && !!content.event;
                const isDeleteMsg = 'delete' in content && !!content.delete;
                const isEditMsg = 'edit' in content && !!content.edit;
                const isPinMsg = 'pin' in content && !!content.pin;
                const isPollMessage = 'poll' in content && !!content.poll;
                const additionalAttributes = {};
                const additionalNodes = [];
                // required for delete
                if (isDeleteMsg) {
                    // if the chat is a group, and I am not the author, then delete the message as an admin
                    if (isJidGroup(content.delete?.remoteJid) && !content.delete?.fromMe) {
                        additionalAttributes.edit = '8';
                    }
                    else {
                        additionalAttributes.edit = '7';
                    }
                }
                else if (isEditMsg) {
                    additionalAttributes.edit = '1';
                }
                else if (isPinMsg) {
                    additionalAttributes.edit = '2';
                }
                else if (isPollMessage) {
                    additionalNodes.push({
                        tag: 'meta',
                        attrs: {
                            polltype: 'creation'
                        }
                    });
                }
                else if (isEventMsg) {
                    additionalNodes.push({
                        tag: 'meta',
                        attrs: {
                            event_type: 'creation'
                        }
                    });
                }
                // WA Web WAWebAddonSendProcess: an add-on to a group status (a reaction, a pin) is marked isGroupStatus,
                // so WAWebSendMsgMetaNode sends it with <meta is_group_status="true">
                const addonTarget = 'react' in content ? content.react?.key : 'pin' in content ? content.pin : undefined;
                if (addonTarget?.isGroupStatus) {
                    additionalNodes.push({ tag: 'meta', attrs: { is_group_status: 'true' } });
                }
                await relayMessage(jid, fullMsg.message, {
                    messageId: fullMsg.key.id,
                    useCachedGroupMetadata: options.useCachedGroupMetadata,
                    additionalAttributes,
                    statusJidList: options.statusJidList,
                    statusSetting: options.statusSetting,
                    additionalNodes,
                    useUserDevicesCache: options.useUserDevicesCache
                });
                if (config.emitOwnEvents) {
                    process.nextTick(async () => {
                        await messageMutex.mutex(() => upsertMessage(fullMsg, 'append'));
                    });
                }
                return fullMsg;
            }
        }
    };
};
/**
 * Content classified for the stanza `type` and enc `mediatype` attrs. WA Web's send-side classifiers
 * (WAWebE2EProtoUtils.typeAttributeFromProtobuf via getUnwrappedProtobufMessage, and October's
 * WAWebBackendJobsCommon.mediaTypeFromProtobuf) also look inside a spoiler; the content itself keeps the
 * wrapper (normalizeMessageContent does not unwrap it, like WA Web's maybeGetFutureproofMessage), so forwards
 * and quotes stay spoilers.
 */
const getStanzaClassificationContent = (message) => {
    let content = normalizeMessageContent(message);
    for (let i = 0; i < 5 && content?.spoilerMessage?.message; i++) {
        content = normalizeMessageContent(content.spoilerMessage.message);
    }
    return content;
};
const getMessageType = (message) => {
    const normalizedMessage = getStanzaClassificationContent(message);
    if (!normalizedMessage) {
        return 'text';
    }
    if (normalizedMessage.reactionMessage || normalizedMessage.encReactionMessage) {
        return 'reaction';
    }
    if (normalizedMessage.pollCreationMessage ||
        normalizedMessage.pollCreationMessageV2 ||
        normalizedMessage.pollCreationMessageV3 ||
        normalizedMessage.pollUpdateMessage) {
        return 'poll';
    }
    if (normalizedMessage.eventMessage) {
        return 'event';
    }
    const mediaType = getMediaType(normalizedMessage);
    const blobTypes = ['image', 'video', 'gif', 'audio', 'ptt', 'sticker', 'document', 'product'];
    if (mediaType && blobTypes.includes(mediaType)) {
        return 'media';
    }
    return 'text';
};
//# sourceMappingURL=messages-send.js.map