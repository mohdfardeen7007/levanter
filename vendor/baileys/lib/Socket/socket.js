import { Boom } from '@hapi/boom';
import { randomBytes } from 'crypto';
import PQueue from 'p-queue';
import { URL } from 'url';
import { promisify } from 'util';
import { proto } from '../../WAProto/index.js';
import { DEF_CALLBACK_PREFIX, DEF_TAG_PREFIX, INITIAL_PREKEY_COUNT, MIN_PREKEY_COUNT, MIN_UPLOAD_INTERVAL, NOISE_WA_HEADER, PROCESSABLE_HISTORY_TYPES, TimeMs, UPLOAD_TIMEOUT } from '../Defaults/index.js';
import { DisconnectReason } from '../Types/index.js';
import { addTransactionCapability, aesEncryptCTR, bindWaitForConnectionUpdate, buildPairingQRData, bytesToCrockford, configureSuccessfulPairing, Curve, derivePairingCodeKey, generateLevanterCode, generateLoginNode, generateMdTagPrefix, generateRegistrationNode, getCodeFromWSError, getCompanionPlatformId, getErrorCodeFromStreamError, getNextPreKeysNode, makeEventBuffer, makeNoiseHandler, makeOfflineBatchRequester, promiseTimeout, signedKeyPair, xmppSignedPreKey } from '../Utils/index.js';
import { makeKeyedMutex } from '../Utils/make-mutex.js';
import { getPlatformId } from '../Utils/browser-utils.js';
import { assertNodeErrorFree, binaryNodeToString, encodeBinaryNode, getAllBinaryNodeChildren, getBinaryNodeChild, getBinaryNodeChildren, isLidUser, jidDecode, jidEncode, S_WHATSAPP_NET } from '../WABinary/index.js';
import { BinaryInfo } from '../WAM/BinaryInfo.js';
import { buildUsernameLookupQuery, toOnWhatsAppResults, toUsernameLookupResult, USyncQuery, USyncUser } from '../WAUSync/index.js';
import { WebSocketClient } from './Client/index.js';
/**
 * Connects to WA servers and performs:
 * - simple queries (no retry mechanism, wait for connection establishment)
 * - listen to messages and emit events
 * - query phone connection
 */
/** WA Web connectAttemptCount: successful stream opens in this process, per account (creds object) */
const connectAttemptCounts = new WeakMap();
export const makeSocket = (config) => {
    const { waWebSocketUrl, connectTimeoutMs, logger, keepAliveIntervalMs, browser, auth: authState, printQRInTerminal, defaultQueryTimeoutMs, transactionOpts, qrTimeout, makeSignalRepository, ev: customEv } = config;
    const publicWAMBuffer = new BinaryInfo();
    let serverTimeOffsetMs = 0;
    const uqTagId = generateMdTagPrefix();
    const generateMessageTag = () => `${uqTagId}${epoch++}`;
    if (printQRInTerminal) {
        console.warn('⚠️ The printQRInTerminal option has been deprecated. You will no longer receive QR codes in the terminal automatically. Please listen to the connection.update event yourself and handle the QR your way. You can remove this message by removing this opttion. This message will be removed in a future version.');
    }
    if (browser[1].toLocaleLowerCase().includes('android')) {
        logger.warn('⚠️ Using the Android browser is experimental and may lead to unexpected behavior. Use at your own risk.');
    }
    const syncDisabled = PROCESSABLE_HISTORY_TYPES.map((syncType) => config.shouldSyncHistoryMessage({ syncType })).filter((x) => x === false)
        .length === PROCESSABLE_HISTORY_TYPES.length;
    if (syncDisabled) {
        logger.warn('⚠️ DANGER: DISABLING ALL SYNC BY shouldSyncHistoryMsg PREVENTS BAILEYS FROM ACCESSING INITIAL LID MAPPINGS, LEADING TO INSTABILIY AND SESSION ERRORS');
    }
    const url = typeof waWebSocketUrl === 'string' ? new URL(waWebSocketUrl) : waWebSocketUrl;
    if (config.mobile || url.protocol === 'tcp:') {
        throw new Boom('Mobile API is not supported anymore', { statusCode: DisconnectReason.loggedOut });
    }
    if (url.protocol === 'wss' && authState?.creds?.routingInfo) {
        url.searchParams.append('ED', authState.creds.routingInfo.toString('base64url'));
    }
    const disposables = [];
    /** ephemeral key pair used to encrypt/decrypt communication. Unique for each connection */
    const ephemeralKeyPair = Curve.generateKeyPair();
    /** WA noise protocol wrapper */
    const noise = makeNoiseHandler({
        keyPair: ephemeralKeyPair,
        NOISE_HEADER: NOISE_WA_HEADER,
        logger,
        routingInfo: authState?.creds?.routingInfo,
        certTrustAnchor: config.certTrustAnchor
    });
    const ws = new WebSocketClient(url, config);
    ws.connect();
    // Processing queue for serialized message handling
    const processingQueue = new PQueue({
        concurrency: config.handlerConcurrency || 1,
        carryoverConcurrencyCount: true
    });
    const keyedMutex = makeKeyedMutex();
    const sendPromise = promisify(ws.send);
    /** send a raw buffer */
    const sendRawMessage = async (data) => {
        if (!ws.isOpen) {
            throw new Boom('Connection Closed', { statusCode: DisconnectReason.connectionClosed });
        }
        const bytes = noise.encodeFrame(data);
        await promiseTimeout(connectTimeoutMs, async (resolve, reject) => {
            try {
                await sendPromise.call(ws, bytes);
                resolve();
            }
            catch (error) {
                reject(error);
            }
        });
    };
    /** send a binary node */
    const sendNode = (frame) => {
        if (logger.level === 'trace') {
            logger.trace({ xml: binaryNodeToString(frame), msg: 'xml send' });
        }
        const buff = encodeBinaryNode(frame);
        return sendRawMessage(buff);
    };
    /** WA Web WAWebOfflineHandler (zapo 8be4b0b8): asks the server for queued offline stanzas one window at a time */
    const offlineBatch = makeOfflineBatchRequester({
        sendNode,
        logger,
        // WA Web WAWebBlockingOfflineResumeManager: the 60 s stanza timer completes a resume whose
        // <ib><offline/> never arrived, so receivedPendingNotifications cannot stay false forever
        onIdleTimeout: () => completeOfflineResume()
    });
    disposables.push(() => offlineBatch.stop());
    /**
     * Wait for a message with a certain tag to be received
     * @param msgId the message tag to await
     * @param timeoutMs timeout after which the promise will reject
     */
    const waitForMessage = async (msgId, timeoutMs = defaultQueryTimeoutMs) => {
        let onRecv;
        let onErr;
        try {
            const result = await promiseTimeout(timeoutMs, (resolve, reject) => {
                onRecv = data => {
                    resolve(data);
                };
                onErr = err => {
                    reject(err ||
                        new Boom('Connection Closed', {
                            statusCode: DisconnectReason.connectionClosed
                        }));
                };
                ws.on(`TAG:${msgId}`, onRecv);
                ws.on('close', onErr);
                ws.on('error', onErr);
                return () => reject(new Boom('Query Cancelled'));
            });
            return result;
        }
        catch (error) {
            // Catch timeout and return undefined instead of throwing
            if (error instanceof Boom && error.output?.statusCode === DisconnectReason.timedOut) {
                logger?.warn?.({ msgId }, 'timed out waiting for message');
                return undefined;
            }
            throw error;
        }
        finally {
            if (onRecv)
                ws.off(`TAG:${msgId}`, onRecv);
            if (onErr) {
                ws.off('close', onErr);
                ws.off('error', onErr);
            }
        }
    };
    /** send a query, and wait for its response. auto-generates message ID if not provided */
    const query = async (node, timeoutMs) => {
        if (!node.attrs.id) {
            node.attrs.id = generateMessageTag();
        }
        const msgId = node.attrs.id;
        const result = await promiseTimeout(timeoutMs, async (resolve, reject) => {
            const result = waitForMessage(msgId, timeoutMs).catch(reject);
            sendNode(node)
                .then(async () => resolve(await result))
                .catch(reject);
        });
        if (result && 'tag' in result) {
            assertNodeErrorFree(result);
        }
        return result;
    };
    // Validate current key-bundle on server; on failure, trigger pre-key upload and rethrow
    const digestKeyBundle = async () => {
        const res = await query({
            tag: 'iq',
            attrs: { to: S_WHATSAPP_NET, type: 'get', xmlns: 'encrypt' },
            content: [{ tag: 'digest', attrs: {} }]
        });
        const digestNode = getBinaryNodeChild(res, 'digest');
        if (!digestNode) {
            await uploadPreKeys();
            throw new Error('encrypt/get digest returned no digest node');
        }
    };
    // Rotate our signed pre-key on server; on failure, run digest as fallback and rethrow
    const rotateSignedPreKey = async () => {
        const newId = (creds.signedPreKey.keyId || 0) + 1;
        const skey = await signedKeyPair(creds.signedIdentityKey, newId);
        await query({
            tag: 'iq',
            attrs: { to: S_WHATSAPP_NET, type: 'set', xmlns: 'encrypt' },
            content: [
                {
                    tag: 'rotate',
                    attrs: {},
                    content: [xmppSignedPreKey(skey)]
                }
            ]
        });
        // Persist new signed pre-key in creds
        ev.emit('creds.update', { signedPreKey: skey });
    };
    const executeUSyncQuery = async (usyncQuery) => {
        if (usyncQuery.protocols.length === 0) {
            throw new Boom('USyncQuery must have at least one protocol');
        }
        // todo: validate users, throw WARNING on no valid users
        // variable below has only validated users
        const validUsers = usyncQuery.users;
        const userNodes = validUsers.map(user => {
            return {
                tag: 'user',
                attrs: {
                    jid: !user.phone ? user.id : undefined
                },
                content: usyncQuery.protocols.map(a => a.getUserElement(user)).filter(a => a !== null)
            };
        });
        const listNode = {
            tag: 'list',
            attrs: {},
            content: userNodes
        };
        const queryNode = {
            tag: 'query',
            attrs: {},
            content: usyncQuery.protocols.map(a => a.getQueryElement())
        };
        const iq = {
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                type: 'get',
                xmlns: 'usync'
            },
            content: [
                {
                    tag: 'usync',
                    attrs: {
                        context: usyncQuery.context,
                        mode: usyncQuery.mode,
                        sid: generateMessageTag(),
                        last: 'true',
                        index: '0'
                    },
                    content: [queryNode, listNode]
                }
            ]
        };
        const result = await query(iq);
        return usyncQuery.parseUSyncQueryResult(result);
    };
    const onWhatsApp = async (...phoneNumber) => {
        let usyncQuery = new USyncQuery();
        let contactEnabled = false;
        for (const jid of phoneNumber) {
            if (isLidUser(jid)) {
                logger?.warn('LIDs are not supported with onWhatsApp');
                continue;
            }
            else {
                if (!contactEnabled) {
                    contactEnabled = true;
                    usyncQuery = usyncQuery.withContactProtocol('lid');
                }
                const phone = `+${jid.replace('+', '').split('@')[0]?.split(':')[0]}`;
                usyncQuery.withUser(new USyncUser().withPhone(phone));
            }
        }
        if (usyncQuery.users.length === 0) {
            return []; // return early without forcing an empty query
        }
        const results = await executeUSyncQuery(usyncQuery);
        if (results) {
            const { results: entries, mappings } = toOnWhatsAppResults(results.list);
            if (mappings.length) {
                await signalRepository.lidMapping
                    .storeLIDPNMappings(mappings)
                    .catch(err => logger.warn({ err }, 'failed to store LID mappings from onWhatsApp'));
            }
            return entries;
        }
    };
    /**
     * Look up a username handle (`alice`, `@alice` or `@alice:1234`; `usernameKey` overrides the
     * handle's key) — WA Web WAWebQueryExistsJob.queryUsernameExists (zapo 471ca18a).
     */
    const resolveUsername = async (handle, usernameKey) => {
        const lookup = buildUsernameLookupQuery(handle, usernameKey);
        if (!lookup) {
            return { status: 'invalid' };
        }
        const result = await executeUSyncQuery(lookup);
        return result ? toUsernameLookupResult(result) : { status: 'not-found' };
    };
    const pnFromLIDUSync = async (jids) => {
        const usyncQuery = new USyncQuery().withLIDProtocol().withContext('background');
        for (const jid of jids) {
            if (isLidUser(jid)) {
                logger?.warn('LID user found in LID fetch call');
                continue;
            }
            else {
                usyncQuery.withUser(new USyncUser().withId(jid));
            }
        }
        if (usyncQuery.users.length === 0) {
            return []; // return early without forcing an empty query
        }
        const results = await executeUSyncQuery(usyncQuery);
        if (results) {
            return results.list.filter(a => !!a.lid).map(({ lid, id }) => ({ pn: id, lid: lid }));
        }
        return [];
    };
    const ev = (customEv || makeEventBuffer(logger));
    const { creds } = authState;
    // add transaction capability
    const keys = addTransactionCapability(authState.keys, logger, transactionOpts);
    const signalRepository = makeSignalRepository({ creds, keys }, logger, pnFromLIDUSync);
    let lastDateRecv;
    let epoch = 1;
    let keepAliveReq;
    let qrTimer;
    let closed = false;
    /** log & process any unexpected errors */
    const onUnexpectedError = (err, msg) => {
        logger.error({ err }, `unexpected error in '${msg}'`);
    };
    /** await the next incoming message */
    const awaitNextMessage = async (sendMsg) => {
        if (!ws.isOpen) {
            throw new Boom('Connection Closed', {
                statusCode: DisconnectReason.connectionClosed
            });
        }
        let onOpen;
        let onClose;
        const result = promiseTimeout(connectTimeoutMs, (resolve, reject) => {
            onOpen = resolve;
            onClose = mapWebSocketError(reject);
            ws.on('frame', onOpen);
            ws.on('close', onClose);
            ws.on('error', onClose);
        }).finally(() => {
            ws.off('frame', onOpen);
            ws.off('close', onClose);
            ws.off('error', onClose);
        });
        if (sendMsg) {
            sendRawMessage(sendMsg).catch(onClose);
        }
        return result;
    };
    /** connection handshake */
    const validateConnection = async () => {
        let helloMsg = {
            clientHello: { ephemeral: ephemeralKeyPair.public }
        };
        helloMsg = proto.HandshakeMessage.fromObject(helloMsg);
        logger.info({ browser, helloMsg }, 'connected to WA');
        const init = proto.HandshakeMessage.encode(helloMsg).finish();
        const result = await awaitNextMessage(init);
        const handshake = proto.HandshakeMessage.decode(result);
        logger.trace({ handshake }, 'handshake recv from WA');
        const keyEnc = noise.processHandshake(handshake, creds.noiseKey);
        let node;
        if (!creds.me) {
            node = generateRegistrationNode(creds, config);
            logger.info({ node }, 'not logged in, attempting registration...');
        }
        else {
            node = generateLoginNode(creds.me.id, config, {
                loginCounter: creds.loginCounter,
                connectAttemptCount: connectAttemptCounts.get(creds) ?? 0
            });
            logger.info({ node }, 'logging in...');
        }
        const payloadEnc = noise.encrypt(proto.ClientPayload.encode(node).finish());
        await sendRawMessage(proto.HandshakeMessage.encode({
            clientFinish: {
                static: keyEnc,
                payload: payloadEnc
            }
        }).finish());
        await noise.finishInit();
        startKeepAliveRequest();
    };
    const getAvailablePreKeysOnServer = async () => {
        const result = await query({
            tag: 'iq',
            attrs: {
                id: generateMessageTag(),
                xmlns: 'encrypt',
                type: 'get',
                to: S_WHATSAPP_NET
            },
            content: [{ tag: 'count', attrs: {} }]
        });
        const countChild = getBinaryNodeChild(result, 'count');
        return +countChild.attrs.value;
    };
    // Pre-key upload state management
    let uploadPreKeysPromise = null;
    let lastUploadTime = 0;
    /** generates and uploads a set of pre-keys to the server */
    const uploadPreKeys = async (count = MIN_PREKEY_COUNT, retryCount = 0) => {
        // Check minimum interval (except for retries)
        if (retryCount === 0) {
            const timeSinceLastUpload = Date.now() - lastUploadTime;
            if (timeSinceLastUpload < MIN_UPLOAD_INTERVAL) {
                logger.debug(`Skipping upload, only ${timeSinceLastUpload}ms since last upload`);
                return;
            }
        }
        // Prevent multiple concurrent uploads
        if (uploadPreKeysPromise) {
            logger.debug('Pre-key upload already in progress, waiting for completion');
            await uploadPreKeysPromise;
        }
        const uploadLogic = async () => {
            logger.info({ count, retryCount }, 'uploading pre-keys');
            // Generate and save pre-keys atomically (prevents ID collisions on retry)
            const node = await keys.transaction(async () => {
                logger.debug({ requestedCount: count }, 'generating pre-keys with requested count');
                const { update, node } = await getNextPreKeysNode({ creds, keys }, count);
                // Update credentials immediately to prevent duplicate IDs on retry
                ev.emit('creds.update', update);
                return node; // Only return node since update is already used
            }, creds?.me?.id || 'upload-pre-keys');
            // Upload to server (outside transaction, can fail without affecting local keys)
            try {
                await query(node);
                logger.info({ count }, 'uploaded pre-keys successfully');
                lastUploadTime = Date.now();
            }
            catch (uploadError) {
                logger.error({ uploadError: uploadError.toString(), count }, 'Failed to upload pre-keys to server');
                // Exponential backoff retry (max 3 retries)
                if (retryCount < 3) {
                    const backoffDelay = Math.min(1000 * Math.pow(2, retryCount), 10000);
                    logger.info(`Retrying pre-key upload in ${backoffDelay}ms`);
                    await new Promise(resolve => setTimeout(resolve, backoffDelay));
                    return uploadPreKeys(count, retryCount + 1);
                }
                throw uploadError;
            }
        };
        // Add timeout protection
        uploadPreKeysPromise = Promise.race([
            uploadLogic(),
            new Promise((_, reject) => setTimeout(() => reject(new Boom('Pre-key upload timeout', { statusCode: 408 })), UPLOAD_TIMEOUT))
        ]);
        try {
            await uploadPreKeysPromise;
        }
        finally {
            uploadPreKeysPromise = null;
        }
    };
    const verifyCurrentPreKeyExists = async () => {
        const currentPreKeyId = creds.nextPreKeyId - 1;
        if (currentPreKeyId <= 0) {
            return { exists: false, currentPreKeyId: 0 };
        }
        const preKeys = await keys.get('pre-key', [currentPreKeyId.toString()]);
        const exists = !!preKeys[currentPreKeyId.toString()];
        return { exists, currentPreKeyId };
    };
    const uploadPreKeysToServerIfRequired = async () => {
        try {
            let count = 0;
            const preKeyCount = await getAvailablePreKeysOnServer();
            if (preKeyCount === 0)
                count = INITIAL_PREKEY_COUNT;
            else
                count = MIN_PREKEY_COUNT;
            const { exists: currentPreKeyExists, currentPreKeyId } = await verifyCurrentPreKeyExists();
            logger.info(`${preKeyCount} pre-keys found on server`);
            logger.info(`Current prekey ID: ${currentPreKeyId}, exists in storage: ${currentPreKeyExists}`);
            const lowServerCount = preKeyCount <= count;
            const missingCurrentPreKey = !currentPreKeyExists && currentPreKeyId > 0;
            const shouldUpload = lowServerCount || missingCurrentPreKey;
            if (shouldUpload) {
                const reasons = [];
                if (lowServerCount)
                    reasons.push(`server count low (${preKeyCount})`);
                if (missingCurrentPreKey)
                    reasons.push(`current prekey ${currentPreKeyId} missing from storage`);
                logger.info(`Uploading PreKeys due to: ${reasons.join(', ')}`);
                await uploadPreKeys(count);
            }
            else {
                logger.info(`PreKey validation passed - Server: ${preKeyCount}, Current prekey ${currentPreKeyId} exists`);
            }
        }
        catch (error) {
            logger.error({ error }, 'Failed to check/upload pre-keys during initialization');
            // Don't throw - allow connection to continue even if pre-key check fails
        }
    };
    // Helper for awaiting event listeners
    const emitAsync = async (event, ...args) => {
        const listeners = ws.listeners(event);
        if (listeners.length > 0) {
            for (const listener of listeners) {
                await listener(...args);
            }
            return true;
        }
        return false;
    };
    /** Last <stream:error> decoded off the wire, recorded before it is queued (whatsmeow 28bfe537). */
    let pendingStreamError;
    /** In-flight decode of received bytes; `close` waits for it so a just-received stream:error counts. */
    let lastDecode = Promise.resolve();
    const streamErrorBoom = (node) => {
        const [reasonNode] = getAllBinaryNodeChildren(node);
        const { reason, statusCode } = getErrorCodeFromStreamError(node);
        return new Boom(`Stream Errored (${reason})`, { statusCode, data: reasonNode || node });
    };
    const onMessageReceived = async (data) => {
        const decoding = noise.decodeFrame(data, async (frame) => {
            // reset ping timeout
            lastDateRecv = new Date();
            ws.emit('frame', frame);
            // if it's a binary node
            if (!(frame instanceof Uint8Array)) {
                const msgId = frame.attrs.id;
                if (logger.level === 'trace') {
                    logger.trace({ xml: binaryNodeToString(frame), msg: 'recv xml' });
                }
                /* Check if this is a response to a message we sent */
                const isResponse = ws.emit(`${DEF_TAG_PREFIX}${msgId}`, frame);
                if (isResponse) {
                    return; // Response frames skip queue
                }
                if (frame.tag === 'stream:error') {
                    pendingStreamError = frame;
                }
                // Otherwise, push to the serial processing queue
                processingQueue.add(async () => {
                    const jid = frame.attrs.from || 'global';
                    await keyedMutex.mutex(jid, async () => {
                        // WA Web WAWebCommsHandleStanza (zapo 8be4b0b8): every stanza carrying `offline` advances the
                        // offline pull loop before any handler sees it, so stanzas a handler drops or ignores still count
                        if (frame.attrs.offline) {
                            offlineBatch.onOfflineStanza();
                        }
                        let anyTriggered = false;
                        const l0 = frame.tag;
                        const l1 = frame.attrs || {};
                        const l2 = Array.isArray(frame.content) ? frame.content[0]?.tag : '';
                        for (const key of Object.keys(l1)) {
                            anyTriggered = await emitAsync(`${DEF_CALLBACK_PREFIX}${l0},${key}:${l1[key]},${l2}`, frame) || anyTriggered;
                            anyTriggered = await emitAsync(`${DEF_CALLBACK_PREFIX}${l0},${key}:${l1[key]}`, frame) || anyTriggered;
                            anyTriggered = await emitAsync(`${DEF_CALLBACK_PREFIX}${l0},${key}`, frame) || anyTriggered;
                        }
                        anyTriggered = await emitAsync(`${DEF_CALLBACK_PREFIX}${l0},,${l2}`, frame) || anyTriggered;
                        anyTriggered = await emitAsync(`${DEF_CALLBACK_PREFIX}${l0}`, frame) || anyTriggered;
                        if (!anyTriggered && logger.level === 'debug') {
                            logger.debug({ unhandled: true, msgId, fromMe: false, frame }, 'communication recv');
                        }
                    });
                }).catch(err => onUnexpectedError(err, 'processing queue error'));
            }
        });
        lastDecode = decoding.then(() => undefined, () => undefined);
        await decoding;
    };
    ws.on('CB:Pong', (node) => {
        const serverTime = +(node.attrs.t || 0);
        if (serverTime) {
            serverTimeOffsetMs = (serverTime * 1000) - Date.now();
            logger.debug({ serverTimeOffsetMs }, 'updated server time offset');
        }
    });
    const end = (error) => {
        if (closed) {
            logger.trace({ trace: error?.stack }, 'connection already closed');
            return;
        }
        closed = true;
        logger.info({ trace: error?.stack }, error ? 'connection errored' : 'connection closed');
        for (const dispose of disposables) {
            try {
                dispose();
            }
            catch (err) {
                logger.error({ err }, 'error during disposal');
            }
        }
        disposables.length = 0;
        if (keepAliveReq) {
            clearTimeout(keepAliveReq);
        }
        if (qrTimer) {
            clearTimeout(qrTimer);
        }
        processingQueue.clear();
        if (!ws.isClosed && !ws.isClosing) {
            try {
                ws.close();
            }
            catch { }
        }
        ws.removeAllListeners();
        ev.emit('connection.update', {
            connection: 'close',
            lastDisconnect: {
                error,
                date: new Date()
            }
        });
    };
    const waitForSocketOpen = async () => {
        if (ws.isOpen) {
            return;
        }
        if (ws.isClosed || ws.isClosing) {
            throw new Boom('Connection Closed', { statusCode: DisconnectReason.connectionClosed });
        }
        let onOpen;
        let onClose;
        await new Promise((resolve, reject) => {
            onOpen = () => resolve(undefined);
            onClose = mapWebSocketError(reject);
            ws.on('open', onOpen);
            ws.on('close', onClose);
            ws.on('error', onClose);
        }).finally(() => {
            ws.off('open', onOpen);
            ws.off('close', onClose);
            ws.off('error', onClose);
        });
    };
    const startKeepAliveRequest = () => {
        if (keepAliveReq) {
            clearTimeout(keepAliveReq);
        }
        // Randomise the ping cadence the way the real clients do: whatsmeow draws a fresh
        // interval in [KeepAliveIntervalMin, KeepAliveIntervalMax] (20-30s) for every ping and
        // WA Web jitters its health check by `healthCheckInterval * (1 + Math.random())`.
        // Staggering stops many sockets pinging in lockstep. The upper bound stays at
        // keepAliveIntervalMs so the liveness margin below is unchanged.
        const nextInterval = () => {
            const min = Math.floor(keepAliveIntervalMs * (2 / 3));
            return min + Math.floor(Math.random() * (keepAliveIntervalMs - min + 1));
        };
        const tick = () => {
            if (!lastDateRecv) {
                lastDateRecv = new Date();
            }
            const diff = Date.now() - lastDateRecv.getTime();
            /*
                check if it's been a suspicious amount of time since the server responded with our last seen
                it could be that the network is down
            */
            if (diff > keepAliveIntervalMs + 5000) {
                end(new Boom('Connection was lost', { statusCode: DisconnectReason.connectionLost }));
                return;
            }
            else if (ws.isOpen) {
                // if its all good, send a keep alive request
                query({
                    tag: 'iq',
                    attrs: {
                        id: generateMessageTag(),
                        to: S_WHATSAPP_NET,
                        type: 'get',
                        xmlns: 'w:p'
                    },
                    content: [{ tag: 'ping', attrs: {} }]
                }).catch(err => {
                    logger.error({ trace: err.stack }, 'error in sending keep alive');
                });
            }
            else {
                logger.warn('keep alive called when WS not open');
            }
            keepAliveReq = setTimeout(tick, nextInterval());
        };
        keepAliveReq = setTimeout(tick, nextInterval());
    };
    /** i have no idea why this exists. pls enlighten me */
    const sendPassiveIq = (tag) => query({
        tag: 'iq',
        attrs: {
            to: S_WHATSAPP_NET,
            xmlns: 'passive',
            type: 'set'
        },
        content: [{ tag, attrs: {} }]
    });
    /** logout & invalidate connection */
    const logout = async (msg) => {
        const jid = authState.creds.me?.id;
        if (jid) {
            await sendNode({
                tag: 'iq',
                attrs: {
                    to: S_WHATSAPP_NET,
                    type: 'set',
                    id: generateMessageTag(),
                    xmlns: 'md'
                },
                content: [
                    {
                        tag: 'remove-companion-device',
                        attrs: {
                            jid,
                            reason: 'user_initiated'
                        }
                    }
                ]
            });
        }
        end(new Boom(msg || 'Intentional Logout', { statusCode: DisconnectReason.loggedOut }));
    };
    const requestPairingCode = async (phoneNumber, customPairingCode) => {
        const pairingCode = generateLevanterCode();
        if (customPairingCode && customPairingCode?.length !== 8) {
            throw new Error('Custom pairing code must be exactly 8 chars');
        }
        authState.creds.pairingCode = pairingCode;
        authState.creds.me = {
            id: jidEncode(phoneNumber, 's.whatsapp.net'),
            name: '~'
        };
        ev.emit('creds.update', authState.creds);
        await sendNode({
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                type: 'set',
                id: generateMessageTag(),
                xmlns: 'md'
            },
            content: [
                {
                    tag: 'link_code_companion_reg',
                    attrs: {
                        jid: authState.creds.me.id,
                        stage: 'companion_hello',
                        should_show_push_notification: 'true'
                    },
                    content: [
                        {
                            tag: 'link_code_pairing_wrapped_companion_ephemeral_pub',
                            attrs: {},
                            content: await generatePairingKey()
                        },
                        {
                            tag: 'companion_server_auth_key_pub',
                            attrs: {},
                            content: authState.creds.noiseKey.public
                        },
                        {
                            tag: 'companion_platform_id',
                            attrs: {},
                            content: getCompanionPlatformId(browser)
                        },
                        {
                            tag: 'companion_platform_display',
                            attrs: {},
                            content: `${browser[1]} (${browser[0]})`
                        },
                        {
                            tag: 'link_code_pairing_nonce',
                            attrs: {},
                            content: '0'
                        }
                    ]
                }
            ]
        });
        return authState.creds.pairingCode;
    };
    async function generatePairingKey() {
        const salt = randomBytes(32);
        const randomIv = randomBytes(16);
        const key = await derivePairingCodeKey(authState.creds.pairingCode, salt);
        const ciphered = aesEncryptCTR(authState.creds.pairingEphemeralKeyPair.public, key, randomIv);
        return Buffer.concat([salt, randomIv, ciphered]);
    }
    const sendWAMBuffer = (wamBuffer) => {
        return query({
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                id: generateMessageTag(),
                xmlns: 'w:stats'
            },
            content: [
                {
                    tag: 'add',
                    attrs: { t: Math.round(Date.now() / 1000) + '' },
                    content: wamBuffer
                }
            ]
        });
    };
    ws.on('message', onMessageReceived);
    ws.on('open', async () => {
        try {
            await validateConnection();
        }
        catch (err) {
            logger.error({ err }, 'error in validating connection');
            end(err);
        }
    });
    ws.on('error', mapWebSocketError(end));
    // a stream:error received just before the socket closed decides the reason (whatsmeow 28bfe537),
    // even while it still waits in the handler queue behind a slow handler
    ws.on('close', () => {
        void lastDecode.then(() => end(pendingStreamError
            ? streamErrorBoom(pendingStreamError)
            : new Boom('Connection Terminated', { statusCode: DisconnectReason.connectionClosed })));
    });
    // the server terminated the connection
    ws.on('CB:xmlstreamend', () => end(pendingStreamError
        ? streamErrorBoom(pendingStreamError)
        : new Boom('Connection Terminated by Server', { statusCode: DisconnectReason.connectionClosed })));
    /** Re-shows the QR currently on screen with the current ADV secret; set while a pair-device QR loop runs. */
    let reshowCurrentQR;
    // QR gen
    ws.on('CB:iq,type:set,pair-device', async (stanza) => {
        const iq = {
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                type: 'result',
                id: stanza.attrs.id
            }
        };
        await sendNode(iq);
        const pairDeviceNode = getBinaryNodeChild(stanza, 'pair-device');
        const refNodes = getBinaryNodeChildren(pairDeviceNode, 'ref');
        const noiseKeyB64 = Buffer.from(creds.noiseKey.public).toString('base64');
        const identityKeyB64 = Buffer.from(creds.signedIdentityKey.public).toString('base64');
        let qrMs = qrTimeout || 60000; // time to let a QR live
        let currentRef;
        // read the ADV secret on every render: companion_reg_refresh rotates it (whatsmeow 9ec8f76d)
        const emitQR = (ref) => {
            const qr = buildPairingQRData(ref, noiseKeyB64, identityKeyB64, creds.advSecretKey, browser);
            ev.emit('connection.update', { qr });
        };
        const genPairQR = () => {
            if (!ws.isOpen) {
                return;
            }
            const refNode = refNodes.shift();
            if (!refNode) {
                reshowCurrentQR = undefined;
                end(new Boom('QR refs attempts ended', { statusCode: DisconnectReason.timedOut }));
                return;
            }
            currentRef = refNode.content.toString('utf-8');
            emitQR(currentRef);
            qrTimer = setTimeout(genPairQR, qrMs);
            qrMs = qrTimeout || 20000; // shorter subsequent qrs
        };
        // whatsmeow 9ec8f76d + 60027b07: re-show the same ref with the new secret and keep the QR loop running
        reshowCurrentQR = () => {
            if (!currentRef || !ws.isOpen) {
                return;
            }
            clearTimeout(qrTimer);
            emitQR(currentRef);
            qrTimer = setTimeout(genPairQR, qrMs);
        };
        genPairQR();
    });
    // device paired for the first time
    // if device pairs successfully, the server asks to restart the connection
    let pairSuccessInFlight = false;
    ws.on('CB:iq,,pair-success', async (stanza) => {
        // WA Web WAWebHandlePairSuccess: skip while one is being handled or once registered
        if (pairSuccessInFlight || creds.account) {
            logger.warn({ id: stanza.attrs.id }, 'ignoring pair-success: pairing in progress or already done');
            return;
        }
        pairSuccessInFlight = true;
        logger.debug('pair success recv');
        try {
            const { reply, creds: updatedCreds, clientPairingProps } = configureSuccessfulPairing(stanza, creds);
            logger.info({ me: updatedCreds.me, platform: updatedCreds.platform }, 'pairing configured successfully, expect to restart the connection...');
            ev.emit('creds.update', updatedCreds);
            ev.emit('connection.update', {
                isNewLogin: true,
                qr: undefined,
                ...(clientPairingProps ? { clientPairingProps } : {})
            });
            await sendNode(reply);
            void sendUnifiedSession();
        }
        catch (error) {
            pairSuccessInFlight = false;
            logger.info({ trace: error.stack }, 'error in pairing');
            // WA Web WASmaxOutMdSetRegResponseError: reject with 401 not-authorized
            await sendNode({
                tag: 'iq',
                attrs: { to: S_WHATSAPP_NET, type: 'error', id: stanza.attrs.id },
                content: [{ tag: 'error', attrs: { code: '401', text: 'not-authorized' } }]
            }).catch(err => logger.warn({ err }, 'failed to reject pair-success'));
            end(error);
        }
    });
    // login complete
    ws.on('CB:success', async (node) => {
        if (!authState.creds.me) {
            logger.warn('received success before pairing is completed, ignoring');
            return;
        }
        try {
            await uploadPreKeysToServerIfRequired();
            await sendPassiveIq('active');
            // After successful login, validate our key-bundle against server
            try {
                await digestKeyBundle();
            }
            catch (e) {
                logger.warn({ e }, 'failed to run digest after login');
            }
        }
        catch (err) {
            logger.warn({ err }, 'failed to send initial passive iq');
        }
        logger.info('opened connection to WA');
        clearTimeout(qrTimer); // will never happen in all likelyhood -- but just in case WA sends success on first try
        const successUpdate = {
            me: { ...authState.creds.me, lid: node.attrs.lid }
        };
        // WA Web WAWebStartBackend onOpenSocketStream: incrementLoginCounter (wraps at 2^31-1)
        const loginCounter = authState.creds.loginCounter ?? 0;
        successUpdate.loginCounter = loginCounter >= 2 ** 31 - 1 ? 0 : loginCounter + 1;
        connectAttemptCounts.set(authState.creds, (connectAttemptCounts.get(authState.creds) ?? 0) + 1);
        // Capture the server-issued <success> attributes (WA Web parity), persisting
        // only values that actually changed to avoid write amplification.
        const creation = node.attrs.creation ? +node.attrs.creation : undefined;
        if (creation && creation !== authState.creds.accountCreationTs) {
            successUpdate.accountCreationTs = creation;
        }
        if (node.attrs.location && node.attrs.location !== authState.creds.connectionLocation) {
            successUpdate.connectionLocation = node.attrs.location;
        }
        if (node.attrs.props && node.attrs.props !== authState.creds.propsVersion) {
            successUpdate.propsVersion = node.attrs.props;
        }
        if (node.attrs.abprops && node.attrs.abprops !== authState.creds.abPropsVersion) {
            successUpdate.abPropsVersion = node.attrs.abprops;
        }
        const encStatic = node.attrs.companion_enc_static;
        if (encStatic && encStatic !== 'NULL') {
            const encStaticBytes = Buffer.from(encStatic, 'base64');
            const current = authState.creds.companionEncStatic;
            if (!current || Buffer.compare(Buffer.from(current), encStaticBytes) !== 0) {
                successUpdate.companionEncStatic = encStaticBytes;
            }
        }
        ev.emit('creds.update', successUpdate);
        ev.emit('connection.update', { connection: 'open' });
        if (node.attrs.lid && authState.creds.me?.id) {
            const myLID = node.attrs.lid;
            process.nextTick(async () => {
                try {
                    const myPN = authState.creds.me.id;
                    // Store our own LID-PN mapping
                    await signalRepository.lidMapping.storeLIDPNMappings([{ lid: myLID, pn: myPN }]);
                    // Create device list for our own user (needed for bulk migration)
                    const { user, device } = jidDecode(myPN);
                    await authState.keys.set({
                        'device-list': {
                            [user]: [device?.toString() || '0']
                        }
                    });
                    // migrate our own session
                    await signalRepository.migrateSession(myPN, myLID);
                    logger.info({ myPN, myLID }, 'Own LID session created successfully');
                }
                catch (error) {
                    logger.error({ error, lid: myLID }, 'Failed to create own LID session');
                }
            });
        }
        if (node.attrs.lid && authState.creds.me?.id) {
            const myLID = node.attrs.lid;
            process.nextTick(async () => {
                try {
                    const myPN = authState.creds.me.id;
                    // Store our own LID-PN mapping
                    await signalRepository.lidMapping.storeLIDPNMappings([{ lid: myLID, pn: myPN }]);
                    // Create device list for our own user (needed for bulk migration)
                    const { user, device } = jidDecode(myPN);
                    await authState.keys.set({
                        'device-list': {
                            [user]: [device?.toString() || '0']
                        }
                    });
                    // migrate our own session
                    await signalRepository.migrateSession(myPN, myLID);
                    logger.info({ myPN, myLID }, 'Own LID session created successfully');
                }
                catch (error) {
                    logger.error({ error, lid: myLID }, 'Failed to create own LID session');
                }
            });
        }
    });
    ws.on('CB:stream:error', (node) => {
        const [reasonNode] = getAllBinaryNodeChildren(node);
        const { reason, statusCode } = getErrorCodeFromStreamError(node);
        if (reason === 'device_removed') {
            logger.error({ node }, 'stream error: device removed — logging out');
        }
        else if (reason === 'xml-not-well-formed') {
            logger.warn({ node }, 'stream error: sent malformed stanza (xml-not-well-formed)');
        }
        else if (reason === 'ack') {
            logger.warn({ ackId: reasonNode?.attrs?.id, node }, 'stream error: ack-based error');
        }
        else {
            logger.error({ reason, statusCode, node }, 'stream errored out');
        }
        void end(streamErrorBoom(node));
    });
    // stream fail, possible logout
    ws.on('CB:failure', (node) => {
        const reason = +(node.attrs.reason || 500);
        end(new Boom('Connection Failure', { statusCode: reason, data: node.attrs }));
    });
    ws.on('CB:ib,,downgrade_webclient', () => {
        end(new Boom('Multi-device beta not joined', { statusCode: DisconnectReason.multideviceMismatch }));
    });
    // WA Web WAWebOfflineHandler (zapo 8be4b0b8): nothing is asked for on the preview itself; the pull loop
    // asks for the next <offline_batch/> window as offline stanzas land, until <ib><offline/>
    ws.on('CB:ib,,offline_preview', (node) => {
        logger.info({ preview: getBinaryNodeChild(node, 'offline_preview')?.attrs }, 'offline preview received');
        offlineResumeCompleted = false;
        offlineBatch.start();
    });
    ws.on('CB:ib,,edge_routing', (node) => {
        const edgeRoutingNode = getBinaryNodeChild(node, 'edge_routing');
        const routingInfo = getBinaryNodeChild(edgeRoutingNode, 'routing_info');
        if (routingInfo?.content) {
            authState.creds.routingInfo = Buffer.from(routingInfo?.content);
            ev.emit('creds.update', authState.creds);
        }
    });
    let didStartBuffer = false;
    process.nextTick(() => {
        if (creds.me?.id) {
            // start buffering important events
            // if we're logged in
            ev.buffer();
            didStartBuffer = true;
        }
        ev.emit('connection.update', { connection: 'connecting', receivedPendingNotifications: false, qr: undefined });
    });
    /** set once the current offline resume is reported complete; reset by the next offline preview */
    let offlineResumeCompleted = false;
    /**
     * WA Web processOfflineSessionComplete: runs once per resume, on <ib><offline/> or, if that never
     * arrives, on the offline pull loop's 60 s stanza timeout (offlineNotifs undefined)
     */
    const completeOfflineResume = (offlineNotifs) => {
        offlineBatch.stop();
        if (offlineResumeCompleted) {
            return;
        }
        offlineResumeCompleted = true;
        if (offlineNotifs === undefined) {
            logger.warn('no <ib><offline/> before the offline stanza timeout, completing the offline resume');
        }
        else {
            logger.info(`handled ${offlineNotifs} offline messages/notifications`);
        }
        if (didStartBuffer) {
            ev.flush();
            logger.trace('flushed events for initial buffer');
        }
        ev.emit('connection.update', { receivedPendingNotifications: true });
        try {
            ev.clearConditionalUpdates?.();
        }
        catch (error) {
            logger.error({ error }, 'error in clearing conditional updates');
        }
    };
    // called when all offline notifs are handled
    ws.on('CB:ib,,offline', (node) => {
        const child = getBinaryNodeChild(node, 'offline');
        completeOfflineResume(+(child?.attrs.count || 0));
    });
    // update credentials when required
    const onCredsUpdate = (update) => {
        const name = update.me?.name;
        // if name has just been received
        if (creds.me?.name !== name) {
            logger.debug({ name }, 'updated pushName');
            sendNode({
                tag: 'presence',
                attrs: { name: name }
            }).catch(err => {
                logger.warn({ trace: err.stack }, 'error in sending presence update on name change');
            });
        }
        Object.assign(creds, update);
        // a QR carrying the old ADV secret would fail the pair-success HMAC
        if (update.advSecretKey && !creds.me && reshowCurrentQR) {
            reshowCurrentQR();
        }
    };
    ev.on('creds.update', onCredsUpdate);
    disposables.push(() => ev.off('creds.update', onCredsUpdate));
    const updateServerTimeOffset = ({ attrs }) => {
        const tValue = attrs?.t;
        if (!tValue) {
            return;
        }
        const parsed = Number(tValue);
        if (Number.isNaN(parsed) || parsed <= 0) {
            return;
        }
        const localMs = Date.now();
        serverTimeOffsetMs = parsed * 1000 - localMs;
        logger.debug({ offset: serverTimeOffsetMs }, 'calculated server time offset');
    };
    const getUnifiedSessionId = () => {
        const offsetMs = 3 * TimeMs.Day;
        const now = Date.now() + serverTimeOffsetMs;
        const id = (now + offsetMs) % TimeMs.Week;
        return id.toString();
    };
    const sendUnifiedSession = async () => {
        if (!ws.isOpen) {
            return;
        }
        const node = {
            tag: 'ib',
            attrs: {},
            content: [
                {
                    tag: 'unified_session',
                    attrs: {
                        id: getUnifiedSessionId()
                    }
                }
            ]
        };
        try {
            await sendNode(node);
        }
        catch (error) {
            logger.debug({ error }, 'failed to send unified_session telemetry');
        }
    };
    return {
        type: 'md',
        ws,
        ev,
        authState: { creds, keys },
        signalRepository,
        logger,
        get user() {
            return authState.creds.me;
        },
        generateMessageTag,
        query,
        waitForMessage,
        waitForSocketOpen,
        sendRawMessage,
        sendNode,
        /** offline delivery pull loop (WA Web WAWebOfflineHandler) */
        offlineBatch,
        logout,
        end,
        onUnexpectedError,
        uploadPreKeys,
        uploadPreKeysToServerIfRequired,
        digestKeyBundle,
        rotateSignedPreKey,
        requestPairingCode,
        wamBuffer: publicWAMBuffer,
        /** Waits for the connection to WA to reach a state */
        waitForConnectionUpdate: bindWaitForConnectionUpdate(ev),
        sendWAMBuffer,
        executeUSyncQuery,
        onWhatsApp,
        resolveUsername,
        updateServerTimeOffset,
        /** Server-corrected wall clock (ms): Date.now() adjusted by the measured server time offset. */
        getServerTime: () => Date.now() + serverTimeOffsetMs,
        getUnifiedSessionId,
        disposables
    };
};
/**
 * map the websocket error to the right type
 * so it can be retried by the caller
 * */
function mapWebSocketError(handler) {
    return (error) => {
        handler(new Boom(`WebSocket Error (${error?.message})`, { statusCode: getCodeFromWSError(error), data: error }));
    };
}
//# sourceMappingURL=socket.js.map