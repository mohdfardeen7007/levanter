import NodeCache from '@cacheable/node-cache';
import { Boom } from '@hapi/boom';
import { proto } from '../../WAProto/index.js';
import { DEFAULT_CACHE_TTLS, HISTORY_SYNC_PAUSED_TIMEOUT_MS, PROCESSABLE_HISTORY_TYPES } from '../Defaults/index.js';
import { ALL_WA_PATCH_NAMES, DisconnectReason } from '../Types/index.js';
import { SyncState } from '../Types/State.js';
import { APP_STATE_FETCH_MAX_DELAY_MS, APP_STATE_FETCH_MAX_RETRIES, APP_STATE_SYNC_MAX_ROUNDS, appStateFetchRetryDelayMs, chatModificationToAppPatch, classifyAppStateFetchError, decodePatches, decodeSyncdSnapshot, delayCancellable, encodeSyncdPatch, ensureLTHashStateVersion, extractSyncdPatches, generateProfilePicture, getHistoryMsg, isAppStateSyncIrrecoverable, isMissingKeyError, MAX_SYNC_ATTEMPTS, newLTHashState, processSyncAction, buildTcTokenFromJid } from '../Utils/index.js';
import { makeMutex } from '../Utils/make-mutex.js';
import processMessage from '../Utils/process-message.js';
import { pickBlocklistIdentifier } from '../Utils/username.js';
import { getBinaryNodeChild, getBinaryNodeChildren, isJidBroadcast, isJidGroup, isHostedLidUser, isHostedPnUser, isLidUser, isPnUser, jidDecode, jidNormalizedUser, reduceBinaryNodeToDictionary, S_WHATSAPP_NET } from '../WABinary/index.js';
import { USyncQuery, USyncUser } from '../WAUSync/index.js';
import { executeWMexQuery } from './mex.js';
import { makeSocket } from './socket.js';
export const buildProfilePictureQueryContent = (type, tcTokenContent) => {
    const picture = { tag: 'picture', attrs: { type, query: 'url' } };
    if (tcTokenContent?.length) {
        picture.content = tcTokenContent;
    }
    return [picture];
};
export const makeChatsSocket = (config) => {
    const { logger, markOnlineOnConnect, fireInitQueries, appStateMacVerification, shouldIgnoreJid, shouldSyncHistoryMessage, getMessage } = config;
    const sock = makeSocket(config);
    const { ev, ws, authState, generateMessageTag, sendNode, query, signalRepository, onUnexpectedError, disposables } = sock;
    let privacySettings;
    /**
     * Server-driven feature flags (AB props) that gate tctoken behavior.
     * Defaults match WA Web's defaults; overwritten by fetchAbProps() on connect.
     */
    const serverProps = {
        /** AB prop 10518: gate tctoken on 1:1 messages. Default true (safe: avoids server error 463). */
        privacyTokenOn1to1: true,
        /** AB prop 9666: gate tctoken on profile picture IQs. WA Web default: true. */
        profilePicPrivacyToken: true,
        /** AB prop 14303: issue tctokens to LID instead of PN. WA Web default: false. */
        lidTrustedTokenIssueToLid: false,
        /** AB prop 19696: name the poster's PN in peer_participant_pn on LID status read receipts. WA Web default: true. */
        lidStatusNonSoakedClientSupport: true
    };
    let syncState = SyncState.Connecting;
    /** late-bound by messages-send, which owns deleteMedia */
    let historySyncBlobProcessedHandler;
    /** this mutex ensures that messages are processed in order */
    const messageMutex = makeMutex();
    /** this mutex ensures that receipts are processed in order */
    const receiptMutex = makeMutex();
    /** this mutex ensures that app state patches are processed in order */
    const appStatePatchMutex = makeMutex();
    /** this mutex ensures that notifications are processed in order */
    const notificationMutex = makeMutex();
    // Timeout for AwaitingInitialSync state
    let awaitingSyncTimeout;
    // In-memory history sync completion tracking (resets on reconnection)
    const historySyncStatus = {
        initialBootstrapComplete: false,
        recentSyncComplete: false
    };
    let historySyncPausedTimeout;
    // Collections blocked on missing app state sync keys (mirrors WA Web's "Blocked" state).
    // When a key arrives via APP_STATE_SYNC_KEY_SHARE, these are re-synced.
    const blockedCollections = new Set();
    const placeholderResendCache = config.placeholderResendCache ||
        new NodeCache({
            stdTTL: DEFAULT_CACHE_TTLS.MSG_RETRY, // 1 hour
            useClones: false
        });
    if (!config.placeholderResendCache) {
        config.placeholderResendCache = placeholderResendCache;
    }
    /** helper function to fetch the given app state sync key */
    const getAppStateSyncKey = async (keyId) => {
        const { [keyId]: key } = await authState.keys.get('app-state-sync-key', [keyId]);
        return key;
    };
    const fetchPrivacySettings = async (force = false) => {
        if (!privacySettings || force) {
            const { content } = await query({
                tag: 'iq',
                attrs: {
                    xmlns: 'privacy',
                    to: S_WHATSAPP_NET,
                    type: 'get'
                },
                content: [{ tag: 'privacy', attrs: {} }]
            });
            privacySettings = reduceBinaryNodeToDictionary(content?.[0], 'category');
        }
        return privacySettings;
    };
    /** helper function to run a privacy IQ query */
    const privacyQuery = async (name, value) => {
        await query({
            tag: 'iq',
            attrs: {
                xmlns: 'privacy',
                to: S_WHATSAPP_NET,
                type: 'set'
            },
            content: [
                {
                    tag: 'privacy',
                    attrs: {},
                    content: [
                        {
                            tag: 'category',
                            attrs: { name, value }
                        }
                    ]
                }
            ]
        });
    };
    const updateMessagesPrivacy = async (value) => {
        await privacyQuery('messages', value);
    };
    const updateCallPrivacy = async (value) => {
        await privacyQuery('calladd', value);
    };
    const updateLastSeenPrivacy = async (value) => {
        await privacyQuery('last', value);
    };
    const updateOnlinePrivacy = async (value) => {
        await privacyQuery('online', value);
    };
    const updateProfilePicturePrivacy = async (value) => {
        await privacyQuery('profile', value);
    };
    const updateStatusPrivacy = async (value) => {
        await privacyQuery('status', value);
    };
    const updateReadReceiptsPrivacy = async (value) => {
        await privacyQuery('readreceipts', value);
    };
    const updateGroupsAddPrivacy = async (value) => {
        await privacyQuery('groupadd', value);
    };
    const updateDefaultDisappearingMode = async (duration) => {
        await query({
            tag: 'iq',
            attrs: {
                xmlns: 'disappearing_mode',
                to: S_WHATSAPP_NET,
                type: 'set'
            },
            content: [
                {
                    tag: 'disappearing_mode',
                    attrs: {
                        duration: duration.toString()
                    }
                }
            ]
        });
    };
    const getBotListV2 = async () => {
        const resp = await query({
            tag: 'iq',
            attrs: {
                xmlns: 'bot',
                to: S_WHATSAPP_NET,
                type: 'get'
            },
            content: [
                {
                    tag: 'bot',
                    attrs: {
                        v: '2'
                    }
                }
            ]
        });
        const botNode = getBinaryNodeChild(resp, 'bot');
        const botList = [];
        for (const section of getBinaryNodeChildren(botNode, 'section')) {
            if (section.attrs.type === 'all') {
                for (const bot of getBinaryNodeChildren(section, 'bot')) {
                    botList.push({
                        jid: bot.attrs.jid,
                        personaId: bot.attrs['persona_id']
                    });
                }
            }
        }
        return botList;
    };
    const fetchStatus = async (...jids) => {
        const usyncQuery = new USyncQuery().withStatusProtocol();
        for (const jid of jids) {
            usyncQuery.withUser(new USyncUser().withId(jid));
        }
        const result = await sock.executeUSyncQuery(usyncQuery);
        if (result) {
            return result.list;
        }
    };
    const fetchDisappearingDuration = async (...jids) => {
        const usyncQuery = new USyncQuery().withDisappearingModeProtocol();
        for (const jid of jids) {
            usyncQuery.withUser(new USyncUser().withId(jid));
        }
        const result = await sock.executeUSyncQuery(usyncQuery);
        if (result) {
            return result.list;
        }
    };
    /** update the profile picture for yourself or a group */
    const updateProfilePicture = async (jid, content, dimensions) => {
        let targetJid;
        if (!jid) {
            throw new Boom('Illegal no-jid profile update. Please specify either your ID or the ID of the chat you wish to update');
        }
        if (jidNormalizedUser(jid) !== jidNormalizedUser(authState.creds.me.id)) {
            targetJid = jidNormalizedUser(jid); // in case it is someone other than us
        }
        else {
            targetJid = undefined;
        }
        const { img } = await generateProfilePicture(content, dimensions);
        await query({
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                type: 'set',
                xmlns: 'w:profile:picture',
                ...(targetJid ? { target: targetJid } : {})
            },
            content: [
                {
                    tag: 'picture',
                    attrs: { type: 'image' },
                    content: img
                }
            ]
        });
    };
    /** remove the profile picture for yourself or a group */
    const removeProfilePicture = async (jid) => {
        let targetJid;
        if (!jid) {
            throw new Boom('Illegal no-jid profile update. Please specify either your ID or the ID of the chat you wish to update');
        }
        if (jidNormalizedUser(jid) !== jidNormalizedUser(authState.creds.me.id)) {
            targetJid = jidNormalizedUser(jid); // in case it is someone other than us
        }
        else {
            targetJid = undefined;
        }
        await query({
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                type: 'set',
                xmlns: 'w:profile:picture',
                ...(targetJid ? { target: targetJid } : {})
            }
        });
    };
    /** update the profile status for yourself */
    const updateProfileStatus = async (status) => {
        const input = typeof status === 'string' ? { text: status } : status;
        const variables = {
            input: {
                // text is a nullable field server-side (whatsmeow SetStatusInput.Text is *string);
                // omit it when unset so setting only emoji/duration doesn't wipe the status text.
                ...(input.text !== undefined ? { text: input.text } : {}),
                ...(input.emoji ? { emoji: { content: input.emoji } } : {}),
                ephemeral_duration_sec: input.ephemeralDurationSec ?? 0
            }
        };
        // WA moved status-message updates to the mex mutation (9152604461510864);
        // the legacy xmlns='status' IQ is deprecated server-side.
        await executeWMexQuery(variables, '9152604461510864', '', query, generateMessageTag);
    };
    const updateProfileName = async (name) => {
        await chatModify({ pushNameSetting: name }, '');
    };
    /**
     * Fetch the account blocklist (WA Web WAWebGetBlocklistJob, whatsmeow b20fb47/4b7a1ef).
     * Pass the `dhash` of the list you hold: while it is current the server answers without a
     * list and this resolves `undefined` ("unchanged"). No stored hash is ever sent on its own —
     * a caller that lost its copy would get "unchanged" forever (whatsmeow leaves it to the caller too).
     */
    const getBlocklist = async ({ dhash } = {}) => {
        const result = await query({
            tag: 'iq',
            attrs: {
                xmlns: 'blocklist',
                to: S_WHATSAPP_NET,
                type: 'get'
            },
            // WA Web WASmaxOutBlocklistsGetBlockListRequest (whatsmeow 4b7a1ef): <item dhash> when a hash is known
            ...(dhash ? { content: [{ tag: 'item', attrs: { dhash } }] } : {})
        });
        // WA Web WASmaxInBlocklistsGetBlockListResponseSuccessWithMatch (whatsmeow b20fb47): no <list> = unchanged
        const listNode = getBinaryNodeChild(result, 'list');
        if (!listNode) {
            return undefined;
        }
        const itemNodes = getBinaryNodeChildren(listNode, 'item');
        // WA Web WAWebGetBlocklistJob: an item without a jid is located by the current LID of its
        // pn_jid; one that cannot be resolved is dropped
        const lidByPn = new Map();
        const unresolvedPns = itemNodes
            .filter(({ attrs }) => !attrs.jid && attrs.pn_jid)
            .map(({ attrs }) => jidNormalizedUser(attrs.pn_jid))
            .filter(Boolean);
        if (unresolvedPns.length) {
            try {
                const resolved = await signalRepository.lidMapping.getLIDsForPNs(unresolvedPns);
                for (const { lid, pn } of resolved || []) {
                    lidByPn.set(jidNormalizedUser(pn), jidNormalizedUser(lid));
                }
            }
            catch (error) {
                logger.warn({ error }, 'failed to resolve the LIDs of blocklist items without a jid');
            }
        }
        const items = [];
        const activeMappings = [];
        for (const { attrs } of itemNodes) {
            const pnJid = attrs.pn_jid;
            const jid = attrs.jid || (pnJid ? lidByPn.get(jidNormalizedUser(pnJid)) : undefined);
            if (!jid) {
                logger.debug({ attrs }, 'dropping a blocklist item without a resolvable jid');
                continue;
            }
            const active = attrs.active === 'true';
            items.push({ jid, pnJid, active, username: attrs.username, displayName: attrs.display_name });
            // whatsmeow 4b7a1ef: only an active LID-PN pair sent by the server is a current mapping. WA Web
            // WAWebFetchAndUpdateBlocklistJob keeps inactive pairs as historical (createdAt 0), which the
            // LID store cannot express -- storing one would replace the current LID of that number.
            // Both sides need a user part: isLidUser / isPnUser only check the suffix ('@lid' passes)
            const lidUser = isLidUser(attrs.jid) ? jidDecode(attrs.jid)?.user : undefined;
            const pnUser = isPnUser(pnJid) ? jidDecode(pnJid)?.user : undefined;
            if (active && lidUser && pnUser) {
                activeMappings.push({ lid: attrs.jid, pn: pnJid });
            }
        }
        if (activeMappings.length) {
            try {
                await signalRepository.lidMapping.storeLIDPNMappings(activeMappings);
            }
            catch (error) {
                logger.warn({ error }, 'failed to store the LID mappings of the blocklist');
            }
        }
        return {
            dhash: listNode.attrs.dhash,
            addressingMode: listNode.attrs.addressing_mode,
            dirty: listNode.attrs.dirty === 'true',
            items
        };
    };
    /** jids of the blocked contacts; see getBlocklist for the list hash, phone numbers and identifiers */
    const fetchBlocklist = async () => {
        const blocklist = await getBlocklist();
        return blocklist?.items.map(({ jid }) => jid) ?? [];
    };
    /**
     * Block or unblock a contact. Blocking sends the LID plus exactly one identifier
     * (WA Web WAWebBlockUserJob): pass the contact's `username`, saved `name` or LID
     * `displayName` when known so the server can match the right account.
     * Pass the `dhash` of the list you hold, as WA Web does with its stored hash.
     */
    const updateBlockStatus = async (jid, action, identity, opts = {}) => {
        const normalizedJid = jidNormalizedUser(jid);
        let lid;
        let pn_jid;
        if (isLidUser(normalizedJid) || isHostedLidUser(normalizedJid)) {
            lid = normalizedJid;
            if (action === 'block') {
                // WA Web: a LID without a known phone number is blocked by username / display name / unknown_identifier
                const pn = await signalRepository.lidMapping.getPNForLID(normalizedJid);
                pn_jid = pn ? jidNormalizedUser(pn) : undefined;
            }
        }
        else if (isPnUser(normalizedJid) || isHostedPnUser(normalizedJid)) {
            const mapped = await signalRepository.lidMapping.getLIDForPN(normalizedJid);
            if (!mapped) {
                throw new Boom(`Unable to resolve LID for PN JID: ${jid}`, { statusCode: 400 });
            }
            lid = mapped;
            if (action === 'block') {
                pn_jid = jidNormalizedUser(normalizedJid);
            }
        }
        else {
            throw new Boom(`Invalid jid: ${jid}`, { statusCode: 400 });
        }
        const itemAttrs = {
            action,
            jid: lid,
            ...(action === 'block' ? pickBlocklistIdentifier(pn_jid, identity) : {}),
            // WA Web WASmaxOutBlocklistsUpdateBlockListRequest (whatsmeow 9399289): the list hash rides on the item
            ...(opts.dhash ? { dhash: opts.dhash } : {})
        };
        await query({
            tag: 'iq',
            attrs: {
                xmlns: 'blocklist',
                to: S_WHATSAPP_NET,
                type: 'set'
            },
            content: [
                {
                    tag: 'item',
                    attrs: itemAttrs
                }
            ]
        });
    };
    const getBusinessProfile = async (jid) => {
        const results = await query({
            tag: 'iq',
            attrs: {
                to: 's.whatsapp.net',
                xmlns: 'w:biz',
                type: 'get'
            },
            content: [
                {
                    tag: 'business_profile',
                    attrs: { v: '244' },
                    content: [
                        {
                            tag: 'profile',
                            attrs: { jid }
                        }
                    ]
                }
            ]
        });
        const profileNode = getBinaryNodeChild(results, 'business_profile');
        const profiles = getBinaryNodeChild(profileNode, 'profile');
        if (profiles) {
            const address = getBinaryNodeChild(profiles, 'address');
            const description = getBinaryNodeChild(profiles, 'description');
            const website = getBinaryNodeChild(profiles, 'website');
            const email = getBinaryNodeChild(profiles, 'email');
            const category = getBinaryNodeChild(getBinaryNodeChild(profiles, 'categories'), 'category');
            const businessHours = getBinaryNodeChild(profiles, 'business_hours');
            const businessHoursConfig = businessHours
                ? getBinaryNodeChildren(businessHours, 'business_hours_config')
                : undefined;
            const websiteStr = website?.content?.toString();
            return {
                wid: profiles.attrs?.jid,
                address: address?.content?.toString(),
                description: description?.content?.toString() || '',
                website: websiteStr ? [websiteStr] : [],
                email: email?.content?.toString(),
                category: category?.content?.toString(),
                business_hours: {
                    timezone: businessHours?.attrs?.timezone,
                    business_config: businessHoursConfig?.map(({ attrs }) => attrs)
                }
            };
        }
    };
    const cleanDirtyBits = async (type, fromTimestamp) => {
        logger.info({ fromTimestamp }, 'clean dirty bits ' + type);
        await sendNode({
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                type: 'set',
                xmlns: 'urn:xmpp:whatsapp:dirty',
                id: generateMessageTag()
            },
            content: [
                {
                    tag: 'clean',
                    attrs: {
                        type,
                        ...(fromTimestamp ? { timestamp: fromTimestamp.toString() } : null)
                    }
                }
            ]
        });
    };
    const newAppStateChunkHandler = (isInitialSync) => {
        return {
            onMutation(mutation) {
                processSyncAction(mutation, ev, authState.creds.me, isInitialSync ? { accountSettings: authState.creds.accountSettings } : undefined, logger);
            }
        };
    };
    /** app-state retry waits in progress; the socket's end cancels them (one disposable, not one per wait) */
    const appStateRetryWaits = new Set();
    disposables.push(() => {
        for (const cancel of appStateRetryWaits) {
            cancel();
        }
        appStateRetryWaits.clear();
    });
    /** waits outside the key lock; false once the socket is closed or ends meanwhile, so the retries stop */
    const waitForAppStateRetry = async (ms) => {
        if (!ws.isOpen) {
            return false;
        }
        const { delay, cancel } = delayCancellable(ms);
        appStateRetryWaits.add(cancel);
        try {
            await delay;
        }
        catch {
            return false;
        }
        finally {
            appStateRetryWaits.delete(cancel);
        }
        return ws.isOpen;
    };
    /**
     * Fetch and apply the given app-state collections. Fetch errors are retried with backoff (WA Web
     * WAWebSyncd); when it gives up (fatal error, retries exhausted, socket closed) it logs and resolves
     * rather than rejecting -- except under a caller already holding the key lock (appPatch), which fails fast.
     */
    const resyncAppState = ev.createBufferedFunction(async (collections, isInitialSync) => {
        const appStateSyncKeyCache = new Map();
        const getCachedAppStateSyncKey = async (keyId) => {
            if (appStateSyncKeyCache.has(keyId)) {
                return appStateSyncKeyCache.get(keyId) ?? undefined;
            }
            const key = await getAppStateSyncKey(keyId);
            appStateSyncKeyCache.set(keyId, key ?? null);
            return key;
        };
        // we use this to determine which events to fire
        // otherwise when we resync from scratch -- all notifications will fire
        const initialVersionMap = {};
        const globalMutationMap = {};
        // a caller already holding the key lock (appPatch) cannot wait for a retry without stalling
        // every send that shares the lock: it keeps failing fast, the fetch error is rethrown
        const failFast = !!authState.keys.isInTransaction?.();
        const collectionsToHandle = new Set(collections);
        // in case something goes wrong -- ensure we don't enter a loop that cannot be exited from
        const attemptsMap = {};
        // collections that failed and need a full snapshot on retry
        // mirrors WA Web's ErrorFatal -> force snapshot behavior
        const forceSnapshotCollections = new Set();
        // WA Web WAWebSyncdResponseParser / WAWebSyncd: a collection answered with a retryable error
        // (or left out of the reply) is fetched again in the next round, after the backoff
        const retryCollections = new Map();
        for (let retry = 0;; retry++) {
            const fetchFailure = await authState.keys.transaction(async () => {
                // keep executing till all collections are done
                // sometimes a single patch request will not return all the patches (God knows why)
                // so we fetch till they're all done (this is determined by the "has_more_patches" flag)
                for (let round = 0; collectionsToHandle.size; round++) {
                    // the socket may have ended while the last reply (asking for more) was in flight: nothing
                    // would reach the wire, so stop here instead of sending a query that rejects at once
                    if (!ws.isOpen) {
                        if (failFast) {
                            throw new Boom('Connection Closed', { statusCode: DisconnectReason.connectionClosed });
                        }
                        const closed = { kind: 'closed' };
                        return closed;
                    }
                    // WA Web WAWebSyncdServerSync: an attempt is capped at 500 rounds and what still asks for
                    // more is retried later (ErrorRetry) -- never looped on forever under the key lock
                    if (round === APP_STATE_SYNC_MAX_ROUNDS) {
                        const unfinished = [...collectionsToHandle];
                        if (failFast) {
                            throw new Boom(`App state sync did not finish in ${round} rounds`, {
                                statusCode: 500,
                                data: { collections: unfinished }
                            });
                        }
                        logger.warn({ collections: unfinished, rounds: round }, 'app state sync reached the round limit');
                        for (const name of unfinished) {
                            retryCollections.set(name, undefined);
                        }
                        collectionsToHandle.clear();
                        break;
                    }
                    const states = {};
                    const nodes = [];
                    const requested = [...collectionsToHandle];
                    for (const name of requested) {
                        const result = await authState.keys.get('app-state-sync-version', [name]);
                        let state = result[name];
                        if (state) {
                            state = ensureLTHashStateVersion(state);
                            if (typeof initialVersionMap[name] === 'undefined') {
                                initialVersionMap[name] = state.version;
                            }
                        }
                        else {
                            state = newLTHashState();
                        }
                        states[name] = state;
                        const shouldForceSnapshot = forceSnapshotCollections.has(name);
                        logger.info(`resyncing ${name} from v${state.version}${shouldForceSnapshot ? ' (forcing snapshot)' : ''}`);
                        nodes.push({
                            tag: 'collection',
                            attrs: {
                                name,
                                version: state.version.toString(),
                                // return snapshot if syncing from scratch or forcing after a failed attempt
                                return_snapshot: (shouldForceSnapshot || !state.version).toString()
                            }
                        });
                    }
                    let decoded;
                    try {
                        const result = await query({
                            tag: 'iq',
                            attrs: {
                                to: S_WHATSAPP_NET,
                                xmlns: 'w:sync:app:state',
                                type: 'set'
                            },
                            content: [
                                {
                                    tag: 'sync',
                                    attrs: {},
                                    content: nodes
                                }
                            ]
                        });
                        if (!result) {
                            // query() ends its 60 s wait with undefined instead of rejecting
                            throw new Boom('App state sync timed out', { statusCode: DisconnectReason.timedOut });
                        }
                        // extract from binary node
                        decoded = await extractSyncdPatches(result, config?.options);
                    }
                    catch (error) {
                        if (failFast) {
                            throw error;
                        }
                        // WA Web WAWebSyncdServerSync: the whole round failed; returning (instead of throwing)
                        // commits the rounds already applied, and the caller retries outside the lock
                        logger.warn({ collections: requested, error }, 'app state fetch failed');
                        return classifyAppStateFetchError(error);
                    }
                    for (const key in decoded) {
                        const name = key;
                        // a collection this round did not ask for (or one without a name) has no loaded state and
                        // must not join the retries; WA Web WAWebSyncdResponseParser even fails the whole round on
                        // an unknown name -- here it is only logged and skipped
                        if (!requested.includes(name)) {
                            logger.warn({ collection: key }, 'ignoring an app state collection that was not requested');
                            continue;
                        }
                        const { patches, hasMorePatches, snapshot, error: collectionError } = decoded[name];
                        // WA Web WAWebSyncdResponseParser: 409 is a conflict whose patches apply as usual
                        // (Success without pending mutations); 400 / 404 are fatal for the collection (WA Web
                        // then logs out; it is only dropped here); any other code is retried
                        if (collectionError && collectionError.code !== 409) {
                            collectionsToHandle.delete(name);
                            if (failFast) {
                                throw new Boom(`Failed to sync ${name}: ${collectionError.text || 'collection error'}`, {
                                    data: collectionError.code
                                });
                            }
                            if (collectionError.code === 400 || collectionError.code === 404) {
                                logger.error({ collections: [name], code: collectionError.code, text: collectionError.text }, `app state collection ${name} failed fatally, dropping it`);
                            }
                            else {
                                retryCollections.set(name, collectionError.code);
                            }
                            continue;
                        }
                        // a forced snapshot counts as answered only here, for a collection that came back without
                        // an error -- an error or a missing reply keeps it for the retry; a failure below sets it again
                        forceSnapshotCollections.delete(name);
                        try {
                            if (snapshot) {
                                const { state: newState, mutationMap } = await decodeSyncdSnapshot(name, snapshot, getCachedAppStateSyncKey, initialVersionMap[name], appStateMacVerification.snapshot, logger);
                                states[name] = newState;
                                Object.assign(globalMutationMap, mutationMap);
                                logger.info(`restored state of ${name} from snapshot to v${newState.version} with mutations`);
                                await authState.keys.set({ 'app-state-sync-version': { [name]: newState } });
                            }
                            // only process if there are syncd patches
                            if (patches.length) {
                                const { state: newState, mutationMap } = await decodePatches(name, patches, states[name], getCachedAppStateSyncKey, config.options, initialVersionMap[name], logger, appStateMacVerification.patch);
                                await authState.keys.set({ 'app-state-sync-version': { [name]: newState } });
                                logger.info(`synced ${name} to v${newState.version}`);
                                initialVersionMap[name] = newState.version;
                                Object.assign(globalMutationMap, mutationMap);
                            }
                            if (hasMorePatches) {
                                logger.info(`${name} has more patches...`);
                            }
                            else {
                                // collection is done with sync
                                collectionsToHandle.delete(name);
                            }
                        }
                        catch (error) {
                            attemptsMap[name] = (attemptsMap[name] || 0) + 1;
                            const logData = {
                                name,
                                attempt: attemptsMap[name],
                                version: states[name].version,
                                statusCode: error.output?.statusCode,
                                errorType: error.name,
                                error: error.stack
                            };
                            if (isMissingKeyError(error) && attemptsMap[name] >= MAX_SYNC_ATTEMPTS) {
                                // WA Web treats missing keys as "Blocked" — park the collection
                                // until the key arrives via APP_STATE_SYNC_KEY_SHARE.
                                logger.warn(logData, `${name} blocked on missing key from v${states[name].version}, parking after ${attemptsMap[name]} attempts`);
                                blockedCollections.add(name);
                                collectionsToHandle.delete(name);
                            }
                            else if (isMissingKeyError(error)) {
                                // Retry with a snapshot which may use a different key.
                                logger.info(logData, `${name} blocked on missing key from v${states[name].version}, retrying with snapshot`);
                                forceSnapshotCollections.add(name);
                            }
                            else if (isAppStateSyncIrrecoverable(error, attemptsMap[name])) {
                                logger.warn(logData, `failed to sync ${name} from v${states[name].version}, giving up`);
                                collectionsToHandle.delete(name);
                            }
                            else {
                                logger.info(logData, `failed to sync ${name} from v${states[name].version}, forcing snapshot retry`);
                                // force a full snapshot on retry to recover from
                                // corrupted local state (e.g. LTHash MAC mismatch)
                                forceSnapshotCollections.add(name);
                            }
                        }
                    }
                    // a requested collection the reply left out would otherwise be re-sent at once, forever;
                    // WA Web leaves it dirty to sync again -- here it waits for the next round
                    for (const name of requested) {
                        if (!decoded[name] && collectionsToHandle.delete(name)) {
                            if (failFast) {
                                throw new Boom(`App state sync reply has no ${name}`, { statusCode: 500 });
                            }
                            retryCollections.set(name, undefined);
                        }
                    }
                }
                return undefined;
            }, authState?.creds?.me?.id || 'resync-app-state');
            // what is left: the collections of a failed round plus the per-collection retries
            for (const name of retryCollections.keys()) {
                collectionsToHandle.add(name);
            }
            if (!collectionsToHandle.size) {
                break;
            }
            const pending = [...collectionsToHandle];
            const failure = fetchFailure ?? {
                kind: 'retryable',
                code: [...retryCollections.values()].find(collectionCode => collectionCode !== undefined)
            };
            const { kind, code, backoffMs } = failure;
            retryCollections.clear();
            if (kind === 'closed') {
                logger.info({ collections: pending }, 'connection closed, abandoning the app state sync');
                break;
            }
            // deliberate deviations from WA Web, which logs out on a fatal error (WAWebSyncdFatal) and
            // retries the rest for days: a fatal error, an exhausted budget or a server backoff above
            // the cap give up on these collections -- the next sync trigger fetches them again
            if (kind === 'fatal' ||
                retry >= APP_STATE_FETCH_MAX_RETRIES ||
                (backoffMs ?? 0) > APP_STATE_FETCH_MAX_DELAY_MS) {
                logger.error({ collections: pending, code, retries: retry, backoffMs }, 'app state sync failed, giving up on these collections');
                break;
            }
            const delayMs = appStateFetchRetryDelayMs(retry, backoffMs);
            logger.info({ collections: pending, code, retry: retry + 1, delayMs }, 'retrying the app state sync');
            if (!(await waitForAppStateRetry(delayMs))) {
                logger.info({ collections: pending }, 'connection closed, abandoning the app state sync');
                break;
            }
            // a key share may have landed during the wait: look the missing keys up again
            for (const [keyId, key] of appStateSyncKeyCache) {
                if (!key) {
                    appStateSyncKeyCache.delete(keyId);
                }
            }
        }
        // mutations of every round that went through are emitted, even when some collections failed
        const { onMutation } = newAppStateChunkHandler(isInitialSync);
        for (const key in globalMutationMap) {
            onMutation(globalMutationMap[key]);
        }
    });
    /**
     * fetch the profile picture of a user/group
     * type = "preview" for a low res picture
     * type = "image for the high res picture"
     */
    const profilePictureUrl = async (jid, type = 'preview', timeoutMs) => {
        const normalizedJid = jidNormalizedUser(jid);
        // Skip tctoken for self-JIDs and non-user JIDs (groups, broadcasts, etc.)
        // WA Web does not include tctoken when fetching own profile picture — causes server hang
        const me = authState.creds.me;
        const isSelf = me && (normalizedJid === jidNormalizedUser(me.id) || (me.lid && normalizedJid === jidNormalizedUser(me.lid)));
        const isUserJid = isPnUser(normalizedJid) || isLidUser(normalizedJid);
        let tcTokenContent;
        // AB prop 9666 (profile_scraping_privacy_token_in_photo_iq) controls whether
        // to attach a tctoken on profile picture queries.
        if (!isSelf && isUserJid && serverProps.profilePicPrivacyToken) {
            tcTokenContent = await buildTcTokenFromJid({
                authState,
                jid: normalizedJid,
                getLIDForPN: signalRepository.lidMapping.getLIDForPN.bind(signalRepository.lidMapping)
            });
        }
        const result = await query({
            tag: 'iq',
            attrs: {
                target: normalizedJid,
                to: S_WHATSAPP_NET,
                type: 'get',
                xmlns: 'w:profile:picture'
            },
            content: buildProfilePictureQueryContent(type, tcTokenContent)
        }, timeoutMs);
        const child = getBinaryNodeChild(result, 'picture');
        return child?.attrs?.url;
    };
    const createCallLink = async (type, event, timeoutMs) => {
        const result = await query({
            tag: 'call',
            attrs: {
                id: generateMessageTag(),
                to: '@call'
            },
            content: [
                {
                    tag: 'link_create',
                    attrs: { media: type },
                    content: event ? [{ tag: 'event', attrs: { start_time: String(event.startTime) } }] : undefined
                }
            ]
        }, timeoutMs);
        const child = getBinaryNodeChild(result, 'link_create');
        return child?.attrs?.token;
    };
    const sendPresenceUpdate = async (type, toJid) => {
        const me = authState.creds.me;
        if (type === 'available' || type === 'unavailable') {
            if (!me.name) {
                logger.warn('no name present, ignoring presence update request...');
                return;
            }
            ev.emit('connection.update', { isOnline: type === 'available' });
            await sendNode({
                tag: 'presence',
                attrs: {
                    name: me.name.replace(/@/g, ''),
                    type
                }
            });
        }
        else {
            // real clients send chatstate with only `to`; the server fills in the sender.
            // (whatsmeow 1494ba7 removed the `from` attribute here.)
            await sendNode({
                tag: 'chatstate',
                attrs: {
                    to: toJid
                },
                content: [
                    {
                        tag: type === 'recording' ? 'composing' : type,
                        attrs: type === 'recording' ? { media: 'audio' } : {}
                    }
                ]
            });
        }
    };
    /**
     * @param toJid the jid to subscribe to
     */
    const presenceSubscribe = async (toJid) => {
        const tcTokenContent = await buildTcTokenFromJid({
            authState,
            jid: toJid,
            getLIDForPN: signalRepository.lidMapping.getLIDForPN.bind(signalRepository.lidMapping)
        });
        return sendNode({
            tag: 'presence',
            attrs: {
                to: toJid,
                id: generateMessageTag(),
                type: 'subscribe'
            },
            content: tcTokenContent
        });
    };
    const handlePresenceUpdate = ({ tag, attrs, content }) => {
        let presence;
        const jid = attrs.from;
        const participant = attrs.participant || attrs.from;
        if (shouldIgnoreJid(jid) && jid !== S_WHATSAPP_NET || attrs.offline) {
            return;
        }
        if (tag === 'presence') {
            presence = {
                lastKnownPresence: attrs.type === 'unavailable' ? 'unavailable' : 'available',
                lastSeen: attrs.last && attrs.last !== 'deny' ? +attrs.last : undefined,
                groupOnlineCount: attrs.count ? +attrs.count : undefined
            };
        }
        else if (Array.isArray(content)) {
            const [firstChild] = content;
            let type = firstChild.tag;
            if (type === 'paused') {
                type = 'available';
            }
            if (firstChild.attrs?.media === 'audio') {
                type = 'recording';
            }
            presence = { lastKnownPresence: type };
        }
        else {
            logger.error({ tag, attrs, content }, 'recv invalid presence node');
        }
        if (presence) {
            ev.emit('presence.update', { id: jid, presences: { [participant]: presence } });
        }
    };
    const appPatch = async (patchCreate) => {
        const name = patchCreate.type;
        const myAppStateKeyId = authState.creds.myAppStateKeyId;
        if (!myAppStateKeyId) {
            throw new Boom('App state key not present!', { statusCode: 400 });
        }
        let initial;
        let encodeResult;
        await appStatePatchMutex.mutex(async () => {
            await authState.keys.transaction(async () => {
                logger.debug({ patch: patchCreate }, 'applying app patch');
                await resyncAppState([name], false);
                const { [name]: currentSyncVersion } = await authState.keys.get('app-state-sync-version', [name]);
                initial = currentSyncVersion ? ensureLTHashStateVersion(currentSyncVersion) : newLTHashState();
                encodeResult = await encodeSyncdPatch(patchCreate, myAppStateKeyId, initial, getAppStateSyncKey);
                const { patch, state } = encodeResult;
                const node = {
                    tag: 'iq',
                    attrs: {
                        to: S_WHATSAPP_NET,
                        type: 'set',
                        xmlns: 'w:sync:app:state'
                    },
                    content: [
                        {
                            tag: 'sync',
                            attrs: {},
                            content: [
                                {
                                    tag: 'collection',
                                    attrs: {
                                        name,
                                        version: (state.version - 1).toString(),
                                        return_snapshot: 'false'
                                    },
                                    content: [
                                        {
                                            tag: 'patch',
                                            attrs: {},
                                            content: proto.SyncdPatch.encode(patch).finish()
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                };
                await query(node);
                await authState.keys.set({ 'app-state-sync-version': { [name]: state } });
            }, authState?.creds?.me?.id || 'app-patch');
        });
        if (config.emitOwnEvents) {
            const { onMutation } = newAppStateChunkHandler(false);
            const { mutationMap } = await decodePatches(name, [{ ...encodeResult.patch, version: { version: encodeResult.state.version } }], initial, getAppStateSyncKey, config.options, undefined, logger);
            for (const key in mutationMap) {
                onMutation(mutationMap[key]);
            }
        }
    };
    /** fetch AB props */
    const fetchProps = async () => {
        const resultNode = await query({
            tag: 'iq',
            attrs: {
                to: S_WHATSAPP_NET,
                xmlns: 'abt',
                type: 'get'
            },
            content: [
                {
                    tag: 'props',
                    attrs: {
                        protocol: '1',
                        ...(authState?.creds?.lastPropHash ? { hash: authState.creds.lastPropHash } : {})
                    }
                }
            ]
        });
        const propsNode = getBinaryNodeChild(resultNode, 'props');
        let props = {};
        if (propsNode) {
            if (propsNode.attrs?.hash) {
                // on some clients, the hash is returning as undefined
                authState.creds.lastPropHash = propsNode?.attrs?.hash;
                ev.emit('creds.update', authState.creds);
            }
            props = reduceBinaryNodeToDictionary(propsNode, 'prop');
        }
        // Extract protocol-relevant AB props (only the ones we use to gate tctoken behavior).
        // Both numeric IDs and human-readable names are checked because the server returns
        // different keys depending on client capability.
        const privacyTokenProp = props['10518'] ?? props['privacy_token_sending_on_all_1_on_1_messages'];
        if (privacyTokenProp !== undefined) {
            serverProps.privacyTokenOn1to1 = privacyTokenProp === 'true' || privacyTokenProp === '1';
        }
        const profilePicProp = props['9666'] ?? props['profile_scraping_privacy_token_in_photo_iq'];
        if (profilePicProp !== undefined) {
            serverProps.profilePicPrivacyToken = profilePicProp === 'true' || profilePicProp === '1';
        }
        const lidIssueProp = props['14303'] ?? props['lid_trusted_token_issue_to_lid'];
        if (lidIssueProp !== undefined) {
            serverProps.lidTrustedTokenIssueToLid = lidIssueProp === 'true' || lidIssueProp === '1';
        }
        const lidStatusProp = props['19696'] ?? props['lid_status_non_soaked_client_support_enabled'];
        if (lidStatusProp !== undefined) {
            serverProps.lidStatusNonSoakedClientSupport = lidStatusProp === 'true' || lidStatusProp === '1';
        }
        logger.debug({ serverProps }, 'fetched props');
        return props;
    };
    /**
     * modify a chat -- mark unread, read etc.
     * lastMessages must be sorted in reverse chronologically
     * requires the last messages till the last message received; required for archive & unread
     */
    const chatModify = (mod, jid) => {
        const patch = chatModificationToAppPatch(mod, jid);
        return appPatch(patch);
    };
    /**
     * Enable/Disable link preview privacy, not related to baileys link preview generation
     */
    const updateDisableLinkPreviewsPrivacy = (isPreviewsDisabled) => {
        return chatModify({
            disableLinkPreviews: { isPreviewsDisabled }
        }, '');
    };
    /**
     * Star or Unstar a message
     */
    const star = (jid, messages, star) => {
        return chatModify({
            star: {
                messages,
                star
            }
        }, jid);
    };
    /**
     * Add or Edit Contact
     */
    const addOrEditContact = (jid, contact) => {
        return chatModify({
            contact
        }, jid);
    };
    /**
     * Remove Contact
     */
    const removeContact = (jid) => {
        return chatModify({
            contact: null
        }, jid);
    };
    /**
     * Adds label
     */
    const addLabel = (jid, labels) => {
        return chatModify({
            addLabel: {
                ...labels
            }
        }, jid);
    };
    /**
     * Adds label for the chats
     */
    const addChatLabel = (jid, labelId) => {
        return chatModify({
            addChatLabel: {
                labelId
            }
        }, jid);
    };
    /**
     * Removes label for the chat
     */
    const removeChatLabel = (jid, labelId) => {
        return chatModify({
            removeChatLabel: {
                labelId
            }
        }, jid);
    };
    /**
     * Adds label for the message
     */
    const addMessageLabel = (jid, messageId, labelId) => {
        return chatModify({
            addMessageLabel: {
                messageId,
                labelId
            }
        }, jid);
    };
    /**
     * Removes label for the message
     */
    const removeMessageLabel = (jid, messageId, labelId) => {
        return chatModify({
            removeMessageLabel: {
                messageId,
                labelId
            }
        }, jid);
    };
    /**
     * Add or Edit Quick Reply
     */
    const addOrEditQuickReply = (quickReply) => {
        return chatModify({
            quickReply
        }, '');
    };
    /**
     * Remove Quick Reply
     */
    const removeQuickReply = (timestamp) => {
        return chatModify({
            quickReply: { timestamp, deleted: true }
        }, '');
    };
    /**
     * WA Web WAWebFetchAndUpdateBlocklistJob: the list fetched on connect replaces the stored one;
     * a reply without a list ("unchanged") never wipes it
     */
    const syncBlocklist = async () => {
        const blocklist = await getBlocklist();
        if (blocklist) {
            ev.emit('blocklist.set', { blocklist: blocklist.items.map(({ jid }) => jid) });
        }
    };
    /**
     * queries need to be fired on connection open
     * help ensure parity with WA Web
     * */
    const executeInitQueries = async () => {
        await Promise.all([fetchProps(), syncBlocklist(), fetchPrivacySettings()]);
    };
    const upsertMessage = ev.createBufferedFunction(async (msg, type) => {
        ev.emit('messages.upsert', { messages: [msg], type });
        if (!!msg.pushName) {
            let jid = msg.key.fromMe ? authState.creds.me.id : msg.key.participant || msg.key.remoteJid;
            jid = jidNormalizedUser(jid);
            if (!msg.key.fromMe) {
                ev.emit('contacts.update', [{ id: jid, notify: msg.pushName, verifiedName: msg.verifiedBizName }]);
            }
            // update our pushname too
            if (msg.key.fromMe && msg.pushName && authState.creds.me?.name !== msg.pushName) {
                ev.emit('creds.update', { me: { ...authState.creds.me, name: msg.pushName } });
            }
        }
        const historyMsg = getHistoryMsg(msg.message);
        const shouldProcessHistoryMsg = historyMsg
            ? shouldSyncHistoryMessage(historyMsg) &&
                PROCESSABLE_HISTORY_TYPES.includes(historyMsg.syncType)
            : false;
        if (historyMsg && shouldProcessHistoryMsg) {
            const syncType = historyMsg.syncType;
            // INITIAL_BOOTSTRAP — fire immediately, no progress check (same as WA Web K function)
            if (syncType === proto.HistorySync.HistorySyncType.INITIAL_BOOTSTRAP &&
                !historySyncStatus.initialBootstrapComplete) {
                historySyncStatus.initialBootstrapComplete = true;
                ev.emit('messaging-history.status', {
                    syncType,
                    status: 'complete',
                    explicit: true
                });
            }
            // RECENT with progress === 100 — explicit completion
            if (syncType === proto.HistorySync.HistorySyncType.RECENT &&
                historyMsg.progress === 100 &&
                !historySyncStatus.recentSyncComplete) {
                historySyncStatus.recentSyncComplete = true;
                clearTimeout(historySyncPausedTimeout);
                historySyncPausedTimeout = undefined;
                ev.emit('messaging-history.status', {
                    syncType,
                    status: 'complete',
                    explicit: true
                });
            }
            // Reset 120s paused timeout on any RECENT chunk (like WA Web's handleChunkProgress)
            if (syncType === proto.HistorySync.HistorySyncType.RECENT && !historySyncStatus.recentSyncComplete) {
                clearTimeout(historySyncPausedTimeout);
                historySyncPausedTimeout = setTimeout(() => {
                    if (!historySyncStatus.recentSyncComplete) {
                        historySyncStatus.recentSyncComplete = true;
                        ev.emit('messaging-history.status', {
                            syncType: proto.HistorySync.HistorySyncType.RECENT,
                            status: 'paused',
                            explicit: false
                        });
                    }
                    historySyncPausedTimeout = undefined;
                }, HISTORY_SYNC_PAUSED_TIMEOUT_MS);
            }
        }
        // State machine: decide on sync and flush
        if (historyMsg && syncState === SyncState.AwaitingInitialSync) {
            if (awaitingSyncTimeout) {
                clearTimeout(awaitingSyncTimeout);
                awaitingSyncTimeout = undefined;
            }
            if (shouldProcessHistoryMsg) {
                syncState = SyncState.Syncing;
                logger.info('Transitioned to Syncing state');
                // Let doAppStateSync handle the final flush after it's done
            }
            else {
                syncState = SyncState.Online;
                logger.info('History sync skipped, transitioning to Online state and flushing buffer');
                ev.flush();
            }
        }
        const doAppStateSync = async () => {
            if (syncState === SyncState.Syncing) {
                // All collections will be synced, so clear any blocked ones
                blockedCollections.clear();
                logger.info('Doing app state sync');
                try {
                    await resyncAppState(ALL_WA_PATCH_NAMES, true);
                }
                catch (error) {
                    // WA Web WAWebSyncd: handleSyncCompleted runs in `finally` -- a failed sync still ends
                    // the round, so the state machine (and the event buffer) never stay in Syncing
                    logger.error({ error }, 'app state sync failed, completing the initial sync anyway');
                }
                // a sync cut short by the socket closing is not a finished initial sync: without the
                // accountSyncCounter bump the next connection runs it again instead of skipping it
                if (!ws.isOpen) {
                    logger.info('connection closed during the app state sync, the initial sync stays unfinished');
                    ev.flush();
                    return;
                }
                // Sync is complete, go online and flush everything
                syncState = SyncState.Online;
                logger.info('App state sync complete, transitioning to Online state and flushing buffer');
                ev.flush();
                const accountSyncCounter = (authState.creds.accountSyncCounter || 0) + 1;
                ev.emit('creds.update', { accountSyncCounter });
            }
        };
        await Promise.all([
            (async () => {
                if (shouldProcessHistoryMsg) {
                    await doAppStateSync();
                }
            })(),
            processMessage(msg, {
                signalRepository,
                shouldProcessHistoryMsg,
                processHistoryStatuses: config.processHistoryStatuses,
                placeholderResendCache,
                ev,
                creds: authState.creds,
                keyStore: authState.keys,
                logger,
                options: config.options,
                getMessage,
                onHistorySyncBlobProcessed: notification => historySyncBlobProcessedHandler?.(notification)
            })
        ]);
        // If the app state key arrives and we are waiting to sync, trigger the sync now.
        if (msg.message?.protocolMessage?.appStateSyncKeyShare && syncState === SyncState.Syncing) {
            logger.info('App state sync key arrived, triggering app state sync');
            await doAppStateSync();
        }
    });
    ws.on('CB:presence', handlePresenceUpdate);
    ws.on('CB:chatstate', handlePresenceUpdate);
    ws.on('CB:ib,,dirty', async (node) => {
        const { attrs } = getBinaryNodeChild(node, 'dirty');
        const type = attrs.type;
        switch (type) {
            case 'account_sync':
                if (attrs.timestamp) {
                    let { lastAccountSyncTimestamp } = authState.creds;
                    if (lastAccountSyncTimestamp) {
                        await cleanDirtyBits('account_sync', lastAccountSyncTimestamp);
                    }
                    lastAccountSyncTimestamp = +attrs.timestamp;
                    ev.emit('creds.update', { lastAccountSyncTimestamp });
                }
                break;
            case 'groups':
                // handled in groups.ts
                break;
            default:
                logger.info({ node }, 'received unknown sync');
                break;
        }
    });
    // App-state sync is independent of history sync (mirrors WA Web / whatsmeow, which
    // fetch app-state on connect / key-share regardless of history). Baileys otherwise
    // syncs app-state only via the history state machine, so history-disabled clients
    // never receive contacts, settings, LID mappings, or the NCT salt (regular_high) that
    // gates cstoken and the 463 reach-out lock. Syncs only not-yet-synced collections
    // (onlyIfNotSynced), so it is a one-time cost per collection.
    const syncUnsyncedAppStateCollections = async () => {
        const versions = await authState.keys.get('app-state-sync-version', [...ALL_WA_PATCH_NAMES]);
        const unsynced = ALL_WA_PATCH_NAMES.filter(name => !versions[name]);
        if (!unsynced.length) {
            return;
        }
        logger.info({ unsynced }, 'syncing app-state collections independent of history');
        await resyncAppState(unsynced, true);
    };
    const onConnectionUpdate = ({ connection, receivedPendingNotifications }) => {
        if (connection === 'close') {
            blockedCollections.clear();
            clearTimeout(historySyncPausedTimeout);
            historySyncPausedTimeout = undefined;
        }
        if (connection === 'open') {
            if (fireInitQueries) {
                executeInitQueries().catch(error => onUnexpectedError(error, 'init queries'));
            }
            sendPresenceUpdate(markOnlineOnConnect ? 'available' : 'unavailable').catch(error => onUnexpectedError(error, 'presence update requests'));
        }
        if (!receivedPendingNotifications || syncState !== SyncState.Connecting) {
            return;
        }
        historySyncStatus.initialBootstrapComplete = false;
        historySyncStatus.recentSyncComplete = false;
        clearTimeout(historySyncPausedTimeout);
        historySyncPausedTimeout = undefined;
        syncState = SyncState.AwaitingInitialSync;
        logger.info('Connection is now AwaitingInitialSync, buffering events');
        ev.buffer();
        const willSyncHistory = shouldSyncHistoryMessage(proto.Message.HistorySyncNotification.create({
            syncType: proto.HistorySync.HistorySyncType.RECENT
        }));
        if (!willSyncHistory) {
            logger.info('History sync is disabled by config, not waiting for notification. Transitioning to Online.');
            // App-state sync is independent of history (WA Web / whatsmeow). With history disabled the
            // normal doAppStateSync path never runs, so trigger it here for not-yet-synced collections —
            // else contacts, settings, LID mappings and the NCT salt (regular_high, gates cstoken/463)
            // never arrive. History-enabled clients are unaffected (they sync via doAppStateSync).
            syncUnsyncedAppStateCollections().catch(error => onUnexpectedError(error, 'app-state sync'));
            syncState = SyncState.Online;
            setTimeout(() => ev.flush(), 0);
            return;
        }
        // On reconnection (accountSyncCounter > 0), the server does not push
        // history sync notifications — the device already has its data.
        // Skip the 20s wait and go online immediately.
        if (authState.creds.accountSyncCounter > 0) {
            logger.info('Reconnection with existing sync data, skipping history sync wait. Transitioning to Online.');
            syncState = SyncState.Online;
            setTimeout(() => ev.flush(), 0);
            return;
        }
        logger.info('First connection, awaiting history sync notification with a 20s timeout.');
        if (awaitingSyncTimeout) {
            clearTimeout(awaitingSyncTimeout);
        }
        awaitingSyncTimeout = setTimeout(() => {
            if (syncState === SyncState.AwaitingInitialSync) {
                logger.warn('Timeout in AwaitingInitialSync, forcing state to Online and flushing buffer');
                syncState = SyncState.Online;
                ev.flush();
                // Increment so subsequent reconnections skip the 20s wait.
                // Late-arriving history is still processed via processMessage
                // regardless of the state machine phase.
                const accountSyncCounter = (authState.creds.accountSyncCounter || 0) + 1;
                ev.emit('creds.update', { accountSyncCounter });
            }
        }, 20000);
    };
    ev.on('connection.update', onConnectionUpdate);
    disposables.push(() => ev.off('connection.update', onConnectionUpdate));
    disposables.push(() => clearTimeout(awaitingSyncTimeout));
    // When an app state sync key arrives (myAppStateKeyId is set) and there are
    // collections blocked on a missing key, trigger a re-sync for just those collections.
    // This mirrors WA Web's Blocked → retry-on-key-arrival behavior.
    ev.on('creds.update', ({ myAppStateKeyId }) => {
        if (!myAppStateKeyId || blockedCollections.size === 0) {
            return;
        }
        // If we're in the middle of a full sync, doAppStateSync handles all collections
        if (syncState === SyncState.Syncing) {
            blockedCollections.clear();
            return;
        }
        const collections = [...blockedCollections];
        blockedCollections.clear();
        logger.info({ collections }, 'app state sync key arrived, re-syncing blocked collections');
        resyncAppState(collections, false).catch((error) => onUnexpectedError(error, 'blocked collections resync'));
    });
    ev.on('lid-mapping.update', async ({ lid, pn }) => {
        try {
            await signalRepository.lidMapping.storeLIDPNMappings([{ lid, pn }]);
        }
        catch (error) {
            logger.warn({ lid, pn, error }, 'Failed to store LID-PN mapping');
        }
    });
    /**
     * Proactively fetch the account's "reach-out time-lock" state (the server-side
     * rate-limit on messaging/calling unknown people that surfaces as error 463).
     * Complements the reactive `account-reachout-timelock.update` notification —
     * call this to learn the current state + expiry on demand, e.g. before a send
     * burst, so the app can pause instead of hammering and worsening the lock.
     * MEX query WAWebMexFetchReachoutTimelockJobQuery (doc id 23983697327930364).
     */
    const fetchAccountReachoutTimelock = async () => {
        const res = await executeWMexQuery({}, '23983697327930364', 'xwa2_fetch_account_reachout_timelock', query, generateMessageTag);
        return {
            isActive: !!res?.is_active,
            timeEnforcementEnds: res?.time_enforcement_ends ? Number(res.time_enforcement_ends) : 0,
            enforcementType: res?.enforcement_type || ''
        };
    };
    return {
        ...sock,
        serverProps,
        setHistorySyncBlobProcessedHandler: (handler) => {
            historySyncBlobProcessedHandler = handler;
        },
        fetchAccountReachoutTimelock,
        createCallLink,
        getBotListV2,
        messageMutex,
        receiptMutex,
        appStatePatchMutex,
        notificationMutex,
        fetchPrivacySettings,
        upsertMessage,
        appPatch,
        sendPresenceUpdate,
        presenceSubscribe,
        profilePictureUrl,
        getBlocklist,
        fetchBlocklist,
        fetchStatus,
        fetchDisappearingDuration,
        updateProfilePicture,
        removeProfilePicture,
        updateProfileStatus,
        updateProfileName,
        updateBlockStatus,
        updateDisableLinkPreviewsPrivacy,
        updateCallPrivacy,
        updateMessagesPrivacy,
        updateLastSeenPrivacy,
        updateOnlinePrivacy,
        updateProfilePicturePrivacy,
        updateStatusPrivacy,
        updateReadReceiptsPrivacy,
        updateGroupsAddPrivacy,
        updateDefaultDisappearingMode,
        getBusinessProfile,
        resyncAppState,
        chatModify,
        cleanDirtyBits,
        addOrEditContact,
        removeContact,
        addLabel,
        addChatLabel,
        removeChatLabel,
        addMessageLabel,
        removeMessageLabel,
        star,
        addOrEditQuickReply,
        removeQuickReply
    };
};
//# sourceMappingURL=chats.js.map