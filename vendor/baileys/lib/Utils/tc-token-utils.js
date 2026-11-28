import { getBinaryNodeChild, getBinaryNodeChildren, isHostedLidUser, isHostedPnUser, isJidMetaAI, isLidUser, isPnUser, jidNormalizedUser } from '../WABinary/index.js';
// Same phone-number pattern as WABinary's isJidBot, applied against the user
// part so the check is invariant to @c.us ↔ @s.whatsapp.net normalization.
const BOT_PHONE_REGEX = /^1313555\d{4}$|^131655500\d{2}$/;
/**
 * Mirrors WA Web's `Wid.isRegularUser()` (user ∧ ¬PSA ∧ ¬Bot). Used to gate tctoken
 * storage against malformed notifications — WA Web filters server-side but we
 * defend here for parity with `WAWebSetTcTokenChatAction.handleIncomingTcToken`.
 */
function isRegularUser(jid) {
    if (!jid)
        return false;
    const user = jid.split('@')[0] ?? '';
    if (user === '0')
        return false; // PSA
    if (BOT_PHONE_REGEX.test(user))
        return false; // Bot by phone pattern
    if (isJidMetaAI(jid))
        return false; // MetaAI (@bot server)
    return !!(isPnUser(jid) || isLidUser(jid) || isHostedPnUser(jid) || isHostedLidUser(jid) || jid.endsWith('@c.us'));
}
/** 7 days in seconds — matches WA Web tctoken_duration */
const TC_TOKEN_BUCKET_DURATION = 604800;
/** 4 buckets — matches WA Web tctoken_num_buckets (~28-day rolling window) */
const TC_TOKEN_NUM_BUCKETS = 4;
/** Sentinel key under `tctoken` store holding a JSON array of tracked storage JIDs for cross-session pruning. */
export const TC_TOKEN_INDEX_KEY = '__index';
/** Read the persisted tctoken JID index and return its entries (never contains the sentinel key itself). */
export async function readTcTokenIndex(keys) {
    const data = await keys.get('tctoken', [TC_TOKEN_INDEX_KEY]);
    const entry = data[TC_TOKEN_INDEX_KEY];
    if (!entry?.token?.length)
        return [];
    try {
        const parsed = JSON.parse(Buffer.from(entry.token).toString());
        if (!Array.isArray(parsed))
            return [];
        return parsed.filter((j) => typeof j === 'string' && j.length > 0 && j !== TC_TOKEN_INDEX_KEY);
    }
    catch {
        return [];
    }
}
/** Build a SignalDataSet fragment that writes the merged index (persisted ∪ added) under the sentinel key. */
export async function buildMergedTcTokenIndexWrite(keys, addedJids) {
    const persisted = await readTcTokenIndex(keys);
    const merged = new Set(persisted);
    for (const jid of addedJids) {
        if (jid && jid !== TC_TOKEN_INDEX_KEY)
            merged.add(jid);
    }
    return {
        [TC_TOKEN_INDEX_KEY]: { token: Buffer.from(JSON.stringify([...merged])) }
    };
}
/**
 * Check if a tctoken has expired based on the rolling bucket algorithm.
 * Tokens older than (NUM_BUCKETS - 1) bucket durations are considered expired.
 */
export function isTcTokenExpired(timestamp) {
    if (timestamp === null || timestamp === undefined)
        return true;
    const ts = typeof timestamp === 'string' ? parseInt(timestamp) : timestamp;
    if (isNaN(ts))
        return true;
    const now = Math.floor(Date.now() / 1000);
    const currentBucket = Math.floor(now / TC_TOKEN_BUCKET_DURATION);
    const cutoffBucket = currentBucket - (TC_TOKEN_NUM_BUCKETS - 1);
    const cutoffTimestamp = cutoffBucket * TC_TOKEN_BUCKET_DURATION;
    return ts < cutoffTimestamp;
}
/**
 * Determine if a new tctoken should be sent to a contact.
 * Returns true if the senderTimestamp has crossed into a new bucket,
 * indicating it's time to re-issue the token.
 */
export function shouldSendNewTcToken(senderTimestamp, nowSeconds = Math.floor(Date.now() / 1000)) {
    if (senderTimestamp === undefined)
        return true;
    const currentBucket = Math.floor(nowSeconds / TC_TOKEN_BUCKET_DURATION);
    const senderBucket = Math.floor(senderTimestamp / TC_TOKEN_BUCKET_DURATION);
    return currentBucket > senderBucket;
}
/**
 * Resolve a JID to its LID for tctoken storage.
 * Mirrors Signal session key patterns — tctokens are stored/looked up by LID.
 */
export async function resolveTcTokenJid(jid, getLIDForPN) {
    if (isLidUser(jid))
        return jid;
    const lid = await getLIDForPN(jid);
    return lid ?? jid;
}
/** Resolve target JID for issuing privacy token based on AB prop 14303 (issue-to-LID toggle). */
export async function resolveIssuanceJid(jid, issueToLid, getLIDForPN, getPNForLID) {
    if (issueToLid) {
        if (isLidUser(jid))
            return jid;
        const lid = await getLIDForPN(jid);
        return lid ?? jid;
    }
    if (!isLidUser(jid))
        return jid;
    if (getPNForLID) {
        const pn = await getPNForLID(jid);
        return pn ?? jid;
    }
    return jid;
}
/**
 * Builds tctoken binary nodes from a JID.
 * Useful for profile picture and presence subscription requests.
 * When getLIDForPN is provided, resolves the JID to LID for storage lookup
 * and performs opportunistic cleanup of expired tokens — preserving any
 * senderTimestamp so shouldSendNewTcToken() dedupe state survives the wipe.
 */
export async function buildTcTokenFromJid({ authState, jid, baseContent = [], getLIDForPN }) {
    try {
        const storageJid = getLIDForPN ? await resolveTcTokenJid(jid, getLIDForPN) : jid;
        const tcTokenData = await authState.keys.get('tctoken', [storageJid]);
        const entry = tcTokenData?.[storageJid];
        const tcTokenBuffer = entry?.token;
        const timestamp = entry?.timestamp;
        if (!tcTokenBuffer?.length || timestamp === undefined || isTcTokenExpired(timestamp)) {
            if (tcTokenBuffer) {
                // Preserve senderTimestamp so shouldSendNewTcToken() keeps its dedupe
                // state after we drop the unusable peer token. Only wipe the record
                // entirely when there's nothing worth keeping.
                const cleared = entry?.senderTimestamp !== undefined
                    ? { token: Buffer.alloc(0), senderTimestamp: entry.senderTimestamp }
                    : null;
                try {
                    await authState.keys.set({ tctoken: { [storageJid]: cleared } });
                }
                catch {
                    /* ignore cleanup errors */
                }
            }
            return baseContent.length > 0 ? baseContent : undefined;
        }
        baseContent.push({
            tag: 'tctoken',
            attrs: { t: String(timestamp) },
            content: tcTokenBuffer
        });
        return baseContent;
    }
    catch {
        return baseContent.length > 0 ? baseContent : undefined;
    }
}
/**
 * Parse tctoken(s) from an IQ result and store them.
 * Includes timestamp monotonicity guard to prevent older tokens from overwriting newer ones,
 * an isRegularUser filter to defend against malformed notifications, and prefers the
 * caller-provided fallbackJid because in notifications tokenNode.attrs.jid is the
 * OWN device JID, not the sender's.
 */
export async function storeTcTokensFromIqResult({ result, fallbackJid, keys, getLIDForPN, onNewJidStored }) {
    const tokensNode = getBinaryNodeChild(result, 'tokens');
    if (!tokensNode)
        return;
    const tokenNodes = getBinaryNodeChildren(tokensNode, 'token');
    for (const tokenNode of tokenNodes) {
        if (tokenNode.attrs.type !== 'trusted_contact' || !(tokenNode.content instanceof Uint8Array)) {
            continue;
        }
        // In notifications tokenNode.attrs.jid is your own device JID, not the sender's
        const rawJid = jidNormalizedUser(fallbackJid || tokenNode.attrs.jid);
        if (!isRegularUser(rawJid))
            continue;
        const storageJid = await resolveTcTokenJid(rawJid, getLIDForPN);
        const existingTcData = await keys.get('tctoken', [storageJid]);
        const existingEntry = existingTcData[storageJid];
        // Timestamp monotonicity guard — drop incoming tokens with no timestamp
        // (they would be immediately expired) and reject older-than-stored.
        const existingTs = existingEntry?.timestamp ? Number(existingEntry.timestamp) : 0;
        const incomingTs = tokenNode.attrs.t ? Number(tokenNode.attrs.t) : 0;
        if (!incomingTs)
            continue;
        if (existingTs > 0 && existingTs > incomingTs)
            continue;
        await keys.set({
            tctoken: {
                [storageJid]: {
                    ...existingEntry,
                    token: Buffer.from(tokenNode.content),
                    timestamp: tokenNode.attrs.t
                }
            }
        });
        onNewJidStored?.(storageJid);
    }
}
//# sourceMappingURL=tc-token-utils.js.map