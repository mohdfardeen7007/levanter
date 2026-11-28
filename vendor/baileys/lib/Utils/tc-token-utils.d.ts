import type { SignalKeyStore, SignalKeyStoreWithTransaction } from '../Types/index.js';
import { type BinaryNode } from '../WABinary/index.js';
/** Sentinel key under `tctoken` store holding a JSON array of tracked storage JIDs for cross-session pruning. */
export declare const TC_TOKEN_INDEX_KEY = "__index";
/** Read the persisted tctoken JID index and return its entries (never contains the sentinel key itself). */
export declare function readTcTokenIndex(keys: SignalKeyStoreWithTransaction): Promise<string[]>;
/** Build a SignalDataSet fragment that writes the merged index (persisted ∪ added) under the sentinel key. */
export declare function buildMergedTcTokenIndexWrite(keys: SignalKeyStoreWithTransaction, addedJids: Iterable<string>): Promise<{
    [TC_TOKEN_INDEX_KEY]: {
        token: Buffer;
    };
}>;
/**
 * Check if a tctoken has expired based on the rolling bucket algorithm.
 * Tokens older than (NUM_BUCKETS - 1) bucket durations are considered expired.
 */
export declare function isTcTokenExpired(timestamp: number | string | null | undefined): boolean;
/**
 * Determine if a new tctoken should be sent to a contact.
 * Returns true if the senderTimestamp has crossed into a new bucket,
 * indicating it's time to re-issue the token.
 */
export declare function shouldSendNewTcToken(senderTimestamp: number | undefined, nowSeconds?: number): boolean;
/**
 * Resolve a JID to its LID for tctoken storage.
 * Mirrors Signal session key patterns — tctokens are stored/looked up by LID.
 */
export declare function resolveTcTokenJid(jid: string, getLIDForPN: (pn: string) => Promise<string | null>): Promise<string>;
/** Resolve target JID for issuing privacy token based on AB prop 14303 (issue-to-LID toggle). */
export declare function resolveIssuanceJid(jid: string, issueToLid: boolean, getLIDForPN: (pn: string) => Promise<string | null>, getPNForLID?: (lid: string) => Promise<string | null>): Promise<string>;
type TcTokenParams = {
    jid: string;
    baseContent?: BinaryNode[];
    authState: {
        keys: SignalKeyStoreWithTransaction;
    };
    /** Optional LID resolver — when provided, storage key will be resolved to LID */
    getLIDForPN?: (pn: string) => Promise<string | null>;
};
/**
 * Builds tctoken binary nodes from a JID.
 * Useful for profile picture and presence subscription requests.
 * When getLIDForPN is provided, resolves the JID to LID for storage lookup
 * and performs opportunistic cleanup of expired tokens — preserving any
 * senderTimestamp so shouldSendNewTcToken() dedupe state survives the wipe.
 */
export declare function buildTcTokenFromJid({ authState, jid, baseContent, getLIDForPN }: TcTokenParams): Promise<BinaryNode[] | undefined>;
export type StoreTcTokensParams = {
    result: BinaryNode;
    fallbackJid: string;
    keys: SignalKeyStore | SignalKeyStoreWithTransaction;
    getLIDForPN: (pn: string) => Promise<string | null>;
    /** Called when a new JID is stored for the first time (for index tracking) */
    onNewJidStored?: (jid: string) => void;
};
/**
 * Parse tctoken(s) from an IQ result and store them.
 * Includes timestamp monotonicity guard to prevent older tokens from overwriting newer ones,
 * an isRegularUser filter to defend against malformed notifications, and prefers the
 * caller-provided fallbackJid because in notifications tokenNode.attrs.jid is the
 * OWN device JID, not the sender's.
 */
export declare function storeTcTokensFromIqResult({ result, fallbackJid, keys, getLIDForPN, onNewJidStored }: StoreTcTokensParams): Promise<void>;
export {};
//# sourceMappingURL=tc-token-utils.d.ts.map