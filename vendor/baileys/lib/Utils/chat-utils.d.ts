import { proto } from '../../WAProto/index.js';
import type { BaileysEventEmitter, ChatModification, ChatMutation, Contact, InitialAppStateSyncOptions, LTHashState, WAPatchCreate, WAPatchName } from '../Types/index.js';
import { type BinaryNode } from '../WABinary/index.js';
import type { ILogger } from './logger.js';
type FetchAppStateSyncKey = (keyId: string) => Promise<proto.Message.IAppStateSyncKeyData | null | undefined>;
export type ChatMutationMap = {
    [index: string]: ChatMutation;
};
type Mac = {
    indexMac: Uint8Array;
    valueMac: Uint8Array;
    operation: proto.SyncdMutation.SyncdOperation;
};
export declare const makeLtHashGenerator: ({ indexValueMap, hash }: Pick<LTHashState, "hash" | "indexValueMap">) => {
    mix: ({ indexMac, valueMac, operation }: Mac) => void;
    finish: () => {
        hash: Buffer<ArrayBuffer>;
        indexValueMap: {
            [indexMacBase64: string]: {
                valueMac: Uint8Array | Buffer;
            };
        };
    };
};
export declare const newLTHashState: () => LTHashState;
export declare const ensureLTHashStateVersion: (state: LTHashState) => LTHashState;
export declare const MAX_SYNC_ATTEMPTS = 2;
/**
 * Check if an error is a missing app state sync key.
 * WA Web treats these as "Blocked" (waits for key arrival), not fatal.
 * In Baileys we retry with a snapshot which may use a different key.
 */
export declare const isMissingKeyError: (error: any) => boolean;
/**
 * Determines if an app state sync error is unrecoverable.
 * TypeError indicates a WASM crash; otherwise we give up after MAX_SYNC_ATTEMPTS.
 * Missing keys are NOT checked here — they are handled separately as "Blocked".
 */
export declare const isAppStateSyncIrrecoverable: (error: any, attempts: number) => boolean;
/** retry rounds after a failed app-state fetch; WA Web keeps retrying for days, a socket gives up sooner */
export declare const APP_STATE_FETCH_MAX_RETRIES = 3;
/** longest wait between two app-state fetch rounds; a larger server backoff ends the retries instead */
export declare const APP_STATE_FETCH_MAX_DELAY_MS = 10000;
/**
 * WA Web WAWebSyncdServerSync (C = 500): one sync attempt sends at most 500 IQ rounds; collections still
 * asking for more patches after that are retried later (ErrorRetry) instead of being looped on
 */
export declare const APP_STATE_SYNC_MAX_ROUNDS = 500;
export type AppStateFetchErrorClass = {
    /** fatal: never retried; retryable: retried after a backoff; closed: the connection is gone */
    kind: 'fatal' | 'retryable' | 'closed';
    /** the IQ error code */
    code?: number;
    /** the server's `<error backoff>` (seconds), in ms */
    backoffMs?: number;
};
/**
 * WA Web WAWebSyncdServerSync: a fetch failing with 400/404/405/406 is fatal; anything else, a
 * timeout included, is retryable (honouring the server backoff). A closed connection ends the sync.
 */
export declare const classifyAppStateFetchError: (error: any) => AppStateFetchErrorClass;
/** WA Web WAWebSyncd: min(max(2^retry s, server backoff), cap); WA Web caps at 1 h */
export declare const appStateFetchRetryDelayMs: (retry: number, serverBackoffMs?: number) => number;
export declare const encodeSyncdPatch: ({ type, index, syncAction, apiVersion, operation }: WAPatchCreate, myAppStateKeyId: string, state: LTHashState, getAppStateSyncKey: FetchAppStateSyncKey) => Promise<{
    patch: proto.ISyncdPatch;
    state: LTHashState;
}>;
export declare const decodeSyncdMutations: (msgMutations: (proto.ISyncdMutation | proto.ISyncdRecord)[], initialState: LTHashState, getAppStateSyncKey: FetchAppStateSyncKey, onMutation: (mutation: ChatMutation) => void, validateMacs: boolean) => Promise<{
    hash: Buffer<ArrayBuffer>;
    indexValueMap: {
        [indexMacBase64: string]: {
            valueMac: Uint8Array | Buffer;
        };
    };
}>;
export declare const decodeSyncdPatch: (msg: proto.ISyncdPatch, name: WAPatchName, initialState: LTHashState, getAppStateSyncKey: FetchAppStateSyncKey, onMutation: (mutation: ChatMutation) => void, validateMacs: boolean) => Promise<{
    hash: Buffer<ArrayBuffer>;
    indexValueMap: {
        [indexMacBase64: string]: {
            valueMac: Uint8Array | Buffer;
        };
    };
}>;
export declare const extractSyncdPatches: (result: BinaryNode, options: RequestInit) => Promise<{
    critical_unblock_low: {
        patches: proto.ISyncdPatch[];
        hasMorePatches: boolean;
        snapshot?: proto.ISyncdSnapshot;
        /** set when the server answered the collection with `type="error"` */
        error?: {
            code?: number;
            text?: string;
        };
    };
    regular_high: {
        patches: proto.ISyncdPatch[];
        hasMorePatches: boolean;
        snapshot?: proto.ISyncdSnapshot;
        /** set when the server answered the collection with `type="error"` */
        error?: {
            code?: number;
            text?: string;
        };
    };
    regular_low: {
        patches: proto.ISyncdPatch[];
        hasMorePatches: boolean;
        snapshot?: proto.ISyncdSnapshot;
        /** set when the server answered the collection with `type="error"` */
        error?: {
            code?: number;
            text?: string;
        };
    };
    critical_block: {
        patches: proto.ISyncdPatch[];
        hasMorePatches: boolean;
        snapshot?: proto.ISyncdSnapshot;
        /** set when the server answered the collection with `type="error"` */
        error?: {
            code?: number;
            text?: string;
        };
    };
    regular: {
        patches: proto.ISyncdPatch[];
        hasMorePatches: boolean;
        snapshot?: proto.ISyncdSnapshot;
        /** set when the server answered the collection with `type="error"` */
        error?: {
            code?: number;
            text?: string;
        };
    };
}>;
export declare const downloadExternalBlob: (blob: proto.IExternalBlobReference, options: RequestInit) => Promise<Buffer<ArrayBuffer>>;
export declare const downloadExternalPatch: (blob: proto.IExternalBlobReference, options: RequestInit) => Promise<proto.SyncdMutations>;
export declare const decodeSyncdSnapshot: (name: WAPatchName, snapshot: proto.ISyncdSnapshot, getAppStateSyncKey: FetchAppStateSyncKey, minimumVersionNumber: number | undefined, validateMacs?: boolean, logger?: ILogger) => Promise<{
    state: LTHashState;
    mutationMap: ChatMutationMap;
}>;
export declare const decodePatches: (name: WAPatchName, syncds: proto.ISyncdPatch[], initial: LTHashState, getAppStateSyncKey: FetchAppStateSyncKey, options: RequestInit, minimumVersionNumber?: number, logger?: ILogger, validateMacs?: boolean) => Promise<{
    state: LTHashState;
    mutationMap: ChatMutationMap;
}>;
export declare const chatModificationToAppPatch: (mod: ChatModification, jid: string) => WAPatchCreate;
export declare const processSyncAction: (syncAction: ChatMutation, ev: BaileysEventEmitter, me: Contact, initialSyncOpts?: InitialAppStateSyncOptions, logger?: ILogger) => void;
export {};
//# sourceMappingURL=chat-utils.d.ts.map