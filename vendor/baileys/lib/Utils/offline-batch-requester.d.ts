import type { BinaryNode } from '../WABinary/index.js';
import type { ILogger } from './logger.js';
/** WA Web WAWebOfflineHandler `C`: stanzas asked for per window, and the most unprocessed ones to still ask with. */
export declare const OFFLINE_BATCH_SIZE = 200;
/** WA Web WAWebOfflineHandler `S`: minimum gap between two window requests. */
export declare const OFFLINE_BATCH_DEBOUNCE_MS = 100;
/** WA Web WAWebOfflineResumeConst.OFFLINE_STANZA_TIMEOUT_MS: the resume is abandoned after this long without progress. */
export declare const OFFLINE_BATCH_IDLE_TIMEOUT_MS = 60000;
/** zapo 8be4b0b8: retries of a rejected request (WA Web casts it and never sees an outcome). */
export declare const OFFLINE_BATCH_MAX_RETRIES = 3;
export type OfflineBatchRequesterOptions = {
    sendNode: (node: BinaryNode) => Promise<unknown>;
    logger: ILogger;
    batchSize?: number;
    debounceMs?: number;
    idleTimeoutMs?: number;
    maxRetries?: number;
    /**
     * WA Web WAWebBlockingOfflineResumeManager: when the 60 s stanza timer fires before `<ib><offline/>`,
     * the resume is completed anyway (processOfflineSessionComplete). Called once, after the loop stops.
     */
    onIdleTimeout?: () => void;
};
export type OfflineBatchRequester = ReturnType<typeof makeOfflineBatchRequester>;
/**
 * Offline delivery pull loop (WA Web WAWebOfflineHandler, zapo 8be4b0b8). WA Web asks for nothing on
 * `<ib><offline_preview/>`; once offline stanzas land it asks with `<ib><offline_batch count=200/>` and
 * keeps asking while they do (zapo saw the server stop after a single window otherwise):
 * - one request in flight: nothing more is asked until an offline stanza arrives after the last request
 * - at most one request per 100 ms, and only while at most 200 offline stanzas wait to be processed
 * - ends on `<ib><offline/>` (stop) or after 60 s without offline stanzas or processing progress,
 *   which also reports the resume as complete through `onIdleTimeout`
 */
export declare const makeOfflineBatchRequester: ({ sendNode, logger, batchSize, debounceMs, idleTimeoutMs, maxRetries, onIdleTimeout }: OfflineBatchRequesterOptions) => {
    /** `<ib><offline_preview/>`: a resume starts; its first request waits for its first offline stanza */
    start(): void;
    /** a stanza carrying `offline` reached the handlers: the last request is being served */
    onOfflineStanza(): void;
    /** a queued offline stanza was processed; the backlog may fit a window again */
    onBacklogProcessed(): void;
    /** count of offline stanzas received but not processed yet */
    setBacklogCounter(counter: () => number): void;
    /** `<ib><offline/>` or connection end */
    stop: () => void;
    isActive: () => boolean;
};
//# sourceMappingURL=offline-batch-requester.d.ts.map