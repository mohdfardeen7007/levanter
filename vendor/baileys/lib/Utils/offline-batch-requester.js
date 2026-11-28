/** WA Web WAWebOfflineHandler `C`: stanzas asked for per window, and the most unprocessed ones to still ask with. */
export const OFFLINE_BATCH_SIZE = 200;
/** WA Web WAWebOfflineHandler `S`: minimum gap between two window requests. */
export const OFFLINE_BATCH_DEBOUNCE_MS = 100;
/** WA Web WAWebOfflineResumeConst.OFFLINE_STANZA_TIMEOUT_MS: the resume is abandoned after this long without progress. */
export const OFFLINE_BATCH_IDLE_TIMEOUT_MS = 60000;
/** zapo 8be4b0b8: retries of a rejected request (WA Web casts it and never sees an outcome). */
export const OFFLINE_BATCH_MAX_RETRIES = 3;
/**
 * Offline delivery pull loop (WA Web WAWebOfflineHandler, zapo 8be4b0b8). WA Web asks for nothing on
 * `<ib><offline_preview/>`; once offline stanzas land it asks with `<ib><offline_batch count=200/>` and
 * keeps asking while they do (zapo saw the server stop after a single window otherwise):
 * - one request in flight: nothing more is asked until an offline stanza arrives after the last request
 * - at most one request per 100 ms, and only while at most 200 offline stanzas wait to be processed
 * - ends on `<ib><offline/>` (stop) or after 60 s without offline stanzas or processing progress,
 *   which also reports the resume as complete through `onIdleTimeout`
 */
export const makeOfflineBatchRequester = ({ sendNode, logger, batchSize = OFFLINE_BATCH_SIZE, debounceMs = OFFLINE_BATCH_DEBOUNCE_MS, idleTimeoutMs = OFFLINE_BATCH_IDLE_TIMEOUT_MS, maxRetries = OFFLINE_BATCH_MAX_RETRIES, onIdleTimeout }) => {
    let active = false;
    /** WA Web `$4`: a request went out and no offline stanza has landed since */
    let requestInFlight = false;
    /** WA Web `$5`: the next request is wanted and waits out the debounce or the backlog */
    let requestWaiting = false;
    /** WA Web `$10`: when the last request of this resume went out */
    let lastRequestAt;
    let retries = 0;
    /** zapo 8be4b0b8: pins a request's outcome to the resume that sent it */
    let resume = 0;
    /** pins a request's failure to the latest request, so an older one failing late cannot re-request */
    let requestSeq = 0;
    let backlog = () => 0;
    let stanzaCheckTimer;
    let debounceTimer;
    let idleTimer;
    const clearTimers = () => {
        clearTimeout(stanzaCheckTimer);
        stanzaCheckTimer = undefined;
        clearTimeout(debounceTimer);
        debounceTimer = undefined;
        clearTimeout(idleTimer);
        idleTimer = undefined;
    };
    const stop = () => {
        active = false;
        requestInFlight = false;
        requestWaiting = false;
        clearTimers();
    };
    /** WA Web shifts its 60 s stanza timer on every offline stanza and every processed message */
    const armIdleTimer = () => {
        clearTimeout(idleTimer);
        idleTimer = setTimeout(() => {
            idleTimer = undefined;
            logger.warn({ idleTimeoutMs }, 'offline delivery went idle, no longer requesting offline batches');
            stop();
            onIdleTimeout?.();
        }, idleTimeoutMs);
    };
    /** monotonic: wall-clock corrections (NTP, VM / WSL clock steps) must not stall the debounce */
    const now = () => performance.now();
    const armDebounce = (delay = debounceMs) => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            debounceTimer = undefined;
            check();
        }, delay);
    };
    const request = async () => {
        clearTimeout(debounceTimer);
        debounceTimer = undefined;
        requestInFlight = true;
        requestWaiting = false;
        lastRequestAt = now();
        const requestResume = resume;
        const seq = ++requestSeq;
        logger.debug({ count: batchSize, backlog: backlog() }, 'requesting offline batch');
        try {
            await sendNode({
                tag: 'ib',
                attrs: {},
                content: [{ tag: 'offline_batch', attrs: { count: batchSize.toString() } }]
            });
            if (requestResume === resume) {
                retries = 0;
            }
        }
        catch (error) {
            if (requestResume !== resume || !active || seq !== requestSeq) {
                return;
            }
            retries += 1;
            if (retries > maxRetries) {
                // stays "in flight": only a stanza from the server (or the idle timeout) moves the loop on
                logger.warn({ error, retries }, 'offline batch request kept failing, giving up');
                return;
            }
            // nothing was asked for, so no stanza will land to schedule the next request
            requestInFlight = false;
            logger.debug({ error, retries }, 'offline batch request failed, retrying');
            requestWaiting = true;
            armDebounce();
        }
    };
    /** WA Web WAWebOfflineHandler `_maybeRequestMoreStanzaDebounced` */
    const check = () => {
        if (!active) {
            return;
        }
        // WA Web asks for more only while the unprocessed backlog fits in one window
        const backlogFits = backlog() <= batchSize;
        // WA Web skipDebouncing: a waiting request goes out once the gap since the last one has passed
        const due = requestWaiting &&
            !requestInFlight &&
            backlogFits &&
            lastRequestAt !== undefined &&
            now() - lastRequestAt >= debounceMs;
        if (((requestInFlight || requestWaiting) && !due) || !backlogFits) {
            // keep a waiting request covered by a timer: its debounce timer can fire a hair before the gap
            // has passed (whole-ms timers), and no stanza or processed node may come to check again
            if (requestWaiting && !requestInFlight && backlogFits && !debounceTimer && lastRequestAt !== undefined) {
                armDebounce(Math.max(1, Math.ceil(debounceMs - (now() - lastRequestAt))));
            }
            return;
        }
        requestWaiting = true;
        // the first request of a resume goes out at once, later ones wait out the debounce
        if (due || lastRequestAt === undefined) {
            void request();
        }
        else {
            armDebounce();
        }
    };
    return {
        /** `<ib><offline_preview/>`: a resume starts; its first request waits for its first offline stanza */
        start() {
            clearTimers();
            resume += 1;
            active = true;
            requestInFlight = false;
            requestWaiting = false;
            lastRequestAt = undefined;
            retries = 0;
            armIdleTimer();
        },
        /** a stanza carrying `offline` reached the handlers: the last request is being served */
        onOfflineStanza() {
            if (!active) {
                return;
            }
            requestInFlight = false;
            armIdleTimer();
            // WA Web `_endBatchCheck`: one check per burst, once the burst is queued for processing
            if (!stanzaCheckTimer) {
                stanzaCheckTimer = setTimeout(() => {
                    stanzaCheckTimer = undefined;
                    check();
                }, 0);
            }
        },
        /** a queued offline stanza was processed; the backlog may fit a window again */
        onBacklogProcessed() {
            if (!active) {
                return;
            }
            armIdleTimer();
            check();
        },
        /** count of offline stanzas received but not processed yet */
        setBacklogCounter(counter) {
            backlog = counter;
        },
        /** `<ib><offline/>` or connection end */
        stop,
        isActive: () => active
    };
};
//# sourceMappingURL=offline-batch-requester.js.map