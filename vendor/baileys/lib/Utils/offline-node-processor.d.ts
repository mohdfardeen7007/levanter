import type { BinaryNode } from '../WABinary/index.js';
export type MessageType = 'message' | 'call' | 'receipt' | 'notification' | 'status';
export type OfflineNodeProcessorDeps = {
    isWsOpen: () => boolean;
    onUnexpectedError: (error: Error, msg: string) => void;
    yieldToEventLoop: () => Promise<void>;
    /** WA Web WAWebOfflineHandler (zapo 8be4b0b8): a queued node finished, so the offline pull loop may ask for more */
    onNodeProcessed?: () => void;
};
/**
 * Creates a processor for offline stanza nodes that:
 * - Queues nodes for sequential processing
 * - Yields to the event loop periodically to avoid blocking
 * - Catches handler errors to prevent the processing loop from crashing
 */
export declare function makeOfflineNodeProcessor(nodeProcessorMap: Map<MessageType, (node: BinaryNode) => Promise<void>>, deps: OfflineNodeProcessorDeps, batchSize?: number): {
    enqueue: (type: MessageType, node: BinaryNode) => void;
    /** offline nodes received but not processed yet: the offline pull loop's backlog */
    pending: () => number;
};
//# sourceMappingURL=offline-node-processor.d.ts.map