import type { ILogger } from './logger.js';
import { type BinaryNode } from '../WABinary/index.js';
import type NodeCache from '@cacheable/node-cache';
export interface IdentityChangeOptions {
    meId?: string;
    meLid?: string;
    validateSession: (jid: string) => Promise<{
        exists: boolean;
    }>;
    assertSessions: (jids: string[], force?: boolean) => Promise<boolean>;
    debounceCache: NodeCache<boolean>;
    logger: ILogger;
}
export type IdentityChangeAction = 'no_identity_node' | 'skipped_offline' | 'skipped_companion' | 'skipped_self' | 'debounced' | 'no_session' | 'session_refreshed';
export declare const handleIdentityChange: (node: BinaryNode, { meId, meLid, validateSession, assertSessions, debounceCache, logger }: IdentityChangeOptions) => Promise<{
    action: IdentityChangeAction;
}>;
//# sourceMappingURL=identity-change-handler.d.ts.map