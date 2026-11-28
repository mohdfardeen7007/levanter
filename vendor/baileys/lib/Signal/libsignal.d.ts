import type { LIDMapping, SignalAuthState } from '../Types/index.js';
import type { SignalRepositoryWithLIDStore } from '../Types/Signal.js';
import type { ILogger } from '../Utils/logger.js';
export declare function makeLibSignalRepository(auth: SignalAuthState, logger: ILogger, pnToLIDFunc?: (jids: string[]) => Promise<LIDMapping[] | undefined>): SignalRepositoryWithLIDStore;
/**
 * Inverse of the PN→LID address resolution: given a LID signal-address string
 * (`user_domainType.device`), return the PN signal-address string if a mapping
 * exists, else null. Used for lazy PN→LID migration on read — when a record is
 * looked up under the LID address but was persisted under the PN address before
 * the mapping was learned. Store-agnostic (no device-list / SQL LIKE needed).
 */
export declare const resolvePNForLIDAddress: (lidAddrStr: string, getPNForLID: (lid: string) => Promise<string | null>) => Promise<string | null>;
/**
 * Inverse resolver for sender-key names: given a LID-keyed name
 * (`group::user_domainType::device`), return the PN-keyed name if a mapping
 * exists, else null. Used for lazy group/status sender-key migration on read.
 */
export declare const resolvePNForLIDSenderKey: (lidKeyId: string, getPNForLID: (lid: string) => Promise<string | null>) => Promise<string | null>;
//# sourceMappingURL=libsignal.d.ts.map