import type { USyncQueryProtocol } from '../Types/USync.js';
import { type BinaryNode } from '../WABinary/index.js';
import { USyncUser } from './USyncUser.js';
export type USyncQueryResultList = {
    [protocol: string]: unknown;
    id: string;
};
export type USyncQueryResult = {
    list: USyncQueryResultList[];
    sideList: USyncQueryResultList[];
    /** users answered without jid / pn_jid but with <contact> (username lookup: key required); id is '' */
    jidless?: USyncQueryResultList[];
    /** per-protocol errors from <result><protocol><error/></protocol></result> (WA Web WAWebUsync) */
    errors?: Record<string, {
        code: number;
        text?: string;
    }>;
};
export declare class USyncQuery {
    protocols: USyncQueryProtocol[];
    users: USyncUser[];
    context: string;
    mode: string;
    constructor();
    withMode(mode: string): this;
    withContext(context: string): this;
    withUser(user: USyncUser): this;
    parseUSyncQueryResult(result: BinaryNode | undefined): USyncQueryResult | undefined;
    withDeviceProtocol(): this;
    withContactProtocol(addressingMode?: 'lid'): this;
    withStatusProtocol(): this;
    withDisappearingModeProtocol(): this;
    withBotProfileProtocol(): this;
    withLIDProtocol(): this;
    withUsernameProtocol(): this;
    /** contact protocol for username lookups (LID addressing, errors reported instead of thrown) */
    withContactLookupProtocol(): this;
    withBusinessProtocol(): this;
}
/**
 * Maps an onWhatsApp usync result (whatsmeow 6e4c3416): `jid` stays the phone number for
 * API compatibility, `lid` carries the LID when the server returned one, and LID↔PN pairs
 * are returned so the caller can store them. Unregistered numbers are left out as before.
 */
export declare const toOnWhatsAppResults: (list: USyncQueryResultList[]) => {
    results: {
        jid: string;
        exists: boolean;
        lid?: string;
    }[];
    mappings: {
        lid: string;
        pn: string;
    }[];
};
export type UsernameLookupResult = {
    status: 'found';
    jid: string;
    username?: string;
    isBusiness: boolean;
    verifiedName?: string;
} | {
    status: 'key-required';
    username?: string;
} | {
    status: 'not-found';
} | {
    status: 'invalid';
};
/**
 * Username lookup query as WA Web builds it (WAWebQueryExistsJob.queryUsernameExists, zapo 471ca18a):
 * contact (LID addressing) + business, one user carrying the username and the optional key as `pin`.
 * Returns undefined for a handle or key WA Web would not look up (useWAWebDebouncedUsernameLookup).
 */
export declare const buildUsernameLookupQuery: (handle: string, usernameKey?: string) => USyncQuery | undefined;
/** WA Web WAWebQueryExistsJob.queryUsernameExists, applied to a parsed usync result */
export declare const toUsernameLookupResult: (result: USyncQueryResult) => UsernameLookupResult;
//# sourceMappingURL=USyncQuery.d.ts.map