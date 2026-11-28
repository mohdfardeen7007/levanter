import { Boom } from '@hapi/boom';
import { isValidUsernameKey, normalizeUsername, parseUsernameHandle } from '../Utils/username.js';
import { getAllBinaryNodeChildren, getBinaryNodeChild, isLidUser, isPnUser } from '../WABinary/index.js';
import { USyncBotProfileProtocol } from './Protocols/UsyncBotProfileProtocol.js';
import { USyncLIDProtocol } from './Protocols/UsyncLIDProtocol.js';
import { USyncBusinessProtocol, USyncContactLookupProtocol, USyncContactProtocol, USyncDeviceProtocol, USyncDisappearingModeProtocol, USyncStatusProtocol, USyncUsernameProtocol } from './Protocols/index.js';
import { USyncUser } from './USyncUser.js';
export class USyncQuery {
    constructor() {
        this.protocols = [];
        this.users = [];
        this.context = 'interactive';
        this.mode = 'query';
    }
    withMode(mode) {
        this.mode = mode;
        return this;
    }
    withContext(context) {
        this.context = context;
        return this;
    }
    withUser(user) {
        this.users.push(user);
        return this;
    }
    parseUSyncQueryResult(result) {
        if (result?.attrs.type !== 'result') {
            return;
        }
        // a Map, so a child tag named like an Object.prototype member is never mistaken for a parser
        const protocolMap = new Map(this.protocols.map(protocol => [protocol.name, protocol.parser]));
        const queryResult = {
            // TODO: implement errors etc.
            list: [],
            sideList: []
        };
        const usyncNode = getBinaryNodeChild(result, 'usync');
        //TODO: implement error backoff, refresh etc.
        const listNode = usyncNode ? getBinaryNodeChild(usyncNode, 'list') : undefined;
        const parseUser = (node) => Array.isArray(node?.content)
            ? Object.fromEntries(node.content
                .map(content => {
                const protocol = content.tag;
                const parser = protocolMap.get(protocol);
                if (parser) {
                    return [protocol, parser(content)];
                }
                else {
                    return [protocol, null];
                }
            })
                .filter(([, b]) => b !== null))
            : {};
        if (listNode?.content && Array.isArray(listNode.content)) {
            queryResult.list = listNode.content.reduce((acc, node) => {
                // whatsmeow d8960d95: numbers that are not on WhatsApp come back with pn_jid only
                const id = node?.attrs.jid || node?.attrs.pn_jid;
                if (id) {
                    acc.push({ ...parseUser(node), id, ...(node.attrs.pn_jid ? { pnJid: node.attrs.pn_jid } : {}) });
                }
                return acc;
            }, []);
        }
        // WA Web WAWebUsync: per-protocol errors under <result><protocol><error/></protocol></result>
        const resultNode = usyncNode ? getBinaryNodeChild(usyncNode, 'result') : undefined;
        if (resultNode) {
            for (const protocolNode of getAllBinaryNodeChildren(resultNode)) {
                const error = getBinaryNodeChild(protocolNode, 'error');
                if (error) {
                    queryResult.errors = {
                        ...queryResult.errors,
                        [protocolNode.tag]: { code: +error.attrs.code, text: error.attrs.text }
                    };
                }
            }
        }
        // WA Web keeps a <user> without jid when it carries <contact> (username lookup: key required)
        if (listNode?.content && Array.isArray(listNode.content)) {
            const jidless = listNode.content.filter(node => !node.attrs.jid && !node.attrs.pn_jid && getBinaryNodeChild(node, 'contact'));
            if (jidless.length) {
                queryResult.jidless = jidless.map(node => ({ ...parseUser(node), id: '' }));
            }
        }
        //TODO: implement side list
        //const sideListNode = getBinaryNodeChild(usyncNode, 'side_list')
        return queryResult;
    }
    withDeviceProtocol() {
        this.protocols.push(new USyncDeviceProtocol());
        return this;
    }
    withContactProtocol(addressingMode) {
        this.protocols.push(new USyncContactProtocol(addressingMode));
        return this;
    }
    withStatusProtocol() {
        this.protocols.push(new USyncStatusProtocol());
        return this;
    }
    withDisappearingModeProtocol() {
        this.protocols.push(new USyncDisappearingModeProtocol());
        return this;
    }
    withBotProfileProtocol() {
        this.protocols.push(new USyncBotProfileProtocol());
        return this;
    }
    withLIDProtocol() {
        this.protocols.push(new USyncLIDProtocol());
        return this;
    }
    withUsernameProtocol() {
        this.protocols.push(new USyncUsernameProtocol());
        return this;
    }
    /** contact protocol for username lookups (LID addressing, errors reported instead of thrown) */
    withContactLookupProtocol() {
        this.protocols.push(new USyncContactLookupProtocol());
        return this;
    }
    withBusinessProtocol() {
        this.protocols.push(new USyncBusinessProtocol());
        return this;
    }
}
/**
 * Maps an onWhatsApp usync result (whatsmeow 6e4c3416): `jid` stays the phone number for
 * API compatibility, `lid` carries the LID when the server returned one, and LID↔PN pairs
 * are returned so the caller can store them. Unregistered numbers are left out as before.
 */
export const toOnWhatsAppResults = (list) => {
    const results = [];
    const mappings = [];
    for (const entry of list) {
        if (!entry.contact) {
            continue;
        }
        const pnJid = typeof entry.pnJid === 'string' ? entry.pnJid : undefined;
        const isLid = isLidUser(entry.id);
        results.push({ jid: isLid && pnJid ? pnJid : entry.id, exists: true, ...(isLid ? { lid: entry.id } : {}) });
        if (isLid && pnJid && isPnUser(pnJid)) {
            mappings.push({ lid: entry.id, pn: pnJid });
        }
    }
    return { results, mappings };
};
/**
 * Username lookup query as WA Web builds it (WAWebQueryExistsJob.queryUsernameExists, zapo 471ca18a):
 * contact (LID addressing) + business, one user carrying the username and the optional key as `pin`.
 * Returns undefined for a handle or key WA Web would not look up (useWAWebDebouncedUsernameLookup).
 */
export const buildUsernameLookupQuery = (handle, usernameKey) => {
    const parsed = parseUsernameHandle(handle);
    const key = usernameKey ?? parsed?.usernameKey;
    if (!parsed || (key !== undefined && !isValidUsernameKey(key))) {
        return undefined;
    }
    const user = new USyncUser().withUsername(parsed.username);
    if (key) {
        user.withUsernameKey(key);
    }
    return new USyncQuery().withContactLookupProtocol().withBusinessProtocol().withUser(user);
};
/** WA Web WAWebQueryExistsJob.queryUsernameExists, applied to a parsed usync result */
export const toUsernameLookupResult = (result) => {
    const error = result.errors?.all ?? result.errors?.contact;
    if (error) {
        throw new Boom(error.text || 'username lookup failed', {
            statusCode: Number.isSafeInteger(error.code) && error.code >= 400 ? error.code : 500,
            data: error
        });
    }
    const entries = [...result.list, ...(result.jidless ?? [])];
    if (entries.length !== 1) {
        return { status: 'not-found' };
    }
    const [entry] = entries;
    const contact = entry.contact;
    if (contact?.type === 'out') {
        return { status: 'not-found' };
    }
    const username = normalizeUsername(contact?.username);
    // WA Web takes the id from `jid` only: a user answered with pn_jid alone has none
    const jid = entry.id && entry.id !== entry.pnJid ? entry.id : undefined;
    if (jid) {
        const business = entry.business;
        return {
            status: 'found',
            jid,
            ...(username ? { username } : {}),
            isBusiness: !!business,
            ...(business?.verifiedName ? { verifiedName: business.verifiedName } : {})
        };
    }
    return { status: 'key-required', ...(username ? { username } : {}) };
};
//# sourceMappingURL=USyncQuery.js.map