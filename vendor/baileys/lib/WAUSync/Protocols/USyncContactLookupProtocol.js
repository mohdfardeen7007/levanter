import { getBinaryNodeChild } from '../../WABinary/index.js';
import { USyncUser } from '../USyncUser.js';
import { USyncContactProtocol } from './USyncContactProtocol.js';
/** WA Web WAWebUsyncContact for username lookups: LID addressing, non-throwing parser */
export class USyncContactLookupProtocol {
    constructor() {
        this.name = 'contact';
        this.contact = new USyncContactProtocol('lid');
    }
    getQueryElement() {
        return this.contact.getQueryElement();
    }
    getUserElement(user) {
        return this.contact.getUserElement(user);
    }
    parser(node) {
        const error = getBinaryNodeChild(node, 'error');
        if (error) {
            return { error: { code: +error.attrs.code, text: error.attrs.text } };
        }
        return { type: node.attrs.type, ...(node.attrs.username ? { username: node.attrs.username } : {}) };
    }
}
//# sourceMappingURL=USyncContactLookupProtocol.js.map