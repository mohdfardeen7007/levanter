import { assertNodeErrorFree } from '../../WABinary/index.js';
import { USyncUser } from '../USyncUser.js';
export class USyncContactProtocol {
    /** whatsmeow 6e4c3416: `addressing_mode="lid"` makes the server answer with the LID + pn_jid pair */
    constructor(addressingMode) {
        this.addressingMode = addressingMode;
        this.name = 'contact';
    }
    getQueryElement() {
        return {
            tag: 'contact',
            attrs: this.addressingMode ? { addressing_mode: this.addressingMode } : {}
        };
    }
    getUserElement(user) {
        if (user.phone) {
            return {
                tag: 'contact',
                attrs: {},
                content: user.phone
            };
        }
        if (user.username) {
            return {
                tag: 'contact',
                attrs: {
                    username: user.username,
                    ...(user.usernameKey ? { pin: user.usernameKey } : {}),
                    ...(user.lid ? { lid: user.lid } : {})
                }
            };
        }
        if (user.type) {
            return {
                tag: 'contact',
                attrs: {
                    type: user.type
                }
            };
        }
        return {
            tag: 'contact',
            attrs: {}
        };
    }
    parser(node) {
        if (node.tag === 'contact') {
            assertNodeErrorFree(node);
            return node?.attrs?.type === 'in';
        }
        return false;
    }
}
//# sourceMappingURL=USyncContactProtocol.js.map