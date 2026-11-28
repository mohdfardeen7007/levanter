import type { USyncQueryProtocol } from '../../Types/USync.js';
import { type BinaryNode } from '../../WABinary/index.js';
import { USyncUser } from '../USyncUser.js';
export declare class USyncContactProtocol implements USyncQueryProtocol {
    private readonly addressingMode?;
    name: string;
    /** whatsmeow 6e4c3416: `addressing_mode="lid"` makes the server answer with the LID + pn_jid pair */
    constructor(addressingMode?: "lid" | undefined);
    getQueryElement(): BinaryNode;
    getUserElement(user: USyncUser): BinaryNode;
    parser(node: BinaryNode): boolean;
}
//# sourceMappingURL=USyncContactProtocol.d.ts.map