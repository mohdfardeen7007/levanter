import type { USyncQueryProtocol } from '../../Types/USync.js';
import { type BinaryNode } from '../../WABinary/index.js';
import { USyncUser } from '../USyncUser.js';
export type USyncContactLookupResult = {
    type?: string;
    username?: string;
    error?: {
        code: number;
        text?: string;
    };
};
/** WA Web WAWebUsyncContact for username lookups: LID addressing, non-throwing parser */
export declare class USyncContactLookupProtocol implements USyncQueryProtocol {
    name: string;
    private readonly contact;
    getQueryElement(): BinaryNode;
    getUserElement(user: USyncUser): BinaryNode | null;
    parser(node: BinaryNode): USyncContactLookupResult;
}
//# sourceMappingURL=USyncContactLookupProtocol.d.ts.map