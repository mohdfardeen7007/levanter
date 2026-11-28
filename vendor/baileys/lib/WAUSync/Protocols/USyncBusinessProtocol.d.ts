import type { USyncQueryProtocol } from '../../Types/USync.js';
import { type BinaryNode } from '../../WABinary/index.js';
export type USyncBusinessResult = {
    verifiedName?: string;
    error?: {
        code: number;
        text?: string;
    };
};
/** WA Web WAWebUsyncBusiness: `<business><verified_name/></business>` */
export declare class USyncBusinessProtocol implements USyncQueryProtocol {
    name: string;
    getQueryElement(): BinaryNode;
    getUserElement(): null;
    parser(node: BinaryNode): USyncBusinessResult;
}
//# sourceMappingURL=USyncBusinessProtocol.d.ts.map