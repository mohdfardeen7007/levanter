import { proto } from '../../../WAProto/index.js';
import { getBinaryNodeChild } from '../../WABinary/index.js';
/** WA Web WAWebUsyncBusiness: `<business><verified_name/></business>` */
export class USyncBusinessProtocol {
    constructor() {
        this.name = 'business';
    }
    getQueryElement() {
        return { tag: 'business', attrs: {}, content: [{ tag: 'verified_name', attrs: {} }] };
    }
    getUserElement() {
        return null;
    }
    parser(node) {
        const error = getBinaryNodeChild(node, 'error');
        if (error) {
            return { error: { code: +error.attrs.code, text: error.attrs.text } };
        }
        // WA Web WAWebCommonParsersVerifiedName: the name comes from the certificate details
        const verifiedName = getBinaryNodeChild(node, 'verified_name');
        if (verifiedName?.content instanceof Uint8Array) {
            try {
                const cert = proto.VerifiedNameCertificate.decode(verifiedName.content);
                const details = proto.VerifiedNameCertificate.Details.decode(cert.details);
                return details.verifiedName ? { verifiedName: details.verifiedName } : {};
            }
            catch {
                return {};
            }
        }
        return {};
    }
}
//# sourceMappingURL=USyncBusinessProtocol.js.map