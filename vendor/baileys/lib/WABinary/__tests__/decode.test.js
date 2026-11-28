import * as constants from '../constants.js';
import { decodeDecompressedBinaryNode } from '../decode.js';
describe('BinaryNode Decoding', () => {
    it('should use a shared EMPTY_ATTRIBUTES object for nodes with no attributes', () => {
        // Minimum node: List of 1 item (the tag "message")
        // [TAGS.LIST_8, 1, token_for_message]
        const messageTokenIndex = constants.SINGLE_BYTE_TOKENS.indexOf('message');
        const buffer = Buffer.from([constants.TAGS.LIST_8, 1, messageTokenIndex]);
        const node1 = decodeDecompressedBinaryNode(buffer, constants);
        const node2 = decodeDecompressedBinaryNode(buffer, constants);
        expect(node1.tag).toBe('message');
        expect(node1.attrs).toEqual({});
        expect(node2.attrs).toEqual({});
        // Verify object identity (optimization check)
        expect(node1.attrs).toBe(node2.attrs);
    });
    it('should throw error when trying to mutate EMPTY_ATTRIBUTES', () => {
        const messageTokenIndex = constants.SINGLE_BYTE_TOKENS.indexOf('message');
        const buffer = Buffer.from([constants.TAGS.LIST_8, 1, messageTokenIndex]);
        const node = decodeDecompressedBinaryNode(buffer, constants);
        expect(() => {
            node.attrs.test = 'value';
        }).toThrow();
    });
    it('should correctly decode nodes with attributes and NOT use EMPTY_ATTRIBUTES', () => {
        // Node: message { id: "123" }
        // List of 3 items (tag + 1 attribute pair)
        // [TAGS.LIST_8, 3, tag_token, key_token, val_token]
        const messageToken = constants.SINGLE_BYTE_TOKENS.indexOf('message');
        const idToken = constants.SINGLE_BYTE_TOKENS.indexOf('id');
        // Let's use BINARY_8 for the value "123"
        const buffer = Buffer.from([
            constants.TAGS.LIST_8, 3,
            messageToken,
            idToken,
            constants.TAGS.BINARY_8, 3, ...Buffer.from('123')
        ]);
        const node = decodeDecompressedBinaryNode(buffer, constants);
        expect(node.tag).toBe('message');
        expect(node.attrs).toEqual({ id: '123' });
        expect(node.attrs).not.toBe(decodeDecompressedBinaryNode(Buffer.from([constants.TAGS.LIST_8, 1, messageToken]), constants).attrs);
    });
});
//# sourceMappingURL=decode.test.js.map