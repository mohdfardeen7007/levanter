/**
 * Builds an ACK stanza for a received node.
 * Pure function -- no I/O, no side effects.
 *
 * Mirrors WhatsApp Web's ACK construction:
 * - WAWebHandleMsgSendAck.sendAck / sendNack
 * - WAWebCreateNackFromStanza.createNackFromStanza
 * - WAWebReceiptAck.buildReceiptAck, WAWebHandleMessageRetryRequest (receipt acks)
 */
export function buildAckStanza(node, errorCode, meId) {
    const { tag, attrs } = node;
    const stanza = {
        tag: 'ack',
        attrs: {
            id: attrs.id,
            to: attrs.from,
            class: tag
        }
    };
    if (errorCode) {
        stanza.attrs.error = errorCode.toString();
    }
    if (attrs.participant) {
        stanza.attrs.participant = attrs.participant;
    }
    // `recipient` is never echoed: WA Web puts it on no ack of an incoming stanza (WAWebReceiptAck,
    // WAWebHandleMessageRetryRequest, WAWebHandleMsgSendAck, WAWebCreateNackFromStanza). whatsmeow copies it.
    // WA Web always includes type when present: `n.type || DROP_ATTR`
    if (attrs.type) {
        stanza.attrs.type = attrs.type;
    }
    // Upstream Baileys behaviour, kept: message-class acks carry from=meId. WA Web
    // WAWebHandleMsgSendAck.sendAck/sendNack send no `from` (id, class, to, participant, type, error only).
    if (tag === 'message' && meId) {
        stanza.attrs.from = meId;
    }
    return stanza;
}
//# sourceMappingURL=stanza-ack.js.map