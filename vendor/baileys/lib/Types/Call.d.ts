export type WACallUpdateType = 'offer' | 'ringing' | 'preaccept' | 'transport' | 'relaylatency' | 'timeout' | 'reject' | 'accept' | 'terminate';
export type WACallEvent = {
    chatId: string;
    from: string;
    callerPn?: string;
    isGroup?: boolean;
    groupJid?: string;
    id: string;
    date: Date;
    isVideo?: boolean;
    status: WACallUpdateType;
    /**
     * raw `<terminate reason>`, e.g. `accepted_elsewhere` / `rejected_elsewhere` (answered or declined on another
     * device of this account), `timeout`, `group_call_ended`; WA Web WAWebVoipActionWebHandleIncomingSignalingMessage
     */
    reason?: string;
    offline: boolean;
    latencyMs?: number;
};
//# sourceMappingURL=Call.d.ts.map