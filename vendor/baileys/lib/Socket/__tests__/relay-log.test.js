import { jest } from '@jest/globals';
import { Boom } from '@hapi/boom';
const mockSendNode = jest.fn();
const mockLidMapping = {
    getLIDForPN: jest.fn().mockResolvedValue(null),
    getLIDsForPNs: jest.fn().mockResolvedValue([]),
    getPNForLID: jest.fn().mockResolvedValue(null),
};
const mockSignalRepository = {
    lidMapping: mockLidMapping,
    jidToSignalProtocolAddress: jest.fn().mockImplementation((jid) => jid),
    encryptMessage: jest.fn().mockResolvedValue({ type: 'msg', ciphertext: Buffer.from('abc') }),
    encryptGroupMessage: jest.fn().mockResolvedValue({ ciphertext: Buffer.from('abc'), senderKeyDistributionMessage: Buffer.from('def') }),
    validateSessions: jest.fn().mockResolvedValue({}),
};
jest.unstable_mockModule('../newsletter', () => {
    return {
        makeNewsletterSocket: jest.fn().mockImplementation(() => {
            return {
                ev: { emit: jest.fn(), on: jest.fn(), off: jest.fn() },
                authState: {
                    creds: {
                        me: { id: 'me@s.whatsapp.net', lid: 'me@lid' },
                        account: { details: Buffer.from('abc'), signature: Buffer.from('def') }
                    },
                    keys: { transaction: jest.fn((cb) => cb()), get: jest.fn().mockResolvedValue({}), set: jest.fn().mockResolvedValue({}) }
                },
                signalRepository: mockSignalRepository,
                sendNode: mockSendNode,
                query: jest.fn().mockResolvedValue({ tag: 'iq', attrs: {}, content: [] }),
                executeUSyncQuery: jest.fn().mockResolvedValue({
                    list: [
                        { id: '123@s.whatsapp.net', devices: { deviceList: [{ id: 0 }] } }
                    ],
                    side_list: []
                }),
                patchMessageBeforeSending: jest.fn((m) => m),
                serverProps: { lidTrustedTokenIssueToLid: false },
                setHistorySyncBlobProcessedHandler: jest.fn(),
            };
        })
    };
});
describe('relayMessage Logging', () => {
    let makeMessagesSocket;
    let logger;
    beforeAll(async () => {
        const mod = await import('../messages-send.js');
        makeMessagesSocket = mod.makeMessagesSocket;
    });
    beforeEach(() => {
        process.env.BAILEYS_RELAY_LOGGING = 'true';
        jest.clearAllMocks();
        // Reset the mock with default implementation
        mockSignalRepository.encryptMessage.mockResolvedValue({ type: 'msg', ciphertext: Buffer.from('abc') });
        mockSignalRepository.encryptGroupMessage.mockResolvedValue({ ciphertext: Buffer.from('abc'), senderKeyDistributionMessage: Buffer.from('def') });
        mockSendNode.mockResolvedValue(undefined);
        logger = {
            error: jest.fn(),
            debug: jest.fn(),
            trace: jest.fn(),
            warn: jest.fn(),
            info: jest.fn(),
        };
    });
    it('should log timeout if send takes > 30s', async () => {
        jest.useFakeTimers();
        const sock = makeMessagesSocket({ logger, patchMessageBeforeSending: jest.fn((m) => m) });
        // Mock sendNode to never resolve
        mockSendNode.mockReturnValue(new Promise(() => { }));
        sock.relayMessage('123@s.whatsapp.net', { conversation: 'hello' }, {}).catch(() => { });
        // Allow microtasks to run
        await Promise.resolve();
        await Promise.resolve();
        // Advance time by 31 seconds
        jest.advanceTimersByTime(31000);
        expect(logger.error).toHaveBeenCalledWith(expect.objectContaining({
            status: 'timeout',
            steps: expect.arrayContaining([
                expect.objectContaining({ name: 'jid_decode_and_lid_mapping' }),
            ])
        }), 'relayMessage timeout');
        jest.useRealTimers();
    });
    it('should log failed if encryption fails', async () => {
        const sock = makeMessagesSocket({ logger, patchMessageBeforeSending: jest.fn((m) => m) });
        // Mock encryption failure
        mockSignalRepository.encryptMessage.mockRejectedValue(new Error('encryption failed'));
        // It should throw because creating participant nodes fails all encryptions
        await expect(sock.relayMessage('123@s.whatsapp.net', { conversation: 'hello' }, {}))
            .rejects.toThrow();
        expect(logger.error).toHaveBeenCalledWith(expect.objectContaining({
            status: 'failed',
            steps: expect.arrayContaining([
                expect.objectContaining({ name: 'jid_decode_and_lid_mapping' }),
                expect.objectContaining({ name: 'usync_devices_fetch' }),
            ])
        }), 'relayMessage failed');
    });
    it('should NOT log if send completes quickly', async () => {
        jest.useFakeTimers();
        const sock = makeMessagesSocket({ logger, patchMessageBeforeSending: jest.fn((m) => m) });
        await sock.relayMessage('123@s.whatsapp.net', { conversation: 'hello' }, {});
        // Advance time and check
        jest.advanceTimersByTime(31000);
        expect(logger.error).not.toHaveBeenCalledWith(expect.anything(), 'relayMessage timeout');
        expect(logger.error).not.toHaveBeenCalledWith(expect.anything(), 'relayMessage failed');
        jest.useRealTimers();
    });
    it('should NOT log even on failure if BAILEYS_RELAY_LOGGING is not set', async () => {
        process.env.BAILEYS_RELAY_LOGGING = 'false';
        const sock = makeMessagesSocket({ logger, patchMessageBeforeSending: jest.fn((m) => m) });
        mockSignalRepository.encryptMessage.mockRejectedValue(new Error('encryption failed'));
        await expect(sock.relayMessage('123@s.whatsapp.net', { conversation: 'hello' }, {}))
            .rejects.toThrow();
        expect(logger.error).not.toHaveBeenCalledWith(expect.anything(), 'relayMessage failed');
        expect(logger.error).not.toHaveBeenCalledWith(expect.anything(), 'relayMessage timeout');
    });
});
//# sourceMappingURL=relay-log.test.js.map