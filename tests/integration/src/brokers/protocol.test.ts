import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";

describe('Nexo Protocol & Socket', () => {

    it('Protocol -> Complex Object Roundtrip (JSON Serialization & Binary Framing)', async () => {
        const complexData = {
            string: "Nexo Engine",
            number: 42.5,
            boolean: true,
            nullValue: null,
            array: [1, "due", { tre: 3 }],
            nested: {
                id: "abc-123",
                tags: ["rust", "typescript", "fast"],
                meta: {
                    active: true,
                    version: 0.2,
                    nestedAgain: {
                        deep: "value"
                    }
                }
            },
            specialChars: "🚀🔥 €$ £",
            unicode: "こんにちは",
            longString: "A".repeat(1000), // Test larger payloads
            binarySim: Buffer.from("hello world").toString('base64') // JSON handles buffers as strings usually, but let's test typical JS usage
        };

        // Test the internal request/send mechanism via the examples namespace
        // This validates: 
        // 1. DataCodec (JSON.stringify)
        // 2. RequestBuilder (Binary framing, Opcode, FrameType, Lengths)
        // 3. Socket Transmission (TCP Fragmentation/Merging)
        // 4. Rust Router (Jump table, Payload parsing)
        // 5. Rust Response encoding
        // 6. RingDecoder (Frame extraction from stream)
        // 7. DataCodec (JSON.parse)
        const response = await (nexo as any).debug.echo(complexData);
        
        expect(response).toEqual(complexData);
    });

    it('Protocol -> Multiple rapid requests (Concurrency & Frame ID matching)', async () => {
        const requests = Array.from({ length: 50 }, (_, i) => ({
            id: i,
            token: Math.random().toString(36),
            timestamp: Date.now()
        }));

        const results = await Promise.all(
            requests.map(data => (nexo as any).debug.echo(data))
        );

        expect(results).toEqual(requests);
    });


});


