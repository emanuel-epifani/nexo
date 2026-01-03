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

    describe('Nexo Protocol under load pressure', () => {

        it('Protocol -> Massive Payload (Force Buffer Resize & Fragmentation)', async () => {
            // 1. Genera un payload di 10MB (superiore al buffer di default che spesso è 64kb o 512kb)
            // Questo obbliga il RingDecoder a fare resize e il TCP a frammentare i dati in molti pacchetti.
            const size = 10 * 1024 * 1024;
            const largeString = "A".repeat(size);
            const payload = { data: largeString, meta: "huge-payload" };

            const start = Date.now();
            const response = await (nexo as any).debug.echo(payload);
            const duration = Date.now() - start;

            expect(response.data.length).toBe(size);
            expect(response.data).toBe(largeString);
            console.log(`[Stress] 10MB Payload Echo: ${duration}ms`);
        }, 30000); // Timeout aumentato per sicurezza

        it('Protocol -> High Throughput Pipelining (10k requests)', async () => {
            // 2. Invia 10.000 richieste in parallelo (senza attendere la precedente)
            // Questo stressa l'ID matching e il parsing di frame multipli in un singolo evento 'data'.
            const COUNT = 10000;
            const requests = Array.from({ length: COUNT }, (_, i) => ({
                id: i,
                val: `req-${i}`
            }));

            const start = Date.now();

            // Promise.all invia tutto "insieme" (nel limite del loop event di JS)
            // Il socket riceverà un fiume di byte.
            const results = await Promise.all(
                requests.map(data => (nexo as any).debug.echo(data))
            );

            const duration = Date.now() - start;
            const rps = Math.floor(COUNT / (duration / 1000));

            expect(results).toHaveLength(COUNT);
            expect(results[0]).toEqual(requests[0]);
            expect(results[COUNT - 1]).toEqual(requests[COUNT - 1]);

            console.log(`[Stress] 10k Requests: ${duration}ms (${rps} req/sec)`);
        }, 30000);

        it('Protocol -> Random Chaos (Mixed Sizes & Concurrent)', async () => {
            // 3. Caos totale: payload piccoli e grandi mischiati per rompere il buffering
            const COUNT = 1000;
            const requests = Array.from({ length: COUNT }, (_, i) => {
                // Random size da 1 byte a 100KB
                const size = Math.floor(Math.random() * 100000) + 1;
                return {
                    idx: i,
                    blob: Buffer.allocUnsafe(size).toString('hex') // Contenuto random
                };
            });

            const results = await Promise.all(
                requests.map(data => (nexo as any).debug.echo(data))
            );

            expect(results).toHaveLength(COUNT);
            // Verifica a campione per non rallentare troppo
            expect(results[50]).toEqual(requests[50]);
        }, 30000);

    });



});

