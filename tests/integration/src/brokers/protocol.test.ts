import { describe, it, expect } from 'vitest';
import { nexo } from "../nexo";

const SERVER_PORT = parseInt(process.env.SERVER_PORT!);

describe('Nexo Protocol & Socket', () => {

    describe('Serialization & Data Types', () => {
        it('JSON Roundtrip - Complex nested object with special chars', async () => {
            const complexData = {
                string: "Nexo Engine",
                number: 42.5,
                boolean: true,
                nullValue: null,
                array: [1, "due", { tre: 3 }],
                nested: {
                    id: "abc-123",
                    tags: ["rust", "typescript", "fast"],
                    meta: { active: true, version: 0.2, deep: { value: "ok" } }
                },
                specialChars: "🚀🔥 €$ £",
                unicode: "こんにちは",
                longString: "A".repeat(1000),
                base64: Buffer.from("hello world").toString('base64')
            };

            const response = await (nexo as any).debug.echo(complexData);
            expect(response).toEqual(complexData);
        });

        it('JSON Roundtrip - Empty object', async () => {
            const result = await (nexo as any).debug.echo({});
            expect(result).toEqual({});
        });

        it('JSON Roundtrip - Null and undefined values', async () => {
            const result = await (nexo as any).debug.echo({ a: null, b: undefined });
            expect(result).toEqual({ a: null }); // undefined stripped by JSON
        });

        it('DataType - Empty string vs null distinction', async () => {
            await nexo.store.kv.set('proto:empty-str', '');
            await nexo.store.kv.set('proto:null-val', null);

            expect(await nexo.store.kv.get('proto:empty-str')).toBe('');
            expect(await nexo.store.kv.get('proto:null-val')).toBeNull();
        });

        it('DataType - Binary-like data via base64', async () => {
            const binaryData = Buffer.from([0x00, 0x01, 0xFF, 0xFE]).toString('base64');
            const result = await (nexo as any).debug.echo({ bin: binaryData });
            expect(result.bin).toBe(binaryData);
        });
    });

});
