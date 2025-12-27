import {nexo} from "../nexo";
import { describe, it, expect } from 'vitest';
import {NexoClient} from "../../../../sdk/ts";
import {performance} from "perf_hooks";


describe('Manual Debug', () => {
  it('should work new sdk client', async () => {
    const [, , emanuele, mario] = await Promise.all([
      nexo.kv.set("user:1", "Emanuele"),
      nexo.kv.set("user:2", "Mario"),
      nexo.kv.get("user:1"),
      nexo.kv.get("user:2")
    ]);
    console.log("3 promise risolte");
    // console.log(emanuele.toString());
    // console.log(mario.toString());
    console.log(emanuele!.toString());
    console.log(mario!.toString());

  });

  it('should measure GET/SET throughput for 1 second', async () => {
    // const nexo = await NexoClient.connect();

    let operations = 0;
    const startTime = Date.now();
    const duration = 1000; // 1 secondo
    const batchSize = 1000; // Inviamo a gruppi per non intasare l'event loop di Node

    console.log("🚀 Partenza benchmark...");

    while (Date.now() - startTime < duration) {
      const batch = [];
      for (let i = 0; i < batchSize; i++) {
        // Alterniamo SET e GET
        batch.push(nexo.kv.set(`bench:${operations + i}`, "payload"));
        // batch.push(nexo.kv.get(`bench:${operations + i}`));
      }

      await Promise.all(batch);
      operations += batchSize;
    }

    const totalTime = (Date.now() - startTime) / 1000;
    const opsPerSec = Math.floor(operations / totalTime);

    console.log(`\n--- RESULT ---`);
    console.log(`Total Operations: ${operations}`);
    console.log(`Throughput: ${opsPerSec} ops/sec`);
    console.log(`--------------\n`);

    await nexo.disconnect();
  }, 10000); // Timeout del test alzato a 10s

  it('should measure GET/SET throughput for 1 second', async () => {
    const CONCURRENCY = 1000;  // Numero di richieste "in volo" simultanee
    const DURATION_MS = 15000;  // Durata del test
    const PAYLOAD = "x".repeat(64); // Payload realistico (non troppo grande)

    console.log(`🚀 Avvio Benchmark: ${CONCURRENCY} concorrenza, ${DURATION_MS}ms...`);

    let operations = 0;
    let isRunning = true;
    const start = performance.now();

    // Funzione worker: Appena finisce una richiesta, ne lancia subito un'altra
    const worker = async () => {
      while (isRunning) {
        try {
          // Alternativa: nexo.kv.get(`key:${operations}`)
          await nexo.kv.set(`bench:${operations}`, PAYLOAD);
          operations++;
        } catch (e) {
          console.error("Error:", e);
        }
      }
    };

    // Avvia N workers in parallelo
    const workers = Array(CONCURRENCY).fill(null).map(() => worker());

    // Timer per fermare il test
    await new Promise(resolve => setTimeout(() => {
      isRunning = false;
      resolve(null);
    }, DURATION_MS));

    // Attendi che i worker finiscano l'ultima richiesta (opzionale, per pulizia)
    await Promise.all(workers);

    const totalTime = (performance.now() - start) / 1000;
    const opsPerSec = Math.floor(operations / totalTime);

    console.log(`\n--- RESULT ---`);
    console.log(`Total Operations: ${operations.toLocaleString()}`);
    console.log(`Throughput:       ${opsPerSec.toLocaleString()} ops/sec`);
    console.log(`Latency (avg):    ${(1000 / opsPerSec * CONCURRENCY).toFixed(3)} ms`);
    console.log(`--------------\n`);

    nexo.disconnect();
  }, 20000); // Timeout del test alzato a 10s

});