import {nexo} from "../nexo";
import { describe, it, expect } from 'vitest';


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

});