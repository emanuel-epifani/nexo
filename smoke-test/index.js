const { NexoClient } = require("@emanuelepifani/nexo-client");

async function run() {
  console.log("🚀 Starting NEXO Smoke Test...");

  try {
    const client = await NexoClient.connect({
      host: "127.0.0.1",
      port: 7654,
    });
    console.log("✅ Connected to NEXO Server");

    // 1. STORE
    console.log("\n📦 Testing STORE...");
    await client.store.map.set("smoke-key", "hello-world");
    const val = await client.store.map.get("smoke-key");
    if (val === "hello-world") console.log("   ✅ STORE OK");
    else console.error("   ❌ STORE FAILED", val);

    // 2. QUEUE
    console.log("\n📨 Testing QUEUE...");
    const q = await client.queue("smoke-queue").create();
    await q.push({ job: "test" });
    const sub = await q.subscribe(
      async (msg) => {
        console.log("   ✅ QUEUE Consumer received:", msg);
      },
      { batchSize: 1 },
    );
    // Wait a bit for consumption
    await new Promise((r) => setTimeout(r, 500));
    sub.stop();

    // 3. PUB/SUB
    console.log("\n🔄 Testing PUB/SUB...");
    let pubsubReceived = false;
    await client.pubsub("smoke/topic").subscribe((msg) => {
      console.log("   ✅ PUB/SUB received:", msg);
      pubsubReceived = true;
    });
    await client.pubsub("smoke/topic").publish({ data: "ping" });
    await new Promise((r) => setTimeout(r, 500));
    if (!pubsubReceived) console.error("   ❌ PUB/SUB FAILED");

    // 4. STREAM
    console.log("\n📜 Testing STREAM...");
    const stream = await client
      .stream("smoke-stream")
      .create({ partitions: 1 });
    await stream.publish({ event: "log" });

    let streamReceived = false;
    const sSub = await stream.subscribe("smoke-group", (msg) => {
      console.log("   ✅ STREAM received:", msg);
      streamReceived = true;
    });
    await new Promise((r) => setTimeout(r, 1000));
    sSub.stop();
    if (!streamReceived) console.error("   ❌ STREAM FAILED");

    console.log("\n🎉 Smoke Test Complete!");
    await client.disconnect();
    process.exit(0);
  } catch (err) {
    console.error("\n❌ FATAL ERROR:", err);
    process.exit(1);
  }
}

run();
