export class BenchmarkProbe {
    private latencies: number[] = [];
    private start: number = 0;
    private currentIndex = 0;

    constructor(private name: string, private totalOps: number) {
        this.latencies = new Array(totalOps);
    }

    startTimer() { this.start = performance.now(); }

    record(val: number) {
        if (this.currentIndex < this.totalOps) {
            this.latencies[this.currentIndex++] = val;
        }
    }

    // Per misurare latenza End-to-End (da timestamp nel payload)
    recordLatency(startMs: number) {
        this.record(Date.now() - startMs);
    }

    printResult() {
        const durationSec = (performance.now() - this.start) / 1000;
        const throughput = Math.floor(this.totalOps / durationSec);

        const validLatencies = this.latencies.slice(0, this.currentIndex).sort((a, b) => a - b);
        const count = validLatencies.length;

        const p50 = validLatencies[Math.floor(count * 0.50)] || 0;
        const p99 = validLatencies[Math.floor(count * 0.99)] || 0;
        const max = validLatencies[count - 1] || 0;

        console.log(`\n\x1b[36m[${this.name}]\x1b[0m`);
        console.log(` 🚀 Throughput:  \x1b[32m${throughput.toLocaleString()} ops/sec\x1b[0m`);

        if (count > 0) {
            const colorMax = max > 100 ? "\x1b[31m" : "\x1b[33m";
            console.log(` ⏱️  Latency:     p50: ${p50.toFixed(2)}ms | p99: ${p99.toFixed(2)}ms | ${colorMax}MAX: ${max.toFixed(2)}ms\x1b[0m (samples: ${count})`);
        } else {
            console.log(` ⏱️  Latency:     (no samples recorded)`);
        }

        return { throughput, p99, max };
    }
}
