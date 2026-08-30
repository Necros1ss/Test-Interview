const { spawn } = require('child_process');
const path = require('path');

console.log('🚀 Launching 6 Microservices for Techlab E-commerce System...');

const services = [
  { name: 'API Gateway', script: 'services/api-gateway/index.js', ready: false },
  { name: 'Order Service', script: 'services/order-service/index.js', ready: false },
  { name: 'Payment Service', script: 'services/payment-service/index.js', ready: false },
  { name: 'Inventory Service', script: 'services/inventory-service/index.js', ready: false },
  { name: 'Notification Service', script: 'services/notification-service/index.js', ready: false },
  { name: 'Analytics Service', script: 'services/analytics-service/index.js', ready: false }
];

const processes = [];

services.forEach((s) => {
  const p = spawn('node', [s.script], { cwd: __dirname, stdio: 'pipe' });

  p.stdout.on('data', (data) => {
    const text = data.toString().trim();
    console.log(`[${s.name}] ${text}`);
    if (text.includes('Listening') || text.includes('ready')) {
      s.ready = true;
      checkAllReady();
    }
  });

  p.stderr.on('data', (data) => {
    const text = data.toString().trim();
    console.error(`[${s.name} ERR] ${text}`);
    if (text.includes('Fallback')) {
      s.ready = true;
      checkAllReady();
    }
  });

  processes.push(p);
});

let startedBenchmark = false;
function checkAllReady() {
  if (startedBenchmark) return;
  const allReady = services.every((s) => s.ready);
  if (allReady) {
    startedBenchmark = true;
    console.log('\n✅ All Microservices fully ready! Launching Benchmark Suite in 1 sec...\n');

    setTimeout(() => {
      const benchmarkProc = spawn('node', ['benchmark/load-test.js', '--mode=both'], { cwd: __dirname, stdio: 'inherit' });

      benchmarkProc.on('close', (code) => {
        console.log(`\n🎉 Benchmark completed with code ${code}. Cleaning up services...`);
        processes.forEach((p) => p.kill());
        process.exit(code);
      });
    }, 1000);
  }
}
