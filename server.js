const ModbusRTU = require("modbus-serial");
const WebSocket = require("ws");

const client = new ModbusRTU();
const wss = new WebSocket.Server({ port: 8080 });

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function decodeTotalizer(r) {
  return r[0] + (r[1] * 65536 + r[2]) / 1_000_000_000;
}

function decodeFlowRate(r) {
  const buf = Buffer.alloc(4);
  buf.writeUInt16BE(r[0], 0);
  buf.writeUInt16BE(r[1], 2);
  const floatBE = buf.readFloatBE(0);
  if (!isNaN(floatBE) && isFinite(floatBE) && Math.abs(floatBE) < 1e6)
    return floatBE;
  return decodeTotalizer(r);
}

async function readAll() {
  const fr = await client.readHoldingRegisters(3003, 2);
  await delay(300);
  const t1 = await client.readHoldingRegisters(3018, 4);
  await delay(300);
  const t2 = await client.readHoldingRegisters(3022, 4);
  await delay(300);
  const t3 = await client.readHoldingRegisters(3026, 4);

  // DEBUG — tanggalon ni after
  console.log("T2 raw:", t2.data);
  console.log("T3 raw:", t3.data);
  console.log("T1 raw:", t1.data);
  console.log("T2 raw:", t2.data);
  console.log("T3 raw:", t3.data);
  return {
    flowRate: decodeFlowRate(fr.data),
    t1: decodeTotalizer(t1.data),
    t2: decodeTotalizer(t2.data),
    t3: decodeTotalizer(t3.data),
    timestamp: new Date().toISOString(),
  };
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

function decodeTotalizer(r) {
  const fracRaw = r[1] * 65536 + r[2];

  // r[3] = 0 → normal positive
  if (r[3] === 0) {
    return r[0] + fracRaw / 1_000_000_000;
  }

  // r[3] = 65535 (0xFFFF) → two's complement, integer part = 0
  if (r[3] === 65535) {
    const fracComplement = 4_294_967_296 - fracRaw;
    return r[0] + fracComplement / 1_000_000_000;
  }

  // r[3] = 1 → carry, integer is complement of r[0]
  if (r[3] === 1) {
    const intPart = 65536 - r[0];
    const fracComplement = 4_294_967_296 - fracRaw;
    return intPart + fracComplement / 1_000_000_000;
  }

  // fallback
  return r[0] + fracRaw / 1_000_000_000;
}
async function startReading() {
  while (true) {
    try {
      const data = await readAll();
      console.log(
        `[${new Date(data.timestamp).toLocaleTimeString()}]` +
          ` Flow: ${data.flowRate.toFixed(3)} m³/h` +
          ` | T1: ${data.t1.toFixed(3)}` +
          ` | T2: ${data.t2.toFixed(3)}` +
          ` | T3: ${data.t3.toFixed(3)} m³`,
      );
      broadcast({ type: "data", ...data });
    } catch (err) {
      console.error("Read error:", err.message);
      broadcast({ type: "error", message: err.message });
      try {
        await client.connectTCP("192.168.0.2", { port: 502 });
        client.setID(1);
        console.log("✅ Reconnected!");
      } catch (e) {
        console.error("Reconnect failed:", e.message);
      }
    }
    await delay(5000);
  }
}

wss.on("connection", (ws) => {
  console.log("📡 Dashboard connected");
  ws.on("close", () => console.log("📡 Dashboard disconnected"));
});

async function start() {
  try {
    await client.connectTCP("192.168.0.2", { port: 502 });
    client.setID(1);
    console.log("✅ Modbus connected to 192.168.0.2:502");
    console.log("🌐 WebSocket server on ws://localhost:8080");
    startReading();
  } catch (err) {
    console.error("❌ Connection failed:", err.message);
    process.exit(1);
  }
}

start();
