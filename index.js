const ModbusRTU = require("modbus-serial");
const WebSocket = require("ws");
const express = require("express");
const cors = require("cors");
const Database = require("better-sqlite3");
const path = require("path");

const client = new ModbusRTU();
const wss = new WebSocket.Server({ port: 8080 });
const app = express();
app.use(cors());

// SQLite setup
const db = new Database(path.join(__dirname, "flowmeter.db"));
db.exec(`
  CREATE TABLE IF NOT EXISTS flow_data (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_rate REAL,
    t1        REAL,
    t2        REAL,
    t3        REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE INDEX IF NOT EXISTS idx_timestamp ON flow_data(timestamp);
`);

const insert = db.prepare(`
  INSERT INTO flow_data (flow_rate, t1, t2, t3, timestamp)
  VALUES (@flowRate, @t1, @t2, @t3, @timestamp)
`);

const cleanup = db.prepare(`
  DELETE FROM flow_data 
  WHERE timestamp < datetime('now', '-30 days')
`);

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function decodeTotalizer(r) {
  const fracRaw = r[1] * 65536 + r[2];
  if (r[3] === 0) return r[0] + fracRaw / 1_000_000_000;
  if (r[3] === 65535) {
    const fracComplement = 4_294_967_296 - fracRaw;
    return r[0] + fracComplement / 1_000_000_000;
  }
  if (r[3] === 1) {
    const intPart = 65536 - r[0];
    const fracComplement = 4_294_967_296 - fracRaw;
    return intPart + fracComplement / 1_000_000_000;
  }
  return r[0] + fracRaw / 1_000_000_000;
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

// History API
app.get("/api/history", (req, res) => {
  const range = req.query.range || "1h";
  const ranges = {
    "1h":  "-1 hours",
    "4h":  "-4 hours",
    "1d":  "-1 days",
    "7d":  "-7 days",
    "14d": "-14 days",
    "30d": "-30 days",
  };
  const limits = {
    "1h":  72,
    "4h":  96,
    "1d":  144,
    "7d":  168,
    "14d": 196,
    "30d": 180,
  };
  const interval = ranges[range] || "-1 hours";
  const rows = db.prepare(`
    SELECT flow_rate, t1, t2, t3, timestamp
    FROM flow_data
    WHERE timestamp >= datetime('now', ?)
    ORDER BY timestamp ASC
    LIMIT ?
  `).all(interval, limits[range] || 144);
  res.json(rows);
});

// Serve static files
app.use(express.static(__dirname));

app.listen(3000, () => {
  console.log("🌐 Dashboard: http://localhost:3000/dashboard.html");
  console.log("📡 API: http://localhost:3000/api/history?range=1h");
});

async function startReading() {
  setInterval(() => {
    cleanup.run();
    console.log("🗑️ Cleanup done");
  }, 3600000);

  while (true) {
    try {
      const data = await readAll();
      insert.run({
        flowRate: data.flowRate,
        t1: data.t1,
        t2: data.t2,
        t3: data.t3,
        timestamp: data.timestamp,
      });
      console.log(
        `[${new Date(data.timestamp).toLocaleTimeString()}]` +
        ` Flow: ${data.flowRate.toFixed(3)} m³/h` +
        ` | T1: ${data.t1.toFixed(3)}` +
        ` | T2: ${data.t2.toFixed(3)}` +
        ` | T3: ${data.t3.toFixed(3)} m³`
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