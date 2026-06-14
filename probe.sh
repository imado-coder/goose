#!/bin/bash
# Raw Binance WS probe — determines exactly what streams arrive at this IP.
set +e
LOG=/var/log/atlas.log
echo "probe_started $(date -u)" > "$LOG"

export DEBIAN_FRONTEND=noninteractive
apt-get update -y >> "$LOG" 2>&1 </dev/null
apt-get install -y python3 python3-pip nginx >> "$LOG" 2>&1 </dev/null

rm -f /etc/nginx/sites-enabled/default
cat > /etc/nginx/sites-available/atlas << 'NGINXEOF'
server {
    listen 80 default_server;
    server_name _;
    location = /log { alias /var/log/atlas.log; default_type text/plain; }
    location = / { default_type text/plain; return 200 'probe — see /log\n'; }
}
NGINXEOF
ln -sf /etc/nginx/sites-available/atlas /etc/nginx/sites-enabled/atlas
nginx -t >> "$LOG" 2>&1 && systemctl restart nginx >> "$LOG" 2>&1

pip3 install --quiet websockets >> "$LOG" 2>&1 </dev/null

echo "=== PROBE A: combined stream (aggTrade + depth + forceOrder), 20s ===" >> "$LOG"
python3 - >> "$LOG" 2>&1 << 'PYEOF'
import asyncio, json, collections, websockets, time
async def probe(url, secs, label):
    streams = collections.Counter()
    etypes = collections.Counter()
    n = 0
    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10, max_size=2**22) as ws:
            end = time.time() + secs
            while time.time() < end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                m = json.loads(raw)
                streams[m.get("stream", "NO_STREAM_FIELD")] += 1
                data = m.get("data", m)
                etypes[data.get("e", "NO_E")] += 1
                n += 1
    except Exception as e:
        print(f"[{label}] ERROR:", repr(e))
    print(f"[{label}] total_msgs={n}")
    print(f"[{label}] streams={dict(streams)}")
    print(f"[{label}] event_types={dict(etypes)}")

async def main():
    base = "wss://fstream.binance.com"
    await probe(base + "/stream?streams=btcusdt@aggTrade/btcusdt@depth@100ms/btcusdt@forceOrder", 20, "COMBINED")
    await probe(base + "/ws/btcusdt@aggTrade", 15, "AGGTRADE_ONLY")
    await probe(base + "/ws/btcusdt@trade", 15, "TRADE_ONLY")

asyncio.run(main())
PYEOF
echo "=== PROBE DONE $(date -u) ===" >> "$LOG"
