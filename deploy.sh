#!/bin/bash
# ATLAS v3 — full server deploy (nginx monitor on :80 + Docker + ATLAS stack)
# Run as root on a fresh Ubuntu 22.04 server.
set +e

LOG=/var/log/atlas.log
echo "deploy_started $(date -u)" > "$LOG"

export DEBIAN_FRONTEND=noninteractive
apt-get update -y >> "$LOG" 2>&1
apt-get install -y curl git nginx >> "$LOG" 2>&1

# ── nginx monitoring layer on port 80 ──────────────────────────────
rm -f /etc/nginx/sites-enabled/default
cat > /etc/nginx/sites-available/atlas << 'NGINXEOF'
server {
    listen 80 default_server;
    server_name _;

    location = /health {
        proxy_pass http://127.0.0.1:8000/health;
        proxy_connect_timeout 2s;
        proxy_read_timeout 4s;
        error_page 502 503 504 = @starting;
    }
    location @starting {
        default_type application/json;
        return 200 '{"status":"starting","msg":"ATLAS container not ready yet"}';
    }
    location = /log {
        alias /var/log/atlas.log;
        default_type text/plain;
    }
    location = / {
        default_type text/plain;
        return 200 'ATLAS deploy in progress — see /log and /health\n';
    }
}
NGINXEOF
ln -sf /etc/nginx/sites-available/atlas /etc/nginx/sites-enabled/atlas
nginx -t >> "$LOG" 2>&1 && systemctl restart nginx >> "$LOG" 2>&1
echo "nginx_ready $(date -u)" >> "$LOG"

# ── Docker ─────────────────────────────────────────────────────────
curl -fsSL https://get.docker.com | sh >> "$LOG" 2>&1
systemctl enable docker >> "$LOG" 2>&1
systemctl start docker >> "$LOG" 2>&1
echo "docker_ready $(date -u)" >> "$LOG"

# ── ATLAS stack ────────────────────────────────────────────────────
echo "atlas_setup_starting $(date -u)" >> "$LOG"
curl -fsSL https://raw.githubusercontent.com/imado-coder/goose/46561b9f9d93ed51134a12658f3a4ea5d6f09d3e/atlas_setup.sh 2>>"$LOG" | bash >> "$LOG" 2>&1
echo "deploy_done $(date -u)" >> "$LOG"
