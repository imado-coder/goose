#!/bin/bash
# ============================================================
# ATLAS Production Server Setup
# Tested on: Ubuntu 22.04 LTS
# Usage: curl -fsSL <raw_url> | bash
# ============================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1"; exit 1; }

[[ $EUID -ne 0 ]] && err "Run as root: sudo bash server_setup.sh"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║    ATLAS Server Setup — Production       ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── 1. System Update ──────────────────────────────────────
log "Updating system packages..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get upgrade -y -qq
apt-get install -y -qq \
    curl wget git build-essential ca-certificates \
    gnupg lsb-release ufw fail2ban htop unzip jq \
    python3 python3-pip net-tools

# ── 2. Docker ─────────────────────────────────────────────
log "Installing Docker..."
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
else
    warn "Docker already installed: $(docker --version)"
fi

# ── 3. Docker Compose V2 ──────────────────────────────────
log "Verifying Docker Compose V2..."
docker compose version &>/dev/null || err "Docker Compose V2 not available"
log "Docker Compose: $(docker compose version --short)"

# ── 4. Git ────────────────────────────────────────────────
log "Git: $(git --version)"

# ── 5. Node.js LTS ────────────────────────────────────────
log "Installing Node.js LTS..."
if ! command -v node &>/dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - &>/dev/null
    apt-get install -y -qq nodejs
else
    warn "Node already installed: $(node --version)"
fi
log "Node: $(node --version) | npm: $(npm --version)"

# ── 6. UFW Firewall ───────────────────────────────────────
log "Configuring UFW firewall..."
ufw --force reset &>/dev/null
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp   comment 'SSH'
ufw allow 80/tcp   comment 'HTTP'
ufw allow 443/tcp  comment 'HTTPS'
ufw allow 9001/tcp comment 'ATLAS Health API'
ufw allow 3003/tcp comment 'Grafana Dashboard'
ufw --force enable
log "UFW enabled"

# ── 7. Fail2Ban (SSH brute-force protection) ──────────────
log "Configuring Fail2Ban..."
cat > /etc/fail2ban/jail.local << 'JAIL'
[DEFAULT]
bantime  = 3600
findtime = 600
maxretry = 5

[sshd]
enabled = true
port    = ssh
logpath = %(sshd_log)s
backend = %(sshd_backend)s
JAIL
systemctl enable fail2ban &>/dev/null
systemctl restart fail2ban
log "Fail2Ban active"

# ── 8. System Limits (for TimescaleDB performance) ────────
log "Setting system limits..."
cat >> /etc/sysctl.conf << 'SYSCTL'

# ATLAS Production Tuning
vm.swappiness = 10
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
fs.file-max = 1000000
SYSCTL
sysctl -p &>/dev/null

cat >> /etc/security/limits.conf << 'LIMITS'
* soft nofile 1000000
* hard nofile 1000000
root soft nofile 1000000
root hard nofile 1000000
LIMITS

# ── 9. Create swap (2GB) if none exists ───────────────────
if ! swapon --show | grep -q swap; then
    log "Creating 2GB swap..."
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile &>/dev/null
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
    log "Swap created"
else
    warn "Swap already exists"
fi

# ── 10. Deploy ATLAS ──────────────────────────────────────
echo ""
log "Running ATLAS Sprint-0 setup..."
curl -fsSL https://raw.githubusercontent.com/imado-coder/goose/claude/sprint-0-data-collection-077apd/atlas_setup.sh | bash

# ── Done ──────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║         Setup Complete ✓                 ║"
echo "╚══════════════════════════════════════════╝"
echo ""
echo "  Docker  : $(docker --version)"
echo "  Compose : $(docker compose version --short)"
echo "  Node    : $(node --version)"
echo "  Git     : $(git --version)"
echo ""
echo "  ATLAS Health : http://$(hostname -I | awk '{print $1}'):9001/health"
echo "  Grafana      : http://$(hostname -I | awk '{print $1}'):3003"
echo ""
log "Check: curl -s http://localhost:9001/health | python3 -m json.tool"
