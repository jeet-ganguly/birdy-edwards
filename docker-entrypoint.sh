#!/bin/bash
# ══════════════════════════════════════════════════════════════════════════════
# BIRDY-EDWARDS — Docker Entrypoint
# ══════════════════════════════════════════════════════════════════════════════

set -e

# Colors
RED='\033[0;31m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RESET='\033[0m'

echo ""
echo -e "${RED}██████╗ ██╗██████╗ ██████╗ ██╗   ██╗      ███████╗██████╗ ██╗    ██╗ █████╗ ██████╗ ██████╗ ███████╗${RESET}"
echo -e "${RED}██╔══██╗██║██╔══██╗██╔══██╗╚██╗ ██╔╝      ██╔════╝██╔══██╗██║    ██║██╔══██╗██╔══██╗██╔══██╗██╔════╝${RESET}"
echo -e "${RED}██████╔╝██║██████╔╝██║  ██║ ╚████╔╝ █████╗█████╗  ██║  ██║██║ █╗ ██║███████║██████╔╝██║  ██║███████╗${RESET}"
echo -e "${RED}██╔══██╗██║██╔══██╗██║  ██║  ╚██╔╝  ╚════╝██╔══╝  ██║  ██║██║███╗██║██╔══██║██╔══██╗██║  ██║╚════██║${RESET}"
echo -e "${RED}██████╔╝██║██║  ██║██████╔╝   ██║         ███████╗██████╔╝╚███╔███╔╝██║  ██║██║  ██║██████╔╝███████║${RESET}"
echo -e "${RED}╚═════╝ ╚═╝╚═╝  ╚═╝╚═════╝    ╚═╝         ╚══════╝╚═════╝  ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝${RESET}"
echo ""
echo -e "${YELLOW}                        Infiltrate & Expose — Setup v1.0${RESET}"
echo -e "${CYAN}                        Developed by Jeet Ganguly${RESET}"
echo ""


# ── Start Xvfb virtual display (required for SeleniumBase scraping) ──
echo "[1/4] Starting virtual display (Xvfb)..."
Xvfb :99 -screen 0 1280x900x24 -nolisten tcp &
XVFB_PID=$!
sleep 2
echo "      Xvfb started (PID $XVFB_PID)"

# ── Check Ollama connectivity ──
echo "[2/4] Checking Ollama connection..."
OLLAMA_URL="${OLLAMA_HOST:-http://host.docker.internal:11434}"
if curl -s --max-time 5 "${OLLAMA_URL}/api/tags" > /dev/null 2>&1; then
    echo "      Ollama reachable at ${OLLAMA_URL} ✓"
else
    echo "      WARNING: Ollama not reachable at ${OLLAMA_URL}"
    echo "      Make sure Ollama is running on your host machine"
    echo "      Run: ollama serve"
fi

# ── Ensure runtime directories exist ──
echo "[3/4] Checking runtime directories..."
mkdir -p /app/reports /app/face_data /app/post_screenshots /app/status
echo "      Directories ready ✓"

# ── Check cookie file ──
echo "[4/4] Checking session cookies..."
if [ -f /app/fb_cookies.pkl ]; then
    echo "      fb_cookies.pkl found ✓"
else
    echo "      WARNING: fb_cookies.pkl not found"
    echo "      Use the Import Session Cookies tool in the web UI"
    echo "      Go to: http://localhost:5000/tools/import-cookies"
fi

echo ""
echo "══════════════════════════════════════════════════════"
echo "  BIRDY-EDWARDS is running at http://localhost:5000"
echo "══════════════════════════════════════════════════════"
echo ""

# ── Start Flask app ──
exec /app/venv/bin/python /app/app.py