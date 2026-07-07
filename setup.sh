#!/bin/bash
set -e

echo "⚡ CF Workers AI Gateway — Quick Setup"
echo "======================================"
echo ""

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Install: https://nodejs.org/"
    exit 1
fi
echo "✓ Node.js $(node -v)"

# Install deps
echo ""
echo "📦 Installing dependencies..."
npm install --production

# Create .env if not exists
if [ ! -f .env ]; then
    API_KEY=$(openssl rand -hex 16 2>/dev/null || head -c 16 /dev/urandom | xxd -p)
    cat > .env << EOF
CF_GATEWAY_HOST=0.0.0.0
CF_GATEWAY_PORT=8750
CF_GATEWAY_API_KEY=cfgw_${API_KEY}
CF_GATEWAY_DB=./data/accounts.db
CF_GATEWAY_COOLDOWN_429=90
CF_GATEWAY_MAX_RETRIES=50
CF_GATEWAY_LOG_LEVEL=INFO
EOF
    echo "✓ Created .env with auto-generated API key"
    echo "  API Key: cfgw_${API_KEY}"
    echo ""
    echo "  ⚠️  Save this key! Clients need it to access the gateway."
else
    echo "✓ .env already exists"
fi

# Create data dir
mkdir -p data
echo "✓ Data directory ready"

echo ""
echo "======================================"
echo "Setup complete! Next steps:"
echo ""
echo "1. Add your CF accounts (pick one method):"
echo ""
echo "   a) Via API (single account):"
echo "      curl -X POST http://localhost:8750/api/accounts \\"
echo "        -H 'Authorization: Bearer YOUR_API_KEY' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"name\":\"my-account\",\"account_id\":\"YOUR_CF_ACCOUNT_ID\",\"api_key\":\"YOUR_CF_API_TOKEN\"}'"
echo ""
echo "   b) Via API (bulk import):"
echo "      curl -X POST http://localhost:8750/api/accounts/bulk \\"
echo "        -H 'Authorization: Bearer YOUR_API_KEY' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"accounts\":[{\"name\":\"acc1\",\"account_id\":\"ID1\",\"api_key\":\"KEY1\"}]}'"
echo ""
echo "   c) From 9router database:"
echo "      curl -X POST http://localhost:8750/api/import \\"
echo "        -H 'Authorization: Bearer YOUR_API_KEY'"
echo ""
echo "2. Start the gateway:"
echo "      npm start"
echo ""
echo "3. Test it:"
echo "      curl http://localhost:8750/v1/chat/completions \\"
echo "        -H 'Authorization: Bearer YOUR_API_KEY' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"model\":\"@cf/zai-org/glm-5.2\",\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}]}'"
echo ""
echo "4. Open dashboard: http://localhost:8750"
echo "======================================"
