# ⚡ CF Workers AI Gateway

Lean Cloudflare Workers AI proxy with **account pool rotation**, **neuron tracking**, and **automatic failover**.

Pool 1001+ CF accounts → single OpenAI-compatible endpoint → auto-skip exhausted/rate-limited accounts.

## Features

- **OpenAI-compatible API** — drop-in replacement for `/v1/chat/completions`, `/v1/embeddings`
- **Account pool rotation** — round-robin across N accounts with automatic failover
- **Neuron tracking** — per-account daily usage tracking (10K neurons/account/day free tier)
- **Auto-skip** — exhausted and rate-limited accounts are automatically bypassed
- **429 retry** — rate-limited accounts get cooldown, request retries on next account
- **Streaming support** — SSE streaming works out of the box
- **Web dashboard** — real-time pool status, account list, request logs
- **Multi-user API keys** — share the gateway without exposing CF accounts

## Quick Start

```bash
# Clone
git clone <your-repo> cf-gateway
cd cf-gateway

# Setup
./setup.sh

# Add your CF accounts
curl -X POST http://localhost:8750/api/accounts \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-account","account_id":"YOUR_CF_ACCOUNT_ID","api_key":"YOUR_CF_API_TOKEN"}'

# Start
npm start
```

## How It Works

```
Client Request
    │
    ▼
┌─────────────────────────┐
│   CF Gateway :8750      │
│   (this project)        │
├─────────────────────────┤
│ 1. Authenticate API key │
│ 2. Pick available account│
│ 3. Forward to CF API    │
│ 4. Track neuron usage   │
│ 5. Retry on 429         │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│   Account Pool (SQLite) │
│   N accounts rotating   │
│   - Available           │
│   - Cooldown (60s)      │
│   - Exhausted (10K/day) │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│   Cloudflare Workers AI │
│   api.cloudflare.com    │
│   /client/v4/accounts/  │
│   {id}/ai/...           │
└─────────────────────────┘
```

## Configuration

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|----------|---------|-------------|
| `CF_GATEWAY_HOST` | `0.0.0.0` | Bind address |
| `CF_GATEWAY_PORT` | `8750` | Server port |
| `CF_GATEWAY_API_KEY` | (required) | Bearer token for client auth |
| `CF_GATEWAY_DB` | `./data/accounts.db` | SQLite database path |
| `CF_GATEWAY_COOLDOWN_429` | `90` | Seconds to cooldown after rate limit |
| `CF_GATEWAY_MAX_RETRIES` | `50` | Max account retries per request |
| `CF_GATEWAY_LOG_LEVEL` | `INFO` | Log level (INFO/WARN/ERROR) |

## API Endpoints

### Proxy (OpenAI-compatible)

```bash
# Chat completions
POST /v1/chat/completions

# Embeddings
POST /v1/embeddings

# CF passthrough
POST /ai/run/{model}
```

### Admin (requires API key)

```bash
# Pool stats
GET /api/stats

# List accounts
GET /api/accounts

# Add single account
POST /api/accounts
{
  "name": "my-account",
  "account_id": "YOUR_CF_ACCOUNT_ID",
  "api_key": "YOUR_CF_API_TOKEN"
}

# Bulk import accounts
POST /api/accounts/bulk
{
  "accounts": [
    {"name": "acc1", "account_id": "ID1", "api_key": "KEY1"},
    {"name": "acc2", "account_id": "ID2", "api_key": "KEY2"}
  ]
}

# Delete account
DELETE /api/accounts/{id}

# Import from 9router DB
POST /api/import

# Request logs
GET /api/logs
DELETE /api/logs
```

### Health

```bash
GET /health
```

## Adding Accounts

### Method 1: Single account via API

```bash
curl -X POST http://localhost:8750/api/accounts \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "my-worker",
    "account_id": "abc123def456...",
    "api_key": "cf_api_token_xxx..."
  }'
```

### Method 2: Bulk import via API

```bash
curl -X POST http://localhost:8750/api/accounts/bulk \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "accounts": [
      {"name": "worker-1", "account_id": "ID1", "api_key": "KEY1"},
      {"name": "worker-2", "account_id": "ID2", "api_key": "KEY2"}
    ]
  }'
```

### Method 3: From 9router database

If you have [9router](https://github.com/...) installed with CF accounts:

```bash
# Set 9router DB path in .env
CF_GATEWAY_9ROUTER_DB=/path/to/.9router/db/data.sqlite

# Import
curl -X POST http://localhost:8750/api/import \
  -H 'Authorization: Bearer YOUR_API_KEY'
```

## Getting Your CF Account ID & API Token

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Navigate to **Workers & Pages** → **AI** → **Workers AI**
3. Your **Account ID** is in the URL: `dash.cloudflare.com/{ACCOUNT_ID}/...`
4. Create an **API Token**:
   - Go to **My Profile** → **API Tokens** → **Create Token**
   - Use **Workers AI** template or create custom with `Workers AI: Read` permission

## Usage Examples

### curl

```bash
curl http://localhost:8750/v1/chat/completions \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "@cf/zai-org/glm-5.2",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

### Python (openai SDK)

```python
from openai import OpenAI

client = OpenAI(
    api_key="YOUR_API_KEY",
    base_url="http://localhost:8750/v1"
)

response = client.chat.completions.create(
    model="@cf/zai-org/glm-5.2",
    messages=[{"role": "user", "content": "Hello!"}]
)
print(response.choices[0].message.content)
```

### JavaScript (fetch)

```javascript
const response = await fetch("http://localhost:8750/v1/chat/completions", {
  method: "POST",
  headers: {
    "Authorization": "Bearer YOUR_API_KEY",
    "Content-Type": "application/json"
  },
  body: JSON.stringify({
    model: "@cf/zai-org/glm-5.2",
    messages: [{ role: "user", content: "Hello!" }]
  })
});
const data = await response.json();
```

### Streaming

```bash
curl http://localhost:8750/v1/chat/completions \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "@cf/zai-org/glm-5.2",
    "messages": [{"role": "user", "content": "Tell me a story"}],
    "stream": true
  }'
```

## Supported Models

| Model | ID |
|-------|-----|
| GLM 5.2 | `@cf/zai-org/glm-5.2` |
| Kimi K2.7 Code | `@cf/moonshotai/kimi-k2.7-code` |
| Kimi K2.6 | `@cf/moonshotai/kimi-k2.6` |
| Llama 3.3 70B | `@cf/meta/llama-3.3-70b-instruct-fp8-fast` |
| Llama 3.1 8B | `@cf/meta/llama-3.1-8b-instruct-fp8-fast` |
| Mistral Small 3.1 | `@cf/mistralai/mistral-small-3.1-24b-instruct` |
| Llama 3.2 3B | `@cf/meta/llama-3.2-3b-instruct` |
| Llama 3.2 1B | `@cf/meta/llama-3.2-1b-instruct` |

Full list: [Cloudflare Workers AI Models](https://developers.cloudflare.com/workers-ai/models/)

## Architecture

```
├── server.js          # Express server, routes, auth
├── lib/
│   ├── pool.js        # Account pool rotation + neuron tracking
│   ├── db.js          # SQLite schema + init
│   ├── cf.js          # Cloudflare API client (normal + streaming)
│   ├── neurons.js     # Neuron estimation from token usage
│   ├── log.js         # In-memory request log ring buffer
│   └── importer.js    # 9router DB importer
├── public/
│   └── index.html     # Web dashboard
├── data/
│   └── accounts.db    # SQLite database (auto-created)
├── .env.example       # Config template
├── setup.sh           # Quick setup script
└── package.json
```

## Neuron Budget

Each CF account gets **10,000 neurons/day** free. The gateway tracks usage per account and auto-skips exhausted accounts (resets 00:00 UTC).

Approximate costs per 1K tokens:

| Model | Input | Output |
|-------|-------|--------|
| GLM 5.2 / Kimi / 70B | ~27 neurons | ~205 neurons |
| Llama 3.1 8B | ~4 neurons | ~35 neurons |
| Llama 3.2 1B | ~2.5 neurons | ~18 neurons |

With 100 accounts × 10K neurons = **1M neurons/day** total budget.

## License

MIT
