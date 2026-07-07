// Neuron estimation from token usage
// cfut_ tokens can't read CF's GraphQL neuron analytics, so we estimate
// Rates from CF pricing (verified 2026-07-06)
// https://developers.cloudflare.com/workers-ai/platform/pricing/

export const NEURON_FREE_DAILY = 10000;

// neurons per 1,000,000 tokens
export const RATES = {
  '@cf/meta/llama-3.2-1b-instruct': { in: 2457, out: 18252 },
  '@cf/meta/llama-3.2-3b-instruct': { in: 4625, out: 30475 },
  '@cf/meta/llama-3.1-8b-instruct-fp8-fast': { in: 4119, out: 34868 },
  '@cf/meta/llama-3.1-8b-instruct-awq': { in: 4119, out: 34868 },
  '@cf/meta/llama-3.2-11b-vision-instruct': { in: 4410, out: 61493 },
  '@cf/meta/llama-3.1-70b-instruct-fp8-fast': { in: 26668, out: 204805 },
  '@cf/meta/llama-3.3-70b-instruct-fp8-fast': { in: 26668, out: 204805 },
  '@cf/deepseek-ai/deepseek-r1-distill-qwen-32b': { in: 45170, out: 443756 },
  '@cf/mistral/mistral-7b-instruct-v0.1': { in: 10000, out: 17300 },
  '@cf/mistralai/mistral-small-3.1-24b-instruct': { in: 31876, out: 50488 },
  '@cf/qwen/qwen2.5-coder-32b-instruct': { in: 60000, out: 90909 },
  // Kimi + GLM models (CF hasn't published rates — use 70b-class fallback)
  '@cf/moonshotai/kimi-k2.7-code': { in: 26668, out: 204805 },
  '@cf/moonshotai/kimi-k2.6': { in: 26668, out: 204805 },
  '@cf/zai-org/glm-5.2': { in: 26668, out: 204805 },
};

export const DEFAULT_RATE = { in: 26668, out: 204805 }; // 70b-class

const warned = new Set();

export function estimate(model, promptTokens = 0, completionTokens = 0, onFallback) {
  let rate = RATES[model];
  if (!rate) {
    rate = DEFAULT_RATE;
    if (onFallback && !warned.has(model)) {
      warned.add(model);
      onFallback(`neuron rate for "${model}" not in table — using 70b-class fallback`);
    }
  }
  return (promptTokens / 1e6) * rate.in + (completionTokens / 1e6) * rate.out;
}

export function todayUTC() {
  return new Date().toISOString().slice(0, 10);
}
