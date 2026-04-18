'use strict';

const fs   = require('fs');
const path = require('path');

const CONFIG_PATH = path.join(__dirname, '..', 'pipeline-config.json');

const DEFAULTS = {
  prospeo: {
    keys:               [],
    cooldownMs:         60000,   // pause key 60s on 429
    dailyLimit:         0,       // 0 = unlimited
    requestsPerSecond:  5,       // starter plan; raise for higher plans
    enabled:            true,
    bulkSize:           50,      // contacts per bulk call (Prospeo max)
  },
  hunter: {
    keys:               [],
    cooldownMs:         60000,
    dailyLimit:         0,
    requestsPerSecond:  10,
    enabled:            true,
  },
  bouncer: {
    keys:               [],
    cooldownMs:         10000,   // 10s cooldown on 429
    dailyLimit:         0,
    requestsPerSecond:  16,      // 1000 req/min ≈ 16/s
    enabled:            true,
  },
  pipeline: {
    maxRetries:           3,
    skipIfEmailExists:    true,   // skip rows that already have enrichment_email
    acceptRiskyEmails:    true,   // treat Bouncer "risky" as valid
  },
};

function loadConfig() {
  if (!fs.existsSync(CONFIG_PATH)) {
    return JSON.parse(JSON.stringify(DEFAULTS));
  }
  try {
    const raw = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    return {
      prospeo:  { ...DEFAULTS.prospeo,  ...(raw.prospeo  || {}) },
      hunter:   { ...DEFAULTS.hunter,   ...(raw.hunter   || {}) },
      bouncer:  { ...DEFAULTS.bouncer,  ...(raw.bouncer  || {}) },
      pipeline: { ...DEFAULTS.pipeline, ...(raw.pipeline || {}) },
    };
  } catch (e) {
    console.error('[PipelineConfig] Failed to load pipeline-config.json:', e.message);
    return JSON.parse(JSON.stringify(DEFAULTS));
  }
}

function saveConfig(config) {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
}

module.exports = { loadConfig, saveConfig, CONFIG_PATH, DEFAULTS };
