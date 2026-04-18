'use strict';

/**
 * KeyPool — manages multiple API keys for a single service.
 *
 * Features:
 *  - True per-key token bucket (refills at requestsPerSecond rate)
 *  - 429 → cooldown for cooldownMs, then auto-reawaken
 *  - 402 → daily pause until next UTC midnight, then auto-reawaken
 *  - Round-robin with least-loaded preference
 *  - acquireKey() waits (never drops) if all keys temporarily on cooldown
 *  - throws AllKeysExhaustedError only when all keys are daily-paused / over daily limit
 */

class AllKeysExhaustedError extends Error {
  constructor(service) {
    super(`All keys for ${service} are daily-paused or permanently exhausted`);
    this.name = 'AllKeysExhaustedError';
    this.service = service;
  }
}

class KeyPool {
  constructor(serviceName, keys = [], options = {}) {
    this.serviceName = serviceName;
    this.cooldownMs = options.cooldownMs || 60000;
    this.dailyLimit = options.dailyLimit || 0;          // 0 = unlimited
    this.requestsPerSecond = options.requestsPerSecond || 5;

    this.keys = keys.filter(Boolean).map(k => ({
      key: k,
      cooldownUntil: 0,
      dailyPausedUntil: 0,
      dailyCount: 0,
      lastUsed: 0,
      tokens: options.requestsPerSecond || 5,   // token bucket
      lastRefill: Date.now(),
    }));

    // Schedule automatic daily reset at midnight UTC
    this._dailyResetTimer = null;
    if (this.keys.length > 0) this._scheduleDailyReset();
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  _msUntilMidnightUTC() {
    const now = new Date();
    const midnight = Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate() + 1  // next day
    );
    return midnight - Date.now();
  }

  _scheduleDailyReset() {
    if (this._dailyResetTimer) clearTimeout(this._dailyResetTimer);
    const ms = this._msUntilMidnightUTC();
    this._dailyResetTimer = setTimeout(() => {
      this._resetDaily();
      this._scheduleDailyReset();   // schedule next day
    }, ms);
    // Don't prevent process exit
    if (this._dailyResetTimer.unref) this._dailyResetTimer.unref();
  }

  _resetDaily() {
    for (const k of this.keys) {
      k.dailyCount = 0;
      k.dailyPausedUntil = 0;
      k.tokens = this.requestsPerSecond;
      k.lastRefill = Date.now();
    }
    console.log(`[KeyPool:${this.serviceName}] Daily reset — all keys re-enabled`);
  }

  _refillTokens(keyObj) {
    const now = Date.now();
    const elapsedSec = (now - keyObj.lastRefill) / 1000;
    keyObj.tokens = Math.min(
      this.requestsPerSecond,
      keyObj.tokens + elapsedSec * this.requestsPerSecond
    );
    keyObj.lastRefill = now;
  }

  _isAvailable(keyObj) {
    const now = Date.now();
    if (keyObj.cooldownUntil > now) return false;
    if (keyObj.dailyPausedUntil > now) return false;
    if (this.dailyLimit > 0 && keyObj.dailyCount >= this.dailyLimit) return false;
    return true;
  }

  _allDailyPaused() {
    const now = Date.now();
    return this.keys.every(k =>
      k.dailyPausedUntil > now ||
      (this.dailyLimit > 0 && k.dailyCount >= this.dailyLimit)
    );
  }

  _getBestAvailableKey() {
    let best = null;
    let bestTokens = -Infinity;

    for (const k of this.keys) {
      if (!this._isAvailable(k)) continue;
      this._refillTokens(k);
      // Prefer key with most tokens (most headroom)
      if (k.tokens > bestTokens) {
        bestTokens = k.tokens;
        best = k;
      }
    }
    return best;
  }

  _shortestCooldownMs() {
    const now = Date.now();
    let shortest = Infinity;
    for (const k of this.keys) {
      if (k.dailyPausedUntil > now) continue; // can't help us
      if (this.dailyLimit > 0 && k.dailyCount >= this.dailyLimit) continue;
      if (k.cooldownUntil > now) {
        shortest = Math.min(shortest, k.cooldownUntil - now);
      }
    }
    return shortest === Infinity ? null : shortest;
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Acquire a key. Waits if all keys are temporarily on cooldown.
   * Throws AllKeysExhaustedError if all keys are daily-paused or over daily limit.
   */
  async acquireKey() {
    if (this.keys.length === 0) throw new AllKeysExhaustedError(this.serviceName);

    while (true) {
      if (this._allDailyPaused()) {
        throw new AllKeysExhaustedError(this.serviceName);
      }

      const keyObj = this._getBestAvailableKey();

      if (keyObj) {
        this._refillTokens(keyObj);
        if (keyObj.tokens >= 1) {
          keyObj.tokens -= 1;
          keyObj.dailyCount++;
          keyObj.lastUsed = Date.now();
          return keyObj.key;
        }
        // Not enough tokens yet — wait a short time and retry
        const waitMs = Math.ceil((1 - keyObj.tokens) / this.requestsPerSecond * 1000);
        await new Promise(r => setTimeout(r, Math.min(waitMs, 100)));
        continue;
      }

      // All keys on cooldown — wait for the shortest cooldown
      const waitMs = this._shortestCooldownMs();
      if (waitMs === null) throw new AllKeysExhaustedError(this.serviceName);

      console.log(`[KeyPool:${this.serviceName}] All keys on cooldown — waiting ${Math.ceil(waitMs / 1000)}s`);
      await new Promise(r => setTimeout(r, waitMs + 100));
    }
  }

  /**
   * Call after every API response. Handles rate limit and quota signals.
   * @param {string} keyStr  - the raw key string that was used
   * @param {number} statusCode - HTTP status returned by the API
   */
  handleResponse(keyStr, statusCode) {
    if (statusCode === 429) {
      this.markCooldown(keyStr, this.cooldownMs);
    } else if (statusCode === 402) {
      this.markDailyPause(keyStr);
    }
  }

  markCooldown(keyStr, durationMs) {
    const k = this.keys.find(k => k.key === keyStr);
    if (!k) return;
    k.cooldownUntil = Date.now() + durationMs;
    k.tokens = 0; // drain tokens so it won't be picked immediately after cooldown
    const suffix = keyStr.slice(-6);
    console.log(`[KeyPool:${this.serviceName}] Key …${suffix} → cooldown ${Math.ceil(durationMs / 1000)}s`);
  }

  markDailyPause(keyStr) {
    const k = this.keys.find(k => k.key === keyStr);
    if (!k) return;
    k.dailyPausedUntil = Date.now() + this._msUntilMidnightUTC();
    k.tokens = 0;
    const suffix = keyStr.slice(-6);
    console.log(`[KeyPool:${this.serviceName}] Key …${suffix} → daily-paused until midnight UTC`);
  }

  /**
   * Returns a snapshot of key health for the dashboard.
   */
  getStats() {
    const now = Date.now();
    return {
      service: this.serviceName,
      total: this.keys.length,
      available: this.keys.filter(k => this._isAvailable(k)).length,
      onCooldown: this.keys.filter(k => k.cooldownUntil > now && k.dailyPausedUntil <= now).length,
      dailyPaused: this.keys.filter(k => k.dailyPausedUntil > now).length,
      totalRequestsToday: this.keys.reduce((s, k) => s + k.dailyCount, 0),
      keys: this.keys.map(k => ({
        suffix: '…' + k.key.slice(-6),
        available: this._isAvailable(k),
        tokens: Math.floor(this._refillTokens(k) || k.tokens),
        dailyCount: k.dailyCount,
        cooldownUntil: k.cooldownUntil > now ? new Date(k.cooldownUntil).toISOString() : null,
        dailyPausedUntil: k.dailyPausedUntil > now ? new Date(k.dailyPausedUntil).toISOString() : null,
      })),
    };
  }

  get keyCount() { return this.keys.length; }
}

module.exports = { KeyPool, AllKeysExhaustedError };
