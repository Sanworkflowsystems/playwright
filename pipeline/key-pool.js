'use strict';

function ts() {
  return new Date().toTimeString().slice(0, 8);
}

const L = {
  info:  (msg) => console.log(`[${ts()}] INFO  ${msg}`),
  warn:  (msg) => console.warn(`[${ts()}] WARN  ${msg}`),
  error: (msg) => console.error(`[${ts()}] ERROR ${msg}`),
};

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
    this.dailyLimit = options.dailyLimit || 0;
    this.requestsPerSecond = options.requestsPerSecond || 5;
    // If a minuteLimit is explicitly set, secondLimit is derived from it (not rps),
    // so a service like Prospeo with 20/min but no per-second limit works correctly.
    this.minuteLimit = options.minuteLimit || Math.max(1, Math.floor(this.requestsPerSecond * 60));
    this.secondLimit = options.secondLimit || Math.max(1, Math.floor(this.requestsPerSecond));

    this.keys = keys.filter(Boolean).map(k => ({
      key: k,
      cooldownUntil: 0,
      dailyPausedUntil: 0,
      dailyCount: 0,
      lastUsed: 0,
      usageWindow: [], // [{ at, cost }] for the last 60 seconds
    }));

    this._dailyResetTimer = null;
    if (this.keys.length > 0) this._scheduleDailyReset();
  }

  _msUntilMidnightUTC() {
    const now = new Date();
    const midnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1);
    return midnight - Date.now();
  }

  _scheduleDailyReset() {
    if (this._dailyResetTimer) clearTimeout(this._dailyResetTimer);
    const ms = this._msUntilMidnightUTC();
    this._dailyResetTimer = setTimeout(() => {
      this._resetDaily();
      this._scheduleDailyReset();
    }, ms);
    if (this._dailyResetTimer.unref) this._dailyResetTimer.unref();
  }

  _resetDaily() {
    for (const k of this.keys) {
      k.dailyCount = 0;
      k.dailyPausedUntil = 0;
      k.usageWindow = [];
    }
    L.info(`[KeyPool:${this.serviceName}] Daily reset - all ${this.keys.length} keys re-enabled`);
  }

  _pruneUsage(keyObj) {
    const cutoff = Date.now() - 60000;
    keyObj.usageWindow = keyObj.usageWindow.filter(entry => entry.at > cutoff);
  }

  _usedInLastMinute(keyObj) {
    this._pruneUsage(keyObj);
    return keyObj.usageWindow.reduce((sum, entry) => sum + entry.cost, 0);
  }

  _usedInLastSecond(keyObj) {
    const cutoff = Date.now() - 1000;
    return keyObj.usageWindow
      .filter(entry => entry.at > cutoff)
      .reduce((sum, entry) => sum + entry.cost, 0);
  }

  _remainingDaily(keyObj) {
    if (this.dailyLimit <= 0) return Infinity;
    return Math.max(0, this.dailyLimit - keyObj.dailyCount);
  }

  _isUsableForCost(keyObj, cost) {
    const now = Date.now();
    if (keyObj.cooldownUntil > now) return false;
    if (keyObj.dailyPausedUntil > now) return false;
    if (this.dailyLimit > 0 && keyObj.dailyCount + cost > this.dailyLimit) return false;
    if (this._usedInLastSecond(keyObj) + cost > this.secondLimit) return false;
    return this._usedInLastMinute(keyObj) + cost <= this.minuteLimit;
  }

  _allDailyExhaustedForCost(cost) {
    const now = Date.now();
    return this.keys.every(k =>
      k.dailyPausedUntil > now ||
      (this.dailyLimit > 0 && k.dailyCount + cost > this.dailyLimit)
    );
  }

  _getBestAvailableKey(cost) {
    let best = null;
    let bestScore = -Infinity;

    for (const k of this.keys) {
      if (!this._isUsableForCost(k, cost)) continue;
      // Pick the key used least recently so load spreads evenly across all keys.
      // Tiebreak by most remaining minute capacity to avoid keys approaching their limit.
      const remainingMinute = this.minuteLimit - this._usedInLastMinute(k);
      const score = remainingMinute * 1e9 + (Date.now() - k.lastUsed);
      if (score > bestScore) {
        bestScore = score;
        best = k;
      }
    }

    return best;
  }

  _shortestWaitMsForCost(cost) {
    const now = Date.now();
    let shortest = Infinity;

    for (const k of this.keys) {
      if (k.dailyPausedUntil > now) continue;
      if (this.dailyLimit > 0 && k.dailyCount + cost > this.dailyLimit) continue;
      if (k.cooldownUntil > now) {
        shortest = Math.min(shortest, k.cooldownUntil - now);
        continue;
      }

      this._pruneUsage(k);
      const usedSecond = this._usedInLastSecond(k);
      let secondBlocking = false;
      if (usedSecond + cost > this.secondLimit) {
        secondBlocking = true;
        const recentSecondEntries = k.usageWindow
          .filter(entry => entry.at > now - 1000)
          .sort((a, b) => a.at - b.at);
        let projectedUsedSecond = usedSecond;
        for (const entry of recentSecondEntries) {
          projectedUsedSecond -= entry.cost;
          const waitMs = Math.max(1, entry.at + 1000 - now);
          if (projectedUsedSecond + cost <= this.secondLimit) {
            shortest = Math.min(shortest, waitMs);
            break;
          }
        }
      }

      const used = this._usedInLastMinute(k);
      // Only return 0 (immediately available) when BOTH limits allow it
      if (used + cost <= this.minuteLimit && !secondBlocking) return 0;

      const sorted = [...k.usageWindow].sort((a, b) => a.at - b.at);
      let projectedUsed = used;
      for (const entry of sorted) {
        projectedUsed -= entry.cost;
        const waitMs = Math.max(0, entry.at + 60000 - now);
        if (projectedUsed + cost <= this.minuteLimit) {
          shortest = Math.min(shortest, waitMs);
          break;
        }
      }
    }

    return shortest === Infinity ? null : shortest;
  }

  async acquireKey(cost = 1) {
    if (this.keys.length === 0) throw new AllKeysExhaustedError(this.serviceName);

    const quotaCost = Math.max(1, Math.ceil(Number(cost) || 1));
    if (quotaCost > this.minuteLimit) {
      throw new Error(`[KeyPool:${this.serviceName}] quota cost ${quotaCost} exceeds minute limit ${this.minuteLimit}`);
    }
    if (quotaCost > this.secondLimit) {
      throw new Error(`[KeyPool:${this.serviceName}] quota cost ${quotaCost} exceeds second limit ${this.secondLimit}`);
    }

    while (true) {
      if (this._allDailyExhaustedForCost(quotaCost)) {
        L.error(`[KeyPool:${this.serviceName}] All keys lack daily quota for cost ${quotaCost} - skipping this service for the rest of the run`);
        throw new AllKeysExhaustedError(this.serviceName);
      }

      const keyObj = this._getBestAvailableKey(quotaCost);
      if (keyObj) {
        keyObj.usageWindow.push({ at: Date.now(), cost: quotaCost });
        keyObj.dailyCount += quotaCost;
        keyObj.lastUsed = Date.now();
        return keyObj.key;
      }

      const waitMs = this._shortestWaitMsForCost(quotaCost);
      if (waitMs === null) throw new AllKeysExhaustedError(this.serviceName);

      // Throttle the log — many goroutines hit this simultaneously, producing spam.
      // Log only if we haven't logged a "waiting" message in the last 2 seconds.
      const now = Date.now();
      if (!this._lastWaitLogAt || now - this._lastWaitLogAt > 2000) {
        L.warn(`[KeyPool:${this.serviceName}] All keys at rate limit - waiting ${Math.ceil(waitMs / 1000)}s for next slot`);
        this._lastWaitLogAt = now;
      }
      await new Promise(r => setTimeout(r, waitMs + 100));
    }
  }

  handleResponse(keyStr, statusCode) {
    if (statusCode === 429) {
      this.markCooldown(keyStr, this.cooldownMs);
    } else if (statusCode === 402) {
      this.markDailyPause(keyStr);
    }
  }

  markCooldown(keyStr, durationMs) {
    const k = this.keys.find(item => item.key === keyStr);
    if (!k) return;
    k.cooldownUntil = Date.now() + durationMs;
    const suffix = keyStr.slice(-6);
    const available = this.keys.filter(item => this._isUsableForCost(item, 1)).length;
    L.warn(`[KeyPool:${this.serviceName}] Key ...${suffix} rate-limited (429) - cooldown ${Math.ceil(durationMs / 1000)}s | ${available} key(s) still available`);
  }

  markDailyPause(keyStr) {
    const k = this.keys.find(item => item.key === keyStr);
    if (!k) return;
    k.dailyPausedUntil = Date.now() + this._msUntilMidnightUTC();
    const suffix = keyStr.slice(-6);
    const available = this.keys.filter(item => this._isUsableForCost(item, 1)).length;
    L.warn(`[KeyPool:${this.serviceName}] Key ...${suffix} daily quota exhausted (402) - paused until midnight UTC | ${available} key(s) still available`);
  }

  getStats() {
    const now = Date.now();
    return {
      service: this.serviceName,
      total: this.keys.length,
      available: this.keys.filter(k => this._isUsableForCost(k, 1)).length,
      onCooldown: this.keys.filter(k => k.cooldownUntil > now && k.dailyPausedUntil <= now).length,
      dailyPaused: this.keys.filter(k => k.dailyPausedUntil > now).length,
      totalRequestsToday: this.keys.reduce((sum, k) => sum + k.dailyCount, 0),
      minuteLimit: this.minuteLimit,
      secondLimit: this.secondLimit,
      dailyLimit: this.dailyLimit,
      keys: this.keys.map(k => ({
        suffix: '...' + k.key.slice(-6),
        available: this._isUsableForCost(k, 1),
        usedLastMinute: this._usedInLastMinute(k),
        dailyCount: k.dailyCount,
        dailyRemaining: this._remainingDaily(k) === Infinity ? null : this._remainingDaily(k),
        cooldownUntil: k.cooldownUntil > now ? new Date(k.cooldownUntil).toISOString() : null,
        dailyPausedUntil: k.dailyPausedUntil > now ? new Date(k.dailyPausedUntil).toISOString() : null,
      })),
    };
  }

  get keyCount() {
    return this.keys.length;
  }
}

module.exports = { KeyPool, AllKeysExhaustedError };
