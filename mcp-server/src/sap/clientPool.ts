// ---------------------------------------------------------------------------
// clientPool.ts — Multi-tenant SAP connection pool manager.
//
// Maintains a cache of NodeRfcSapClient instances keyed by connection
// fingerprint (SHA-256 hash of ashost+sysnr+client+user). This allows
// a single MCP server to serve multiple SAP systems simultaneously,
// with each JWT-authenticated session routing to its own connection pool.
//
// Features:
//   - Lazy pool creation on first access per fingerprint.
//   - Idle eviction: pools unused for 30 minutes are closed automatically.
//   - Deduplication: concurrent requests for the same fingerprint share
//     a single pool creation promise.
//   - Graceful shutdown via closeAll().
// ---------------------------------------------------------------------------

import type { SapClient, SapConfig } from "../types.js";
import { NodeRfcSapClient } from "./nodeRfcClient.js";

// ── Constants ──────────────────────────────────────────────────────────────

/** Close idle pools after 30 minutes of inactivity. */
const IDLE_EVICTION_MS = 30 * 60 * 1000;

/** Run the eviction sweep every 5 minutes. */
const EVICTION_INTERVAL_MS = 5 * 60 * 1000;

// ── Types ──────────────────────────────────────────────────────────────────

interface PoolEntry {
  client: NodeRfcSapClient;
  fingerprint: string;
  label: string;
  lastUsed: number;
}

// ── Main class ─────────────────────────────────────────────────────────────

export class SapClientPool {
  private readonly pools = new Map<string, PoolEntry>();
  private readonly pendingCreations = new Map<string, Promise<PoolEntry>>();
  private readonly evictionTimer: ReturnType<typeof setInterval>;
  private readonly baseSapConfig: SapConfig;

  constructor(baseSapConfig: SapConfig) {
    this.baseSapConfig = baseSapConfig;

    // Start idle eviction sweep
    this.evictionTimer = setInterval(() => {
      void this.evictIdle();
    }, EVICTION_INTERVAL_MS);

    // Allow the process to exit even if the timer is still running
    if (this.evictionTimer.unref) {
      this.evictionTimer.unref();
    }
  }

  /**
   * Get or create a SapClient for the given connection parameters.
   * Results are cached by fingerprint for connection reuse.
   */
  async getOrCreate(
    fingerprint: string,
    connectionParameters: Record<string, string>,
    label?: string,
  ): Promise<SapClient> {
    // Check existing pool
    const existing = this.pools.get(fingerprint);
    if (existing) {
      existing.lastUsed = Date.now();
      return existing.client;
    }

    // Deduplicate concurrent creation requests
    const pending = this.pendingCreations.get(fingerprint);
    if (pending) {
      const entry = await pending;
      entry.lastUsed = Date.now();
      return entry.client;
    }

    // Create new pool
    const creationPromise = this.createPoolEntry(
      fingerprint,
      connectionParameters,
      label,
    );
    this.pendingCreations.set(fingerprint, creationPromise);

    try {
      const entry = await creationPromise;
      this.pools.set(fingerprint, entry);
      return entry.client;
    } finally {
      this.pendingCreations.delete(fingerprint);
    }
  }

  /** Close all pooled connections. Called on server shutdown. */
  async closeAll(): Promise<void> {
    clearInterval(this.evictionTimer);

    const closePromises = Array.from(this.pools.values()).map(
      async (entry) => {
        try {
          await entry.client.close();
        } catch {
          // Ignore close errors during shutdown
        }
      },
    );

    await Promise.all(closePromises);
    this.pools.clear();
  }

  /** Return a summary of all active pool entries for diagnostics. */
  listActive(): Array<{ fingerprint: string; label: string; lastUsed: number }> {
    return Array.from(this.pools.values()).map((entry) => ({
      fingerprint: entry.fingerprint,
      label: entry.label,
      lastUsed: entry.lastUsed,
    }));
  }

  // ── Internals ───────────────────────────────────────────────────────────

  private async createPoolEntry(
    fingerprint: string,
    connectionParameters: Record<string, string>,
    label?: string,
  ): Promise<PoolEntry> {
    // Build a SapConfig using the base config's pool/timeout/breaker settings
    // but with the JWT-provided connection parameters.
    const sapConfig: SapConfig = {
      ...this.baseSapConfig,
      connectionParameters,
      connectionMode: "direct",
    };

    const client = new NodeRfcSapClient(sapConfig);

    return {
      client,
      fingerprint,
      label: label ?? `jwt-${fingerprint.slice(0, 8)}`,
      lastUsed: Date.now(),
    };
  }

  private async evictIdle(): Promise<void> {
    const now = Date.now();
    const toEvict: string[] = [];

    for (const [fingerprint, entry] of this.pools) {
      if (now - entry.lastUsed > IDLE_EVICTION_MS) {
        toEvict.push(fingerprint);
      }
    }

    for (const fingerprint of toEvict) {
      const entry = this.pools.get(fingerprint);
      if (entry) {
        console.log(
          `[pool] Evicting idle SAP connection pool: ${entry.label} (${fingerprint.slice(0, 8)}...)`,
        );
        this.pools.delete(fingerprint);
        try {
          await entry.client.close();
        } catch {
          // Ignore close errors during eviction
        }
      }
    }
  }
}
