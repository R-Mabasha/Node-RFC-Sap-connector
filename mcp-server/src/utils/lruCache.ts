// ---------------------------------------------------------------------------
// lruCache.ts — Size-bounded LRU cache with TTL eviction.
// Replaces the unbounded global Map in executor.ts.
// ---------------------------------------------------------------------------

export interface LruCacheOptions {
  maxSize: number;
  defaultTtlMs: number;
}

interface CacheEntry<V> {
  value: V;
  expiresAt: number;
  lastAccessed: number;
}

export class LruCache<K, V> {
  private readonly store = new Map<K, CacheEntry<V>>();
  private readonly maxSize: number;
  private readonly defaultTtlMs: number;

  constructor(options: LruCacheOptions) {
    this.maxSize = options.maxSize;
    this.defaultTtlMs = options.defaultTtlMs;
  }

  get(key: K): V | undefined {
    const entry = this.store.get(key);
    if (!entry) return undefined;

    if (entry.expiresAt <= Date.now()) {
      this.store.delete(key);
      return undefined;
    }

    entry.lastAccessed = Date.now();
    return entry.value;
  }

  set(key: K, value: V, ttlMs?: number): void {
    const effectiveTtl = ttlMs ?? this.defaultTtlMs;

    if (this.store.size >= this.maxSize && !this.store.has(key)) {
      this.evictLru();
    }

    this.store.set(key, {
      value,
      expiresAt: Date.now() + effectiveTtl,
      lastAccessed: Date.now(),
    });
  }

  clear(): void {
    this.store.clear();
  }

  size(): number {
    return this.store.size;
  }

  private evictLru(): void {
    let oldestKey: K | undefined;
    let oldestTime = Infinity;

    for (const [key, entry] of this.store) {
      if (entry.lastAccessed < oldestTime) {
        oldestTime = entry.lastAccessed;
        oldestKey = key;
      }
    }

    if (oldestKey !== undefined) {
      this.store.delete(oldestKey);
    }
  }
}
