// ---------------------------------------------------------------------------
// retry.ts — Shared retry utility with configurable delays and predicates.
// Replaces duplicated retry loops in nodeRfcClient and executor.
// ---------------------------------------------------------------------------

export interface RetryOptions {
  delaysMs: number[];
  retryIf: (error: unknown) => boolean;
  label?: string;
}

export async function withRetries<T>(
  fn: () => Promise<T>,
  options: RetryOptions,
): Promise<T> {
  let lastError: unknown;
  const { delaysMs, retryIf, label } = options;

  for (let attempt = 0; attempt <= delaysMs.length; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      if (!retryIf(error) || attempt === delaysMs.length) {
        throw error;
      }

      await sleep(delaysMs[attempt] ?? 0);
    }
  }

  throw lastError;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
