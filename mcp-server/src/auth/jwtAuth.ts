// ---------------------------------------------------------------------------
// jwtAuth.ts — JWT-based SAP credential extraction and verification.
//
// The Python orchestrator encodes SAP connection parameters inside a JWT
// token (signed with a shared secret). This module:
//   1. Verifies the signature and expiry of the token.
//   2. Extracts SAP connection parameters from the payload.
//   3. Generates a SHA-256 fingerprint for connection pool caching.
// ---------------------------------------------------------------------------

import { createHash } from "node:crypto";
import jwt from "jsonwebtoken";

// ── Types ──────────────────────────────────────────────────────────────────

export interface SapJwtPayload {
  sap: {
    ashost: string;
    sysnr: string;
    client: string;
    user: string;
    passwd: string;
    lang?: string;
  };
  iat?: number;
  exp?: number;
}

export interface ResolvedJwtCredentials {
  /** node-rfc connection parameters ready to use. */
  connectionParameters: Record<string, string>;
  /** SHA-256 fingerprint of the connection for pool cache keying. */
  fingerprint: string;
}

// ── Helpers ────────────────────────────────────────────────────────────────

function isSapPayload(payload: unknown): payload is SapJwtPayload {
  if (!payload || typeof payload !== "object") return false;

  const candidate = payload as Record<string, unknown>;
  if (!candidate.sap || typeof candidate.sap !== "object") return false;

  const sap = candidate.sap as Record<string, unknown>;
  const requiredFields = ["ashost", "sysnr", "client", "user", "passwd"];

  return requiredFields.every(
    (field) => typeof sap[field] === "string" && (sap[field] as string).trim().length > 0,
  );
}

// ── Public API ─────────────────────────────────────────────────────────────

/**
 * Verify and decode a JWT token containing SAP credentials.
 * Throws on invalid signature, expired token, or missing SAP fields.
 */
export function verifySapJwt(
  token: string,
  secret: string,
): ResolvedJwtCredentials {
  let decoded: unknown;

  try {
    decoded = jwt.verify(token, secret, { algorithms: ["HS256"] });
  } catch (error) {
    if (error instanceof jwt.TokenExpiredError) {
      throw new Error("JWT token has expired. Please generate a new token.");
    }
    if (error instanceof jwt.JsonWebTokenError) {
      throw new Error(`JWT verification failed: ${error.message}`);
    }
    throw error;
  }

  if (!isSapPayload(decoded)) {
    throw new Error(
      "JWT payload is missing required SAP fields: sap.ashost, sap.sysnr, sap.client, sap.user, sap.passwd",
    );
  }

  const sap = decoded.sap;

  const connectionParameters: Record<string, string> = {
    ashost: sap.ashost.trim(),
    sysnr: sap.sysnr.trim(),
    client: sap.client.trim(),
    user: sap.user.trim(),
    passwd: sap.passwd.trim(),
    lang: (sap.lang ?? "EN").trim(),
  };

  const fingerprint = getConnectionFingerprint(connectionParameters);

  return { connectionParameters, fingerprint };
}

/**
 * Extract the raw Bearer token from an Authorization header value.
 * Returns undefined if the header is missing or malformed.
 */
export function extractBearerToken(
  authorizationHeader: string | undefined,
): string | undefined {
  if (!authorizationHeader) return undefined;

  const parts = authorizationHeader.split(" ");
  if (parts.length !== 2 || parts[0]?.toLowerCase() !== "bearer") {
    return undefined;
  }

  const token = parts[1]?.trim();
  return token && token.length > 0 ? token : undefined;
}

/**
 * Generate a SHA-256 fingerprint of SAP connection parameters.
 * Used as a cache key for the connection pool — never stores raw credentials.
 */
export function getConnectionFingerprint(
  params: Record<string, string>,
): string {
  const normalized = [
    params.ashost ?? "",
    params.sysnr ?? "",
    params.client ?? "",
    params.user ?? "",
  ]
    .map((value) => value.trim().toUpperCase())
    .join("|");

  return createHash("sha256").update(normalized).digest("hex").slice(0, 16);
}
