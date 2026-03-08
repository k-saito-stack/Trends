const MAX_URL_LENGTH = 2048;
const ALLOWED_PROTOCOLS = new Set(["http:", "https:"]);

function isPrivateOrLocalHost(hostname: string): boolean {
  const normalized = hostname.trim().toLowerCase();
  if (!normalized) {
    return true;
  }
  if (normalized === "localhost" || normalized === "0.0.0.0" || normalized.endsWith(".local")) {
    return true;
  }
  if (/^127\./.test(normalized)) {
    return true;
  }
  if (/^10\./.test(normalized)) {
    return true;
  }
  if (/^192\.168\./.test(normalized)) {
    return true;
  }
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(normalized)) {
    return true;
  }
  return false;
}

export function safeExternalUrl(rawUrl: string): string | null {
  const value = String(rawUrl || "").trim();
  if (!value || value.length > MAX_URL_LENGTH) {
    return null;
  }

  try {
    const parsed = new URL(value);
    if (!ALLOWED_PROTOCOLS.has(parsed.protocol)) {
      return null;
    }
    if (parsed.username || parsed.password) {
      return null;
    }
    if (isPrivateOrLocalHost(parsed.hostname)) {
      return null;
    }
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return null;
  }
}
