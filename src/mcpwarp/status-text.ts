/**
 * describeStatus(): a createTunnel() status as short localized UI text, so
 * hosts need not translate states and codes themselves. Plain string tables,
 * no Intl, so it runs in plugin sandboxes too. The strings are normative:
 * they must equal ws-mixer-spec's spec/fixtures/mcpwarp/status-text/<locale>.json
 * (test/spec-status-text-fixtures.test.ts), and change only with a spec release.
 */
import type { TunnelState, TunnelStatus, TunnelStatusCode } from "./tunnel.js";

export interface StatusText {
  /** The state, localized. */
  label: string;
  /** Why, localized; undefined when there is nothing to explain (online, a plain off) or it would repeat `label`. */
  detail?: string;
}

export interface StatusMessages {
  states: Record<TunnelState, string>;
  codes: Record<TunnelStatusCode, string>;
}

export type StatusLocale = "en" | "de" | "es" | "fr" | "ja" | "zh-CN";

const en: StatusMessages = {
  states: {
    off: "Off",
    connecting: "Connecting…",
    online: "Online",
    disabled: "Disabled in the mcpwarp dashboard",
    conflict: "Another instance took over this name",
    error: "Error",
  },
  codes: {
    UNAUTHORIZED: "Access token was rejected",
    TAKEN_OVER: "Another instance took over this name",
    DISCONNECTED: "Connection lost, reconnecting",
    RETRYING: "Connection failed, retrying",
    DISABLED: "Disabled in the mcpwarp dashboard",
    INVALID_CONFIG: "Invalid settings",
    INVALID_NAME: "Invalid service name (lowercase letters, digits and inner dashes, at most 30 characters)",
    USERNAME_REQUIRED: "Set a username in your mcpwarp account",
    QUOTA_EXCEEDED: "Plan limit reached",
    CONFLICT: "Name already in use",
    UNSUPPORTED_VERSION: "Plugin update required",
    DISCONNECTED_FATAL: "Connection to mcpwarp failed",
    INTERNAL: "Unexpected error",
  },
};

const de: StatusMessages = {
  states: {
    off: "Aus",
    connecting: "Verbinden…",
    online: "Online",
    disabled: "Im mcpwarp-Dashboard deaktiviert",
    conflict: "Eine andere Instanz hat diesen Namen übernommen",
    error: "Fehler",
  },
  codes: {
    UNAUTHORIZED: "Zugriffstoken wurde abgelehnt",
    TAKEN_OVER: "Eine andere Instanz hat diesen Namen übernommen",
    DISCONNECTED: "Verbindung unterbrochen, wird wiederhergestellt",
    RETRYING: "Verbindung fehlgeschlagen, neuer Versuch",
    DISABLED: "Im mcpwarp-Dashboard deaktiviert",
    INVALID_CONFIG: "Ungültige Einstellungen",
    INVALID_NAME: "Ungültiger Dienstname (Kleinbuchstaben, Ziffern und innere Bindestriche, höchstens 30 Zeichen)",
    USERNAME_REQUIRED: "Legen Sie in Ihrem mcpwarp-Konto einen Benutzernamen fest",
    QUOTA_EXCEEDED: "Tariflimit erreicht",
    CONFLICT: "Name wird bereits verwendet",
    UNSUPPORTED_VERSION: "Plugin-Update erforderlich",
    DISCONNECTED_FATAL: "Verbindung zu mcpwarp fehlgeschlagen",
    INTERNAL: "Unerwarteter Fehler",
  },
};

const es: StatusMessages = {
  states: {
    off: "Desactivado",
    connecting: "Conectando…",
    online: "En línea",
    disabled: "Desactivado en el panel de mcpwarp",
    conflict: "Otra instancia ha tomado este nombre",
    error: "Error",
  },
  codes: {
    UNAUTHORIZED: "Token de acceso rechazado",
    TAKEN_OVER: "Otra instancia ha tomado este nombre",
    DISCONNECTED: "Conexión perdida, reconectando",
    RETRYING: "Error de conexión, reintentando",
    DISABLED: "Desactivado en el panel de mcpwarp",
    INVALID_CONFIG: "Configuración no válida",
    INVALID_NAME: "Nombre de servicio no válido (letras minúsculas, dígitos y guiones internos, 30 caracteres como máximo)",
    USERNAME_REQUIRED: "Define un nombre de usuario en tu cuenta de mcpwarp",
    QUOTA_EXCEEDED: "Límite del plan alcanzado",
    CONFLICT: "El nombre ya está en uso",
    UNSUPPORTED_VERSION: "Es necesario actualizar el plugin",
    DISCONNECTED_FATAL: "No se pudo conectar con mcpwarp",
    INTERNAL: "Error inesperado",
  },
};

const fr: StatusMessages = {
  states: {
    off: "Désactivé",
    connecting: "Connexion…",
    online: "En ligne",
    disabled: "Désactivé dans le tableau de bord mcpwarp",
    conflict: "Une autre instance a repris ce nom",
    error: "Erreur",
  },
  codes: {
    UNAUTHORIZED: "Jeton d'accès refusé",
    TAKEN_OVER: "Une autre instance a repris ce nom",
    DISCONNECTED: "Connexion perdue, reconnexion en cours",
    RETRYING: "Échec de la connexion, nouvel essai en cours",
    DISABLED: "Désactivé dans le tableau de bord mcpwarp",
    INVALID_CONFIG: "Paramètres non valides",
    INVALID_NAME: "Nom de service non valide (lettres minuscules, chiffres et tirets internes, 30 caractères au maximum)",
    USERNAME_REQUIRED: "Définissez un nom d'utilisateur dans votre compte mcpwarp",
    QUOTA_EXCEEDED: "Limite de l'offre atteinte",
    CONFLICT: "Nom déjà utilisé",
    UNSUPPORTED_VERSION: "Mise à jour du plugin requise",
    DISCONNECTED_FATAL: "Échec de la connexion à mcpwarp",
    INTERNAL: "Erreur inattendue",
  },
};

const ja: StatusMessages = {
  states: {
    off: "オフ",
    connecting: "接続中…",
    online: "オンライン",
    disabled: "mcpwarp ダッシュボードで無効化されています",
    conflict: "別のインスタンスがこの名前を取得しました",
    error: "エラー",
  },
  codes: {
    UNAUTHORIZED: "アクセストークンが拒否されました",
    TAKEN_OVER: "別のインスタンスがこの名前を取得しました",
    DISCONNECTED: "接続が切れたため再接続しています",
    RETRYING: "接続に失敗したため再試行しています",
    DISABLED: "mcpwarp ダッシュボードで無効化されています",
    INVALID_CONFIG: "設定が無効です",
    INVALID_NAME: "サービス名が無効です（英小文字、数字、先頭と末尾以外のハイフン、30 文字以内）",
    USERNAME_REQUIRED: "mcpwarp アカウントでユーザー名を設定してください",
    QUOTA_EXCEEDED: "プランの上限に達しました",
    CONFLICT: "この名前は既に使用されています",
    UNSUPPORTED_VERSION: "プラグインの更新が必要です",
    DISCONNECTED_FATAL: "mcpwarp への接続に失敗しました",
    INTERNAL: "予期しないエラー",
  },
};

const zhCN: StatusMessages = {
  states: {
    off: "已关闭",
    connecting: "正在连接…",
    online: "在线",
    disabled: "已在 mcpwarp 控制台中停用",
    conflict: "另一个实例已接管该名称",
    error: "错误",
  },
  codes: {
    UNAUTHORIZED: "访问令牌被拒绝",
    TAKEN_OVER: "另一个实例已接管该名称",
    DISCONNECTED: "连接已断开，正在重新连接",
    RETRYING: "连接失败，正在重试",
    DISABLED: "已在 mcpwarp 控制台中停用",
    INVALID_CONFIG: "设置无效",
    INVALID_NAME: "服务名称无效（小写字母、数字和中间的连字符，最多 30 个字符）",
    USERNAME_REQUIRED: "请在 mcpwarp 账户中设置用户名",
    QUOTA_EXCEEDED: "已达到套餐限额",
    CONFLICT: "名称已被占用",
    UNSUPPORTED_VERSION: "需要更新插件",
    DISCONNECTED_FATAL: "无法连接到 mcpwarp",
    INTERNAL: "意外错误",
  },
};

const TABLES: Readonly<Record<StatusLocale, StatusMessages>> = Object.freeze({ en, de, es, fr, ja, "zh-CN": zhCN });
for (const t of Object.values(TABLES)) {
  Object.freeze(t.states);
  Object.freeze(t.codes);
  Object.freeze(t);
}

/** The shipped locales, in resolution order: a bare language picks the first table with that language. */
export const statusLocales: readonly StatusLocale[] = Object.freeze(["en", "de", "es", "fr", "ja", "zh-CN"] as const);

/** Between the INVALID_CONFIG sentence and `lastError`; fr has a no-break space (U+00A0) before the colon. */
const JOINERS: Partial<Record<StatusLocale, string>> = { fr: " : ", ja: "：", "zh-CN": "：" };

/** tunnel.ts INVALID_SETTINGS.lastError: it adds nothing to the sentence. */
const GENERIC_INVALID = "The tunnel settings are invalid.";

const hasOwn = (o: object, k: string): boolean => Object.prototype.hasOwnProperty.call(o, k);

/**
 * Exact tag (case-insensitive, "_" read as "-"), then the language subtag
 * ("de-AT" is "de", any "zh-*" is "zh-CN"), then "en". Never throws.
 */
function resolveLocale(locale: unknown): StatusLocale {
  if (typeof locale !== "string") return "en";
  const tag = locale.trim().replace(/_/g, "-").toLowerCase();
  for (const l of statusLocales) if (l.toLowerCase() === tag) return l;
  const lang = tag.split("-")[0];
  for (const l of statusLocales) if (l.toLowerCase().split("-")[0] === lang) return l;
  return "en";
}

function merge<T extends Record<string, string>>(base: T, override: unknown): T {
  if (override === null || typeof override !== "object") return base;
  const out = { ...base };
  for (const k of Object.keys(base)) {
    const v = hasOwn(override, k) ? (override as Record<string, unknown>)[k] : undefined;
    if (typeof v === "string") (out as Record<string, string>)[k] = v;
  }
  return out;
}

/**
 * The sentence, a colon (per locale) and `lastError`, whose own ": " becomes "; ".
 * Just one of them when `lastError` is the generic one or already starts with the sentence.
 */
function invalidConfigDetail(sentence: string, lastError: string, locale: StatusLocale): string {
  if (lastError === GENERIC_INVALID) return sentence;
  if (lastError.startsWith(sentence)) return lastError;
  return `${sentence}${JOINERS[locale] ?? ": "}${lastError.replace(/:\s+/g, "; ")}`;
}

/**
 * `label` is the state; `detail` the code's sentence (for INVALID_CONFIG
 * followed by a colon, per locale, and `lastError`, which names the field, in
 * English), or `lastError` when there is no known code; left out when empty
 * or equal to `label`. `messages` overrides single strings of the resolved locale.
 */
export function describeStatus(
  status: TunnelStatus,
  opts?: {
    locale?: StatusLocale | (string & {});
    messages?: { states?: Partial<StatusMessages["states"]>; codes?: Partial<StatusMessages["codes"]> };
  },
): StatusText {
  const locale = resolveLocale(opts?.locale);
  const table = TABLES[locale];
  const states = merge(table.states, opts?.messages?.states);
  const codes = merge(table.codes, opts?.messages?.codes);
  const { state, code, lastError } = status;
  const label = typeof state === "string" && hasOwn(states, state) ? states[state] : String(state);
  const sentence = typeof code === "string" && hasOwn(codes, code) ? codes[code] : undefined;
  let detail: string | undefined;
  if (sentence === undefined) detail = lastError;
  else if (code === "INVALID_CONFIG" && lastError) detail = invalidConfigDetail(sentence, lastError, locale);
  else detail = sentence;
  const out: StatusText = { label };
  if (detail && detail !== label) out.detail = detail;
  return out;
}
