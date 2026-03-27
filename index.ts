/**
 * ERP2 Bridge Channel Plugin for OpenClaw
 *
 * Connects OpenClaw to a Java backend server via a persistent WebSocket (WSS)
 * connection. Supports session isolation via sessionKey, uid-based identity
 * metadata, configurable DM and group policies, automatic reconnection with
 * exponential backoff, and ping/pong heartbeat.
 *
 * Install:  openclaw plugins install @openclaw/erp2-bridge
 * Docs:     https://docs.openclaw.ai/channels/erp2-bridge
 */

import type {
  ChannelPlugin,
  OpenClawConfig,
  PluginRuntime,
} from "openclaw/plugin-sdk";
import { createDedupeCache, type DedupeCache } from "openclaw/plugin-sdk";

// ── Global runtime singleton (injected by register()) ────────────────────────
let _wssRuntime: PluginRuntime | null = null;
function setWssRuntime(r: PluginRuntime) { _wssRuntime = r; }
function getWssRuntime(): PluginRuntime {
  if (!_wssRuntime) throw new Error("[wss-bridge] PluginRuntime not initialized – register() not called");
  return _wssRuntime;
}

// ── Wire protocol types ───────────────────────────────────────────────────────

/** Message received from the Java backend over the WebSocket. */
interface WireInboundMessage {
  /** Message type: PING | PONG | CHAT | CHAT_RESPONSE | BROADCAST | SYSTEM | ERROR */
  type: string;
  /** Message content (mapped from Java `content` field). */
  content?: string;
  /** Sender ID (mapped from Java `sender` field). */
  sender?: string;
  /** Receiver ID (mapped from Java `receiver` field). */
  receiver?: string;
  /** UNIX timestamp (ms). */
  timestamp?: number;
  /** Extended data (optional). */
  data?: unknown;
  /** Bearer token for downstream API authentication. */
  token?: string;
  /** Tenant code for multi-tenant routing. */
  tenantCode?: string;

  // ── Derived fields (populated by normalizeInbound) ──
  /** Unique message ID (used for dedup and reply correlation). */
  messageId?: string;
  /** Java-side user identity – derived from sender. */
  uid: string;
  /** DX user ID – passed directly from the Java backend dxuid field. */
  dxuid?: string;
  /** Human-readable sender name – passed directly from the Java backend fromName field. */
  fromName?: string;
  /** Session key for conversation isolation. */
  sessionKey: string;
  /** Optional human-readable sender name. */
  senderName?: string;
  /** Message text – mapped from content. */
  text: string;
  /** True when the message is from a group/channel context. */
  isGroup?: boolean;
  /** Group or channel ID when isGroup=true. */
  groupId?: string;
}

/** Normalize a raw Java backend message into WireInboundMessage. Returns null if should be ignored. */
function normalizeInbound(raw: unknown): WireInboundMessage | null {
  if (typeof raw !== "object" || raw === null) return null;
  const m = raw as Record<string, unknown>;

  // 兼容后端字段名 messageType 和 type
  const rawType = typeof m["messageType"] === "string" ? m["messageType"] : (typeof m["type"] === "string" ? m["type"] : "");
  const type = rawType.toUpperCase();

  // Ignore non-chat messages (PING/PONG handled separately, SYSTEM/ERROR ignored)
  if (!type || type === "PING" || type === "PONG" || type === "ERROR" || type === "SYSTEM") {
    return null;
  }

  const content = typeof m["content"] === "string" ? m["content"].trim() : "";
  const sender = typeof m["sender"] === "string" ? m["sender"].trim() : "";
  // 忽略自己发出去的消息（sender=openclaw），避免死循环
  if (sender === "openclaw") return null;
  const receiver = typeof m["receiver"] === "string" ? m["receiver"].trim() : "";
  const timestamp = typeof m["timestamp"] === "number" ? m["timestamp"] : Date.now();
  const token = typeof m["token"] === "string" ? m["token"].trim() : undefined;
  const tenantCode = typeof m["tenantCode"] === "string" ? m["tenantCode"].trim() : undefined;
  const dxuid = typeof m["dxuid"] === "string" ? m["dxuid"].trim() : undefined;
  const fromName = typeof m["fromName"] === "string" ? m["fromName"].trim() : undefined;

  if (!content || !sender) return null;

  // sessionKey: use sender as peer id so each user gets an isolated session
  // messageId: append a random suffix so two messages from the same sender within
  // the same millisecond get distinct IDs and the dedupe cache doesn't merge them.
  const sessionKey = sender;
  const rawMessageId = typeof m["messageId"] === "string" ? m["messageId"].trim() : "";
  const messageId = rawMessageId || `${sender}-${timestamp}-${Math.random().toString(36).slice(2, 8)}`;

  return {
    type,
    content,
    sender,
    receiver,
    timestamp,
    data: m["data"],
    token,
    tenantCode,
    dxuid,
    fromName,
    // derived
    uid: sender,
    sessionKey,
    text: content,
    senderName: sender,
    messageId,
    isGroup: type === "BROADCAST",
    groupId: type === "BROADCAST" ? (receiver || undefined) : undefined,
  };
}

/** Message sent to the Java backend over the WebSocket (Java backend format). */
interface WireOutboundMessage {
  /** Message type – always CHAT_RESPONSE for replies. */
  type: string;
  /** Reply text (mapped to Java `content`). */
  content: string;
  /** Sender ID – this agent/bot id. */
  sender: string;
  /** Recipient ID (mapped to Java `receiver`). */
  receiver: string;
  /** Server-side timestamp. */
  timestamp: number;
  /** Extended data (optional). */
  data?: unknown;
  /** DX user ID – echoed back from the inbound dxuid field. */
  dxuid?: string;
  /** Human-readable sender name – echoed back from the inbound fromName field. */
  fromName?: string;
}

// ── Account resolution ────────────────────────────────────────────────────────

interface WssBridgeAccountConfig {
  enabled?: boolean;
  wsUrl?: string;
  /** Bearer token for Authorization header. */
  token?: string;
  /** Path to a file that contains the token (takes priority over token). */
  tokenFile?: string;
  dmPolicy?: "pairing" | "open" | "allowlist" | "disabled";
  allowFrom?: string[];
  groupPolicy?: "open" | "allowlist" | "disabled";
  groupAllowFrom?: string[];
  groups?: Record<
    string,
    {
      requireMention?: boolean;
      allowFrom?: string[];
      systemPrompt?: string;
    }
  >;
  pingIntervalMs?: number;
  reconnectBaseMs?: number;
  reconnectMaxMs?: number;
  connectTimeoutMs?: number;
}

interface ResolvedAccount {
  accountId: string;
  enabled: boolean;
  configured: boolean;
  wsUrl: string;
  token: string;
  tokenSource: "env" | "config" | "none";
  config: WssBridgeAccountConfig;
}

type CoreConfig = OpenClawConfig & {
  channels?: {
    "erp2-bridge"?: WssBridgeAccountConfig & {
      accounts?: Record<string, WssBridgeAccountConfig>;
      defaultAccount?: string;
    };
  };
};

function readTokenFromFile(filePath: string): string {
  try {
    // Node built-in – available at runtime inside OpenClaw gateway process
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const fs = require("fs") as typeof import("fs");
    return fs.readFileSync(filePath.trim(), "utf8").trim();
  } catch {
    return "";
  }
}

function resolveAccount(cfg: OpenClawConfig, accountId?: string | null): ResolvedAccount {
  const core = cfg as CoreConfig;
  const channelCfg = core.channels?.["erp2-bridge"];
  const id = accountId?.trim() || channelCfg?.defaultAccount?.trim() || "default";
  const accountOverride = channelCfg?.accounts?.[id] ?? {};
  // Merge top-level config with per-account overrides (account wins)
  const merged: WssBridgeAccountConfig = { ...channelCfg, ...accountOverride };

  // Resolve token: env > tokenFile > inline config
  let token = "";
  let tokenSource: "env" | "config" | "none" = "none";
  const envToken = (id === "default" ? process.env.WSS_BRIDGE_TOKEN : undefined) ?? "";
  if (envToken.trim()) {
    token = envToken.trim();
    tokenSource = "env";
  } else if (merged.tokenFile?.trim()) {
    const fileToken = readTokenFromFile(merged.tokenFile);
    if (fileToken) {
      token = fileToken;
      tokenSource = "config";
    }
  } else if (merged.token?.trim()) {
    token = merged.token.trim();
    tokenSource = "config";
  }

  const wsUrlFromEnv = (id === "default" ? process.env.WSS_BRIDGE_URL : undefined) ?? "";
  const wsUrl = (merged.wsUrl?.trim() || wsUrlFromEnv.trim()) ?? "";

  return {
    accountId: id,
    enabled: channelCfg?.enabled !== false && merged.enabled !== false,
    configured: Boolean(wsUrl),
    wsUrl,
    token,
    tokenSource,
    config: merged,
  };
}

function listAccountIds(cfg: OpenClawConfig): string[] {
  const channelCfg = (cfg as CoreConfig).channels?.["erp2-bridge"];
  if (!channelCfg) return [];
  const accountKeys = Object.keys(channelCfg.accounts ?? {});
  // Always include "default" if the top-level channel config has a wsUrl
  const ids = new Set(accountKeys);
  if (channelCfg.wsUrl) ids.add("default");
  return Array.from(ids);
}

// ── WebSocket connection management ──────────────────────────────────────────

type SendFn = (msg: WireOutboundMessage) => void;
type InboundHandler = (msg: WireInboundMessage, sendFn: SendFn) => Promise<void>;

// Active connection registry: keyed by accountId, stores the sendFn of the
// currently-open persistent WebSocket so outbound sendText can reuse it.
const activeSendFns = new Map<string, SendFn>();

// Per-account dedupe cache: prevents duplicate AI invocations for the same
// message (can happen on WSS reconnect delivering the same frame again).
// TTL of 5 minutes covers typical reconnect windows; maxSize 2000 prevents unbounded growth.
const dedupeCaches = new Map<string, DedupeCache>();

function getDedupeCache(accountId: string): DedupeCache {
  const existing = dedupeCaches.get(accountId);
  if (existing) return existing;
  const cache = createDedupeCache({ ttlMs: 5 * 60_000, maxSize: 2000 });
  dedupeCaches.set(accountId, cache);
  return cache;
}

interface MonitorOptions {
  account: ResolvedAccount;
  abortSignal: AbortSignal;
  log: (level: "info" | "warn" | "error" | "debug", msg: string) => void;
  onMessage: InboundHandler;
  /** Called when connection becomes ready. */
  onConnected?: () => void;
  /** Called when connection is lost (before reconnect). */
  onDisconnected?: () => void;
}

async function monitorWssConnection(opts: MonitorOptions): Promise<void> {
  const { account, abortSignal, log, onMessage } = opts;

  // Lazy-require 'ws' so the plugin can also be loaded in environments that
  // don't bundle ws (the WebSocket global is used as fallback in browsers).
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const WebSocketImpl: typeof import("ws").default = (() => {
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      return require("ws");
    } catch {
      // Fallback: use the global WebSocket (e.g., Bun / browser environments)
      return (globalThis as unknown as { WebSocket: typeof import("ws").default }).WebSocket;
    }
  })();

  const reconnectBaseMs = account.config.reconnectBaseMs ?? 1000;
  const reconnectMaxMs = account.config.reconnectMaxMs ?? 30_000;
  const pingIntervalMs = account.config.pingIntervalMs ?? 15_000;
  const connectTimeoutMs = account.config.connectTimeoutMs ?? 10_000;

  let attempt = 0;

  while (!abortSignal.aborted) {
    const delay = attempt === 0 ? 0 : Math.min(reconnectBaseMs * 2 ** Math.min(attempt - 1, 30), reconnectMaxMs);
    if (delay > 0) {
      log("info", `[${account.accountId}] reconnecting in ${delay}ms (attempt ${attempt})`);
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, delay);
        abortSignal.addEventListener("abort", () => { clearTimeout(t); resolve(); }, { once: true });
      });
      if (abortSignal.aborted) break;
    }

    attempt++;

    await new Promise<void>((resolve) => {
      log("info", `[${account.accountId}] connecting to ${account.wsUrl}`);

      const headers: Record<string, string> = {};
      if (account.token) {
        headers["Authorization"] = `Bearer ${account.token}`;
      }

      let ws: import("ws").default;
      try {
        // Use a direct TCP agent to bypass any HTTP_PROXY / HTTPS_PROXY env vars
        // that would route WebSocket traffic through a proxy and cause timeouts
        // when connecting to internal network addresses.
        const net = require("node:net");
        const isSecure = account.wsUrl.startsWith("wss://") || account.wsUrl.startsWith("https://");
        const directAgent = isSecure
          ? new (require("node:https").Agent)({ rejectUnauthorized: false })
          : new (require("node:http").Agent)({ createConnection: net.createConnection });
        ws = new WebSocketImpl(account.wsUrl, { headers, agent: directAgent });
      } catch (err) {
        log("error", `[${account.accountId}] failed to create WebSocket: ${String(err)}`);
        resolve();
        return;
      }

      let pingTimer: ReturnType<typeof setInterval> | undefined;
      let connectTimer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;

      const cleanup = () => {
        if (pingTimer) clearInterval(pingTimer);
        if (connectTimer) clearTimeout(connectTimer);
      };

      const finish = () => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve();
      };

      // Connection timeout
      connectTimer = setTimeout(() => {
        log("warn", `[${account.accountId}] connection timed out after ${connectTimeoutMs}ms`);
        ws.terminate?.();
        finish();
      }, connectTimeoutMs);

      abortSignal.addEventListener("abort", () => {
        log("info", `[${account.accountId}] abort signal received, closing`);
        ws.close(1000, "shutdown");
        finish();
      }, { once: true });

      ws.on("open", () => {
        if (connectTimer) clearTimeout(connectTimer);
        attempt = 0; // reset backoff on successful connect
        log("info", `[${account.accountId}] WebSocket connected`);

        // Register sendFn so outbound sendText can reuse this connection
        const boundSendFn: SendFn = (outbound) => {
          if (ws.readyState === WebSocketImpl.OPEN) {
            ws.send(JSON.stringify(outbound));
          }
        };
        activeSendFns.set(account.accountId, boundSendFn);

        opts.onConnected?.();

        // Start heartbeat pings
        pingTimer = setInterval(() => {
          if (ws.readyState === WebSocketImpl.OPEN) {
            try {
              ws.ping();
            } catch {
              // ignore ping errors – the close handler will handle reconnect
            }
          }
        }, pingIntervalMs);
      });

      ws.on("message", (data: import("ws").RawData) => {
        let raw: string;
        try {
          raw = typeof data === "string" ? data : data.toString("utf8");
        } catch {
          log("warn", `[${account.accountId}] could not decode incoming frame`);
          return;
        }

        let parsed: unknown;
        try {
          parsed = JSON.parse(raw);
        } catch {
          log("warn", `[${account.accountId}] non-JSON frame received: ${raw.slice(0, 120)}`);
          return;
        }

        // Define sendFn before any usage (including PING reply)
        const sendFn: SendFn = (outbound) => {
          if (ws.readyState === WebSocketImpl.OPEN) {
            ws.send(JSON.stringify(outbound));
          }
        };

        // Handle PING → reply PONG
        if (typeof parsed === "object" && parsed !== null) {
          const m = parsed as Record<string, unknown>;
          const t = typeof m["messageType"] === "string" ? m["messageType"] : (typeof m["type"] === "string" ? m["type"] : "");
          if (t.toUpperCase() === "PING") {
            sendFn({ type: "PONG", content: "", sender: "openclaw", receiver: "", timestamp: Date.now() });
            return;
          }
        }

        const msg = normalizeInbound(parsed);
        if (!msg) {
          log("debug", `[${account.accountId}] ignoring non-chat message: ${raw.slice(0, 120)}`);
          return;
        }

        // Handle asynchronously; errors are caught inside onMessage
        onMessage(msg, sendFn).catch((err) => {
          log("error", `[${account.accountId}] inbound handler error: ${String(err)}`);
        });
      });

      ws.on("error", (err: Error) => {
        log("error", `[${account.accountId}] WebSocket error: ${err.message}`);
      });

      ws.on("close", (code: number, reason: Buffer) => {
        cleanup();
        // Remove sendFn so that outbound sendText falls back to creating a new connection
        activeSendFns.delete(account.accountId);
        opts.onDisconnected?.();
        log(
          "info",
          `[${account.accountId}] WebSocket closed (code=${code} reason=${reason.toString().slice(0, 80)})`,
        );
        if (!abortSignal.aborted) {
          finish();
        }
      });
    });
  }

  log("info", `[${account.accountId}] monitor stopped`);
}

// ── Inbound message handling ──────────────────────────────────────────────────

function normalizeEntry(s: string): string {
  return s.trim().toLowerCase();
}

function isInAllowlist(uid: string, list: string[]): boolean {
  const n = normalizeEntry(uid);
  return list.some((e) => normalizeEntry(e) === n);
}

async function handleInbound(
  msg: WireInboundMessage,
  sendFn: SendFn,
  account: ResolvedAccount,
  cfg: OpenClawConfig,
  channelRuntime: PluginRuntime["channel"],
  log: (level: "info" | "warn" | "error" | "debug", text: string) => void,
): Promise<void> {
  const rawBody = msg.text.trim();
  if (!rawBody) return;

  // ── Deduplicate ───────────────────────────────────────────────────────────
  // Reject messages whose ID was already seen within the TTL window.
  // This guards against WSS reconnect delivering the same frame twice.
  const dedupeCache = getDedupeCache(account.accountId);
  if (dedupeCache.check(msg.messageId)) {
    log("debug", `[${account.accountId}] duplicate message dropped: id=${msg.messageId}`);
    return;
  }

  // Record inbound channel activity so `openclaw channels status` can show lastInboundAt.
  channelRuntime.activity.record({
    channel: "erp2-bridge",
    accountId: account.accountId,
    direction: "inbound",
  });

  const dmPolicy = account.config.dmPolicy ?? "pairing";
  const groupPolicy = account.config.groupPolicy ?? "allowlist";

  // ── Resolve group config ─────────────────────────────────────────────────
  const groupKey = msg.groupId ?? msg.sessionKey;
  const groupConfig =
    account.config.groups?.[groupKey] ?? account.config.groups?.["*"] ?? undefined;

  // ── Group access gate ─────────────────────────────────────────────────────
  if (msg.isGroup) {
    if (groupPolicy === "disabled") {
      log("debug", `drop group (groupPolicy=disabled) uid=${msg.uid}`);
      return;
    }
    if (groupPolicy === "allowlist") {
      const groupKeys = Object.keys(account.config.groups ?? {});
      const hasWildcard = groupKeys.includes("*");
      const isAllowed = groupKeys.includes(groupKey);
      if (!hasWildcard && !isAllowed) {
        log("debug", `drop group=${groupKey} (not in groups allowlist)`);
        return;
      }
    }
  }

  // ── DM policy gate ────────────────────────────────────────────────────────
  const configAllowFrom = (account.config.allowFrom ?? []).map(normalizeEntry);
  const configGroupAllowFrom = (account.config.groupAllowFrom ?? []).map(normalizeEntry);

  if (!msg.isGroup) {
    if (dmPolicy === "disabled") {
      log("debug", `drop DM (dmPolicy=disabled) uid=${msg.uid}`);
      return;
    }
    if (dmPolicy === "allowlist") {
      if (!isInAllowlist(msg.uid, configAllowFrom)) {
        log("debug", `drop DM (dmPolicy=allowlist, uid not allowed) uid=${msg.uid}`);
        return;
      }
    }
    if (dmPolicy === "pairing") {
      // Check pairing store for allow-from
      let storeAllowFrom: string[] = [];
      try {
        const stored = await channelRuntime.pairing.readAllowFromStore({
          channel: "erp2-bridge",
          accountId: account.accountId,
        });
        storeAllowFrom = (stored ?? []).map(String).map(normalizeEntry);
      } catch {
        // pairing store may not be available in all environments
      }
      const effectiveAllowFrom = [...configAllowFrom, ...storeAllowFrom];
      if (!isInAllowlist(msg.uid, effectiveAllowFrom)) {
        // Issue pairing challenge: upsert request → get code → build reply text
        try {
          const { code } = await channelRuntime.pairing.upsertPairingRequest({
            channel: "erp2-bridge",
            id: normalizeEntry(msg.uid),
            accountId: account.accountId,
            meta: { name: msg.senderName ?? msg.uid },
          });
          const challenge = channelRuntime.pairing.buildPairingReply({
            channel: "erp2-bridge",
            idLine: `Your ERP2 Bridge uid: ${msg.uid}`,
            code,
          });
          const outbound: WireOutboundMessage = {
            type: "CHAT_RESPONSE",
            content: challenge,
            sender: "openclaw",
            receiver: msg.sender ?? msg.uid,
            timestamp: Date.now(),
          };
          sendFn(outbound);
        } catch (err) {
          log("warn", `pairing challenge failed: ${String(err)}`);
        }
        log("debug", `drop DM (pairing pending) uid=${msg.uid}`);
        return;
      }
    }
  }

  // ── Group sender gate ─────────────────────────────────────────────────────
  if (msg.isGroup && groupPolicy === "allowlist") {
    const innerAllowFrom = (groupConfig?.allowFrom ?? []).map(normalizeEntry);
    const effectiveGroupAllowFrom =
      innerAllowFrom.length > 0 ? innerAllowFrom : configGroupAllowFrom;
    if (effectiveGroupAllowFrom.length > 0 && !isInAllowlist(msg.uid, effectiveGroupAllowFrom)) {
      log("debug", `drop group sender uid=${msg.uid} (not in group allowFrom)`);
      return;
    }
  }

  // ── Mention gate ──────────────────────────────────────────────────────────
  const requireMention = msg.isGroup ? (groupConfig?.requireMention ?? true) : false;
  if (requireMention) {
    const mentionRegexes = channelRuntime.mentions.buildMentionRegexes(cfg);
    const mentioned =
      mentionRegexes.length > 0
        ? channelRuntime.mentions.matchesMentionPatterns(rawBody, mentionRegexes)
        : false;
    if (!mentioned) {
      // Also allow control commands even without mention
      const isCmd = channelRuntime.commands.isControlCommandMessage(rawBody, cfg);
      if (!isCmd) {
        log("debug", `drop group (requireMention=true, not mentioned) uid=${msg.uid}`);
        return;
      }
    }
  }

  // ── Route resolution ──────────────────────────────────────────────────────
  //
  // The sessionKey IS the conversation isolation key.
  // - Group chats: groupId (or sessionKey fallback) → independent group context
  // - DMs: sessionKey → per-session isolation (one user can have many sessions)
  const peerId = msg.isGroup ? (msg.groupId ?? msg.sessionKey) : msg.sessionKey;

  const route = channelRuntime.routing.resolveAgentRoute({
    cfg,
    channel: "erp2-bridge",
    accountId: account.accountId,
    peer: {
      kind: msg.isGroup ? "group" : "direct",
      id: peerId,
    },
  });

  // ── Build envelope ─────────────────────────────────────────────────────────
  const fromLabel = msg.isGroup
    ? (msg.groupId ?? msg.sessionKey)
    : (msg.senderName ?? msg.uid);

  const storePath = channelRuntime.session.resolveStorePath(cfg.session?.store, {
    agentId: route.agentId,
  });
  const envelopeOptions = channelRuntime.reply.resolveEnvelopeFormatOptions(cfg);
  const previousTimestamp = channelRuntime.session.readSessionUpdatedAt({
    storePath,
    sessionKey: route.sessionKey,
  });

  // Build metadata prefix lines to inject into the envelope body.
  // These are visible to the AI as part of the user message but cannot be
  // spoofed without access to the WebSocket connection itself.
  const metaParts: string[] = [
    `uid:${msg.uid}`,
    `session_key:${route.sessionKey}`,
  ];
  if (msg.token) metaParts.push(`token:${msg.token}`);
  if (msg.tenantCode) metaParts.push(`tenantCode:${msg.tenantCode}`);
  const metaPrefix = metaParts.join(" ");

  const body = channelRuntime.reply.formatAgentEnvelope({
    channel: "ERP2 Bridge",
    from: fromLabel,
    timestamp: msg.timestamp,
    previousTimestamp,
    envelope: envelopeOptions,
    body: `${metaPrefix}\n${rawBody}`,
  });

  const groupSystemPrompt = groupConfig?.systemPrompt?.trim() || undefined;

  // Build finalized context.
  // uid and sessionKey are embedded in From/SenderId so downstream tools
  // and prompts have full visibility into the Java-side user identity.
  const ctxPayload = channelRuntime.reply.finalizeInboundContext({
    Body: body,
    // BodyForAgent carries the envelope (with uid/token metadata) so the AI
    // can see the full context. Without this, the framework falls back to
    // CommandBody (= rawBody, plain text) and the metadata prefix is lost.
    // This mirrors the pattern used by the LINE plugin.
    BodyForAgent: body,
    RawBody: rawBody,
    CommandBody: rawBody,
    From: msg.isGroup
      ? `erp2-bridge:group:${peerId}:uid:${msg.uid}`
      : `erp2-bridge:uid:${msg.uid}:session:${msg.sessionKey}`,
    To: `erp2-bridge:${peerId}`,
    SessionKey: route.sessionKey,
    AccountId: route.accountId,
    ChatType: msg.isGroup ? ("group" as const) : ("direct" as const),
    ConversationLabel: fromLabel,
    SenderName: msg.senderName ?? msg.uid,
    // SenderId is the Java-side uid – used for allowFrom matching and sender identity display
    SenderId: msg.uid,
    GroupSubject: msg.isGroup ? peerId : undefined,
    GroupSystemPrompt: msg.isGroup ? groupSystemPrompt : undefined,
    Provider: "erp2-bridge",
    Surface: "erp2-bridge",
    MessageSid: msg.messageId,
    Timestamp: msg.timestamp,
    OriginatingChannel: "erp2-bridge",
    OriginatingTo: `erp2-bridge:${peerId}`,
    UntrustedContext: [],
  });

  // ── Record session + update last route ───────────────────────────────────
  // recordInboundSession persists session metadata AND updates lastRoute so
  // `openclaw send --last` / proactive outbound can find the right target.
  await channelRuntime.session.recordInboundSession({
    storePath,
    sessionKey: route.sessionKey,
    ctx: ctxPayload,
    updateLastRoute: !msg.isGroup
      ? {
            sessionKey: route.sessionKey,
            channel: "erp2-bridge",
            to: msg.uid,
            accountId: route.accountId,
          }
      : undefined,
    onRecordError: (err) => {
      log("warn", `[erp2-bridge] recordInboundSession failed: ${String(err)}`);
    },
  });

  // ── Dispatch to OpenClaw AI engine ────────────────────────────────────────
  // Wrap in try/catch to send user-visible error replies when the model call fails.
  try {
    await channelRuntime.reply.dispatchReplyWithBufferedBlockDispatcher({
      ctx: ctxPayload,
      cfg,
      replyOptions: {
        // 强制开启 block streaming：AI 每生成一段就立即推送，不等完整回复
        disableBlockStreaming: false,
      },
      dispatcherOptions: {
        deliver: async (payload) => {
          // payload is ReplyPayload; extract text to send back to the Java backend
          const text = payload.text?.trim();
          if (!text) return;
          const outbound: WireOutboundMessage = {
            type: "CHAT",
            content: text,
            sender: "openclaw",
            receiver: msg.sender ?? msg.uid,
            timestamp: Date.now(),
            dxuid: msg.dxuid,
            fromName: msg.fromName,
          };
          sendFn(outbound);
          // Record outbound activity for status visibility.
          channelRuntime.activity.record({
            channel: "erp2-bridge",
            accountId: account.accountId,
            direction: "outbound",
          });
        },
      },
    });
  } catch (err) {
    // P0: On AI error, send an error reply back to the user so they know something went wrong.
    log("error", `[erp2-bridge] dispatchReply failed: ${String(err)}`);
    const errorReply: WireOutboundMessage = {
      type: "CHAT",
      content: "[OpenClaw] 处理您的消息时发生错误，请稍后重试。",
      sender: "openclaw",
      receiver: msg.sender ?? msg.uid,
      timestamp: Date.now(),
    };
    try {
      sendFn(errorReply);
      channelRuntime.activity.record({
        channel: "erp2-bridge",
        accountId: account.accountId,
        direction: "outbound",
      });
    } catch {
      // Best-effort: ignore if error reply also fails
    }
  }
}

// ── Plugin definition ─────────────────────────────────────────────────────────

const wssBridgePlugin: ChannelPlugin<ResolvedAccount> = {
  id: "erp2-bridge",

  meta: {
    id: "erp2-bridge",
    label: "ERP2 Bridge",
    selectionLabel: "ERP2 Bridge (Java Backend via WebSocket)",
    detailLabel: "ERP2 Bridge",
    docsPath: "/channels/erp2-bridge",
    docsLabel: "erp2-bridge",
    blurb:
      "Connect OpenClaw to a Java backend server via a persistent WebSocket with " +
      "session isolation and uid/sessionKey identity metadata.",
    systemImage: "network",
    order: 90,
  },

  capabilities: {
    chatTypes: ["direct", "group"],
    media: false,
    blockStreaming: true,
  },

  config: {
    listAccountIds,
    resolveAccount,
    isConfigured: (account) => account.configured,
    isEnabled: (account) => account.enabled,
    defaultAccountId: (cfg) => {
      const channelCfg = (cfg as CoreConfig).channels?.["erp2-bridge"];
      return channelCfg?.defaultAccount?.trim() || "default";
    },
    describeAccount: (account) => ({
      accountId: account.accountId,
      configured: account.configured,
      enabled: account.enabled,
      extra: {
        wsUrl: account.wsUrl,
        tokenSource: account.tokenSource,
      },
    }),
  },

  configSchema: {
    schema: {
      type: "object",
      additionalProperties: false,
      properties: {
        enabled: { type: "boolean" },
        wsUrl: { type: "string", description: "WebSocket URL of the Java backend" },
        token: { type: "string", description: "Bearer token for Authorization header" },
        tokenFile: { type: "string", description: "Path to file containing the bearer token" },
        dmPolicy: { type: "string", enum: ["pairing", "open", "allowlist", "disabled"] },
        allowFrom: { type: "array", items: { type: "string" } },
        groupPolicy: { type: "string", enum: ["open", "allowlist", "disabled"] },
        groupAllowFrom: { type: "array", items: { type: "string" } },
        pingIntervalMs: { type: "integer", minimum: 1000 },
        reconnectBaseMs: { type: "integer", minimum: 100 },
        reconnectMaxMs: { type: "integer", minimum: 1000 },
        connectTimeoutMs: { type: "integer", minimum: 1000 },
        defaultAccount: { type: "string" },
        groups: {
          type: "object",
          additionalProperties: {
            type: "object",
            additionalProperties: false,
            properties: {
              requireMention: { type: "boolean" },
              allowFrom: { type: "array", items: { type: "string" } },
              systemPrompt: { type: "string" },
            },
          },
        },
        accounts: {
          type: "object",
          additionalProperties: {
            type: "object",
            additionalProperties: false,
            required: ["wsUrl"],
            properties: {
              enabled: { type: "boolean" },
              wsUrl: { type: "string" },
              token: { type: "string" },
              tokenFile: { type: "string" },
              dmPolicy: { type: "string", enum: ["pairing", "open", "allowlist", "disabled"] },
              allowFrom: { type: "array", items: { type: "string" } },
              groupPolicy: { type: "string", enum: ["open", "allowlist", "disabled"] },
              groupAllowFrom: { type: "array", items: { type: "string" } },
              pingIntervalMs: { type: "integer", minimum: 1000 },
              reconnectBaseMs: { type: "integer", minimum: 100 },
              reconnectMaxMs: { type: "integer", minimum: 1000 },
              connectTimeoutMs: { type: "integer", minimum: 1000 },
            },
          },
        },
      },
    },
  },

  gateway: {
    startAccount: async (ctx) => {
      const { account, cfg, abortSignal, log } = ctx;

      if (!account.configured) {
        log?.error?.(
          `[erp2-bridge/${account.accountId}] not configured – set channels.erp2-bridge.wsUrl`,
        );
        return;
      }

      let channelRuntime: PluginRuntime["channel"];
      try {
        channelRuntime = getWssRuntime().channel;
      } catch {
        log?.error?.("[erp2-bridge] PluginRuntime not initialized – register() may not have been called");
        return;
      }

      const logger = (level: "info" | "warn" | "error" | "debug", msg: string) => {
        log?.[level]?.(msg);
      };

      await monitorWssConnection({
        account,
        abortSignal,
        log: logger,
        onMessage: (msg, sendFn) =>
          handleInbound(msg, sendFn, account, cfg, channelRuntime, logger),
        onConnected: () => {
          ctx.setStatus({
            ...ctx.getStatus(),
            running: true,
            configured: true,
          });
        },
        onDisconnected: () => {
          if (!abortSignal.aborted) {
            ctx.setStatus({
              ...ctx.getStatus(),
              running: false,
            });
          }
        },
      });
    },
  },

  outbound: {
    deliveryMode: "direct",
    textChunkLimit: 4096,
    sendText: async (outboundCtx) => {
      const account = resolveAccount(outboundCtx.cfg, outboundCtx.accountId);
      if (!account.configured) {
        throw new Error("erp2-bridge not configured – set channels.erp2-bridge.wsUrl");
      }

      // Parse the 'to' target: expected format is "uid" or "uid:sessionKey"
      const [uid] = outboundCtx.to.includes(":")
        ? outboundCtx.to.split(":")
        : [outboundCtx.to];

      const outbound: WireOutboundMessage = {
        type: "CHAT",
        content: outboundCtx.text,
        sender: "openclaw",
        receiver: uid ?? outboundCtx.to,
        timestamp: Date.now(),
      };

      // Helper to record outbound activity (best-effort, ignore errors)
      const recordOutbound = () => {
        try {
          const runtime = getWssRuntime();
          runtime.channel.activity.record({
            channel: "erp2-bridge",
            accountId: account.accountId,
            direction: "outbound",
          });
        } catch {
          // Ignore if runtime not available
        }
      };

      // ── Bind uid ↔ sessionKey so proactive pushes reuse the existing session ──
      // Without this, OpenClaw sees the `to` uid with no session mapping and creates
      // a brand-new session for every outbound push, breaking conversation continuity.
      // We mirror what handleInbound does: resolveAgentRoute → recordInboundSession
      // (with updateLastRoute) so the uid→sessionKey association is persisted.
      const bindOutboundSession = async () => {
        try {
          const runtime = getWssRuntime();
          const channelRuntime = runtime.channel;
          const effectiveUid = uid ?? outboundCtx.to;

          const route = channelRuntime.routing.resolveAgentRoute({
            cfg: outboundCtx.cfg,
            channel: "erp2-bridge",
            accountId: account.accountId,
            peer: { kind: "direct", id: effectiveUid },
          });

          const storePath = channelRuntime.session.resolveStorePath(
            outboundCtx.cfg.session?.store,
            { agentId: route.agentId },
          );

          // Build a minimal ctx so recordInboundSession can persist the session
          // metadata and update the lastRoute index. We use the outbound text as
          // Body so the session record is non-empty but clearly outbound-initiated.
          const minCtx = channelRuntime.reply.finalizeInboundContext({
            Body: outboundCtx.text,
            BodyForAgent: outboundCtx.text,
            RawBody: outboundCtx.text,
            CommandBody: outboundCtx.text,
            From: `erp2-bridge:uid:${effectiveUid}:session:${route.sessionKey}`,
            To: `erp2-bridge:${effectiveUid}`,
            SessionKey: route.sessionKey,
            AccountId: route.accountId,
            ChatType: "direct" as const,
            ConversationLabel: effectiveUid,
            SenderName: effectiveUid,
            SenderId: effectiveUid,
            Provider: "erp2-bridge",
            Surface: "erp2-bridge",
            OriginatingChannel: "erp2-bridge",
            OriginatingTo: `erp2-bridge:${effectiveUid}`,
            UntrustedContext: [],
          });

          await channelRuntime.session.recordInboundSession({
            storePath,
            sessionKey: route.sessionKey,
            ctx: minCtx,
            updateLastRoute: {
              sessionKey: route.sessionKey,
              channel: "erp2-bridge",
              to: effectiveUid,
              accountId: route.accountId,
            },
          });
        } catch {
          // best-effort: session binding failure must not block message delivery
        }
      };

      // Reuse the existing persistent connection if available
      const existingSendFn = activeSendFns.get(account.accountId);
      if (existingSendFn) {
        existingSendFn(outbound);
        recordOutbound();
        await bindOutboundSession();
        return {
          channel: "erp2-bridge" as const,
          messageId: `erp2-${Date.now()}`,
          timestamp: outbound.timestamp,
        };
      }

      // Fallback: open a short-lived connection when the persistent one is not ready
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const WS: typeof import("ws").default = (() => {
        try {
          // eslint-disable-next-line @typescript-eslint/no-require-imports
          return require("ws");
        } catch {
          return (globalThis as unknown as { WebSocket: typeof import("ws").default }).WebSocket;
        }
      })();

      await new Promise<void>((resolve, reject) => {
        const headers: Record<string, string> = {};
        if (account.token) headers["Authorization"] = `Bearer ${account.token}`;
        const ws = new WS(account.wsUrl, { headers });
        const timeout = setTimeout(() => {
          ws.terminate?.();
          reject(new Error("erp2-bridge sendText timeout"));
        }, 10_000);
        ws.on("open", () => {
          ws.send(JSON.stringify(outbound), (err) => {
            clearTimeout(timeout);
            ws.close();
            if (err) reject(err);
            else {
              recordOutbound();
              resolve();
            }
          });
        });
        ws.on("error", (err: Error) => {
          clearTimeout(timeout);
          reject(err);
        });
      });

      await bindOutboundSession();

      return {
        channel: "erp2-bridge" as const,
        messageId: `erp2-${Date.now()}`,
        timestamp: outbound.timestamp,
      };
    },
  },

  // ── Messaging adapter ────────────────────────────────────────────────────────
  // ERP2 uid format (e.g. erp_private_2207154104) does not match any of the
  // core built-in id patterns (phone numbers, @/@# prefixes, known prefixes).
  // Registering looksLikeId = () => true tells OpenClaw to treat any non-empty
  // string as a direct peer ID and bypass the directory lookup, which would
  // otherwise produce an "Unknown target" error.
  messaging: {
    targetResolver: {
      looksLikeId: (raw: string): boolean => raw.trim().length > 0,
      hint: "uid of the ERP2 user (e.g. erp_private_2207154104)",
    },
  },

  security: {
    resolveDmPolicy: (ctx) => {
      const policy = ctx.account.config.dmPolicy ?? "pairing";
      return {
        policy,
        allowFrom: ctx.account.config.allowFrom ?? [],
        // allowFromPath and approveHint are used by the UI/CLI to show where to edit config
        allowFromPath: `channels.erp2-bridge${ctx.accountId && ctx.accountId !== "default" ? `.accounts.${ctx.accountId}` : ""}.allowFrom`,
        approveHint: `openclaw pairing approve erp2-bridge <code>`,
      };
    },
  },

  status: {
    buildChannelSummary: ({ account, snapshot }) => ({
      accountId: account.accountId,
      configured: account.configured,
      enabled: account.enabled,
      running: snapshot.running ?? false,
      wsUrl: account.wsUrl,
      tokenSource: account.tokenSource,
    }),
  },
};

/** Plugin registration entry point – called by the OpenClaw plugin loader. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export default function register(api: any): void {
  if (api.runtime) setWssRuntime(api.runtime);
  api.registerChannel({ plugin: wssBridgePlugin });
}

export { wssBridgePlugin };
