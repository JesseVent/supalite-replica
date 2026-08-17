/**
 * @jessevent/supalite-replica — thin continuous SQLite replication for @supabase/lite.
 *
 * Wraps an operator-installed Litestream binary: it writes a private Litestream
 * config, starts `litestream replicate` against the live file-backed SQLite
 * database, exposes status/flush/close, and restores the replica to a separate
 * verified file. The package does NOT bundle or download Litestream — install it
 * yourself (`brew install litestream`, download a release, or build one) and
 * either put it on PATH or pass `binaryPath`.
 *
 * Deliberately thin: no binary provenance, no manifest/lineage store, no fork/
 * scrubber, no CLI. Those exist in the full `@jessevent/supabase-lite-replica`
 * package if you need them; this one is the lifecycle + restore you actually use.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, realpathSync, statSync } from "node:fs";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { SqliteConnection } from "@supabase/lite";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type ReplicaDestination =
  | { type: "file"; path: string }
  | { type: "s3"; url: string; endpoint?: string; region?: string };

export interface StartReplicaOptions {
  /** Live file-backed Supalite connection. The controller never closes it. */
  connection: SqliteConnection;
  /** Canonical path of the connection's main database file. */
  databaseFile: string;
  replica: ReplicaDestination;
  /** Absolute path to a `litestream` binary. Defaults to `litestream` on PATH. */
  binaryPath?: string;
  /** Litestream sync interval as a Go duration string ("100ms", "1s"). Default "1s". */
  syncInterval?: string;
  /** Commit durability. "full" (default) sets synchronous=FULL; "normal" sets NORMAL. */
  durability?: "full" | "normal";
  /** Acknowledge that the running SQLite may be a WAL-reset-affected build. */
  unsafe?: { allowUnpatchedSqlite?: boolean };
  /** Observer for lifecycle/health events. */
  onEvent?: (event: ReplicaEvent) => void;
}

export type ReplicaEvent =
  | { type: "started"; binaryVersion: string }
  | { type: "degraded"; reason: string }
  | { type: "recovered" }
  | { type: "stopped"; reason: "closed" | "crashed" | "timeout" };

export type ReplicaState = "replicating" | "degraded" | "stopped";

export interface ReplicaStatus {
  state: ReplicaState;
  /** RFC 3339 timestamp of the last successful remote sync, if any. */
  lastSyncAt?: string;
  /** Current WAL file size in bytes (0 if none). */
  walBytes: number;
  binaryVersion: string;
}

export interface ReplicaPoint {
  /** Decimal durable remote TXID reported by `litestream sync -wait`. */
  txid: string;
}

export interface ReplicaController {
  status(): Promise<ReplicaStatus>;
  /** Force a synchronous sync and return the acknowledged durable point. */
  flush(timeoutMs?: number): Promise<ReplicaPoint>;
  /** SIGTERM the sidecar, wait, SIGKILL if needed, remove the private config. Idempotent. */
  close(timeoutMs?: number): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

export interface RestoreOptions {
  /** Canonical path of the source database that was replicated. */
  databaseFile: string;
  replica: ReplicaDestination;
  /** Restore target. Must not exist and must not be the source. */
  target: string;
  binaryPath?: string;
  /** Recovery point. Omit for latest. */
  point?: "latest" | { txid: string } | { timestamp: string };
}

export interface RestoreResult {
  target: string;
  /** SQLite integrity result; always "ok" (restore rejects otherwise). */
  integrity: "ok";
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class ReplicaError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(`${code}: ${message}`);
    this.name = "ReplicaError";
    this.code = code;
  }
}

// ---------------------------------------------------------------------------
// SQLite WAL-reset safety (the one rigor worth keeping: silent corrupt backups)
// ---------------------------------------------------------------------------

function parseSqliteVersion(version: string): { major: number; minor: number; patch: number } | undefined {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
  if (match === null || match[1] === undefined || match[2] === undefined || match[3] === undefined) {
    return undefined;
  }
  const major = Number.parseInt(match[1], 10);
  const minor = Number.parseInt(match[2], 10);
  const patch = Number.parseInt(match[3], 10);
  return Number.isSafeInteger(major) && Number.isSafeInteger(minor) && Number.isSafeInteger(patch)
    ? { major, minor, patch }
    : undefined;
}

/**
 * SQLite WAL-reset defect was fixed in 3.44.6, 3.50.7, 3.51.3 and all 3.52+.
 * See https://github.com/litestream/litestream and SQLite changelogs.
 */
function isWalResetSafe(version: string): boolean {
  const v = parseSqliteVersion(version);
  if (v === undefined || v.major !== 3 || v.minor < 7) return false;
  if (v.minor > 51) return true;
  if (v.minor === 51) return v.patch >= 3;
  if (v.minor === 50) return v.patch >= 7;
  if (v.minor === 44) return v.patch >= 6;
  return false;
}

// ---------------------------------------------------------------------------
// Supalite connection preflight via the public exec() surface
// ---------------------------------------------------------------------------

async function queryOne(connection: SqliteConnection, statement: string, field: string): Promise<unknown> {
  const result = await connection.exec<Record<string, unknown>>(statement);
  const rows = (result && (result as { rows?: unknown }).rows) ?? undefined;
  if (!Array.isArray(rows) || rows.length !== 1) {
    throw new ReplicaError("SQLITE_PROBE_FAILED", "unexpected probe result");
  }
  const row = rows[0];
  if (typeof row !== "object" || row === null || !Object.hasOwn(row, field)) {
    throw new ReplicaError("SQLITE_PROBE_FAILED", `row missing field '${field}'`);
  }
  return (row as Record<string, unknown>)[field];
}

async function preflightConnection(
  connection: SqliteConnection,
  databaseFile: string,
  durability: "full" | "normal",
  allowUnsafeSqlite: boolean,
): Promise<void> {
  const actualFile = await queryOne(connection, "SELECT file AS file FROM pragma_database_list WHERE name = 'main'", "file");
  if (typeof actualFile !== "string" || actualFile.length === 0) {
    throw new ReplicaError("FILE_BACKED_REQUIRED", "replication needs a file-backed main database");
  }
  // realpathSync collapses symlinks (macOS /var → /private/var); path.resolve
  // does not, so pragma_database_list's realpath and a tmpdir()-derived path
  // would otherwise mismatch even when they name the same file.
  if (realpathSync(databaseFile) !== realpathSync(actualFile)) {
    throw new ReplicaError("DATABASE_PATH_MISMATCH", "databaseFile does not match the connection's main database");
  }

  const version = await queryOne(connection, "SELECT sqlite_version() AS version", "version");
  if (typeof version !== "string") {
    throw new ReplicaError("SQLITE_PROBE_FAILED", "sqlite_version() did not return a string");
  }
  if (!isWalResetSafe(version) && !allowUnsafeSqlite) {
    throw new ReplicaError(
      "UNSAFE_SQLITE_VERSION",
      `SQLite ${version} is affected by the WAL-reset defect; upgrade to >=3.51.3 or set unsafe.allowUnpatchedSqlite`,
    );
  }

  const synchronousValue = durability === "full" ? 2 : 1;
  const checks: Array<[set: string, read: string, field: string, expected: unknown]> = [
    ["PRAGMA journal_mode=WAL", "SELECT journal_mode FROM pragma_journal_mode", "journal_mode", "wal"],
    ["PRAGMA foreign_keys=ON", "SELECT foreign_keys FROM pragma_foreign_keys", "foreign_keys", 1],
    ["PRAGMA busy_timeout=5000", "SELECT timeout FROM pragma_busy_timeout", "timeout", 5_000],
    [`PRAGMA synchronous=${durability === "full" ? "FULL" : "NORMAL"}`, "SELECT synchronous FROM pragma_synchronous", "synchronous", synchronousValue],
  ];
  for (const [set, read, field, expected] of checks) {
    await connection.exec(set);
    const got = await queryOne(connection, read, field);
    if (got !== expected) {
      throw new ReplicaError("PRAGMA_FAILED", `could not set/verify '${set}' (got ${JSON.stringify(got)}, expected ${JSON.stringify(expected)})`);
    }
  }
}

// ---------------------------------------------------------------------------
// Litestream subprocess helpers
// ---------------------------------------------------------------------------

const MAX_OUTPUT = 64 * 1024;

/** Environment passed to every Litestream child: provider creds + minimum runtime vars. */
function childEnvironment(databaseFile: string, workDir: string): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { NO_COLOR: "1", TMPDIR: workDir };
  for (const [name, value] of Object.entries(process.env)) {
    if (value !== undefined && /^(AWS_|LITESTREAM_|SSL_|PATH)$/.test(name)) {
      env[name] = value;
    }
  }
  return env;
}

interface RunResult {
  stdout: string;
  stderr: string;
}

function runOnce(binary: string, args: string[], env: NodeJS.ProcessEnv, timeoutMs: number): Promise<RunResult> {
  return new Promise((resolveP, rejectP) => {
    const child = spawn(binary, args, { env, stdio: ["ignore", "pipe", "pipe"], shell: false });
    let stdout = "";
    let stderr = "";
    let done = false;
    const timer = setTimeout(() => {
      if (!done) {
        done = true;
        child.kill("SIGKILL");
        rejectP(new ReplicaError("LITESTREAM_TIMEOUT", `${binary} ${args.join(" ")} exceeded ${timeoutMs}ms`));
      }
    }, timeoutMs);
    child.stdout.on("data", (c: Buffer) => { stdout = (stdout + c.toString("utf8")).slice(-MAX_OUTPUT); });
    child.stderr.on("data", (c: Buffer) => { stderr = (stderr + c.toString("utf8")).slice(-MAX_OUTPUT); });
    child.once("error", (err) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      rejectP(new ReplicaError("LITESTREAM_SPAWN_FAILED", `could not run '${binary}' — ${err.message}`));
    });
    child.once("exit", (code) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      if (code !== 0) {
        rejectP(new ReplicaError("LITESTREAM_EXIT", `${binary} ${args.join(" ")} exited ${code}\n${stderr}`));
        return;
      }
      resolveP({ stdout, stderr });
    });
  });
}

function tryJson(stdout: string): unknown | undefined {
  const candidates = [stdout.trim()];
  for (const m of stdout.matchAll(/(?:^|\n)(?=[\[{])/g)) {
    const off = m.index + (m[0] === "\n" ? 1 : 0);
    candidates.push(stdout.slice(off).trim());
  }
  for (const c of candidates) {
    try {
      return JSON.parse(c);
    } catch {
      // litestream may emit log lines before its JSON payload
    }
  }
  return undefined;
}

// `litestream list -json` returns `{ databases: [{ path, status, last_sync_at }] }`.
// Accept a bare array too in case a build deviates.
function listDatabaseEntry(parsed: unknown): Record<string, unknown> | undefined {
  const candidates: unknown[] = [];
  if (parsed !== null && typeof parsed === "object" && "databases" in parsed) {
    const databases = (parsed as { databases?: unknown }).databases;
    if (Array.isArray(databases)) candidates.push(...databases);
  } else if (Array.isArray(parsed)) {
    candidates.push(...parsed);
  }
  const entry = candidates[0];
  return typeof entry === "object" && entry !== null ? (entry as Record<string, unknown>) : undefined;
}

// ---------------------------------------------------------------------------
// Litestream config generation (file-backed proven shape, generalized to s3)
// ---------------------------------------------------------------------------

function yamlString(value: string): string {
  return JSON.stringify(value);
}

function generateConfig(opts: {
  databaseFile: string;
  replica: ReplicaDestination;
  socketPath: string;
  syncInterval: string;
}): string {
  const lines: string[] = [
    "socket:",
    "  enabled: true",
    `  path: ${yamlString(opts.socketPath)}`,
    "  permissions: 0600",
    "",
    "shutdown-sync-timeout: 10s",
    "shutdown-sync-interval: 100ms",
    "",
    "logging:",
    "  type: text",
    "  level: info",
    "",
    "dbs:",
    `  - path: ${yamlString(opts.databaseFile)}`,
    "    restore-if-db-not-exists: false",
    "    monitor-interval: 100ms",
    "    checkpoint-interval: 1s",
    "    busy-timeout: 5s",
    "    replica:",
    `      type: ${opts.replica.type}`,
  ];
  if (opts.replica.type === "file") {
    lines.push(`      path: ${yamlString(opts.replica.path)}`);
  } else {
    lines.push(`      url: ${yamlString(opts.replica.url)}`);
    if (opts.replica.endpoint !== undefined) {
      lines.push(`      endpoint: ${yamlString(opts.replica.endpoint)}`, "      force-path-style: true");
    }
    if (opts.replica.region !== undefined) {
      lines.push(`      region: ${yamlString(opts.replica.region)}`);
    }
    lines.push("      skip-verify: false");
  }
  lines.push(`      sync-interval: ${opts.syncInterval}`, "      auto-recover: false", "");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Binary resolution
// ---------------------------------------------------------------------------

async function resolveBinary(binaryPath: string | undefined): Promise<string> {
  const candidate = binaryPath ?? "litestream";
  if (binaryPath !== undefined) {
    if (!isAbsolute(binaryPath) || !existsSync(binaryPath)) {
      throw new ReplicaError("BINARY_NOT_FOUND", `binaryPath '${binaryPath}' is not an existing absolute path`);
    }
    return binaryPath;
  }
  // Bare "litestream": confirm it actually runs. spawn resolves via PATH.
  try {
    await runOnce("litestream", ["version"], { ...childEnvironment("", ""), NO_COLOR: "1" }, 5_000);
  } catch {
    throw new ReplicaError(
      "BINARY_NOT_FOUND",
      "litestream is not on PATH; install it (e.g. `brew install litestream`) or pass binaryPath",
    );
  }
  return candidate;
}

// ---------------------------------------------------------------------------
// startReplica
// ---------------------------------------------------------------------------

export async function startReplica(options: StartReplicaOptions): Promise<ReplicaController> {
  const databaseFile = resolve(options.databaseFile);
  const syncInterval = options.syncInterval ?? "1s";
  const durability = options.durability ?? "full";
  const allowUnsafeSqlite = options.unsafe?.allowUnpatchedSqlite === true;

  if (syncInterval.length === 0) {
    throw new ReplicaError("INVALID_CONFIG", "syncInterval must be a non-empty Go duration");
  }
  if (options.replica.type === "file" && resolve(options.replica.path) === databaseFile) {
    throw new ReplicaError("INVALID_CONFIG", "file replica path must differ from the source database");
  }

  await preflightConnection(options.connection, databaseFile, durability, allowUnsafeSqlite);

  const binary = await resolveBinary(options.binaryPath);
  const versionResult = await runOnce(binary, ["version"], { ...childEnvironment("", ""), NO_COLOR: "1" }, 5_000);
  const binaryVersion = versionResult.stdout.trim().split("\n")[0] ?? "unknown";

  // Private working directory + config + socket, beside the database's directory
  // (kept short so the unix socket path stays under the platform limit).
  const workDir = await mkdtemp(join(tmpdir(), "supalite-replica-"));
  await chmod(workDir, 0o700);
  const socketPath = join(workDir, "control.sock");
  const configPath = join(workDir, "litestream.yml");
  const config = generateConfig({ databaseFile, replica: options.replica, socketPath, syncInterval });
  await writeFile(configPath, config, { encoding: "utf8", mode: 0o600 });
  await chmod(configPath, 0o600);

  const env = childEnvironment(databaseFile, workDir);
  const child = spawn(binary, ["replicate", "-no-expand-env", "-config", configPath, "-log-level", "info"], {
    env,
    stdio: ["ignore", "pipe", "pipe"],
    shell: false,
  });

  let stderr = "";
  child.stderr.on("data", (c: Buffer) => { stderr = (stderr + c.toString("utf8")).slice(-MAX_OUTPUT); });

  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((res, rej) => {
    child.once("error", rej);
    child.once("exit", (code, signal) => res({ code, signal }));
  });

  let state: ReplicaState = "replicating";
  let lastSyncAt: string | undefined;
  let stopped = false;
  const emit = options.onEvent;

  // Wait until the daemon reports the database as replicating.
  try {
    const deadline = Date.now() + 15_000;
    let ready = false;
    while (Date.now() < deadline && !ready) {
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new ReplicaError("LITESTREAM_EXIT", `sidecar exited before ready\n${stderr}`);
      }
      try {
        const res = await runOnce(binary, ["list", "-json", "-socket", socketPath, "-timeout", "1"], childEnvironment(databaseFile, workDir), 2_000);
        const entry = listDatabaseEntry(tryJson(res.stdout));
        if (entry !== undefined && entry.status === "replicating") {
          ready = true;
          lastSyncAt = typeof entry.last_sync_at === "string" ? entry.last_sync_at : undefined;
        }
      } catch {
        // not ready yet
      }
      if (!ready) await new Promise((r) => setTimeout(r, 100));
    }
    if (!ready) throw new ReplicaError("LITESTREAM_TIMEOUT", `sidecar did not reach 'replicating' within 15s\n${stderr}`);
  } catch (err) {
    if (!stopped && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await rm(workDir, { recursive: true, force: true }).catch(() => undefined);
    throw err;
  }

  emit?.({ type: "started", binaryVersion });

  async function readStatus(): Promise<ReplicaStatus> {
    if (stopped) {
      return { state: "stopped", walBytes: walSize(), binaryVersion };
    }
    try {
      const res = await runOnce(binary, ["list", "-json", "-socket", socketPath, "-timeout", "1"], childEnvironment(databaseFile, workDir), 2_000);
      const entry = listDatabaseEntry(tryJson(res.stdout));
      if (entry !== undefined) {
        const replicating = entry.status === "replicating";
        const sync = typeof entry.last_sync_at === "string" ? entry.last_sync_at : lastSyncAt;
        const newState: ReplicaState = replicating ? "replicating" : "degraded";
        if (newState === "degraded" && state === "replicating") {
          state = "degraded";
          emit?.({ type: "degraded", reason: `litestream status: ${String(entry.status)}` });
        } else if (newState === "replicating" && state === "degraded") {
          state = "replicating";
          emit?.({ type: "recovered" });
        }
        lastSyncAt = sync;
        const out: ReplicaStatus = { state, walBytes: walSize(), binaryVersion };
        if (sync !== undefined) out.lastSyncAt = sync;
        return out;
      }
    } catch {
      // fall through to stopped check
    }
    if (child.exitCode !== null || child.signalCode !== null) {
      if (state !== "stopped") {
        state = "stopped";
        emit?.({ type: "stopped", reason: "crashed" });
      }
      return { state: "stopped", walBytes: walSize(), binaryVersion };
    }
    return { state: "degraded", walBytes: walSize(), binaryVersion };
  }

  function walSize(): number {
    try {
      return statSync(`${databaseFile}-wal`).size;
    } catch {
      return 0;
    }
  }

  async function flush(timeoutMs = 15_000): Promise<ReplicaPoint> {
    if (stopped) throw new ReplicaError("REPLICA_STOPPED", "cannot flush a stopped replica");
    const seconds = String(Math.max(1, Math.ceil(timeoutMs / 1_000)));
    const res = await runOnce(
      binary,
      ["sync", "-wait", "-timeout", seconds, "-json", "-socket", socketPath, databaseFile],
      childEnvironment(databaseFile, workDir),
      timeoutMs,
    );
    const parsed = tryJson(res.stdout) as Record<string, unknown> | undefined;
    // Litestream emits `replica_txid` as a raw JSON number; coerce to string.
    const raw = parsed?.replica_txid;
    const txid = typeof raw === "number" ? String(raw) : typeof raw === "string" ? raw : undefined;
    if (txid === undefined || txid === "0") {
      throw new ReplicaError("FLUSH_FAILED", `sync did not report a durable point\n${res.stdout}\n${res.stderr}`);
    }
    return { txid };
  }

  async function close(timeoutMs = 10_000): Promise<void> {
    if (stopped) return;
    stopped = true;
    state = "stopped";
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGTERM");
      let timer: NodeJS.Timeout | undefined;
      try {
        await Promise.race([
          exit,
          new Promise<never>((_, rej) => {
            timer = setTimeout(() => rej(new Error("shutdown timeout")), timeoutMs);
          }),
        ]);
      } catch {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
        emit?.({ type: "stopped", reason: "timeout" });
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
      emit?.({ type: "stopped", reason: "closed" });
    } else {
      emit?.({ type: "stopped", reason: "crashed" });
    }
    await rm(workDir, { recursive: true, force: true }).catch(() => undefined);
  }

  return Object.freeze({
    status: readStatus,
    flush,
    close,
    [Symbol.asyncDispose]: () => close(),
  }) as ReplicaController;
}

// ---------------------------------------------------------------------------
// restoreReplica
// ---------------------------------------------------------------------------

export async function restoreReplica(options: RestoreOptions): Promise<RestoreResult> {
  const databaseFile = resolve(options.databaseFile);
  const target = resolve(options.target);
  if (target === databaseFile) {
    throw new ReplicaError("INVALID_TARGET", "restore target must not be the source database");
  }
  if (existsSync(target)) {
    throw new ReplicaError("INVALID_TARGET", "restore target already exists (pass a fresh path)");
  }
  const binary = await resolveBinary(options.binaryPath);

  const workDir = await mkdtemp(join(tmpdir(), "supalite-restore-"));
  await chmod(workDir, 0o700);
  const socketPath = join(workDir, "control.sock");
  const configPath = join(workDir, "litestream.yml");
  const config = generateConfig({
    databaseFile,
    replica: options.replica,
    socketPath,
    syncInterval: "1s",
  });
  await writeFile(configPath, config, { encoding: "utf8", mode: 0o600 });
  const env = childEnvironment(databaseFile, workDir);

  try {
    const args = ["restore", "-no-expand-env", "-config", configPath, "-o", target];
    if (options.point !== undefined && options.point !== "latest") {
      if ("txid" in options.point) args.push("-txid", options.point.txid);
      else args.push("-timestamp", options.point.timestamp);
    }
    args.push(databaseFile);
    await runOnce(binary, args, env, 60_000);

    // Verify the restored file in isolation (no WAL sidecars via immutable read).
    const db = new DatabaseSync(`file:${target}?immutable=1`, { readOnly: true });
    try {
      const row = db.prepare("PRAGMA quick_check").get() as { quick_check?: unknown };
      if (row?.quick_check !== "ok") {
        await rm(target, { force: true }).catch(() => undefined);
        throw new ReplicaError("RESTORE_INTEGRITY_FAILED", `PRAGMA quick_check returned '${String(row?.quick_check)}'`);
      }
    } finally {
      db.close();
    }
    return { target, integrity: "ok" };
  } finally {
    await rm(workDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

// Re-export for callers that want to reuse the proven config shape.
export { generateConfig as litestreamConfig };