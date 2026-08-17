# supalite-replica

> Thin continuous SQLite replication + point-in-time restore for
> [`@supabase/lite`](https://www.npmjs.com/package/@supabase/lite), powered by an
> **operator-installed** [Litestream](https://litestream.io/). ~600 lines. No
> bundled binary, no manifest store, no fork engine, no CLI. Just the lifecycle
> you actually use.

`@jessevent/supalite-replica` wraps Litestream for a file-backed Supabase Lite
database: it writes a private config, starts `litestream replicate` against
your live DB, gives you `status()` / `flush()` / `close()`, and restores the
replica to a separate verified file. You install Litestream yourself
(`brew install litestream`, a GitHub release, or any build you trust) and point
the package at it.

---

## Use case

Supabase Lite lets you embed a full Postgres-flavoured SQLite database directly
in a Node.js process — no separate database server. That is great for
single-tenant deployments, edge appliances, agents, and local-first apps: the
database is a file on disk, reads/writes are in-process, and you get Supabase's
auth, migrations, and Postgres dialect for free.

What you do **not** get for free is durability beyond the host. If the machine
dies — disk failure, kernel panic, a botched deploy that overwrites the file,
an accidental `rm` — the file is the only copy. SQLite's WAL gives you crash
*consistency* (no torn writes on power loss), not *disaster recovery* (the
whole replica is one file on one disk).

This package closes that gap the way databases normally do: ship every WAL
change to a second copy somewhere else, as it happens, so recovery is a matter
of replaying it onto a fresh file. Concretely:

| Without `supalite-replica` | With `supalite-replica` |
|---|---|
| DB is one file on one disk | DB is mirrored to file/S3 in near-real-time |
| Host loss = total data loss | Host loss = restore to a new file, lose ≤ sync interval |
| Backups are your problem | `flush()` gives a named durable point on demand |
| No point-in-time recovery | `restoreReplica({ point: { txid } })` replays to any moment |
| Forks need a copy + re-init | Restore to a fresh file = an instant isolated fork |

It is deliberately a **library**, not a daemon or a platform. You call
`startReplica()` next to the connection you already open; you call `close()`
on shutdown; you call `restoreReplica()` when you need a recovered copy. The
one thing it does not do is ship a binary — Litestream is an operator concern
(which build, which version, where it runs), and forcing a specific one was the
trap that made prior attempts unshippable.

### When this is the wrong tool

- **You run a managed Postgres.** Use Supabase's hosted DB; this is for the
  embedded SQLite path.
- **`@supabase/lite` is not in your stack.** This package is a Lite-specific
  lifecycle; plain Litestream alone covers a vanilla SQLite DB.
- **You need multi-writer replication / sharding.** Litestream is a single
  source → one or more replicas; it is not a distributed database.
- **You want `@supabase/lite` to be production-stable today.** It is currently
  **alpha** (`0.8.x`). See [Status](#status) below.

---

## Install

```bash
bun add @jessevent/supalite-replica @supabase/lite
# Litestream is an operator dependency — install it yourself:
brew install litestream          # macOS
# or download a release: https://github.com/benbjohnson/litestream/releases
# or build from source (any build you trust)
```

Requires Node ≥ 22 (for the built-in `node:sqlite` that `@supabase/lite` uses).

---

## Quickstart

```ts
import { App } from "@supabase/lite";
import { createConnection } from "@supabase/lite/sqlite";
import { startReplica, restoreReplica } from "@jessevent/supalite-replica";

const databaseFile = "/var/lib/myapp/app.db";

const connection = await createConnection({
  url: `file:${databaseFile}`,
  ddlDialect: "postgres",
});

// 1. Establish your schema FIRST (see "Ordering" below), then start replicating.
const app = new App({ connection, auth: { enabled: true } });
await app.ensureSystemSchema();
await connection.createMigrator(SCHEMA).migrate();

// 2. Start replicating. durability:"full" (the default) sets synchronous=FULL,
//    so every commit your app observes is already durable on disk.
const controller = await startReplica({
  connection,
  databaseFile,
  replica: { type: "file", path: "/var/lib/myapp/replica" },
  // durability: "full",     // default
  // syncInterval: "1s",     // default; 100ms for tighter RPO
});

// 3. ...your app writes through `connection` as normal...

// 4. Force all in-flight WAL to the replica and get a durable recovery point:
const point = await controller.flush();
console.log("durable up to txid", point.txid);

await controller.close();
await connection.close();
```

### Restore to a separate, verified file

```ts
const result = await restoreReplica({
  databaseFile,                                            // the original source path
  replica: { type: "file", path: "/var/lib/myapp/replica" },
  target: "/var/lib/myapp/recovered.db",                   // must NOT already exist
  // point: "latest"                                              // default
  // point: { txid: "142" }                                       // point-in-time
  // point: { timestamp: "2026-08-09T12:00:00Z" }
});

console.log(result.target, result.integrity);   // integrity === "ok"
```

`restoreReplica` writes the recovered database to `target` (which must not
exist and must differ from the source), then reopens it read-only via
`node:sqlite` with `immutable=1` and asserts `PRAGMA quick_check` returns `ok`.
If the integrity check fails, it throws `RESTORE_INTEGRITY_FAILED` and cleans up.

### S3 instead of a local file replica

```ts
const controller = await startReplica({
  connection,
  databaseFile,
  replica: {
    type: "s3",
    url: "s3://my-bucket/myapp",
    region: "us-east-1",
    // endpoint: "http://minio:9000",   // optional — MinIO etc. (force-path-style)
  },
});
```

S3 credentials are read from the environment. The child process is given an
**allowlisted** environment — only `AWS_*`, `LITESTREAM_*`, `SSL_*`, `PATH`,
and `TMPDIR` pass through (plus `NO_COLOR`); everything else is stripped. No
secrets are written to the generated config or to error messages.

---

## What `startReplica` actually does

1. **Preflight** the connection:
   - verifies the main DB is file-backed and is the same file as `databaseFile`
     (realpath-canonicalized, so macOS `/var → /private/var` symlinks don't
     false-fire a mismatch),
   - checks the SQLite version against the [WAL-reset safe set](#the-one-safeguard-kept),
   - sets and verifies `journal_mode=WAL`, `foreign_keys=ON`, `busy_timeout=5000`,
     and `synchronous=FULL` (or `NORMAL`).
2. **Resolves the binary**: `binaryPath` (must be absolute and exist) else
   `litestream` on `PATH` (confirmed via `litestream version`).
3. Writes a private `litestream.yml` (mode `0600`) in a `0700` working directory,
   spawns `litestream replicate` with `shell:false` and the allowlisted env, and
   polls `litestream list -json -socket` until the database reports
   `status: "replicating"`.
4. Returns a `ReplicaController` with `status()`, `flush()`, `close()`, and
   `[Symbol.asyncDispose]`.

`close()` sends `SIGTERM`, waits for exit (or `SIGKILL`s after a timeout), and
removes the working directory. It is idempotent — safe to call in `finally`.

### The generated config

Exposed for inspection / CI as `litestreamConfig(options)`:

```yaml
socket:
  enabled: true
  path: "/tmp/.../control.sock"
  permissions: 0600
shutdown-sync-timeout: 10s
shutdown-sync-interval: 100ms
logging:
  type: text
  level: info
dbs:
  - path: "/var/lib/myapp/app.db"
    restore-if-db-not-exists: false
    monitor-interval: 100ms
    checkpoint-interval: 1s
    busy-timeout: 5s
    replica:
      type: file
      path: "/var/lib/myapp/replica"
      sync-interval: 1s
      auto-recover: false
```

---

## The one safeguard kept

The SQLite [WAL-reset defect](https://www.sqlite.org/changes.html) corrupted
backups on `CHECKPOINT` for builds between `3.44.5` and the fixes in `3.44.6`,
`3.50.7`, `3.51.3` (and `3.52+`). Replicating against an affected build yields
**silently corrupt** backups — the worst failure mode, because `quick_check`
passes on the source but the replica is wrong. `startReplica` rejects with
`UNSAFE_SQLITE_VERSION` unless the detected version is in the safe set.

If you know your build is patched (a custom Litestream build, a vendor backport),
opt out explicitly:

```ts
await startReplica({
  connection, databaseFile, replica,
  unsafe: { allowUnpatchedSqlite: true },
});
```

Everything else from heavier attempts — binary provenance / certification, a
manifest / lineage store, a fork-and-scrubber, redaction tokens, a DI seam, a
CLI — is intentionally absent. Those exist when you need them; this package is
the lifecycle + restore you use every time.

---

## Ordering: migrate before `startReplica`

Litestream writes `_litestream_seq` / `_litestream_lock` tracking tables into
the source DB on its first sync. Run your schema migration **before**
`startReplica` so the migrator's data-loss guard never sees those tables. Once
replication is running, don't re-run a full migration (the guard will flag the
litestream tables for drop). This is shown in the Quickstart above.

---

## Verifying the drill

`tests/kill-and-restore.test.ts` proves the package end-to-end, not with mocks:

1. Spawns a real `@supabase/lite` app (`tests/fixtures/app.ts`) writing a stream
   of committed rows to `app.db` while Litestream replicates to `./replica`.
2. `SIGKILL`s the **whole process group** (Node app + Litestream sidecar)
   mid-write.
3. Resumes the same replica chain via this package's own `startReplica`, and
   flushes to capture the surviving on-disk WAL.
4. Calls `restoreReplica` to a fresh `restored.db`.
5. Asserts the source and the restored file both pass `PRAGMA quick_check`, and
   that `source == restored == the contiguous 1..N committed prefix`.

```bash
pnpm install
pnpm test
```

The drill resolves Litestream the same way the library does — the
`LITESTREAM_BINARY` env var, else `litestream` on `PATH` — and **skips** (not
fails) if neither is available. So a bare `pnpm test` with no Litestream
installed still passes: the config unit tests run, the drill skips.

To run the drill for real, ensure a WAL-reset-safe Litestream is on `PATH`
(`litestream version` should report SQLite `>= 3.51.3`), or point at one
explicitly:

```bash
LITESTREAM_BINARY=/path/to/litestream pnpm test
```

---

## API

```ts
startReplica(options: StartReplicaOptions): Promise<ReplicaController>
restoreReplica(options: RestoreOptions): Promise<RestoreResult>
litestreamConfig(options): string   // the generated YAML, for inspection / CI
```

| Option | `startReplica` | `restoreReplica` |
|---|---|---|
| `connection` | `SqliteConnection` (file-backed) | — |
| `databaseFile` | source DB path | source DB path |
| `replica` | `{ type: "file" \| "s3", ... }` | same |
| `target` | — | restore destination (must not exist) |
| `binaryPath` | absolute path, else `litestream` on PATH | same |
| `point` | — | `"latest"` \| `{ txid }` \| `{ timestamp }` |
| `durability` | `"full"` (default) \| `"normal"` | — |
| `syncInterval` | Go duration, default `"1s"` | — |
| `unsafe.allowUnpatchedSqlite` | bypass WAL-reset guard | — |
| `onEvent` | lifecycle event callback | — |

`ReplicaController`: `{ status(), flush(timeoutMs?), close(timeoutMs?), [Symbol.asyncDispose] }`.

Errors are `ReplicaError` instances with a `code`: `BINARY_NOT_FOUND`,
`UNSAFE_SQLITE_VERSION`, `DATABASE_PATH_MISMATCH`, `FILE_BACKED_REQUIRED`,
`PRAGMA_FAILED`, `LITESTREAM_TIMEOUT`, `LITESTREAM_SPAWN_FAILED`,
`LITESTREAM_EXIT`, `INVALID_CONFIG`, `FLUSH_FAILED`, `REPLICA_STOPPED`,
`INVALID_TARGET`, `RESTORE_INTEGRITY_FAILED`, `SQLITE_PROBE_FAILED`. No
secrets appear in error messages.

---

## Examples

Usage patterns built on the library API above, not new library surface —
plain scripts in [`examples/`](./examples), each independently useful:

| Script | What it does |
|---|---|
| [`hosted.mjs`](./examples/hosted.mjs) | Write path: boots a local writable connection, restoring from the replica if it already has data (else runs your `migrate` once), then `startReplica`s so every commit ships back out. The complement of a read-only replica: this is how you treat S3 as the durable home of a database you actively write to. |
| [`bunnyhop.mjs`](./examples/bunnyhop.mjs) | Read path: polls `restoreReplica` on an interval and atomically promotes each fresh snapshot over a stable path — an always-current local copy, no writes, no load on the live database. |
| [`analytics.mjs`](./examples/analytics.mjs) | Queries a snapshot (e.g. `bunnyhop.mjs`'s promoted file) with DuckDB instead of plain SQL — aggregates, regex, window functions — by shelling out to an operator-installed `duckdb` binary, same trust model this package already uses for Litestream. |

None of these add a dependency or touch `src/` — they're thin compositions
of `startReplica`/`restoreReplica`, kept out of the library itself for the
same reason listed under [Contributing](#contributing).

---

## Status

**Alpha / 0.1.0.** This package targets `@supabase/lite@0.8.x` as a peer
dependency, and `@supabase/lite` is itself alpha (~2.4k downloads/week). The
lifecycle and the kill-and-restore drill are proven against `@supabase/lite`
0.8.0 + Litestream 0.5.16, but a breaking change in Lite's `connection.exec` or
migrator shape would break this package. Treat it as code to evaluate and
contribute to, not as a production dependency — yet. When `@supabase/lite`
stabilizes past alpha, this moves to `1.0.0` and the `private` flag is lifted
for npm publishing.

## Contributing

Bug reports and reproducible failing cases are welcome via Issues. PRs that
keep the package thin are preferred: a new feature is in scope only if it
concerns the replicate/restore lifecycle itself. Re-adding a binary supply
chain, a manifest store, a fork engine, or a CLI is out of scope for this
package — those belong in a companion package, not here.

## License

[MIT](./LICENSE) © Jesse Vent