#!/usr/bin/env node
/**
 * analytics — query a SQLite snapshot with DuckDB, via the operator-installed
 * `duckdb` CLI. Same binary-shelling pattern this package already uses for
 * Litestream — no new npm dependency, no bundled DuckDB build.
 *
 * This does NOT restore or refresh anything — point it at whatever
 * bunnyhop.mjs (or hosted.mjs) is already keeping fresh on disk. It composes
 * with those; it doesn't replace them. Reads are read-only (ATTACH ...
 * READ_ONLY) so this is always safe to point at a file another process is
 * actively writing/replicating.
 *
 * Usage:
 *   import { queryAnalytics } from "./examples/analytics.mjs";
 *   const rows = await queryAnalytics("SELECT count(*) FROM db.items", {
 *     databaseFile: "/var/lib/myapp/current.db",
 *   });
 *
 * `sql` must be a single statement — DuckDB's `-json` mode prints one JSON
 * array per statement with no separator, so multiple statements can't be
 * told apart from stdout alone.
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export async function queryAnalytics(sql, { databaseFile, alias = "db", binaryPath = "duckdb" } = {}) {
  const script = `ATTACH '${databaseFile}' AS ${alias} (TYPE sqlite, READ_ONLY);\n${sql}`;
  const { stdout } = await execFileAsync(binaryPath, ["-json", "-c", script]);
  return stdout.trim() ? JSON.parse(stdout) : [];
}
