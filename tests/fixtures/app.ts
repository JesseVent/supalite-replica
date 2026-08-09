/**
 * Kill-and-restore drill app for @jessevent/supalite-replica.
 *
 * A minimal "real" Supabase Lite app: file-backed connection → startReplica →
 * App + schema → write a bounded stream of committed rows, reporting each
 * commit as NDJSON on stdout. The drill test spawns this in its own process
 * group, SIGKILLs it mid-write, then proves the on-disk committed prefix is
 * fully recoverable from the replica.
 *
 * Run by the drill via `node --import tsx tests/fixtures/app.ts <data-directory>`.
 */
import { join } from "node:path";

import { App } from "@supabase/lite";
import { createConnection } from "@supabase/lite/sqlite";

import { startReplica } from "../../src/index.ts";

const SCHEMA = `
create table public.items (
  id integer primary key,
  payload text not null
);
`;

async function main(): Promise<void> {
  const root = process.argv[2];
  if (root === undefined) {
    throw new Error("Usage: app.ts <data-directory>");
  }
  const binaryPath = process.env.LITESTREAM_BINARY; // undefined → litestream on PATH
  const databaseFile = join(root, "app.db");

  const connection = await createConnection({
    url: `file:${databaseFile}`,
    ddlDialect: "postgres",
  });

  // Migrate before starting replication: Litestream writes `_litestream_seq`/
  // `_litestream_lock` tracking tables into the source DB on its first sync, and
  // the migrator's data-loss guard would flag dropping those as destructive.
  // Establishing the schema first means the migrator never re-runs after they
  // appear. (The full package sidesteps this with a looser, pre-sync readiness.)
  const app = new App({ connection, auth: { enabled: true } });
  await app.ensureSystemSchema();
  await connection.createMigrator(SCHEMA).migrate();

  const controller = await startReplica({
    connection,
    databaseFile,
    replica: { type: "file", path: join(root, "replica") },
    syncInterval: "100ms",
    durability: "full",
    ...(binaryPath !== undefined ? { binaryPath } : {}),
  });

  process.stdout.write(`${JSON.stringify({ type: "ready", databaseFile })}\n`);

  // Wait for the drill to signal "start writing".
  process.stdin.setEncoding("utf8");
  process.stdin.resume();
  await new Promise<void>((resolve) => {
    process.stdin.once("data", () => resolve());
  });

  for (let id = 1; id <= 1_000; id += 1) {
    await connection.exec(`INSERT INTO items (id, payload) VALUES (${id}, 'row-${id}')`);
    process.stdout.write(`${JSON.stringify({ type: "committed", id })}\n`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  await controller.flush(15_000);
  await controller.close(10_000);
  await connection.close();
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});