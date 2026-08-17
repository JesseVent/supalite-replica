#!/usr/bin/env node
/**
 * hosted — the write-side complement to bunnyhop.mjs. S3 is the durable home
 * of the database; this boots a local writable copy from it (or creates one,
 * on a true first boot), and keeps replicating local writes back to S3 as
 * they happen. Your app writes to `connection` exactly like a plain local
 * SQLite app — S3 durability is transparent from there.
 *
 * Boot sequence:
 *   1. databaseFile already present locally (this host has run before)?
 *      -> use it as-is, skip straight to startReplica.
 *   2. Otherwise, try restoring the latest snapshot from `replica` into
 *      databaseFile. Success -> a previous host already wrote data to S3;
 *      schema + rows are already there, do NOT run `migrate`.
 *   3. Restore failed (true first boot, replica has nothing yet) -> create
 *      the connection fresh and run the caller's `migrate(connection)`.
 *   4. Either way, startReplica() so every subsequent commit ships to S3.
 *
 * ponytail: any restore failure in step 2 (including a transient network/S3
 * error, not just "no generations found yet") falls through to "treat as
 * first boot". Litestream doesn't give restoreReplica a distinct error code
 * for "replica is genuinely empty" vs. "couldn't reach S3 this time" (both
 * surface as LITESTREAM_EXIT) — safe on a real fresh replica, but a flaky
 * network at boot could start a second, divergent generation on top of real
 * data instead of retrying. Add stderr sniffing or a retry-before-fallback
 * if that risk matters for your deployment.
 *
 * Usage (as a library — this package is a library, not a daemon, and
 * `migrate` is app-specific, so there's no standalone CLI form):
 *
 *   import { openHostedConnection } from "./examples/hosted.mjs";
 *
 *   const { connection, controller } = await openHostedConnection({
 *     databaseFile: "/data/app.db",              // stable across boots
 *     replica: { type: "s3", url: "s3://bucket/myapp", region: "us-east-1" },
 *     migrate: (connection) => connection.createMigrator(SCHEMA).migrate(),
 *   });
 *
 *   // ...app writes through `connection` as normal...
 *   await controller.flush();
 *   await controller.close();
 *   await connection.close();
 */
import { existsSync } from "node:fs";
import { rename, rm } from "node:fs/promises";
import { createConnection } from "@supabase/lite/sqlite";
import { restoreReplica, startReplica } from "../dist/index.js";

export async function openHostedConnection({
  databaseFile,
  replica,
  migrate,
  ddlDialect = "postgres",
  binaryPath,
  syncInterval,
  durability,
}) {
  let restored = existsSync(databaseFile);

  if (!restored) {
    const restoreTarget = `${databaseFile}.restore-${Date.now()}`;
    try {
      await restoreReplica({ databaseFile, replica, target: restoreTarget, ...(binaryPath ? { binaryPath } : {}) });
      await rename(restoreTarget, databaseFile);
      restored = true;
    } catch {
      await rm(restoreTarget, { force: true }).catch(() => undefined);
    }
  }

  const connection = await createConnection({ url: `file:${databaseFile}`, ddlDialect });

  if (!restored) {
    await migrate(connection);
  }

  const controller = await startReplica({
    connection,
    databaseFile,
    replica,
    ...(binaryPath ? { binaryPath } : {}),
    ...(syncInterval ? { syncInterval } : {}),
    ...(durability ? { durability } : {}),
  });

  return { connection, controller };
}
