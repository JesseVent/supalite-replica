#!/usr/bin/env node
/**
 * bunnyhop — poll an S3 (or file) Litestream replica and promote a fresh,
 * verified SQLite snapshot on an interval, without ever touching the live
 * hosted database. Each cycle: restoreReplica() to a new timestamped path,
 * then atomically rename it over CURRENT_PATH.
 *
 * Same restore-on-a-schedule shape as icebridge's poll loop, minus the
 * diff/Iceberg write — just "give me an up-to-date local copy".
 *
 * Usage:
 *   BUNNYHOP_DATABASE_FILE=/var/lib/myapp/app.db \
 *   BUNNYHOP_REPLICA_JSON='{"type":"s3","url":"s3://bucket/myapp","region":"us-east-1"}' \
 *   BUNNYHOP_CURRENT_PATH=/var/lib/myapp/current.db \
 *   node examples/bunnyhop.mjs
 *
 * CURRENT_PATH and WORKDIR must be on the same filesystem — rename() across
 * filesystems (EXDEV) is not atomic and this script doesn't fall back to copy.
 *
 * ponytail: a failed restore throws and exits the process rather than
 * retrying — same choice icebridge's main.rs makes. Add retry/backoff if
 * you're running this unsupervised (systemd Restart=on-failure, etc. covers
 * it without code).
 */
import { rename } from "node:fs/promises";
import { restoreReplica } from "../dist/index.js";

function envVar(name) {
  const value = process.env[name];
  if (!value) throw new Error(`missing required env var ${name}`);
  return value;
}

const databaseFile = envVar("BUNNYHOP_DATABASE_FILE");
const replica = JSON.parse(envVar("BUNNYHOP_REPLICA_JSON"));
const currentPath = envVar("BUNNYHOP_CURRENT_PATH");
const workdir = process.env.BUNNYHOP_WORKDIR ?? process.env.TMPDIR ?? "/tmp";
const pollIntervalMs = Number(process.env.BUNNYHOP_POLL_INTERVAL_MS ?? 5000);

async function hop() {
  const target = `${workdir}/bunnyhop-${Date.now()}.db`;
  const result = await restoreReplica({ databaseFile, replica, target, point: "latest" });
  await rename(result.target, currentPath); // atomic overwrite on POSIX, same filesystem
  console.log(`hopped (${result.integrity}) -> ${currentPath}`);
}

while (true) {
  await hop();
  await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
}
