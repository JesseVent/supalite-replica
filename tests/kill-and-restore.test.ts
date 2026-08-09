/**
 * Kill-and-restore drill for @jessevent/supalite-replica.
 *
 * Proves the thin package's whole value proposition end-to-end: a real
 * file-backed Supabase Lite app replicating through `startReplica` survives a
 * hard SIGKILL of the whole process group (Node app + Litestream sidecar), and
 * every committed write is recoverable from the replica to a separate verified
 * file via `restoreReplica`.
 *
 * Binary resolution (keeps the drill self-contained and shippable):
 *   1. LITESTREAM_BINARY env (explicit)
 *   2. this repo's provisioned Phase-0 candidate, if present (dev convenience)
 *   3. `litestream` on PATH (the operator-installed case — the real shipping story)
 * The test skips if none is available.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

import { createConnection } from "@supabase/lite/sqlite";
import { afterEach, describe, expect, it } from "vitest";

import { restoreReplica, startReplica } from "../src/index.ts";

interface ReadyEvent {
  type: "ready";
  databaseFile: string;
}
interface CommitEvent {
  type: "committed";
  id: number;
}
type AppEvent = ReadyEvent | CommitEvent;

const appScript = fileURLToPath(new URL("./fixtures/app.ts", import.meta.url));
const repoRoot = fileURLToPath(new URL("../", import.meta.url));

// Binary resolution — the real shipping story: an explicit override, else an
// operator-installed `litestream` on PATH. The drill skips if neither is present.
function resolveBinary(): { path: string | undefined; onPath: boolean } {
  if (process.env.LITESTREAM_BINARY !== undefined) {
    return { path: process.env.LITESTREAM_BINARY, onPath: false };
  }
  const probe = spawnSync("litestream", ["version"], { timeout: 5_000 });
  return probe.status === 0 ? { path: "litestream", onPath: true } : { path: undefined, onPath: false };
}

const binary = resolveBinary();
const binaryAvailable = binary.path !== undefined;
// `binaryPath` option is left undefined when litestream is on PATH (spawn resolves it).
const passBinary = binary.onPath ? undefined : binary.path;

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true }),
    ),
  );
});

function readIds(databaseFile: string): number[] {
  const database = new DatabaseSync(`file:${databaseFile}?immutable=1`, { readOnly: true });
  try {
    expect(
      (database.prepare("PRAGMA quick_check").get() as { quick_check: unknown }).quick_check,
    ).toBe("ok");
    return database
      .prepare("SELECT id FROM items ORDER BY id")
      .all()
      .map((row) => Number((row as { id: unknown }).id));
  } finally {
    database.close();
  }
}

function isEsrch(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === "ESRCH"
  );
}

function killGroup(pid: number | undefined): void {
  if (pid === undefined) {
    return;
  }
  try {
    process.kill(-pid, "SIGKILL");
  } catch (error) {
    if (!isEsrch(error)) {
      throw error;
    }
  }
}

describe.skipIf(!binaryAvailable)("supalite-replica kill-and-restore drill", () => {
  it(
    "recovers every committed write after the app is SIGKILLed",
    async () => {
      const root = await mkdtemp(join(tmpdir(), "supalite-drill-"));
      temporaryDirectories.push(root);
      const databaseFile = join(root, "app.db");
      const restoredFile = join(root, "restored.db");
      const replica = { type: "file" as const, path: join(root, "replica") };

      const childEnv: NodeJS.ProcessEnv = {
        NO_COLOR: "1",
        PATH: process.env.PATH ?? "",
        TMPDIR: process.env.TMPDIR,
      };
      if (!binary.onPath && binary.path !== undefined) {
        childEnv.LITESTREAM_BINARY = binary.path;
      }

      const child = spawn(process.execPath, ["--import", "tsx", appScript, root], {
        cwd: repoRoot,
        detached: true,
        env: childEnv,
        stdio: ["pipe", "pipe", "pipe"],
      });

      const events: AppEvent[] = [];
      const waiters = new Set<() => void>();
      let stdoutBuffer = "";
      let stderr = "";
      child.stdout.setEncoding("utf8");
      child.stderr.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        stdoutBuffer += chunk;
        for (;;) {
          const newline = stdoutBuffer.indexOf("\n");
          if (newline < 0) {
            break;
          }
          const line = stdoutBuffer.slice(0, newline);
          stdoutBuffer = stdoutBuffer.slice(newline + 1);
          if (line !== "") {
            events.push(JSON.parse(line) as AppEvent);
            for (const notify of waiters) {
              notify();
            }
          }
        }
      });
      child.stderr.on("data", (chunk: string) => {
        stderr = (stderr + chunk).slice(-64 * 1024);
      });

      const childExit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
        (resolvePromise, rejectPromise) => {
          child.once("error", rejectPromise);
          child.once("exit", (code, signal) => resolvePromise({ code, signal }));
        },
      );

      async function waitForEvent(
        predicate: (event: AppEvent) => boolean,
        timeoutMs = 30_000,
      ): Promise<void> {
        const deadline = Date.now() + timeoutMs;
        for (;;) {
          if (events.some(predicate)) {
            return;
          }
          if (child.exitCode !== null || child.signalCode !== null) {
            throw new Error(`Example app exited early:\n${stderr}`);
          }
          if (Date.now() >= deadline) {
            throw new Error(`Timed out waiting for example app:\n${stderr}`);
          }
          await new Promise<void>((resolvePromise) => {
            const notify = () => {
              clearTimeout(timer);
              waiters.delete(notify);
              resolvePromise();
            };
            const timer = setTimeout(notify, 250);
            waiters.add(notify);
          });
        }
      }

      const committedBeforeKill = 25;
      try {
        await waitForEvent((event): event is ReadyEvent => event.type === "ready");
        child.stdin.write("go\n");

        await waitForEvent(
          (event): event is CommitEvent =>
            event.type === "committed" && event.id >= committedBeforeKill,
        );

        // SIGKILL the whole group: Node app + Litestream sidecar. synchronous=FULL
        // means every observed commit is already durable in the on-disk WAL.
        killGroup(child.pid);
        const exit = await childExit;
        expect(exit.signal).toBe("SIGKILL");
        // Let the OS reap the sidecar and release the replica directory handles.
        await new Promise((resolvePromise) => setTimeout(resolvePromise, 200));

        // Resume the SAME replica chain and flush once. This captures the
        // surviving on-disk WAL into the replica so the restore recovers the full
        // committed prefix, not just what was synced before the kill.
        const resumeConnection = await createConnection({
          url: `file:${databaseFile}`,
          ddlDialect: "postgres",
        });
        try {
          const controller = await startReplica({
            connection: resumeConnection,
            databaseFile,
            replica,
            syncInterval: "100ms",
            ...(passBinary !== undefined ? { binaryPath: passBinary } : {}),
          });
          await controller.flush(15_000);
          await controller.close(10_000);
        } finally {
          await resumeConnection.close();
        }

        await restoreReplica({
          databaseFile,
          replica,
          target: restoredFile,
          ...(passBinary !== undefined ? { binaryPath: passBinary } : {}),
        });

        const sourceIds = readIds(databaseFile);
        const restoredIds = readIds(restoredFile);

        expect(sourceIds.length).toBeGreaterThanOrEqual(committedBeforeKill);
        expect(sourceIds).toEqual(Array.from({ length: sourceIds.length }, (_, i) => i + 1));
        expect(restoredIds).toEqual(sourceIds);
      } finally {
        if (child.exitCode === null && child.signalCode === null) {
          killGroup(child.pid);
          await childExit;
        }
      }
    },
    90_000,
  );
});