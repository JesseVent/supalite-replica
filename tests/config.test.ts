/**
 * Pure unit tests for `litestreamConfig()` — runnable in CI with no Litestream
 * binary installed (the kill-and-restore drill skips when no binary is present,
 * but these always run and assert the generated YAML stays the proven shape).
 */
import { describe, expect, it } from "vitest";

import { litestreamConfig } from "../src/index.ts";

describe("litestreamConfig", () => {
  it("generates a file-replica config with the proven socket + db block", () => {
    const yaml = litestreamConfig({
      databaseFile: "/data/app.db",
      replica: { type: "file", path: "/data/replica" },
      socketPath: "/tmp/supalite/control.sock",
      syncInterval: "100ms",
    });

    expect(yaml).toContain("socket:");
    expect(yaml).toContain("enabled: true");
    expect(yaml).toContain('path: "/tmp/supalite/control.sock"');
    expect(yaml).toContain("permissions: 0600");
    expect(yaml).toContain("shutdown-sync-timeout: 10s");
    expect(yaml).toContain('path: "/data/app.db"');
    expect(yaml).toContain("restore-if-db-not-exists: false");
    expect(yaml).toContain("monitor-interval: 100ms");
    expect(yaml).toContain("checkpoint-interval: 1s");
    expect(yaml).toContain("busy-timeout: 5s");
    expect(yaml).toContain("type: file");
    expect(yaml).toContain('path: "/data/replica"');
    expect(yaml).toContain("sync-interval: 100ms");
    expect(yaml).toContain("auto-recover: false");
    // No S3-only fields leak into a file config.
    expect(yaml).not.toContain("force-path-style");
    expect(yaml).not.toContain("skip-verify");
  });

  it("generates an s3 replica config with endpoint + region", () => {
    const yaml = litestreamConfig({
      databaseFile: "/data/app.db",
      replica: {
        type: "s3",
        url: "s3://my-bucket/myapp",
        endpoint: "http://minio:9000",
        region: "us-east-1",
      },
      socketPath: "/tmp/supalite/control.sock",
      syncInterval: "1s",
    });

    expect(yaml).toContain("type: s3");
    expect(yaml).toContain('url: "s3://my-bucket/myapp"');
    expect(yaml).toContain('endpoint: "http://minio:9000"');
    expect(yaml).toContain("force-path-style: true");
    expect(yaml).toContain('region: "us-east-1"');
    expect(yaml).toContain("skip-verify: false");
    expect(yaml).toContain("sync-interval: 1s");
    // No file-replica `path:` under the replica block.
    expect(yaml).not.toMatch(/replica:\s*\n\s*type: s3\s*\n\s*path:/);
  });

  it("quotes paths so spaces / colons cannot break the YAML", () => {
    const yaml = litestreamConfig({
      databaseFile: "/data/my app.db",
      replica: { type: "file", path: "/data/my replica" },
      socketPath: "/tmp/supalite/control.sock",
      syncInterval: "100ms",
    });
    expect(yaml).toContain('path: "/data/my app.db"');
    expect(yaml).toContain('path: "/data/my replica"');
  });
});