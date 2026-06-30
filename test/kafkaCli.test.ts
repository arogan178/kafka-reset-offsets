import { describe, expect, it } from "vitest";
import { buildResetCommand, formatCommand } from "../src/kafkaCli.js";
import type { ResetPlan } from "../src/types.js";

describe("buildResetCommand", () => {
  it("adds one --topic flag per selected topic", () => {
    const command = buildResetCommand(basePlan(), "/opt/kafka/bin/kafka-consumer-groups.sh", "dry-run");

    expect(command.args).toContain("--dry-run");
    expect(command.args).not.toContain("--execute");
    expect(command.args.filter((arg) => arg === "--topic")).toHaveLength(3);
    expect(command.args).toEqual(
      expect.arrayContaining(["--topic", "orders", "--topic", "payments", "--topic", "shipments"]),
    );
  });

  it("uses --all-topics for all-topic selections", () => {
    const plan = {
      ...basePlan(),
      topicSelection: {
        mode: "all",
      },
    } satisfies ResetPlan;

    const command = buildResetCommand(plan, "kafka-consumer-groups", "execute");

    expect(command.args).toContain("--all-topics");
    expect(command.args).toContain("--execute");
  });

  it("adds TZ=UTC when the normalized timestamp uses UTC mode", () => {
    const command = buildResetCommand(
      {
        ...basePlan(),
        normalizedDateTime: {
          ...basePlan().normalizedDateTime,
          timezone: "utc",
        },
      },
      "kafka-consumer-groups",
      "dry-run",
    );

    expect(command.env?.TZ).toBe("UTC");
    expect(formatCommand(command)).toContain("TZ=UTC");
  });
});

function basePlan(): ResetPlan {
  return {
    connection: {
      mode: "direct",
      bootstrapServer: "localhost:9092",
    },
    group: "my-group",
    topicSelection: {
      mode: "topics",
      topics: ["orders", "payments", "shipments"],
    },
    normalizedDateTime: {
      input: "2026-06-29",
      kafkaDateTime: "2026-06-29T00:00:00.000",
      note: "Date normalized to start of day",
      timezone: "local",
    },
    execute: false,
    yes: false,
  };
}
