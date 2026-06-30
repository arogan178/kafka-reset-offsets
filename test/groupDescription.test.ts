import { describe, expect, it } from "vitest";
import {
  extractCurrentOffsetsFromGroupDescription,
  extractPlannedOffsetsFromResetOutput,
  extractTopicsFromGroupDescription,
  inferTopicFromConsumerGroupName,
} from "../src/groupDescription.js";

describe("extractTopicsFromGroupDescription", () => {
  it("extracts unique topics from kafka-consumer-groups describe output", () => {
    const output = `
GROUP           TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
corex-orders    orders-created   0          10              20              10
corex-orders    orders-created   1          11              21              10
corex-orders    orders-updated   0          5               5               0
`;

    expect(extractTopicsFromGroupDescription(output)).toEqual(["orders-created", "orders-updated"]);
  });

  it("ignores informational and error lines", () => {
    const output = `
Error: Executing consumer group command failed
Consumer group 'missing' has no active members.
`;

    expect(extractTopicsFromGroupDescription(output)).toEqual([]);
  });

  it("extracts current offsets from describe output", () => {
    const output = `
GROUP           TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
corex-orders    orders-created   0          10              20              10
corex-orders    orders-created   1          11              21              10
`;

    expect(extractCurrentOffsetsFromGroupDescription(output)).toEqual([
      { group: "corex-orders", topic: "orders-created", partition: 0, offset: "10" },
      { group: "corex-orders", topic: "orders-created", partition: 1, offset: "11" },
    ]);
  });

  it("extracts planned offsets from reset dry-run output", () => {
    const output = `
GROUP           TOPIC            PARTITION  NEW-OFFSET
corex-orders    orders-created   0          42
corex-orders    orders-created   1          43
`;

    expect(extractPlannedOffsetsFromResetOutput(output)).toEqual([
      { group: "corex-orders", topic: "orders-created", partition: 0, offset: "42" },
      { group: "corex-orders", topic: "orders-created", partition: 1, offset: "43" },
    ]);
  });

  it("infers topic from source-target-topic consumer group names", () => {
    expect(inferTopicFromConsumerGroupName("apf_cert-data_stg-endeavour_bonuses_user_profile_bonuses")).toBe(
      "endeavour_bonuses_user_profile_bonuses",
    );
  });

  it("strips the provided consumer group prefix before inferring topic", () => {
    expect(
      inferTopicFromConsumerGroupName(
        "apf_cert-data_stg-endeavour_bonuses_user_profile_bonuses",
        "apf_cert-data_stg",
      ),
    ).toBe("endeavour_bonuses_user_profile_bonuses");
  });

  it("keeps hyphenated topic suffixes when inferring from group names", () => {
    expect(inferTopicFromConsumerGroupName("source-target-topic-with-hyphens")).toBe("topic-with-hyphens");
  });
});
