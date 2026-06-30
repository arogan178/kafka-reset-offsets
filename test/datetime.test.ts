import { describe, expect, it } from "vitest";
import { normalizeDateTime } from "../src/datetime.js";

describe("normalizeDateTime", () => {
  it("normalizes a date to the start of day", () => {
    expect(normalizeDateTime("2026-06-29", "local").kafkaDateTime).toBe("2026-06-29T00:00:00.000");
  });

  it("normalizes a date and minute value", () => {
    expect(normalizeDateTime("2026-06-29 13:45", "local").kafkaDateTime).toBe("2026-06-29T13:45:00.000");
  });

  it("pads fractional seconds to milliseconds", () => {
    expect(normalizeDateTime("2026-06-29T13:45:00.7", "local").kafkaDateTime).toBe("2026-06-29T13:45:00.700");
  });

  it("converts epoch seconds in UTC mode", () => {
    expect(normalizeDateTime("1782737100", "utc").kafkaDateTime).toBe("2026-06-29T12:45:00.000");
  });

  it("converts epoch milliseconds in UTC mode", () => {
    expect(normalizeDateTime("1782737100123", "utc").kafkaDateTime).toBe("2026-06-29T12:45:00.123");
  });

  it("rejects invalid calendar dates", () => {
    expect(() => normalizeDateTime("2026-02-31", "local")).toThrow("invalid calendar date");
  });
});
