import { describe, expect, it } from "vitest";
import { clampCursor, ensureCursorVisible, filterChoices } from "../src/searchCheckbox.js";

describe("filterChoices", () => {
  it("matches typed tokens in any order", () => {
    expect(filterChoices(["orders.created", "orders.cancelled", "payments.created"], "orders created")).toEqual([
      "orders.created",
    ]);
  });

  it("returns all choices for an empty query", () => {
    expect(filterChoices(["orders", "payments"], "")).toEqual(["orders", "payments"]);
  });

  it("keeps cursor within list bounds", () => {
    expect(clampCursor(-1, 3)).toBe(0);
    expect(clampCursor(5, 3)).toBe(2);
    expect(clampCursor(0, 0)).toBe(0);
  });

  it("scrolls the viewport to keep cursor visible", () => {
    expect(ensureCursorVisible(0, 0, 3)).toBe(0);
    expect(ensureCursorVisible(3, 0, 3)).toBe(1);
    expect(ensureCursorVisible(1, 3, 3)).toBe(1);
  });
});
