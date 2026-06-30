import { describe, expect, it } from "vitest";
import { filterChoices } from "../src/searchCheckbox.js";

describe("filterChoices", () => {
  it("matches typed tokens in any order", () => {
    expect(filterChoices(["orders.created", "orders.cancelled", "payments.created"], "orders created")).toEqual([
      "orders.created",
    ]);
  });

  it("returns all choices for an empty query", () => {
    expect(filterChoices(["orders", "payments"], "")).toEqual(["orders", "payments"]);
  });
});
