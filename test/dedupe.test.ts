import { describe, expect, it } from "vitest";

import { TtlDedupe } from "../src/dedupe.js";

describe("TtlDedupe", () => {
  it("expires keys and evicts the oldest entries", () => {
    const dedupe = new TtlDedupe(10, 2);
    dedupe.add("one", 0);
    dedupe.add("two", 1);
    expect(dedupe.has("one", 5)).toBe(true);

    dedupe.add("three", 2);
    expect(dedupe.has("one", 2)).toBe(false);
    expect(dedupe.has("two", 2)).toBe(true);
    expect(dedupe.has("three", 2)).toBe(true);
    expect(dedupe.has("two", 12)).toBe(false);
  });
});
