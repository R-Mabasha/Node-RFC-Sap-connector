import assert from "node:assert/strict";
import test from "node:test";

import { normalizeTableReadWhere } from "./nodeRfcClient.js";

test("normalizeTableReadWhere compacts standalone clauses for BBP_RFC_READ_TABLE", () => {
  assert.deepEqual(
    normalizeTableReadWhere("BBP_RFC_READ_TABLE", [
      "STATUS EQ 'A'",
      "ENDDATE GE '20260401'",
      "ENDDATE LE '20260405'",
    ]),
    ["STATUS EQ 'A' AND ENDDATE GE '20260401' AND ENDDATE LE '20260405'"],
  );
});

test("normalizeTableReadWhere preserves advanced caller-managed clause splitting", () => {
  assert.deepEqual(
    normalizeTableReadWhere("BBP_RFC_READ_TABLE", [
      "STATUS EQ 'A' AND",
      "ENDDATE GE '20260401'",
    ]),
    ["STATUS EQ 'A' AND", "ENDDATE GE '20260401'"],
  );
});

test("normalizeTableReadWhere leaves non-BBP readers untouched", () => {
  assert.deepEqual(
    normalizeTableReadWhere("RFC_READ_TABLE", [
      "FIELD1 EQ 'X'",
      "FIELD2 EQ 'Y'",
    ]),
    ["FIELD1 EQ 'X'", "FIELD2 EQ 'Y'"],
  );
});
