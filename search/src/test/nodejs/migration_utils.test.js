import { matchers } from "jest-json-schema";
import { convertSearchResults } from "../../main/nodejs/migration_utils";

import lm2emInputJSON from "../resources/old_search_lm2em.json";
import lm2emOutputJSON from "../resources/converted_search_lm2em.json";

import em2lmInputJSON from "../resources/old_search_em2lm.json";
import em2lmOutputJSON from "../resources/converted_search_em2lm.json";

import customMatchesSchema from "../resources/customMatches.schema.json";

// update the expect matchers to use the matchers from jest-json-schema,
// so that we can use the json schema files to make sure the converted
// output has the required format.
expect.extend(matchers);

describe("migration utils tests", () => {
  it("converts an lm2em search result", () => {
    const converted = convertSearchResults(lm2emInputJSON, 'brain', 'lm2em');
    expect(converted).toEqual(lm2emOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });
  it("converts an em2lm search result", () => {
    const converted = convertSearchResults(em2lmInputJSON, 'brain', 'em2lm');
    expect(converted).toEqual(em2lmOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });

});
