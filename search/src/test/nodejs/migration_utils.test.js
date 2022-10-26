import { matchers } from "jest-json-schema";
import { convertSearchResults } from "../../main/nodejs/migration_utils";

import lm2emBrainInputJSON from "../resources/old_brain_search_lm2em.json";
import lm2emBrainOutputJSON from "../resources/converted_brain_search_lm2em.json";

import em2lmBrainInputJSON from "../resources/old_brain_search_em2lm.json";
import em2lmBrainOutputJSON from "../resources/converted_brain_search_em2lm.json";

import lm2emVNCInputJSON from "../resources/old_vnc_search_lm2em.json";
import lm2emVNCOutputJSON from "../resources/converted_vnc_search_lm2em.json";

import customMatchesSchema from "../resources/customMatches.schema.json";

// update the expect matchers to use the matchers from jest-json-schema,
// so that we can use the json schema files to make sure the converted
// output has the required format.
expect.extend(matchers);

describe("migration utils tests", () => {
  it("converts a Brain lm2em search result", () => {
    const converted = convertSearchResults(lm2emBrainInputJSON, 'brain', 'lm2em');
    expect(converted).toEqual(lm2emBrainOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });

  it("converts a Brain em2lm search result", () => {
    const converted = convertSearchResults(em2lmBrainInputJSON, 'brain', 'em2lm');
    expect(converted).toEqual(em2lmBrainOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });

  it("converts a VNC lm2em search result", () => {
    const converted = convertSearchResults(lm2emVNCInputJSON, 'vnc', 'lm2em');
    expect(converted).toEqual(lm2emVNCOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });
});
