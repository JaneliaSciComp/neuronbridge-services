import { matchers } from "jest-json-schema";
import { convertSearchResults } from "../../main/nodejs/migration_utils";

import * as clientDbUtils from '../../main/nodejs/clientDbUtils';

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
  const OLD_ENV = process.env;

  beforeEach(() => {
    jest.resetAllMocks();
    process.env = {
      ...OLD_ENV,
      LM_PUBLISHED_STACKS_TABLE: 'lm-published-stacks',
      EM_PUBLISHED_SKELETONS_TABLE: 'em-published-skeletons',
    };
  });

  it("converts a Brain lm2em search result", async () => {
    jest.spyOn(clientDbUtils, 'queryDb')
      .mockResolvedValue({
        Items: [
        ],
      })

    const converted = await convertSearchResults(lm2emBrainInputJSON, 'brain', 'lm2em');
    expect(converted).toEqual(lm2emBrainOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });

  it("converts a Brain em2lm search result", async () => {
    jest.spyOn(clientDbUtils, 'queryDb')
      .mockResolvedValueOnce({
        Items: [
          {
              files: {
                  VisuallyLosslessStack: 'https://aws/bucket/Gen1+MCFO/VT007350/VT007350-20180803_63_H2-f-40x-brain-GAL4-JRC2018_Unisex_20x_HR-aligned_stack.h5j',
              },
          }
        ],
      })
      .mockResolvedValueOnce({
        Items: [
          {
              files: {
                  VisuallyLosslessStack: 'https://aws/bucket/Gen1+MCFO/VT007350/VT007350-20180803_63_H2-f-40x-brain-GAL4-JRC2018_Unisex_20x_HR-aligned_stack.h5j',
              },
          }
        ],
      });

    const converted = await convertSearchResults(em2lmBrainInputJSON, 'brain', 'em2lm');
    expect(converted).toEqual(em2lmBrainOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });

  it("converts a VNC lm2em search result", async () => {
    jest.spyOn(clientDbUtils, 'queryDb')
      .mockResolvedValueOnce({
        Items: [
          {
            skeletonobj: 'https://aws/bucket/OBJ/an.obj',
            skeletonswc: 'https://aws/bucket/SWC/an.swc',
          }
        ],
      })
      .mockResolvedValueOnce({
        Items: [
          {
            skeletonswc: 'https://aws/bucket/SWC/an.swc',
          }
        ],
      })
      .mockResolvedValueOnce({
        Items: [
          {
          }
        ],
      })
      .mockResolvedValue({
        Items: [
          {
            skeletonobj: 'https://aws/bucket/OBJ/an.obj',
            skeletonswc: 'https://aws/bucket/SWC/an.swc',
          }
        ],
      })
      ;

    const converted = await convertSearchResults(lm2emVNCInputJSON, 'vnc', 'lm2em');
    expect(converted).toEqual(lm2emVNCOutputJSON);
    expect(converted).toMatchSchema(customMatchesSchema);
  });
});
