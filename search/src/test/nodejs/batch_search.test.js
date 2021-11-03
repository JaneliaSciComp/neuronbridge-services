import * as batch_search from '../../main/nodejs/batch_search';
import * as load_mip from '../../main/nodejs/load_mip';
import * as utils from '../../main/nodejs/utils';

test('batchSearch', () => {
    const batchParams = {
        searchPrefix: "janelia-flylight-color-depth-dev",
        libraries: ["FlyEM_Hemibrain_v1.1"],
        maskPrefix: "janelia-flylight-color-depth-dev",
        maskKeys: [
            "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.1/1002360103-RT-JRC2018_Unisex_20x_HR-CDM.png"
        ],
        dataThreshold: 100,
        maskThresholds: [
            100
        ],
        pixColorFluctuation: 2,
        xyShift: 2,
        mirrorMask: true,
        numLevels: 3,
        batchSize: 15
    }

    const search_params = {
        maskKeys: batchParams.maskKeys,
        maskThresholds: batchParams.maskThresholds,
        libraries: batchParams.libraries,
        awsMasksBucket: batchParams.maskPrefix,
        awsLibrariesBucket: batchParams.searchPrefix,
        awsLibrariesThumbnailsBucket: process.env.SEARCHED_THUMBNAILS_BUCKET || batchParams.searchPrefix,
        dataThreshold: batchParams.dataThreshold,
        pixColorFluctuation: batchParams.pixColorFluctuation,
        xyShift: batchParams.xyShift,
        mirrorMask: batchParams.mirrorMask,
        minMatchingPixRatio: batchParams.minMatchingPixRatio
    }

    const width = 1210;
    const height = 566;

    let testdata = new Uint8Array(width * height * 3);
    for (let y = 300; y < 400; y++)
    {
        for (let x = 100; x < 200; x++)
        {
            testdata[(y * width + x) * 3 + 0] = 254;
            testdata[(y * width + x) * 3 + 1] = 255;
            testdata[(y * width + x) * 3 + 2] = 0;
        }
        for (let x = 200; x < 300; x++)
        {
            testdata[(y * width + x) * 3 + 0] = 54;
            testdata[(y * width + x) * 3 + 1] = 255;
            testdata[(y * width + x) * 3 + 2] = 201;
        }
    }

    let testdata2 = new Uint8Array(width * height * 3);
    for (let y = 300; y < 400; y++)
    {
        for (let x = 102; x < 202; x++)
        {
            testdata2[(y * width + x) * 3 + 0] = 254;
            testdata2[(y * width + x) * 3 + 1] = 255;
            testdata2[(y * width + x) * 3 + 2] = 0;
        }
        for (let x = 202; x < 302; x++)
        {
            testdata2[(y * width + x) * 3 + 0] = 54;
            testdata2[(y * width + x) * 3 + 1] = 255;
            testdata2[(y * width + x) * 3 + 2] = 201;
        }
    }

    const loadMipRangeSpy = jest.spyOn(load_mip, 'loadMIPRange')
        .mockResolvedValueOnce({data: testdata, width: width, height: height})
        .mockResolvedValueOnce({data: testdata2, width: width, height: height});

    const getKeysSpy = jest.spyOn(utils, 'getObjectWithRetry')
        .mockResolvedValueOnce([
            "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.1/1002360103-RT-JRC2018_Unisex_20x_HR-CDM.png"
        ])

    return batch_search.findAllColorDepthMatches(search_params).then((result) => {
        expect(result[0].matchingPixels).toEqual(20000);
        expect(loadMipRangeSpy).toHaveBeenCalled();
    });

});