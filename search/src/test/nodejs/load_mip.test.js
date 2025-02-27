jest.mock('../../main/nodejs/utils');

import fs from 'fs';
import * as utils from '../../main/nodejs/utils';
import { loadMIPRange } from '../../main/nodejs/load_mip';

describe('Load mips', () => {
    it('loads PNG mip', async () => {
        const bucketName = 'testDataBucket';
        const key = 'src/test/resources/mips/testMIP.png';

        jest.spyOn(utils, 'getS3ContentAsByteBufferWithRetry')
            .mockResolvedValueOnce(fs.readFileSync(key));

        const mip = await loadMIPRange(bucketName, key, 0, 0);
        expect(mip.width).toBe(1210);
        expect(mip.height).toBe(566);
        expect(mip.data.length).toBe(mip.width * mip.height * 3);
    });

    it('loads TIFF mip', async () => {
        const bucketName = 'testDataBucket';
        const key = 'src/test/resources/mips/testMIP.tif';

        jest.spyOn(utils, 'getS3ContentAsByteBufferWithRetry')
            .mockResolvedValueOnce(fs.readFileSync(key));

        const mip = await loadMIPRange(bucketName, key, 0, 0);
        expect(mip.width).toBe(1210);
        expect(mip.height).toBe(566);
        expect(mip.data.length).toBe(mip.width * mip.height * 3);
    });

})