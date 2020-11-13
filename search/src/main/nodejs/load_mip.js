import path from "path";
import fs from "fs";
import {getObjectDataArray} from "./utils";
import Jimp from "jimp";
import {fromArrayBuffer} from "geotiff";

export const loadMIPRange = async (bucketName, key, start, end) => {
    const mipPath = path.parse(key);
    const mipExt = mipPath.ext;

    const isEFS = bucketName.startsWith("/mnt/");

    const imgfile = isEFS ?
        fs.readFileSync(bucketName + "/" + key) /* return Uint8Array */ :
        await getObjectDataArray(bucketName, key);

    let outdata = null;
    let width = 0;
    let height = 0;

    let img = null;
    if (mipExt === ".png") {
        img = await Jimp.read(imgfile);
        width = img.bitmap.width;
        height = img.bitmap.height;

        outdata = new Uint8Array(width * height * 3);

        img.scan(0, 0, img.bitmap.width, img.bitmap.height, function(x, y, idx) {
            let i = x + y * width;
            outdata[3*i] = this.bitmap.data[idx + 0];
            outdata[3*i+1] = this.bitmap.data[idx + 1];
            outdata[3*i+2] = this.bitmap.data[idx + 2];
        });
    }
    else if (mipExt === '.tif' || mipExt === '.tiff') {
        const tartiff = await fromArrayBuffer(isEFS ? imgfile.buffer : imgfile);
        const tarimage = await tartiff.getImage();

        width = tarimage.getWidth();
        height = tarimage.getHeight();
        const outdatasize = width * height * 3;

        let outoffset = 0;
        outdata = new Uint8Array(outdatasize);

        if (isEFS) {
            const input = imgfile;

            const ifd = tarimage.getFileDirectory();

            const b_end = end > 0 ? end * 3 : outdatasize;

            if (ifd.Compression == 32773) {
                // PackBits compression
                for (let s = 0; s < ifd.StripOffsets.length; s++) {
                    const stripoffset = ifd.StripOffsets[s];
                    const byteCount = ifd.StripByteCounts[s];

                    let index = stripoffset;
                    while (outoffset < b_end && outoffset < outdatasize && index < stripoffset + byteCount) {
                        const n = input[index++] << 24 >> 24;
                        if (n >= 0) { // 0 <= n <= 127
                            for (let i = 0; i < n + 1; i++) {
                                outdata[outoffset++] = input[index++];
                            }
                        } else if (n != -128) { // -127 <= n <= -1
                            const len = -n + 1;
                            const val = input[index++];
                            for (let i = 0; i < len; i++) outdata[outoffset++] = val;
                        }
                    }

                    if (outoffset >= b_end)
                        break;
                }
            } else {
                // RAW TIFF
                for (let s = 0; s < ifd.StripOffsets.length; s++) {
                    const stripoffset = ifd.StripOffsets[s];
                    const byteCount = ifd.StripByteCounts[s];

                    for (let i = stripoffset; i < byteCount; ++i) {
                        outdata[outoffset] = input[i];
                        outoffset++;
                        if (outoffset >= b_end) break;
                    }
                }
            }
        }
        else
        {
            const input = new DataView(imgfile);

            const ifd = tarimage.getFileDirectory();

            const b_end = end > 0 ? end * 3 : outdatasize;

            if (ifd.Compression == 32773) {
                // PackBits compression
                for (let s = 0; s < ifd.StripOffsets.length; s++) {
                    const stripoffset = ifd.StripOffsets[s];
                    const byteCount = ifd.StripByteCounts[s];

                    let index = stripoffset;
                    while (outoffset < b_end && outoffset < outdatasize && index < stripoffset + byteCount) {
                        const n = input.getInt8(index++);
                        if (n >= 0) { // 0 <= n <= 127
                            for (let i = 0; i < n + 1; i++) {
                                outdata[outoffset++] = input.getUint8(index++);
                            }
                        } else if (n != -128) { // -127 <= n <= -1
                            const len = -n + 1;
                            const val = input.getUint8(index++);
                            for (let i = 0; i < len; i++) outdata[outoffset++] = val;
                        }
                    }

                    if (outoffset >= b_end)
                        break;
                }
            } else {
                // RAW TIFF
                for (let s = 0; s < ifd.StripOffsets.length; s++) {
                    const stripoffset = ifd.StripOffsets[s];
                    const byteCount = ifd.StripByteCounts[s];

                    for (let i = stripoffset; i < byteCount; ++i) {
                        outdata[outoffset] = input.getUint8(i);
                        outoffset++;
                        if (outoffset >= b_end) break;
                    }
                }
            }
        }
    }

    return {data: outdata, width: width, height: height};
};
