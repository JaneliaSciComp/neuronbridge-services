/**
 * Calculate the Z-gap between two RGB pixels
 * @param red1 - red component of the first pixel
 * @param green1 - green component of the first pixel
 * @param blue1 - blue component of the first pixel
 * @param red2 - red component of the second pixel
 * @param green2 - green component of the second pixel
 * @param blue2 - blue component of the second pixel
 * @return
 */
const calculatePixelGap = (red1, green1, blue1, red2, green2, blue2) =>
{
    let RG1 = 0;
    let BG1 = 0;
    let GR1 = 0;
    let GB1 = 0;
    let RB1 = 0;
    let BR1 = 0;
    let RG2 = 0;
    let BG2 = 0;
    let GR2 = 0;
    let GB2 = 0;
    let RB2 = 0;
    let BR2 = 0;
    let rb1 = 0.0;
    let rg1 = 0.0;
    let gb1 = 0.0;
    let gr1 = 0.0;
    let br1 = 0.0;
    let bg1 = 0.0;
    let rb2 = 0.0;
    let rg2 = 0.0;
    let gb2 = 0.0;
    let gr2 = 0.0;
    let br2 = 0.0;
    let bg2 = 0.0;
    let pxGap = 10000.0;
    let BrBg = 0.354862745;
    let BgGb = 0.996078431;
    let GbGr = 0.505882353;
    let GrRg = 0.996078431;
    let RgRb = 0.505882353;
    let BrGap = 0;
    let BgGap = 0;
    let GbGap = 0;
    let GrGap = 0;
    let RgGap = 0;
    let RbGap = 0;

    if (blue1 > red1 && blue1 > green1) { //1,2
        if (red1 > green1) {
            BR1 = blue1 + red1;//1
            if (blue1 != 0 && red1 != 0)
                br1 = red1 / blue1;
        } else {
            BG1 = blue1 + green1;//2
            if (blue1 != 0 && green1 != 0)
                bg1 = green1 / blue1;
        }
    } else if (green1 > blue1 && green1 > red1) { //3,4
        if (blue1 > red1) {
            GB1 = green1 + blue1;//3
            if (green1 != 0 && blue1 != 0)
                gb1 = blue1 / green1;
        } else {
            GR1 = green1 + red1;//4
            if (green1 != 0 && red1 != 0)
                gr1 = red1 / green1;
        }
    } else if (red1 > blue1 && red1 > green1) { //5,6
        if (green1 > blue1) {
            RG1 = red1 + green1;//5
            if (red1 != 0 && green1 != 0)
                rg1 = green1 / red1;
        } else {
            RB1 = red1 + blue1;//6
            if (red1 != 0 && blue1 != 0)
                rb1 = blue1 / red1;
        }
    }

    if (blue2 > red2 && blue2 > green2) {
        if (red2 > green2) { //1, data
            BR2 = blue2 + red2;
            if (blue2 != 0 && red2 != 0)
                br2 = red2 / blue2;
        } else { //2, data
            BG2 = blue2 + green2;
            if (blue2 != 0 && green2 != 0)
                bg2 = green2 / blue2;
        }
    } else if (green2 > blue2 && green2 > red2) {
        if (blue2 > red2) { //3, data
            GB2 = green2 + blue2;
            if (green2 != 0 && blue2 != 0)
                gb2 = blue2 / green2;
        } else { //4, data
            GR2 = green2 + red2;
            if (green2 != 0 && red2 != 0)
                gr2 = red2 / green2;
        }
    } else if (red2 > blue2 && red2 > green2) {
        if (green2 > blue2) { //5, data
            RG2 = red2 + green2;
            if (red2 != 0 && green2 != 0)
                rg2 = green2 / red2;
        } else { //6, data
            RB2 = red2 + blue2;
            if (red2 != 0 && blue2 != 0)
                rb2 = blue2 / red2;
        }
    }

    ///////////////////////////////////////////////////////
    if (BR1 > 0) { //1, mask// 2 color advance core
        if (BR2 > 0) { //1, data
            if (br1 > 0 && br2 > 0) {
                if (br1 != br2) {
                    pxGap = br2 - br1;
                    pxGap = Math.abs(pxGap);
                } else
                    pxGap = 0;

                if (br1 == 255 & br2 == 255)
                    pxGap = 1000;
            }
        } else if (BG2 > 0) { //2, data
            if (br1 < 0.44 && bg2 < 0.54) {
                BrGap = br1 - BrBg;//BrBg=0.354862745;
                BgGap = bg2 - BrBg;//BrBg=0.354862745;
                pxGap = BrGap + BgGap;
            }
        }
    } else if (BG1 > 0) { //2, mask/////////////////////////////
        if (BG2 > 0) { //2, data, 2,mask
            if (bg1 > 0 && bg2 > 0) {
                if (bg1 != bg2) {
                    pxGap = bg2 - bg1;
                    pxGap = Math.abs(pxGap);

                } else if (bg1 == bg2)
                    pxGap = 0;
                if (bg1 == 255 & bg2 == 255)
                    pxGap = 1000;
            }
        } else if (GB2 > 0) { //3, data, 2,mask
            if (bg1 > 0.8 && gb2 > 0.8) {
                BgGap = BgGb - bg1;//BgGb=0.996078431;
                GbGap = BgGb - gb2;//BgGb=0.996078431;
                pxGap = BgGap + GbGap;
            }
        } else if (BR2 > 0) { //1, data, 2,mask
            if (bg1 < 0.54 && br2 < 0.44) {
                BgGap = bg1 - BrBg;//BrBg=0.354862745;
                BrGap = br2 - BrBg;//BrBg=0.354862745;
                pxGap = BrGap + BgGap;
            }
        }
    } else if (GB1 > 0) { //3, mask/////////////////////////////
        if (GB2 > 0) { //3, data, 3mask
            if (gb1 > 0 && gb2 > 0) {
                if (gb1 != gb2) {
                    pxGap = gb2 - gb1;
                    pxGap = Math.abs(pxGap);
                } else
                    pxGap = 0;
                if (gb1 == 255 & gb2 == 255)
                    pxGap = 1000;
            }
        } else if (BG2 > 0) { //2, data, 3mask
            if (gb1 > 0.8 && bg2 > 0.8) {
                BgGap = BgGb - gb1;//BgGb=0.996078431;
                GbGap = BgGb - bg2;//BgGb=0.996078431;
                pxGap = BgGap + GbGap;
            }
        } else if (GR2 > 0) { //4, data, 3mask
            if (gb1 < 0.7 && gr2 < 0.7) {
                GbGap = gb1 - GbGr;//GbGr=0.505882353;
                GrGap = gr2 - GbGr;//GbGr=0.505882353;
                pxGap = GbGap + GrGap;
            }
        }//2,3,4 data, 3mask
    } else if (GR1 > 0) { //4mask/////////////////////////////
        if (GR2 > 0) { //4, data, 4mask
            if (gr1 > 0 && gr2 > 0) {
                if (gr1 != gr2) {
                    pxGap = gr2 - gr1;
                    pxGap = Math.abs(pxGap);
                } else
                    pxGap = 0;
                if (gr1 == 255 & gr2 == 255)
                    pxGap = 1000;
            }
        } else if (GB2 > 0) { //3, data, 4mask
            if (gr1 < 0.7 && gb2 < 0.7) {
                GrGap = gr1 - GbGr;//GbGr=0.505882353;
                GbGap = gb2 - GbGr;//GbGr=0.505882353;
                pxGap = GrGap + GbGap;
            }
        } else if (RG2 > 0) { //5, data, 4mask
            if (gr1 > 0.8 && rg2 > 0.8) {
                GrGap = GrRg - gr1;//GrRg=0.996078431;
                RgGap = GrRg - rg2;
                pxGap = GrGap + RgGap;
            }
        }//3,4,5 data
    } else if (RG1 > 0) { //5, mask/////////////////////////////
        if (RG2 > 0) { //5, data, 5mask
            if (rg1 > 0 && rg2 > 0) {
                if (rg1 != rg2) {
                    pxGap = rg2 - rg1;
                    pxGap = Math.abs(pxGap);
                } else
                    pxGap = 0;
                if (rg1 == 255 & rg2 == 255)
                    pxGap = 1000;
            }

        } else if (GR2 > 0) { //4 data, 5mask
            if (rg1 > 0.8 && gr2 > 0.8) {
                GrGap = GrRg - gr2;//GrRg=0.996078431;
                RgGap = GrRg - rg1;//GrRg=0.996078431;
                pxGap = GrGap + RgGap;
            }
        } else if (RB2 > 0) { //6 data, 5mask
            if (rg1 < 0.7 && rb2 < 0.7) {
                RgGap = rg1 - RgRb;//RgRb=0.505882353;
                RbGap = rb2 - RgRb;//RgRb=0.505882353;
                pxGap = RbGap + RgGap;
            }
        }//4,5,6 data
    } else if (RB1 > 0) { //6, mask/////////////////////////////
        if (RB2 > 0) { //6, data, 6mask
            if (rb1 > 0 && rb2 > 0) {
                if (rb1 != rb2) {
                    pxGap = rb2 - rb1;
                    pxGap = Math.abs(pxGap);
                } else if (rb1 == rb2)
                    pxGap = 0;
                if (rb1 == 255 & rb2 == 255)
                    pxGap = 1000;
            }
        } else if (RG2 > 0) { //5, data, 6mask
            if (rg2 < 0.7 && rb1 < 0.7) {
                RgGap = rg2 - RgRb;//RgRb=0.505882353;
                RbGap = rb1 - RgRb;//RgRb=0.505882353;
                pxGap = RgGap + RbGap;
            }
        }
    }//2 color advance core

    return pxGap;
};

const calculateScore = (params) => {
    const src = params.source;
    const tar = params.target;
    const srcPositions = params.sourcePositions;
    const targetPositions = params.targetPositions;
    const searchThreshold = params.searchThreshold;
    const zTolerance = params.zTolerance;

    const masksize = srcPositions.length <= targetPositions.length ? srcPositions.length : targetPositions.length;
    let posi = 0;

    let masksig;
    for (masksig = 0; masksig < masksize; masksig++) {
        if (srcPositions[masksig] == -1 || targetPositions[masksig] == -1) continue;

        const p = srcPositions[masksig]*3;
        const red1 = src[p];
        const green1 = src[p+1];
        const blue1 = src[p+2];

        const p2 = targetPositions[masksig]*3;
        const red2 = tar[p2];
        const green2 = tar[p2+1];
        const blue2 = tar[p2+2];

        if (red2 > searchThreshold || green2 > searchThreshold || blue2 > searchThreshold) {
            const pxGap = calculatePixelGap(red1, green1, blue1, red2, green2, blue2);
            if (pxGap <= zTolerance) {
                posi++;
            }
        }
    }

    return posi;
};

const generateShiftedMasks = (input, xyshift, imageWidth, imageHeight) => {
    let out = [];
    let i, xx, yy;
    for (i = 2; i <= xyshift; i += 2) {
        for (xx = -i; xx <= i; xx += i) {
            for (yy = -i; yy <= i; yy += i) {
                out.push(shiftMaskPosArray(input, xx, yy, imageWidth, imageHeight));
            }
        }
    }

    return out;
};

const shiftMaskPosArray = (src, xshift, yshift, imageWidth, imageHeight) => {
    let pos = [];
    let i, x, y;
    for (i = 0; i < src.length; i++) {
        const val = src[i];
        x = (val % imageWidth) + xshift;
        y = Math.floor(val / imageWidth) + yshift;
        if (x >= 0 && x < imageWidth && y >= 0 && y < imageHeight)
            pos.push(y * imageWidth + x);
        else
            pos.push(-1);
    }
    return pos;
};

const generateMirroredMask = (input, ypitch) => {
    let out = [];
    const masksize = input.length;
    let j;
    for (j = 0; j < masksize; j++) {
        const val = input[j];
        const x = val % ypitch;
        out.push(val + (ypitch - 1) - 2 * x);
    }
    return out;
};

const getMaskPosArray = (mskarray, width, height, thresm) => {
    let sumpx = mskarray.length / 3;
    let pos = [];
    let red, green, blue, pi;
    for (pi = 0; pi < sumpx; pi++) {
        let x = pi % width;
        let y = Math.floor(pi / width);
        if (x < 330 && y < 100 || x >= 950 && y < 85) {
            // label regions are not to be searched
            continue;
        }

        red = mskarray[3*pi];
        green = mskarray[3*pi + 1];
        blue = mskarray[3*pi + 2];

        if (red > thresm || green > thresm || blue > thresm) {
            pos.push(pi);
        }
    }

    return pos;
};

exports.GenerateColorMIPMasks = (params) => {
    let i;

    const width = params.width;
    const height = params.height;
    const queryImage = params.queryImage;
    const maskThreshold = params.maskThreshold;
    const negQueryImage = params.negQueryImage;
    const negMaskThreshold = params.negMaskThreshold;
    const xyshift = params.xyShift;
    const mirrorMask = params.mirrorMask;
    const mirrorNegMask = params.mirrorNegMask;

    const maskPositions = getMaskPosArray(queryImage, width, height, maskThreshold);
    let negMaskPositions;
    if (negQueryImage != null) {
        negMaskPositions = getMaskPosArray(negQueryImage, width, height, negMaskThreshold);
    } else {
        negMaskPositions = null;
    }

    // shifting
    const targetMasksList = generateShiftedMasks(maskPositions, xyshift, width, height);
    let negTargetMasksList;
    if (negQueryImage != null) {
        negTargetMasksList = generateShiftedMasks(negMaskPositions, xyshift, width, height);
    } else {
        negTargetMasksList = null;
    }

    // mirroring
    let mirrorTargetMasksList = [];
    if (mirrorMask) {
        for (i = 0; i < targetMasksList.length; i++)
            mirrorTargetMasksList.push(generateMirroredMask(targetMasksList[i], width));
    } else {
        mirrorTargetMasksList = null;
    }
    let negMirrorTargetMasksList = [];
    if (mirrorNegMask && negQueryImage != null) {
        for (i = 0; i < negTargetMasksList.length; i++)
        negMirrorTargetMasksList.push(generateMirroredMask(negTargetMasksList[i], width));
    } else {
        negMirrorTargetMasksList = null;
    }

    let maskpos_st = width * height;
    let maskpos_ed = 0;
    for (i = 0; i < targetMasksList.length; i++) {
        if (targetMasksList[i][0] < maskpos_st) maskpos_st = targetMasksList[i][0];
        if (targetMasksList[i][targetMasksList[i].length-1] > maskpos_ed) maskpos_ed = targetMasksList[i][targetMasksList[i].length-1];
    }
    if (mirrorMask) {
        for (i = 0; i < mirrorTargetMasksList.length; i++) {
            if (mirrorTargetMasksList[i][0] < maskpos_st) maskpos_st = mirrorTargetMasksList[i][0];
            if (mirrorTargetMasksList[i][mirrorTargetMasksList[i].length-1] > maskpos_ed) maskpos_ed = mirrorTargetMasksList[i][mirrorTargetMasksList[i].length-1];
        }
    }
    if (negQueryImage != null) {
        for (i = 0; i < negTargetMasksList.length; i++) {
            if (negTargetMasksList[i][0] < maskpos_st) maskpos_st = negTargetMasksList[i][0];
            if (negTargetMasksList[i][negTargetMasksList[i].length-1] > maskpos_ed) maskpos_ed = negTargetMasksList[i][negTargetMasksList[i].length-1];
        }
        if (mirrorNegMask) {
            for (i = 0; i < negMirrorTargetMasksList.length; i++) {
                if (negMirrorTargetMasksList[i][0] < maskpos_st) maskpos_st = negMirrorTargetMasksList[i][0];
                if (negMirrorTargetMasksList[i][negMirrorTargetMasksList[i].length-1] > maskpos_ed) maskpos_ed = negMirrorTargetMasksList[i][negMirrorTargetMasksList[i].length-1];
            }
        }
    }

    return {
        queryImage: queryImage,
        negQueryImage: negQueryImage,
        maskPositions: maskPositions,
        negMaskPositions: negMaskPositions,
        targetMasksList: targetMasksList,
        negTargetMasksList: negTargetMasksList,
        mirrorTargetMasksList: mirrorTargetMasksList,
        negMirrorTargetMasksList: negMirrorTargetMasksList,
        maskpos_st: maskpos_st,
        maskpos_ed: maskpos_ed
    };
};

exports.ColorMIPSearch = (targetImage, searchThreshold, zTolerance, params) => {
    const queryImage = params.queryImage;
    const negQueryImage = params.negQueryImage;
    const maskPositions = params.maskPositions;
    const negMaskPositions = params.negMaskPositions;
    const targetMasksList = params.targetMasksList;
    const negTargetMasksList = params.negTargetMasksList;
    const mirrorTargetMasksList = params.mirrorTargetMasksList;
    const negMirrorTargetMasksList = params.negMirrorTargetMasksList;

    let posi = 0;
    let posipersent = 0.0;
    const masksize = maskPositions.length;
    const negmasksize = negMaskPositions != null ? negMaskPositions.length : 0;

    let i;
    for (i = 0; i < targetMasksList.length; i++) {
        const tmpposi = calculateScore({
            source: queryImage,
            target: targetImage,
            sourcePositions: maskPositions,
            targetPositions: targetMasksList[i],
            searchThreshold: searchThreshold,
            zTolerance: zTolerance
        });
        if (tmpposi > posi) {
            posi = tmpposi;
            posipersent = posi / masksize;
        }
    }
    if (negTargetMasksList != null) {
        let nega = 0;
        let negapersent = 0.0;
        for (i = 0; i < negTargetMasksList.length; i++) {
            const tmpnega = calculateScore({
                source: negQueryImage,
                target: targetImage,
                sourcePositions: negMaskPositions,
                targetPositions: negTargetMasksList[i],
                searchThreshold: searchThreshold,
                zTolerance: zTolerance
            });
            if (tmpnega > nega) {
                nega = tmpnega;
                negapersent = nega / negmasksize;
            }
        }
        posipersent -= negapersent;
        posi = Math.round(posi - nega * (masksize / negmasksize));
    }

    if (mirrorTargetMasksList != null) {
        let mirror_posi = 0;
        let mirror_posipersent = 0.0;
        for (i = 0; i < mirrorTargetMasksList.length; i++) {
            const tmpposi = calculateScore({
                source: queryImage,
                target: targetImage,
                sourcePositions: maskPositions,
                targetPositions: mirrorTargetMasksList[i],
                searchThreshold: searchThreshold,
                zTolerance: zTolerance
            });
            if (tmpposi > mirror_posi) {
                mirror_posi = tmpposi;
                mirror_posipersent = mirror_posi / masksize;
            }
        }
        if (negMirrorTargetMasksList != null) {
            let nega = 0;
            let negapersent = 0.0;
            for (i = 0; i < negMirrorTargetMasksList.length; i++) {
                const tmpnega = calculateScore({
                    source: negQueryImage,
                    target: targetImage,
                    sourcePositions: negMaskPositions,
                    targetPositions: negMirrorTargetMasksList[i],
                    searchThreshold: searchThreshold,
                    zTolerance: zTolerance
                });
                if (tmpnega > nega) {
                    nega = tmpnega;
                    negapersent = nega / negmasksize;
                }
            }
            mirror_posipersent -= negapersent;
            mirror_posi = Math.round(mirror_posi - nega * (masksize / negmasksize));
        }
        if (posipersent < mirror_posipersent) {
            posi = mirror_posi;
            posipersent = mirror_posipersent;
        }
    }

    return {
        matchingPixNum: posi,
        matchingPixNumToMaskRatio: posipersent
    };
};
