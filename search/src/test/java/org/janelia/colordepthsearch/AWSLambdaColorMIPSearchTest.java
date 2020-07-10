package org.janelia.colordepthsearch;


import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;

import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPCompareOutput;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPMaskCompare;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.api.imageprocessing.ImageArrayUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AWSLambdaColorMIPSearchTest {

    private static final String AWS_MASKS_BUCKET = "janelia-neuronbridge-searches-dev";
    private static final String AWS_LIBRARIES_BUCKET = "janelia-flylight-color-depth";
    private static final String AWS_LIBRARIES_THUMBNAILS_BUCKET = "janelia-flylight-color-depth-thumbnails";

    private AWSMIPLoader mipLoader;
    private ColorMIPSearch colorMIPSearch;
    private ColorMIPMaskCompare maskComparator;
    private AWSLambdaColorMIPSearch awsLambdaColorMIPSearch;

    @Before
    public void setUp() {
        mipLoader = mock(AWSMIPLoader.class);
        colorMIPSearch = mock(ColorMIPSearch.class);
        maskComparator = mock(ColorMIPMaskCompare.class);
        awsLambdaColorMIPSearch = new AWSLambdaColorMIPSearch(
                mipLoader,
                colorMIPSearch,
                AWS_MASKS_BUCKET,
                AWS_LIBRARIES_BUCKET,
                AWS_LIBRARIES_THUMBNAILS_BUCKET
        );
    }

    @Test
    public void colorDepthMatches() {
        prepareColorDepthSearchInvocation();
        List<String> maskKeys = Arrays.asList(
                "private/us-east-1:853b7e81-c739-4434-99dd-aafeed3265e3/rc-upload-1594225719194-2/rc-upload-1594225719194-2.png"
        );
        List<String> libraryKeys = Arrays.asList(
                "JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/searchable_neurons/LH2453-20121114_31_E5-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1-001.tif",
                "JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/LH2453-20121114_31_E5-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1-001.tif",
                "JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/searchable_neurons/LH2453-20121114_31_E5-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.tif",
                "JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/LH2453-20121114_31_E5-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.tif",
                "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.1/searchable_neurons/1005308608-EL-RT-JRC2018_Unisex_20x_HR-CDM.tif",
                "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.1/searchable_neurons/1002360103-AVLP464-RT-JRC2018_Unisex_20x_HR-CDM.tif",
                "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.1/searchable_neurons/1002507131-PEN-a(PEN1)-JRC2018_Unisex_20x_HR-CDM-FL.tif"
        );
        List<ColorMIPSearchResult> searchResults = awsLambdaColorMIPSearch.findAllColorDepthMatches(
                maskKeys,
                Arrays.asList(100),
                libraryKeys
        );
        assertEquals(libraryKeys.size(), searchResults.size());
    }

    private void prepareColorDepthSearchInvocation() {
        when(mipLoader.loadMIP(anyString(), any(MIPMetadata.class)))
                .then(invocation -> {
                    MIPMetadata mip = invocation.getArgument(1);
                    return new MIPImage(mip,
                            ImageArrayUtils.readImageArray(
                                    mip.getId(),
                                    "test.png",
                                    new FileInputStream("src/test/resources/mips/testMIP.png")));
                });
        when(colorMIPSearch.createMaskComparator(any(MIPImage.class), anyInt()))
                .thenReturn(maskComparator);
        ColorMIPCompareOutput sr = mock(ColorMIPCompareOutput.class);
        when(maskComparator.runSearch(any(ImageArray.class))).thenReturn(sr);
        when(colorMIPSearch.isMatch(sr)).thenReturn(true);
        when(sr.getMatchingPixNum()).thenReturn(100); // a random test value
        when(sr.getMatchingPixNumToMaskRatio()).thenReturn(0.1); // a random test value
    }

}
