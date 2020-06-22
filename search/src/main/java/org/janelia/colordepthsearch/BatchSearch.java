package org.janelia.colordepthsearch;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.xray.AWSXRay;

import org.janelia.colormipsearch.api.ColorMIPCompareOutput;
import org.janelia.colormipsearch.api.ColorMIPMaskCompare;
import org.janelia.colormipsearch.api.ColorMIPMaskCompareFactory;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.api.imageprocessing.ImageArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Search a list of color depth images using a list of masks.
 *
 *  Implements the BatchSearchService API.
 *
 *  @see org.janelia.colordepthsearch.BatchSearchService
 *
 *  @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearch implements RequestHandler<BatchSearchParameters, Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchSearch.class);

    @Override
    public Boolean handleRequest(BatchSearchParameters params, Context context) {

        AWSXRay.beginSubsegment("Read parameters");
        final String region = LambdaUtils.getMandatoryEnv("AWS_REGION");

        LOG.debug("Environment:\n  region: {}", region);
        LOG.debug("Received color depth search request: {}", LambdaUtils.toJson(params));

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

        if (LambdaUtils.isEmpty(params.getSearchKeys())) {
            throw new IllegalArgumentException("No images to search");
        }

        if (LambdaUtils.isEmpty(params.getMaskKeys())) {
            throw new IllegalArgumentException("No masks to search");
        }

        if (LambdaUtils.isEmpty(params.getMaskThresholds())) {
            throw new IllegalArgumentException("No mask thresholds specified");
        }

        if (params.getMaskThresholds().size()!=params.getMaskKeys().size()) {
            throw new IllegalArgumentException("Number of mask thresholds does not match number of masks");
        }

        // Preload all masks into memory
        List<ImageArray> maskImages = new ArrayList<>();

        AWSXRay.endSubsegment();
        AWSXRay.beginSubsegment("Load masks");

        try {
            for (String maskKey : params.getMaskKeys()) {
                S3Object maskObject = s3.getObject(params.getMaskPrefix(), maskKey);
                try (S3ObjectInputStream s3is = maskObject.getObjectContent()) {
                    maskImages.add(ImageArrayUtils.readImageArray(maskKey, maskKey, s3is));
                }
            }
            if (maskImages.isEmpty()) {
                throw new IllegalStateException("Could not load search masks.");
            }
        }
        catch (Exception e) {
            throw new IllegalStateException("Error loading mask images", e);
        }

        AWSXRay.endSubsegment();
        AWSXRay.beginSubsegment("Search");

        LOG.debug("Searching {} images with {} masks", params.getSearchKeys().size(), maskImages.size());
        
        // Create a result array for each mask
        List<List<MaskSearchResult>> results = new ArrayList<>();
        for (ImageArray maskImage : maskImages) {
            results.add(new ArrayList<>());
        }    

        // Load each search image and compare it to all the masks already in memory
        for (String searchKey : params.getSearchKeys()) {
            try {
                S3Object searchObject = s3.getObject(params.getSearchPrefix(), searchKey);
                if (searchObject == null) {
                    LOG.error("Error loading search image {}", searchKey);
                }
                else {
                    ImageArray searchImage;
                    try (S3ObjectInputStream s3is = searchObject.getObjectContent()) {
                        searchImage = ImageArrayUtils.readImageArray(searchKey, searchKey, s3is);
                    }

                    int maskIndex = 0;
                    for (ImageArray maskImage : maskImages) {
                        Integer maskThreshold = params.getMaskThresholds().get(maskIndex);

                        double pixfludub = params.getPixColorFluctuation() / 100;
                        final ColorMIPMaskCompare cc = ColorMIPMaskCompareFactory.createMaskComparator(
                                maskImage,
                                maskThreshold,
                                params.isMirrorMask(),
                                params.getDataThreshold(),
                                pixfludub,
                                params.getXyShift()
                        );
                        ColorMIPCompareOutput output = cc.runSearch(searchImage);

                        if (output.getMatchingPixNum() > params.getMinMatchingPix()) {
                            results.get(maskIndex).add(new MaskSearchResult(
                                    searchKey,
                                    output.getMatchingPixNum()));
                        }

                        maskIndex++;
                    }
                }
            } catch (Exception e) {
                LOG.error("Error searching {}", searchKey, e);
            }
        }

        LOG.info("Found {} matches.", results.size());

        AWSXRay.endSubsegment();
        AWSXRay.beginSubsegment("Sort and save results");

        // Sort the results for each mask
        for (List<MaskSearchResult> maskResults : results) {
            maskResults.sort((o1, o2) -> {
                Double i1 = o1.getScore();
                Double i2 = o2.getScore();
                return i2.compareTo(i1); // reverse sort
            });
        }

        if (params.getOutputFile()==null) {
            // Print some results to the log
            int maskIndex = 0;
            for (List<MaskSearchResult> maskResults : results) {
                LOG.info("Mask #{}", maskIndex);
                int i = 0;
                for (MaskSearchResult result : maskResults) {
                    LOG.info("Match {} - {}", result.getScore(), result.getFilepath());
                    if (i > 9) break;
                    i++;
                }
                maskIndex++;
            }
        }
        else {
            try {
                AmazonS3URI outputUri = new AmazonS3URI(params.getOutputFile());
                LambdaUtils.putObject(s3, outputUri, results);
                LOG.info("Results written to {}", outputUri);
            }
            catch (Exception e) {
                throw new RuntimeException("Error writing results", e);
            }
        }

        AWSXRay.endSubsegment();

        return true;
    }

}
