package org.janelia.colordepthsearch;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.xray.AWSXRay;

import org.janelia.colormipsearch.tools.ColorMIPSearch;
import org.janelia.colormipsearch.tools.ColorMIPSearchResult;
import org.janelia.colormipsearch.tools.ColorMIPSearchResultUtils;
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

        AWSXRay.endSubsegment();
        AWSXRay.beginSubsegment("Run search");

        AWSMIPLoader mipLoader = new AWSMIPLoader(s3);
        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(
                0,
                params.getDataThreshold(),
                params.getPixColorFluctuation(),
                params.getXyShift(),
                params.isMirrorMask(),
                params.getMinMatchingPixRatio());
        AWSLambdaColorMIPSearch awsColorMIPSearch = new AWSLambdaColorMIPSearch(
                mipLoader,
                colorMIPSearch,
                params.getMaskPrefix(),
                params.getSearchPrefix()
        );

        LOG.debug("Comparing {} masks with {} library mips", params.getMaskKeys().size(), params.getSearchKeys().size());
        List<ColorMIPSearchResult> cdsResults = awsColorMIPSearch.findAllColorDepthMatches(
                params.getMaskKeys(),
                params.getMaskThresholds(),
                params.getSearchKeys()
        );
        LOG.info("Found {} matches.", cdsResults.size());
        AWSXRay.endSubsegment();

        AWSXRay.beginSubsegment("Sort and save results");
        if (params.getOutputURI() != null) {
            try {
                AmazonS3URI outputUri = new AmazonS3URI(params.getOutputURI());
                LambdaUtils.putObject(
                        s3,
                        outputUri,
                        ColorMIPSearchResultUtils.groupResults(cdsResults, ColorMIPSearchResult::perMaskMetadata));
                LOG.info("Results written to {}", outputUri);
            } catch (Exception e) {
                throw new RuntimeException("Error writing results", e);
            }
        }
        AWSXRay.endSubsegment();
        return true;
    }

}
