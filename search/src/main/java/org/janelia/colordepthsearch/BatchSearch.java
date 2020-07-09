package org.janelia.colordepthsearch;

import java.net.URI;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;
import com.amazonaws.xray.AWSXRay;

import software.amazon.awssdk.services.s3.S3Client;

/**
 *  Search a list of color depth images using a list of masks.
 *
 *  Implements the BatchSearch AWS Lambda Handler
 *
 *  @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearch implements RequestHandler<BatchSearchParameters, Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchSearch.class);

    @Override
    public Boolean handleRequest(BatchSearchParameters params, Context context) {
        final Region region = Region.of(LambdaUtils.getMandatoryEnv("AWS_REGION"));

        AWSXRay.beginSubsegment("Read parameters");

        LOG.debug("Environment:\n  region: {}", region);
        LOG.debug("Received color depth search request: {}", LambdaUtils.toJson(params));

        S3Client s3 = S3Client.builder().region(region).build();

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
                params.getSearchPrefix(),
                LambdaUtils.getOptionalEnv("SEARCHED_THUMBNAILS_BUCKET", params.getSearchPrefix())
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
                URI outputURI = URI.create(params.getOutputURI());
                LambdaUtils.putObject(
                        s3,
                        outputURI.getHost(),
                        outputURI.getPath(),
                        ColorMIPSearchResultUtils.groupResults(cdsResults, ColorMIPSearchResult::perMaskMetadata));
                LOG.info("Results written to {}", outputURI);
            } catch (Exception e) {
                throw new RuntimeException("Error writing results", e);
            }
        }
        AWSXRay.endSubsegment();
        return true;
    }

}
