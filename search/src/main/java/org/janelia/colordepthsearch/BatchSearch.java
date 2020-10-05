package org.janelia.colordepthsearch;

import java.net.URI;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.xray.AWSXRay;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPMatchScore;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * AWS Lambda Handler that performs a pairwise color depth search between all provided MIPs to be searched and all provided masks.
 * The handler writes down the result to s specified location and returns the number of found matches.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearch implements RequestHandler<BatchSearchParameters, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchSearch.class);
    private static final int DEFAULT_MASK_THRESHOLD = 100;

    @Override
    public Integer handleRequest(BatchSearchParameters params, Context context) {
        if (StringUtils.isNotBlank(params.getSearchId())) {
            MDC.put("searchId", params.getSearchId());
        }
        LOG.trace("Batch search invoked with {}", params);
        verifyCDSParams(params);
        S3Client s3 = LambdaUtils.createS3();
        List<ColorMIPSearchResult> cdsResults = performColorDepthSearch(params, s3);
        writeCDSResults(cdsResults, s3, params.getOutputURI());
        return cdsResults.size();
    }

    private void verifyCDSParams(BatchSearchParameters params) {
        AWSXRay.beginSubsegment("Read parameters");
        LOG.debug("Received color depth search request: {}", LambdaUtils.toJson(params));
        LOG.info("Monitor: {}", params.getMonitorName());
        // This next log statement is parsed by the analyzer. DO NOT CHANGE.
        LOG.info("Batch Id: {}", params.getBatchId());
        LOG.info("Searching {} images using {} masks", params.getSearchKeys().size(), params.getMaskKeys().size());
        if (LambdaUtils.isEmpty(params.getSearchKeys())) {
            throw new IllegalArgumentException("No images to search");
        }
        if (LambdaUtils.isEmpty(params.getMaskKeys())) {
            throw new IllegalArgumentException("No masks to search");
        }
        if (LambdaUtils.isEmpty(params.getMaskThresholds())) {
            throw new IllegalArgumentException("No mask thresholds specified");
        }
        if (params.getMaskThresholds().size() != params.getMaskKeys().size()) {
            throw new IllegalArgumentException("Number of mask thresholds does not match number of masks");
        }
        AWSXRay.endSubsegment();
    }

    private List<ColorMIPSearchResult> performColorDepthSearch(BatchSearchParameters params, S3Client s3) {
        AWSXRay.beginSubsegment("Run search");
        AWSMIPLoader mipsLoader = new AWSMIPLoader(s3);
        ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider;
        if (params.isWithGradientScore() && !LambdaUtils.isEmpty(params.getGradientKeys())) {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchWithNegativeScoreCDSAlgorithmProvider(
                    DEFAULT_MASK_THRESHOLD,
                    params.isMirrorMask(),
                    params.getDataThreshold(),
                    params.getPixColorFluctuation(),
                    params.getXyShift(),
                    params.getNegativeRadius(),
                    mipsLoader.readImageWithRetry(params.getSearchPrefix(), params.getMaskROIKey(), 2)
            );
        } else {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchCDSAlgorithmProvider(
                    DEFAULT_MASK_THRESHOLD,
                    params.isMirrorMask(),
                    params.getDataThreshold(),
                    params.getPixColorFluctuation(),
                    params.getXyShift()
            );
        }

        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(
                params.getMinMatchingPixRatio(),
                cdsAlgorithmProvider);
        AWSLambdaColorMIPSearch awsColorMIPSearch = new AWSLambdaColorMIPSearch(
                mipsLoader,
                colorMIPSearch,
                params.getMaskPrefix(),
                params.getSearchPrefix(),
                LambdaUtils.getOptionalEnv("SEARCHED_THUMBNAILS_BUCKET", params.getSearchPrefix())
        );

        LOG.debug("Comparing {} masks with {} library mips", params.getMaskKeys().size(), params.getSearchKeys().size());
        List<ColorMIPSearchResult> cdsResults = awsColorMIPSearch.findAllColorDepthMatches(
                params.getMaskKeys(),
                params.getMaskThresholds(),
                params.getSearchKeys(),
                params.getGradientKeys(),
                params.getZgapMaskKeys()
        );
        LOG.info("Found {} matches.", cdsResults.size());
        AWSXRay.endSubsegment();
        return cdsResults;
    }

    private void writeCDSResults(List<ColorMIPSearchResult> cdsResults, S3Client s3, String outputLocation) {
        if (outputLocation != null) {
            AWSXRay.beginSubsegment("Sort and save results");
            try {
                LambdaUtils.putObject(
                        s3,
                        URI.create(outputLocation),
                        ColorMIPSearchResultUtils.groupResults(cdsResults, ColorMIPSearchResult::perMaskMetadata));
                LOG.info("Results written to {}", outputLocation);
            } catch (Exception e) {
                throw new IllegalStateException("Error writing results", e);
            }
            AWSXRay.endSubsegment();
        }
    }
}
