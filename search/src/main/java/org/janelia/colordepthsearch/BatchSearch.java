package org.janelia.colordepthsearch;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;

/**
 * AWS Lambda Handler that performs a pairwise color depth search between all provided MIPs to be searched and all provided masks.
 * The handler writes down the result to s specified location and returns the number of found matches.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearch implements RequestHandler<BatchSearchParameters, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchSearch.class);

    @Override
    public Integer handleRequest(BatchSearchParameters params, Context context) {
        if (StringUtils.isNotBlank(params.getJobId())) {
            MDC.put("jobId", params.getJobId());
        }
        verifyCDSParams(params);
        S3Client s3 = LambdaUtils.createS3();

        List<ColorMIPSearchResult> cdsResults = performColorDepthSearch(params, s3);
        List<CDSMatches> results = ColorMIPSearchResultUtils.groupResults(cdsResults, ColorMIPSearchResult::perMaskMetadata);

        // Write results to DynamoDB
        String tableName = LambdaUtils.getOptionalEnv("JOB_TABLE_NAME", null);
        if (tableName != null && params.getJobId() != null && params.getBatchId() != null) {
            DynamoDbClient dynamoDbClient = LambdaUtils.createDynamoDB();
            writeCDSResults(results, dynamoDbClient, tableName, params.getJobId(), params.getBatchId());
        }

        // Write results to S3
        if (params.getOutputURI() != null) {
            writeCDSResults(results, s3, params.getOutputURI());
        }

        return cdsResults.size();
    }

    private void verifyCDSParams(BatchSearchParameters params) {
        LOG.debug("Received color depth search request: {}", LambdaUtils.toJson(params));

        // The next three log statements are parsed by the analyzer. DO NOT CHANGE.
        LOG.info("Monitor: {}", params.getMonitorName());
        LOG.info("Search Id: {}", params.getBatchId());
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
    }

    private List<ColorMIPSearchResult> performColorDepthSearch(BatchSearchParameters params, S3Client s3) {

        long start = System.currentTimeMillis();

        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(
                0,
                params.getDataThreshold(),
                params.getPixColorFluctuation(),
                params.getXyShift(),
                params.isMirrorMask(),
                params.getMinMatchingPixRatio());
        AWSLambdaColorMIPSearch awsColorMIPSearch = new AWSLambdaColorMIPSearch(
                new AWSMIPLoader(s3),
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

        long elapsed = System.currentTimeMillis() - start;
        LOG.info("Found {} matches in {} ms.", cdsResults.size(), elapsed);

        return cdsResults;
    }

    private void writeCDSResults(List<CDSMatches> results, DynamoDbClient dynamoDbClient, String tableName, String jobId, Integer batchId) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(jobId).build());
        item.put("batchId", AttributeValue.builder().n(batchId.toString()).build());
        item.put("results", AttributeValue.builder().s(LambdaUtils.toJson(results)).build());
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();
        dynamoDbClient.putItem(putItemRequest);
        LOG.info("Results written to DynamoDB table {} with id={} and batchId={}", tableName, jobId, batchId);
    }

    private void writeCDSResults(List<CDSMatches> results, S3Client s3, String outputLocation) {
        try {
            LambdaUtils.putObject(s3, URI.create(outputLocation), results);
            LOG.info("Results written to {}", outputLocation);
        } catch (Exception e) {
            throw new IllegalStateException("Error writing results", e);
        }
    }
}
