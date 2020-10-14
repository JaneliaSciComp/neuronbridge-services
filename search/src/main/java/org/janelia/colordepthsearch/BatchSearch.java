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

import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        String tableName = params.getTasksTableName();
        if (tableName != null && params.getJobId() != null && params.getBatchId() != null) {
            DynamoDbClient dynamoDbClient = LambdaUtils.createDynamoDB();
            writeCDSResults(results, dynamoDbClient, tableName, params.getJobId(), params.getBatchId());
        }

        return cdsResults.size();
    }

    private void verifyCDSParams(BatchSearchParameters params) {
        LOG.debug("Received color depth search request: {}", LambdaUtils.toJson(params));

        // The next two log statements are parsed by the analyzer. DO NOT CHANGE.
        LOG.info("Job Id: {}", params.getJobId());
        LOG.info("Batch Id: {}", params.getBatchId());

        ColorDepthSearchParameters jobParams = params.getJobParameters();
        if (jobParams == null) {
            throw new IllegalArgumentException("No color depth search parameters");
        }
        if (LambdaUtils.isEmpty(jobParams.getLibraries())) {
            throw new IllegalArgumentException("No images to search");
        }
        if (LambdaUtils.isEmpty(jobParams.getMaskKeys())) {
            throw new IllegalArgumentException("No masks to search");
        }
        if (LambdaUtils.isEmpty(jobParams.getMaskThresholds())) {
            throw new IllegalArgumentException("No mask thresholds specified");
        }
        if (jobParams.getMaskThresholds().size() != jobParams.getMaskKeys().size()) {
            throw new IllegalArgumentException("Number of mask thresholds does not match number of masks");
        }
        LOG.info("Searching using {} libraries and {} masks",
                jobParams.getLibraries().size(), jobParams.getMaskKeys().size());
    }

    private List<ColorMIPSearchResult> performColorDepthSearch(BatchSearchParameters params, S3Client s3) {

        long start = System.currentTimeMillis();

        ColorDepthSearchParameters jobParams = params.getJobParameters();

        List<String> searchKeys = getSearchKeys(s3,
                jobParams.getLibraryBucket(),
                jobParams.getLibraries(),
                params.getStartIndex(),
                params.getEndIndex());
        LOG.info("Loaded {} search keys", searchKeys.size());
        
        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(
                0,
                jobParams.getDataThreshold(),
                jobParams.getPixColorFluctuation(),
                jobParams.getXyShift(),
                jobParams.isMirrorMask(),
                jobParams.getMinMatchingPixRatio());
        AWSLambdaColorMIPSearch awsColorMIPSearch = new AWSLambdaColorMIPSearch(
                new AWSMIPLoader(s3),
                colorMIPSearch,
                jobParams.getSearchBucket(),
                jobParams.getLibraryBucket(),
                LambdaUtils.getOptionalEnv("SEARCHED_THUMBNAILS_BUCKET", jobParams.getLibraryBucket())
        );

        LOG.debug("Comparing {} masks with {} library mips", jobParams.getMaskKeys().size(), searchKeys.size());
        List<ColorMIPSearchResult> cdsResults = awsColorMIPSearch.findAllColorDepthMatches(
                jobParams.getMaskKeys(),
                jobParams.getMaskThresholds(),
                searchKeys
        );

        long elapsed = System.currentTimeMillis() - start;
        LOG.info("Found {} matches in {} ms.", cdsResults.size(), elapsed);

        return cdsResults;
    }

    private void writeCDSResults(List<CDSMatches> results, DynamoDbClient dynamoDbClient, String tableName, String jobId, Integer batchId) {

        long now = Instant.now().getEpochSecond(); // unix time
        long ttl = now + 60 * 60; // 60 minutes

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("jobId", AttributeValue.builder().s(jobId).build());
        item.put("batchId", AttributeValue.builder().n(batchId.toString()).build());
        item.put("ttl", AttributeValue.builder().n(ttl+"").build());
        item.put("results", AttributeValue.builder().s(LambdaUtils.toJson(results)).build());
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(putItemRequest);
        LOG.info("Results written to DynamoDB table {} with id={} and batchId={}", tableName, jobId, batchId);
    }

    private List<String> getSearchKeys(S3Client s3, String libraryBucket, List<String> libraries, int startIndex, int endIndex) {
        List<String> keys = new ArrayList<>();
        int i = 0;
        for (String library : libraries) {
            String keyListKey = library + "/keys_denormalized.json";
            LOG.info("Retrieving keys in s3://{}/{}", libraryBucket, keyListKey);
            InputStream object = LambdaUtils.getObject(s3, libraryBucket, keyListKey);
            List<String> libraryKeys = LambdaUtils.fromJson(object, List.class);
            for (String key : libraryKeys) {
                i++;
                if (i > startIndex && i <= endIndex) {
                    keys.add(key);
                }
                if (i >= endIndex) {
                    return keys;
                }
            }
        }

        throw new IllegalStateException("Could not find items " + startIndex + "-" + endIndex + " in library keys");
    }

    private void writeCDSResults(List<ColorMIPSearchResult> cdsResults, S3Client s3, String outputLocation) {
        if (outputLocation != null) {
            try {
                LambdaUtils.putObject(
                        s3,
                        URI.create(outputLocation),
                        ColorMIPSearchResultUtils.groupResults(cdsResults, ColorMIPSearchResult::perMaskMetadata));
                LOG.info("Results written to {}", outputLocation);
            } catch (Exception e) {
                throw new IllegalStateException("Error writing results", e);
            }
        }
    }
}
