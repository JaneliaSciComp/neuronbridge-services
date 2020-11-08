package org.janelia.colordepthsearch;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPMatchScore;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * AWS Lambda Handler that performs a pairwise color depth search between all provided MIPs to be searched and all provided masks.
 * The handler writes down the result to s specified location and returns the number of found matches.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearch implements RequestHandler<BatchSearchParameters, Integer> {

    private static class SearchTarget {
        final String searchKey;
        final String gradientKey;
        final String zgapMaskKey;

        SearchTarget(String searchKey, String gradientKey, String zgapMaskKey) {
            this.searchKey = searchKey;
            this.gradientKey = gradientKey;
            this.zgapMaskKey = zgapMaskKey;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("searchKey", searchKey)
                    .append("gradientKey", gradientKey)
                    .append("zgapMaskKey", zgapMaskKey)
                    .toString();
        }
    }
    private static final Logger LOG = LoggerFactory.getLogger(BatchSearch.class);

    private final Random randomGen = new Random();

    @Override
    public Integer handleRequest(BatchSearchParameters params, Context context) {
        long startTime = System.currentTimeMillis();
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
        } else {
            LOG.error("Could not write results to DynamoDB. Missing tableName, jobId, and/or batchId.");
        }
        LOG.info("Completed batch {}:{} in {}s", params.getJobId(), params.getBatchId(), (System.currentTimeMillis() - startTime) / 1000.);
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
        LOG.info("Invoke color depth search with {}", params);
        ColorDepthSearchParameters jobParams = params.getJobParameters();

        List<SearchTarget> searchTargets = getSearchTargets(s3,
                jobParams.getLibraryBucket(),
                jobParams.getLibraries(),
                jobParams.getGradientsFolders(),
                jobParams.getZgapMasksFolders(),
                params.getStartIndex(),
                params.getEndIndex());
        LOG.info("Loaded {} search keys", searchTargets.size());
        ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider;
        if (jobParams.isWithGradientScores()) {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchWithNegativeScoreCDSAlgorithmProvider(
                    jobParams.isMirrorMask(),
                    jobParams.getDataThreshold(),
                    jobParams.getPixColorFluctuation(),
                    jobParams.getXyShift(),
                    jobParams.getNegativeRadius(),
                    null
            );
        } else {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchCDSAlgorithmProvider(
                    jobParams.isMirrorMask(),
                    jobParams.getDataThreshold(),
                    jobParams.getPixColorFluctuation(),
                    jobParams.getXyShift()
            );
        }

        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(jobParams.getMinMatchingPixRatio(), ColorDepthSearchParameters.DEFAULT_MASK_THRESHOLD, cdsAlgorithmProvider);
        AWSLambdaColorMIPSearch awsColorMIPSearch = new AWSLambdaColorMIPSearch(
                new AWSMIPLoader(s3),
                colorMIPSearch,
                jobParams.getSearchBucket(),
                jobParams.getLibraryBucket(),
                LambdaUtils.getOptionalEnv("SEARCHED_THUMBNAILS_BUCKET", jobParams.getLibraryBucket())
        );

        LOG.debug("Comparing {} masks with {} library mips", jobParams.getMaskKeys().size(), searchTargets.size());
        List<ColorMIPSearchResult> cdsResults = awsColorMIPSearch.findAllColorDepthMatches(
                jobParams.getMaskKeys(),
                jobParams.getMaskThresholds(),
                searchTargets.stream().map(t -> t.searchKey).collect(Collectors.toList()),
                searchTargets.stream().map(t -> t.gradientKey).collect(Collectors.toList()),
                searchTargets.stream().map(t -> t.zgapMaskKey).collect(Collectors.toList())
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

    private List<SearchTarget> getSearchTargets(S3Client s3,
                                                String libraryBucket,
                                                List<String> searcheableFolders,
                                                List<String> gradientsFolders,
                                                List<String> zgapMasksFolders,
                                                int startIndex,
                                                int endIndex) {
        List<SearchTarget> searchTargets = new ArrayList<>();
        int targetIndex = 0;
        List<SearchTarget> searchTargetFolders = IntStream.range(0, searcheableFolders.size())
                .boxed()
                .map(index -> {
                    String searcheableFolder = searcheableFolders.get(index);
                    String gradientsFolder = CollectionUtils.size(gradientsFolders) < index
                            ? null
                            : IterableUtils.get(gradientsFolders, index);
                    String zgapMasksFolder = CollectionUtils.size(zgapMasksFolders) < index
                            ? null
                            : IterableUtils.get(zgapMasksFolders, index);
                    return new SearchTarget(searcheableFolder, gradientsFolder, zgapMasksFolder);
                })
                .collect(Collectors.toList());

        int randomPrefix = randomGen.nextInt(100);
        for (SearchTarget searchTargetFolder : searchTargetFolders) {
            String keyListKey = searchTargetFolder.searchKey + "/KEYS/" + randomPrefix + "/keys_denormalized.json";
            LOG.info("Retrieving keys in s3://{}/{}", libraryBucket, keyListKey);
            InputStream object = LambdaUtils.getObject(s3, libraryBucket, keyListKey);
            List<String> searchableKeys = LambdaUtils.fromJson(object, List.class);
            for (String key : searchableKeys) {
                if (targetIndex >= startIndex && targetIndex < endIndex) {
                    // replace the search folder and remove the extension
                    String gradientKey = StringUtils.isNotBlank(searchTargetFolder.gradientKey)
                        ? key.replace(searchTargetFolder.searchKey, searchTargetFolder.gradientKey).replaceAll("\\..*$", "")
                        : null;
                    String zgapMaskKey = StringUtils.isNotBlank(searchTargetFolder.zgapMaskKey)
                            ? key.replace(searchTargetFolder.searchKey, searchTargetFolder.zgapMaskKey).replaceAll("\\..*$", "")
                            : null;
                    SearchTarget searchTarget = new SearchTarget(key, gradientKey, zgapMaskKey);
                    searchTargets.add(searchTarget);
                }
                targetIndex++;
                if (targetIndex >= endIndex) {
                    LOG.info("Return {} search targets starting with index {} to {}", searchTargets.size(), startIndex, endIndex);
                    return searchTargets;
                }
            }
        }
        throw new IllegalStateException("Could not find items " + startIndex + "-" + endIndex + " in library keys");
    }
}
