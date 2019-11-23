package org.janelia.colordepthsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ParallelSearch implements RequestHandler<ParallelSearchParameters, Long> {

    private static final Logger log = LoggerFactory.getLogger(ParallelSearch.class);

    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final int MAX_PARALLELISM = 1000;

    @Override
    public Long handleRequest(ParallelSearchParameters params, Context context) {

        final String region = LambdaUtils.getMandatoryEnv("AWS_REGION");
        final String maskBucket = LambdaUtils.getMandatoryEnv("MASK_BUCKET");
        final String libraryBucket = LambdaUtils.getMandatoryEnv("LIBRARY_BUCKET");
        final String searchFunction = LambdaUtils.getMandatoryEnv("SEARCH_FUNCTION");

        log.debug("Environment:\n  region: {}\n  maskBucket: {}\n  libraryBucket: {}\n  searchFunction: {}",
                region, maskBucket, libraryBucket, searchFunction);
        log.debug("Received color depth parallel search request: {}", LambdaUtils.toJson(params));

        if (LambdaUtils.isEmpty(params.getLibraries())) {
            log.error("No color depth libraries specified");
            System.exit(1);
        }

        if (LambdaUtils.isEmpty(params.getMaskKeys())) {
            log.error("No masks specified");
            System.exit(1);
        }

        if (LambdaUtils.isEmpty(params.getMaskThresholds())) {
            log.error("No mask thresholds specified");
            System.exit(1);
        }

        if (params.getMaskThresholds().size()!=params.getMaskKeys().size()) {
            log.error("Number of mask thresholds does not match number of masks ({}!={})",
                    params.getMaskThresholds().size(), params.getMaskKeys().size());
            System.exit(1);
        }

        // Find all keys to search
        List<String> libraries = params.getLibraries();
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
        List<String> keys = new ArrayList<>();
        for(String library : libraries) {

            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(libraryBucket).withPrefix(library);
            ListObjectsV2Result result;
            do {
                result = s3.listObjectsV2(req);
                keys.addAll(result.getObjectSummaries().stream()
                        .map(S3ObjectSummary::getKey) // get the keys
                        .filter(k -> !k.equals(library+"/")) // exclude folder
                        .collect(Collectors.toList()));

                // If there are more than maxKeys keys in the bucket, get a continuation token
                // and list the next objects.
                req.setContinuationToken(result.getNextContinuationToken());
            }
            while (result.isTruncated());
        }

        if (keys.isEmpty()) {
            log.error("No images to search");
            System.exit(1);
        }

        log.info("Total number of images to search: {}", keys.size());

        // Calculate batch size
        int batchSize = DEFAULT_BATCH_SIZE;
        int total = keys.size();
        int numBatches = total / batchSize;
        if (numBatches>MAX_PARALLELISM) {
            batchSize = total / MAX_PARALLELISM;
        }
        log.info("Batch size: {}", batchSize);

        // Create partitions
        List<List<String>> partitions = Lists.partition(keys, batchSize);
        log.info("Num partitions: {}", partitions.size());

        // TODO: put batches in DB

        AWSLambdaAsync client = AWSLambdaAsyncClientBuilder.standard()
                .withRegion(region)
                .build();

        final BatchSearchService batchSearch = LambdaInvokerFactory.builder()
                .lambdaClient(client)
                .lambdaFunctionNameResolver((method, annotation, config) -> searchFunction)
                .build(BatchSearchService.class);

        // Dispatch all batches
        int i = 0;
        for (List<String> batchKeys : partitions) {
            BatchSearchParameters searchParameters = new BatchSearchParameters();
            searchParameters.setDataThreshold(params.getDataThreshold());
            searchParameters.setMaskKeys(params.getMaskKeys());
            searchParameters.setMaskPrefix(maskBucket);
            searchParameters.setMaskThresholds(params.getMaskThresholds());
            searchParameters.setMirrorMask(params.isMirrorMask());
            searchParameters.setPixColorFluctuation(params.getPixColorFluctuation());
            searchParameters.setXyShift(params.getXyShift());
            searchParameters.setSearchPrefix(libraryBucket);
            searchParameters.setSearchKeys(batchKeys);
            batchSearch.search(searchParameters);
            log.info("Invoked batch #{}", ++i);
        }

        log.info("Parallel search started");

        // TODO: return id of the search
        return 1L;
    }

}
