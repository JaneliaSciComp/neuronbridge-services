package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Useful utility functions for writing AWS Lambda functions in Java.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class LambdaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LambdaUtils.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false)
                ;

    static S3Client createS3() {
        final Region region = Region.of(LambdaUtils.getMandatoryEnv("AWS_REGION"));
        LOG.debug("Environment:\n  region: {}", region);
        return S3Client.builder().region(region).build();
    }

    static String getMandatoryEnv(String name) {
        if (StringUtils.isBlank(System.getenv(name))) {
            throw new IllegalStateException(String.format("Missing environment variable: %s", name));
        }
        return System.getenv(name);
    }

    static String getOptionalEnv(String name, String defaultValue) {
        if (StringUtils.isBlank(System.getenv(name))) {
            return defaultValue;
        }
        return System.getenv(name);
    }

    static String toJson(Object object) {
        try {
            return JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (Exception e) {
            LOG.error("Serialization error", e);
            return "{\"error\":\"Could not serialize object\"}";
        }
    }

    static <T> T fromJson(InputStream objectStream, Class<T> objectType) {
        try {
            return JSON_MAPPER.readValue(objectStream, objectType);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error trying to read on object of type " + objectType, e);
        }
    }

    static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    static InputStream getObject(S3Client s3, String bucket, String key) {
        try {
            return s3.getObject(GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build(),
                    ResponseTransformer.toInputStream());
        } catch (Exception e) {
            LOG.error("Error reading object from {}:{}", bucket, key, e);
            throw new IllegalArgumentException(e);
        }
    }

    static InputStream getObject(S3Client s3, String bucket, String key, long range_st, long range_ed) {
        try {
            return s3.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range(String.format("%d-%d", range_st, range_ed))
                    .build(),
                    ResponseTransformer.toInputStream());
        } catch (Exception e) {
            LOG.error("Error reading object from {}:{}", bucket, key, e);
            throw new IllegalArgumentException(e);
        }
    }

    static void putObject(S3Client s3, URI s3URI, Object object) {
        putObject(s3, s3URI.getHost(), StringUtils.removeStart(s3URI.getPath(), "/"), object);
    }

    static void putObject(S3Client s3, String bucket, String key, Object object) {
        String s = LambdaUtils.toJson(object);
        try {
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("application/json")
                            .contentLength((long) StringUtils.length(s))
                            .build(),
                    RequestBody.fromString(s));
        } catch (Exception e) {
            LOG.error("Error writing object {} to {}:{}", s, bucket, key, e);
            throw new IllegalStateException(e);
        }
    }

    static List<S3Object> listObjects(S3Client s3, String bucket, String prefix) {
        ListObjectsResponse res = s3.listObjects(ListObjectsRequest
                .builder()
                .bucket(bucket)
                .prefix(prefix)
                .build());
        if (res.hasContents()) {
            return res.contents();
        } else {
            return Collections.emptyList();
        }
    }

}
