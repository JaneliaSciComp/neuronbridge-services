package org.janelia.colordepthsearch;

import java.io.InputStream;
import java.net.URI;
import java.util.Collection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Useful utility functions for writing AWS Lambda functions in Java.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LambdaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LambdaUtils.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false)
                ;

    public static String getMandatoryEnv(String name) {
        if (StringUtils.isBlank(System.getenv(name))) {
            throw new IllegalStateException(String.format("Missing environment variable: %s", name));
        }
        return System.getenv(name);
    }

    public static String getOptionalEnv(String name, String defaultValue) {
        if (StringUtils.isBlank(System.getenv(name))) {
            return defaultValue;
        }
        return System.getenv(name);
    }

    public static String toJson(Object object) {
        try {
            return JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (Exception e) {
            LOG.error("Serialization error", e);
            return "{\"error\":\"Could not serialize object\"}";
        }
    }

    public static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    public static InputStream getObject(S3Client s3, String bucket, String key) {
        return s3.getObject(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build(),
                ResponseTransformer.toInputStream());
    }

    public static void putObject(S3Client s3, URI s3URI, Object object) {
        putObject(s3, s3URI.getHost(), s3URI.getPath(), object);
    }

    public static void putObject(S3Client s3, String bucket, String key, Object object) {
        String s = LambdaUtils.toJson(object);
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/json")
                .contentLength((long) StringUtils.length(s))
                .build(),
                RequestBody.fromString(s));
    }
}
