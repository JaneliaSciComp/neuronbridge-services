package org.janelia.colordepthsearch;

import java.util.Collection;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringInputStream;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (StringUtils.isNullOrEmpty(System.getenv(name))) {
            throw new IllegalStateException(String.format("Missing environment variable: %s", name));
        }
        return System.getenv(name);
    }

    public static String getOptionalEnv(String name, String defaultValue) {
        if (StringUtils.isNullOrEmpty(System.getenv(name))) {
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

    public static void putObject(AmazonS3 s3, AmazonS3URI uri, Object object) throws Exception {
        putObject(s3, uri.getBucket(), uri.getKey(), object);
    }

    public static void putObject(AmazonS3 s3, String bucket, String key, Object object) throws Exception {
        String s = LambdaUtils.toJson(object);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("application/json");
        objectMetadata.setContentLength(s.length());
        s3.putObject(new PutObjectRequest(
                bucket,
                key,
                new StringInputStream(s),
                objectMetadata));
    }
}
