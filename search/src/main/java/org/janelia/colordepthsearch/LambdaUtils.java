package org.janelia.colordepthsearch;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringInputStream;
import com.amazonaws.util.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Collection;

/**
 * Useful utility functions for writing AWS Lambda functions in Java.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LambdaUtils {

    private static final Logger log = LoggerFactory.getLogger(LambdaUtils.class);
    private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").setPrettyPrinting().create();

    public static String getMandatoryEnv(String name) {
        if (StringUtils.isNullOrEmpty(System.getenv(name))) {

            throw new IllegalStateException(String.format("Missing environment variable: %s", name));
        }
        return System.getenv(name);
    }

    public static String getOptionalEnv(String name, String defaultValue){
        if (StringUtils.isNullOrEmpty(System.getenv(name))) {
            return defaultValue;
        }
        return System.getenv(name);
    }

    public static String toJson(Object object) {
        return gson.toJson(object);
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
