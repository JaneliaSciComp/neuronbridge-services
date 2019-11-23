package org.janelia.colordepthsearch;

import java.util.Collection;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

/**
 * Useful utility functions for writing AWS Lambda functions in Java.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LambdaUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

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
        try {
            return mapper.writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            return "{\"error\":\"Could not serialize object\"}";
        }
    }

    public static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }
}
