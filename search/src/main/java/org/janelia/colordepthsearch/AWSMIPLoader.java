package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.api.imageprocessing.ImageArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

class AWSMIPLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AWSMIPLoader.class);

    private final S3Client s3;
    private final int defaultMaxRetries;
    private final long defaultPauseBetweenRetries;

    AWSMIPLoader(S3Client s3) {
        this.s3 = s3;
        this.defaultMaxRetries = 5;
        this.defaultPauseBetweenRetries = 200;
    }

    ImageArray<?> readImageWithRetry(Supplier<ImageArray<?>> imageReader, int nretries) {
        for (int retry = 0; retry < nretries; retry++) {
            if (retry > 0) {
                try {
                    Thread.sleep(defaultPauseBetweenRetries);
                } catch (Exception ignore) {
                }
            }
            try {
                return imageReader.get();
            } catch (Exception ignore) {
            }
        }
        throw new IllegalStateException(String.format("Error retrieving image after %d retries", nretries));
    }

    private ImageArray<?> readImage(String bucketName, String imageKey) {
        long startTime = System.currentTimeMillis();
        LOG.trace("Load image {}:{}", bucketName, imageKey);
        InputStream inputStream;
        try {
            inputStream = LambdaUtils.getObject(s3, bucketName, imageKey);
            if (inputStream == null) {
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error loading {}:{}", bucketName, imageKey, e);
            return null;
        }
        try {
            return ImageArrayUtils.readImageArray(imageKey, imageKey, inputStream);
        } catch (Exception e) {
            LOG.error("Error loading {}:{}", bucketName, imageKey, e);
            return null;
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
            LOG.trace("Loaded image {}:{} in {}ms", bucketName, imageKey, System.currentTimeMillis() - startTime);
        }
    }

    private ImageArray<?> readImageRange(String bucketName, String imageKey, long start, long end) {
        long startTime = System.currentTimeMillis();
        LOG.trace("Load image range {}:{}:{}:{}", bucketName, imageKey, start, end);
        InputStream inputStream;
        try {
            inputStream = LambdaUtils.getObject(s3, bucketName, imageKey);
            if (inputStream == null) {
                return null;
            }
        } catch (Exception e) {
            LOG.error("Error loading {}:{}", bucketName, imageKey, e);
            return null;
        }
        try {
            return ImageArrayUtils.readImageArrayRange(imageKey, imageKey, inputStream, start, end);
        } catch (Exception e) {
            LOG.error("Error loading image range {}:{}:{}:{}", bucketName, imageKey, start, end, e);
            return null;
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
            LOG.trace("Loaded image range {}:{}:{}:{} in {}ms", bucketName, imageKey, start, end, System.currentTimeMillis() - startTime);
        }
    }

    MIPImage loadMIP(String bucketName, MIPMetadata mip) {
        return new MIPImage(mip, readImageWithRetry(() -> readImage(bucketName, mip.getImagePath()), defaultMaxRetries));
    }

    MIPImage loadMIPRange(String bucketName, MIPMetadata mip, long start, long end) {
        return new MIPImage(mip, readImageWithRetry(() -> readImageRange(bucketName, mip.getImagePath(), start, end), defaultMaxRetries));
    }

    ImageArray<?> loadFirstMatchingImageRange(String bucketName, String imageKey, long start, long end) {
        String imageKeyPrefix = RegExUtils.replacePattern(imageKey, "\\..*$", "");
        String imageName;
        try {
            LOG.trace("List candidates for: '{}'", imageKeyPrefix);
            List<S3Object> matchingImages = LambdaUtils.listObjects(s3, bucketName, imageKeyPrefix);
            if (CollectionUtils.isEmpty(matchingImages)) {
                return null;
            } else {
                imageName = matchingImages.get(0).key();
                LOG.info("Load '{}' - first match from {}", imageName, matchingImages);
            }
        } catch (Exception e) {
            LOG.error("Error looking up {}:{}", bucketName, imageKey, e);
            return null;
        }
        return readImageWithRetry(() -> readImageRange(bucketName, imageKey, start, end), defaultMaxRetries);
    }

}
