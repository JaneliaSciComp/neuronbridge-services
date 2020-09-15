package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

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

    AWSMIPLoader(S3Client s3) {
        this.s3 = s3;
    }

    ImageArray readImage(String bucketName, String imageKey) {
        long startTime = System.currentTimeMillis();
        LOG.trace("Load image {}:{}", bucketName, imageKey);
        InputStream inputStream;
        try {
            inputStream = LambdaUtils.getObject(s3, bucketName, imageKey);
            if (inputStream == null) {
                return null;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        try {
            return ImageArrayUtils.readImageArray(imageKey, imageKey, inputStream);
        } catch (Exception e) {
            LOG.error("Error loading {}:{}", bucketName, imageKey, e);
            throw new IllegalStateException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
            LOG.trace("Loaded image {}:{} in {}ms", bucketName, imageKey, System.currentTimeMillis() - startTime);
        }
    }

    MIPImage loadMIP(String bucketName, MIPMetadata mip) {
        long startTime = System.currentTimeMillis();
        LOG.trace("Load MIP {}", mip);
        InputStream inputStream;
        try {
            inputStream = LambdaUtils.getObject(s3, bucketName, mip.getImagePath());
            if (inputStream == null) {
                return null;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        try {
            return new MIPImage(mip, ImageArrayUtils.readImageArray(mip.getId(), mip.getImageName(), inputStream));
        } catch (Exception e) {
            LOG.error("Error loading {}", mip, e);
            throw new IllegalStateException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
            LOG.trace("Loaded MIP {} in {}ms", mip, System.currentTimeMillis() - startTime);
        }
    }

    ImageArray loadFirstMatchingImage(String bucketName, String imageKey) {
        long startTime = System.currentTimeMillis();
        String imageKeyPrefix = RegExUtils.replacePattern(imageKey, "\\..*$", "");
        String imageName;
        try {
            LOG.trace("List candidates for: {}", imageKeyPrefix);
            List<S3Object> matchingImages = LambdaUtils.listObjects(s3, bucketName, imageKeyPrefix);
            if (CollectionUtils.isEmpty(matchingImages)) {
                return null;
            } else {
                imageName = matchingImages.get(0).key();
                LOG.info("Loading {} using first match from {}", imageName, matchingImages);
                return readImage(bucketName, imageName);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            LOG.trace("Loaded image matching {} in {}ms", imageKey, System.currentTimeMillis() - startTime);
        }
    }

}
