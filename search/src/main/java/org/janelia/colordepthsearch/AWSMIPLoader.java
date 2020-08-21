package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
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

    MIPImage loadMIPRange(String bucketName, MIPMetadata mip, long start, long end) {
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
            return new MIPImage(mip, ImageArrayUtils.readImageArrayRange(mip.getId(), mip.getImageName(), inputStream, start, end));
            //return new MIPImage(mip, ImageArrayUtils.readImageArray(mip.getId(), mip.getImageName(), inputStream));
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

    MIPImage loadFirstMatchingMIP(String bucketName, MIPMetadata mip, String mipExt, String... otherMipExts) {
        long startTime = System.currentTimeMillis();
        InputStream inputStream;
        String mipImagePrefix = RegExUtils.replacePattern(mip.getImagePath(), "\\..*$", "");
        String mipImageName;
        try {
            LOG.trace("List MIP candidates for: {}", mipImagePrefix);
            List<S3Object> matchingMIPs = LambdaUtils.listObjects(s3, bucketName, mipImagePrefix);
            if (CollectionUtils.isEmpty(matchingMIPs)) {
                return null;
            } else {
                mipImageName = matchingMIPs.get(0).key();
                LOG.info("Loading MIP {} using first match from {}", mipImageName, matchingMIPs);
                inputStream = LambdaUtils.getObject(s3, bucketName, matchingMIPs.get(0).key());
                if (inputStream == null) {
                    return null;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        try {
            return new MIPImage(mip, ImageArrayUtils.readImageArray(mip.getId(), mipImageName, inputStream));
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

}
