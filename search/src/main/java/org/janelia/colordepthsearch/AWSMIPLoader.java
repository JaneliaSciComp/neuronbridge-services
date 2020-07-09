package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;

import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.imageprocessing.ImageArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

class AWSMIPLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AWSMIPLoader.class);

    private final S3Client s3;

    AWSMIPLoader(S3Client s3) {
        this.s3 = s3;
    }

    MIPImage loadMIP(String bucketName, MIPMetadata mip) {
        long startTime = System.currentTimeMillis();
        LOG.trace("Load MIP {}", mip);
        InputStream inputStream;
        try {
            inputStream = LambdaUtils.getObject(s3, bucketName, mip.getImageName());
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

}
