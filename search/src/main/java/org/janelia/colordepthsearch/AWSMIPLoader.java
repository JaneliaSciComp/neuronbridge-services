package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;

import org.janelia.colormipsearch.api.imageprocessing.ImageArrayUtils;
import org.janelia.colormipsearch.tools.MIPImage;
import org.janelia.colormipsearch.tools.MIPMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AWSMIPLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AWSMIPLoader.class);

    private final AmazonS3 s3;

    AWSMIPLoader(AmazonS3 s3) {
        this.s3 = s3;
    }

    MIPImage loadMIP(String bucketName, MIPMetadata mip) {
        long startTime = System.currentTimeMillis();
        LOG.trace("Load MIP {}", mip);
        InputStream inputStream;
        try {
            S3Object mipObject = s3.getObject(bucketName, mip.getImageName());
            if (mipObject == null) {
                return null;
            } else {
                inputStream = mipObject.getObjectContent();
            }
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
