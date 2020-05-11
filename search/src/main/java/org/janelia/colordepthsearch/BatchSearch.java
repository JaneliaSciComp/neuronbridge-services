package org.janelia.colordepthsearch;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.xray.AWSXRay;
import ij.ImagePlus;
import ij.io.Opener;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *  Search a list of color depth images using a list of masks.
 *
 *  Implements the BatchSearchService API.
 *
 *  @see org.janelia.colordepthsearch.BatchSearchService
 *
 *  @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearch implements RequestHandler<BatchSearchParameters, Void> {

    private static final Logger log = LoggerFactory.getLogger(BatchSearch.class);

    @Override
    public Void handleRequest(BatchSearchParameters params, Context context) {

        AWSXRay.beginSubsegment("Read parameters");
        final String region = LambdaUtils.getMandatoryEnv("AWS_REGION");

        log.debug("Environment:\n  region: {}",
                region);
        log.debug("Received color depth search request: {}", LambdaUtils.toJson(params));

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

        if (LambdaUtils.isEmpty(params.getSearchKeys())) {
            throw new IllegalArgumentException("No images to search");
        }

        if (LambdaUtils.isEmpty(params.getMaskKeys())) {
            throw new IllegalArgumentException("No masks to search");
        }

        if (LambdaUtils.isEmpty(params.getMaskThresholds())) {
            throw new IllegalArgumentException("No mask thresholds specified");
        }

        if (params.getMaskThresholds().size()!=params.getMaskKeys().size()) {
            throw new IllegalArgumentException("Number of mask thresholds does not match number of masks");
        }

        // Preload all masks into memory
        List<ImagePlus> maskImages = new ArrayList<>();

        AWSXRay.endSubsegment();
        AWSXRay.beginSubsegment("Load masks");

        try {
            for (String maskKey : params.getMaskKeys()) {
                S3Object maskObject = s3.getObject(params.getMaskPrefix(), maskKey);
                try (S3ObjectInputStream s3is = maskObject.getObjectContent()) {
                    maskImages.add(readImagePlus(maskKey, maskKey, s3is));
                }
            }
            if (maskImages.isEmpty()) {
                throw new IllegalStateException("Could not load search masks.");
            }
        }
        catch (Exception e) {
            throw new IllegalStateException("Error loading mask images", e);
        }

        AWSXRay.endSubsegment();
        AWSXRay.beginSubsegment("Search");

        log.debug("Searching {} images with {} masks", params.getSearchKeys().size(), maskImages.size());
        List<MaskSearchResult> results = new ArrayList<>();

        // Load each search image and compare it to all the masks already in memory
        for (String searchKey : params.getSearchKeys()) {
            try {
                S3Object searchObject = s3.getObject(params.getSearchPrefix(), searchKey);
                if (searchObject == null) {
                    log.error("Error loading search image {}", searchKey);
                }
                else {
                    ImagePlus searchImage;
                    try (S3ObjectInputStream s3is = searchObject.getObjectContent()) {
                        searchImage = readImagePlus(searchKey, searchKey, s3is);
                    }

                    int maskIndex = 0;
                    for (ImagePlus maskImage : maskImages) {
                        Integer maskThreshold = params.getMaskThresholds().get(maskIndex);

                        double pixfludub = params.getPixColorFluctuation() / 100;
                        final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
                                maskImage.getProcessor(), maskThreshold, params.isMirrorMask(),
                                null, 0, false,
                                params.getDataThreshold(), pixfludub, params.getXyShift());
                        ColorMIPMaskCompare.Output output = cc.runSearch(searchImage.getProcessor(), null);

                        if (output.matchingPixNum > 0) {
                            results.add(new MaskSearchResult(
                                    searchImage.getTitle(),
                                    maskIndex,
                                    output.matchingPixNum));
                        }

                        maskIndex++;
                    }
                }
            } catch (Exception e) {
                log.error("Error searching {}", searchKey, e);
            }
        }

        log.info("Found {} matches.", results.size());

        AWSXRay.endSubsegment();
        AWSXRay.beginSubsegment("Sort and save results");

        // Sort the results
        results.sort((o1, o2) -> {
            Double i1 = o1.getScore();
            Double i2 = o2.getScore();
            return i2.compareTo(i1); // reverse sort
        });

        if (params.getOutputFile()==null) {
            // Print some results to the log
            int i = 0;
            for (MaskSearchResult result : results) {
                log.info("Match {} - {}", result.getScore(), result.getFilepath());
                if (i > 9) break;
                i++;
            }
        }
        else {
            try {
                AmazonS3URI outputUri = new AmazonS3URI(params.getOutputFile());
                LambdaUtils.putObject(s3, outputUri, results);
                log.info("Results written to {}", outputUri);
            }
            catch (Exception e) {
                throw new RuntimeException("Error writing results", e);
            }
        }

        AWSXRay.endSubsegment();
        return null; // null response because this lambda runs asynchronously and updates a database
    }

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    private ImageFormat getImageFormat(String filepath) {

        String lowerPath = filepath.toLowerCase();

        if (lowerPath.endsWith(".png")) {
            return ImageFormat.PNG;
        }
        else if (lowerPath.endsWith(".tiff") || lowerPath.endsWith(".tif")) {
            return ImageFormat.TIFF;
        }

        log.info("Image format unknown: {}", filepath);
        return ImageFormat.UNKNOWN;
    }

    private ImagePlus readPngToImagePlus(String title, InputStream stream) throws IOException {
        StopWatch s = new StopWatch();
        s.start();
        ImagePlus imagePlus = new ImagePlus(title, ImageIO.read(stream));
        log.debug("Reading {} took {} ms", title, s.getTime());
        return imagePlus;
    }

    private ImagePlus readTiffToImagePlus(String title, InputStream stream) throws IOException {
        StopWatch s = new StopWatch();
        s.start();
        ImagePlus imagePlus = new Opener().openTiff(stream, title);
        log.debug("Reading {} took {} ms", title, s.getTime());
        return imagePlus;
    }

    private ImagePlus readImagePlus(String filepath, String title, InputStream stream) throws IOException {
        try {
            switch (getImageFormat(filepath)) {
                case PNG:
                    return readPngToImagePlus(title, stream);
                case TIFF:
                    return readTiffToImagePlus(title, stream);
            }
        }
        catch (IOException e) {
            throw new IOException("Error reading "+filepath, e);
        }
        throw new IllegalArgumentException("Image must be in PNG or TIFF format");
    }

}
