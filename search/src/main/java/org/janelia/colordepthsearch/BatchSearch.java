package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import ij.ImagePlus;
import ij.io.Opener;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Search a list of color depth images using a list of masks.
 *
 *  Implements the BatchSearchService Lambda "interface".
 *
 *  @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearch implements RequestHandler<BatchSearchParameters, Void> {

    private static final Logger log = LoggerFactory.getLogger(BatchSearch.class);

    @Override
    public Void handleRequest(BatchSearchParameters params, Context context) {

        final String region = LambdaUtils.getMandatoryEnv("AWS_REGION");

        log.debug("Environment:\n  region: {}",
                region);
        log.debug("Received color depth search request: {}", LambdaUtils.toJson(params));

        // TODO: parameterize region with env variable
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

        if (LambdaUtils.isEmpty(params.getSearchKeys())) {
            log.error("No images to search");
            System.exit(1);
        }

        if (LambdaUtils.isEmpty(params.getMaskKeys())) {
            log.error("No masks to search");
            System.exit(1);
        }

        if (LambdaUtils.isEmpty(params.getMaskThresholds())) {
            log.error("No mask thresholds specified");
            System.exit(1);
        }

        if (params.getMaskThresholds().size()!=params.getMaskKeys().size()) {
            log.error("Number of mask thresholds does not match number of masks ({}!={})",
                    params.getMaskThresholds().size(), params.getMaskKeys().size());
            System.exit(1);
        }

        // Preload all masks into memory
        List<ImagePlus> maskImages = new ArrayList<>();
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
            log.error("Error loading mask images", e);
            System.exit(1);
        }

        log.debug("Searching {} images with {} masks", params.getSearchKeys().size(), maskImages.size());

        // Load each search image and compare it to all the masks already in memory
        List<MaskSearchResult> results = new ArrayList<>();
        for (String searchKey : params.getSearchKeys()) {
            try {
                S3Object searchObject = s3.getObject(params.getSearchPrefix(), searchKey);
                if (searchObject==null) {
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

                        if (output.matchingPixNum>0) {
                            results.add(new MaskSearchResult(searchImage.getTitle(), maskIndex,
                                    output.matchingPixNum, output.matchingPct));
                        }

                        maskIndex++;
                    }
                }
            }
            catch (Exception e) {
                log.error("Error searching {}", searchKey, e);
            }
        }

        StringWriter sw = new StringWriter();

        if (!results.isEmpty()) {
            log.info("Found {} matches.", results.size());

            // Sort the results
            results.sort((o1, o2) -> {
                Integer i1 = o1.getMatchingSlices();
                Integer i2 = o2.getMatchingSlices();
                return i2.compareTo(i1); // reverse sort
            });

            if (params.getOutputFile()==null) {
                // Print some results to the log
                int i = 0;
                for (MaskSearchResult result : results) {
                    log.info("Match {} - {}", result.getMatchingSlices(), result.getFilepath());
                    if (i > 8) break;
                    i++;
                }
            }
            else {
                // Save results to the output file
                for (MaskSearchResult result : results) {
                    sw.write(String.format("%d\t%d\t%2.4f\t%s\n",
                            result.getMaskIndex(), result.getMatchingSlices(), result.getMatchingSlicesPct(), result.getFilepath()));
                }
            }
        }
        else {
            log.info("No matches found.");
        }

        if (params.getOutputFile()!=null) {
            AmazonS3URI outputUri = new AmazonS3URI(params.getOutputFile());
            s3.putObject(outputUri.getBucket(), outputUri.getKey(), sw.toString());
            log.info("Results written to {}", outputUri);
        }

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
