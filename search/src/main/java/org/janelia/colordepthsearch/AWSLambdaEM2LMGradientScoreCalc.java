package org.janelia.colordepthsearch;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import org.apache.commons.lang3.RegExUtils;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.api.gradienttools.GradientAreaGapUtils;
import org.janelia.colormipsearch.api.gradienttools.MaskGradientAreaGapCalculator;
import org.janelia.colormipsearch.api.gradienttools.MaskGradientAreaGapCalculatorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/**
 *  AWS Lambda handler that calculates gradient scores for the specifed results
 */
public class AWSLambdaEM2LMGradientScoreCalc implements RequestHandler<GradientScoreParameters, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(AWSLambdaEM2LMGradientScoreCalc.class);

    @Override
    public Void handleRequest(GradientScoreParameters params, Context context) {
        S3Client s3 = LambdaUtils.createS3();
        CDSMatches cdsMatches = readCDSResults(s3, params.getResultsBucket(), params.getResultsKeyNoGradScore());
        if (isInvalid(cdsMatches)) {
            return null;
        }
        Map<MIPMetadata, List<ColorMIPSearchMatchMetadata>> resultsGroupedById = ColorMIPSearchResultUtils.selectCDSResultForGradientScoreCalculation(
                cdsMatches.results,
                params.getNumberOfPublishedNamesToRank(),
                params.getNumberOfSamplesPerPublishedNameToRank(),
                params.getNumberOfMatchesPerSampleToRank());
        MaskGradientAreaGapCalculatorProvider maskAreaGapCalculatorProvider =
                MaskGradientAreaGapCalculator.createMaskGradientAreaGapCalculatorProvider(
                        params.getMaskThreshold(), params.getNegativeRadius(), params.isMirrorMask()
                );
        Executor gradientScoreExecutor = createGradientScoreCalcExecutor();
        LOG.info("Begin gradient area calculations");
        List<CompletableFuture<List<ColorMIPSearchMatchMetadata>>> gradientAreaGapComputations =
                resultsGroupedById.entrySet().stream()
                        .map(resultsEntry -> {
                            LOG.info("Begin gradient area calculation for mask {}", resultsEntry.getKey());
                            return asyncCalculateGradientAreaScoreForCDSResults(
                                    resultsEntry.getKey(),
                                    resultsEntry.getValue(),
                                    params.getMasksBucket(),
                                    params.getGradientsBucket(),
                                    params.getGradientsSuffix(),
                                    params.getZgapsBucket(),
                                    params.getZgapsSuffix(),
                                    new AWSMIPLoader(s3),
                                    maskAreaGapCalculatorProvider,
                                    gradientScoreExecutor);
                        })
                        .collect(Collectors.toList());
        // wait for all results to complete
        CompletableFuture.allOf(gradientAreaGapComputations.toArray(new CompletableFuture<?>[0])).join();
        LOG.info("Gather gradient area gap results");
        List<ColorMIPSearchMatchMetadata> srWithGradScores = gradientAreaGapComputations.stream()
                .flatMap(gac -> gac.join().stream())
                .collect(Collectors.toList());
        LOG.info("Sort results using gradient scores");
        ColorMIPSearchResultUtils.sortCDSResults(srWithGradScores);
        LOG.info("Write {} results with gradient scores to {}:{}",
                srWithGradScores.size(), params.getResultsBucket(), params.getResultsKeyWithGradScore());
        writeCDSResults(
                CDSMatches.singletonfromResultsOfColorMIPSearchMatches(srWithGradScores),
                s3,
                params.getResultsBucket(),
                params.getResultsKeyWithGradScore()
        );
        return null;
    }

    private CDSMatches readCDSResults(S3Client s3, String cdsResultsBucket, String cdsResultsKey) {
        InputStream cdsResultsStream = LambdaUtils.getObject(s3, cdsResultsBucket, cdsResultsKey);
        if (cdsResultsStream == null) {
            return null;
        }
        try {
            return LambdaUtils.fromJson(cdsResultsStream, CDSMatches.class);
        } finally {
            try {
                cdsResultsStream.close();
            } catch (IOException ignore) {
            }
        }
    }

    private void writeCDSResults(CDSMatches cdsMatches, S3Client s3, String outputBucket, String outputLocation) {
        if (outputBucket != null && outputLocation != null) {
            try {
                LOG.info("Write matches for {} to {}:{}", cdsMatches.getMaskId(), outputBucket, outputLocation);
                LambdaUtils.putObject(
                        s3,
                        outputBucket,
                        outputLocation,
                        cdsMatches);
                LOG.info("Results written to {}:{}", outputBucket, outputLocation);
            } catch (Exception e) {
                throw new IllegalStateException("Error writing results", e);
            }
        }
    }

    private boolean isInvalid(CDSMatches cdsMatches) {
        return cdsMatches == null || cdsMatches.results == null || cdsMatches.results.isEmpty();
    }

    private Executor createGradientScoreCalcExecutor() {
        return Executors.newFixedThreadPool(4);
    }

    private CompletableFuture<List<ColorMIPSearchMatchMetadata>> asyncCalculateGradientAreaScoreForCDSResults(MIPMetadata inputMaskMIP,
                                                                                                              List<ColorMIPSearchMatchMetadata> selectedCDSResultsForInputMIP,
                                                                                                              String maskBucket,
                                                                                                              String gradientsBucket,
                                                                                                              String gradientSuffix,
                                                                                                              String zgapsBucket,
                                                                                                              String zgapsSuffix,
                                                                                                              AWSMIPLoader mipLoader,
                                                                                                              MaskGradientAreaGapCalculatorProvider maskAreaGapCalculatorProvider,
                                                                                                              Executor executor) {
        CompletableFuture<MaskGradientAreaGapCalculator> gradientGapCalculatorPromise = CompletableFuture.supplyAsync(() -> {
            LOG.info("Load input mask {}", inputMaskMIP);
            MIPImage inputMaskImage = mipLoader.loadMIP(maskBucket, inputMaskMIP); // no caching for the mask
            return maskAreaGapCalculatorProvider.createMaskGradientAreaGapCalculator(inputMaskImage.getImageArray());
        }, executor);
        List<CompletableFuture<Long>> areaGapComputations = selectedCDSResultsForInputMIP.stream()
                .map(csr -> gradientGapCalculatorPromise.thenApplyAsync(gradientGapCalculator -> {
                    MIPMetadata matchedMIP = new MIPMetadata();
                    matchedMIP.setImageArchivePath(csr.getImageArchivePath());
                    matchedMIP.setImageName(csr.getImageName());
                    matchedMIP.setImageType(csr.getImageType());
                    MIPImage matchedImage = mipLoader.loadMIP(extractBucketName(csr.getImageURL()), matchedMIP);
                    MIPImage matchedGradientImage = mipLoader.loadMIP(gradientsBucket, getAncillaryMIP(matchedMIP, gradientSuffix));
                    MIPImage matchedZGapImage = mipLoader.loadFirstMatchingMIP(zgapsBucket, getAncillaryMIP(matchedMIP, zgapsSuffix), "png", "tif");
                    long areaGap;
                    if (matchedImage != null && matchedGradientImage != null) {
                        // only calculate the area gap if the gradient exist
                        LOG.info("Calculate area gap for {}", csr);
                        areaGap = gradientGapCalculator.calculateMaskAreaGap(
                                matchedImage.getImageArray(),
                                matchedGradientImage.getImageArray(),
                                matchedZGapImage != null ? matchedZGapImage.getImageArray() : null);
                        LOG.info("Area gap for {} -> {}", csr, areaGap);
                    } else {
                        areaGap = -1;
                    }
                    csr.setGradientAreaGap(areaGap);
                    return areaGap;
                }, executor))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(areaGapComputations.toArray(new CompletableFuture<?>[0]))
                .thenApply(vr -> {
                    Integer maxMatchingPixels = selectedCDSResultsForInputMIP.stream()
                            .map(ColorMIPSearchMatchMetadata::getMatchingPixels)
                            .max(Integer::compare)
                            .orElse(0);
                    List<Long> areaGaps = areaGapComputations.stream()
                            .map(areaGapComputation -> areaGapComputation.join())
                            .collect(Collectors.toList());
                    long maxAreaGap = areaGaps.stream()
                            .max(Long::compare)
                            .orElse(-1L);
                    // set the normalized area gap values
                    if (maxAreaGap >= 0 && maxMatchingPixels > 0) {
                        selectedCDSResultsForInputMIP.stream().filter(csr -> csr.getGradientAreaGap() >= 0)
                                .forEach(csr -> {
                                    csr.setNormalizedGapScore(GradientAreaGapUtils.calculateAreaGapScore(
                                            csr.getGradientAreaGap(),
                                            maxAreaGap,
                                            csr.getMatchingPixels(),
                                            csr.getMatchingRatio(),
                                            maxMatchingPixels));
                                });
                    }
                    ;
                    return selectedCDSResultsForInputMIP;
                });
    }

    private String extractBucketName(String imageURL) {
        Path fullImagePath = Paths.get(URI.create(imageURL).getPath());
        return fullImagePath.getName(0).toString();
    }

    private MIPMetadata getAncillaryMIP(MIPMetadata mip, String suffix) {
        MIPMetadata ancilaryMIP = new MIPMetadata();
        mip.copyTo(ancilaryMIP);
        ancilaryMIP.setImageArchivePath(null);
        ancilaryMIP.setImageName(RegExUtils.replacePattern(
                mip.getImagePath().replace("searchable_neurons", suffix),
                "\\..*$",
                ".png")); // ancillary images use png
        ancilaryMIP.setImageURL(null);
        ancilaryMIP.setThumbnailURL(null);
        return ancilaryMIP;
    }
}
