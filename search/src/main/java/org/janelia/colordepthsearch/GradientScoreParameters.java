package org.janelia.colordepthsearch;

/**
 * Search parameters for a color depth search with multiple masks.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class GradientScoreParameters {

    private String masksBucket;
    private String resultsBucket;
    private String resultsKeyNoGradScore;
    private String resultsKeyWithGradScore;
    private String gradientsBucket;
    private String gradientsSuffix;
    private String zgapsBucket;
    private String zgapsSuffix;
    private Integer maskThreshold = 100;
    private Integer negativeRadius = 10;
    private boolean mirrorMask = false;
    private Integer numberOfPublishedNamesToRank;
    private Integer numberOfSamplesPerPublishedNameToRank;
    private Integer numberOfMatchesPerSampleToRank;

    public String getMasksBucket() {
        return masksBucket;
    }

    public void setMasksBucket(String masksBucket) {
        this.masksBucket = masksBucket;
    }

    public String getResultsBucket() {
        return resultsBucket;
    }

    public void setResultsBucket(String resultsBucket) {
        this.resultsBucket = resultsBucket;
    }

    public String getResultsKeyNoGradScore() {
        return resultsKeyNoGradScore;
    }

    public void setResultsKeyNoGradScore(String resultsKeyNoGradScore) {
        this.resultsKeyNoGradScore = resultsKeyNoGradScore;
    }

    public String getResultsKeyWithGradScore() {
        return resultsKeyWithGradScore;
    }

    public void setResultsKeyWithGradScore(String resultsKeyWithGradScore) {
        this.resultsKeyWithGradScore = resultsKeyWithGradScore;
    }

    public String getGradientsBucket() {
        return gradientsBucket;
    }

    public void setGradientsBucket(String gradientsBucket) {
        this.gradientsBucket = gradientsBucket;
    }

    public String getGradientsSuffix() {
        return gradientsSuffix;
    }

    public void setGradientsSuffix(String gradientsSuffix) {
        this.gradientsSuffix = gradientsSuffix;
    }

    public String getZgapsBucket() {
        return zgapsBucket;
    }

    public void setZgapsBucket(String zgapsBucket) {
        this.zgapsBucket = zgapsBucket;
    }

    public String getZgapsSuffix() {
        return zgapsSuffix;
    }

    public void setZgapsSuffix(String zgapsSuffix) {
        this.zgapsSuffix = zgapsSuffix;
    }

    public Integer getMaskThreshold() {
        return maskThreshold;
    }

    public void setMaskThreshold(Integer maskThreshold) {
        this.maskThreshold = maskThreshold;
    }

    public Integer getNegativeRadius() {
        return negativeRadius == null ? 0 : negativeRadius;
    }

    public void setNegativeRadius(Integer negativeRadius) {
        this.negativeRadius = negativeRadius;
    }

    public boolean isMirrorMask() {
        return mirrorMask;
    }

    public void setMirrorMask(boolean mirrorMask) {
        this.mirrorMask = mirrorMask;
    }

    public Integer getNumberOfPublishedNamesToRank() {
        return numberOfPublishedNamesToRank != null ? 0 : numberOfPublishedNamesToRank;
    }

    public void setNumberOfPublishedNamesToRank(Integer numberOfPublishedNamesToRank) {
        this.numberOfPublishedNamesToRank = numberOfPublishedNamesToRank;
    }

    public Integer getNumberOfSamplesPerPublishedNameToRank() {
        return numberOfSamplesPerPublishedNameToRank == null ? 0 : numberOfSamplesPerPublishedNameToRank;
    }

    public void setNumberOfSamplesPerPublishedNameToRank(Integer numberOfSamplesPerPublishedNameToRank) {
        this.numberOfSamplesPerPublishedNameToRank = numberOfSamplesPerPublishedNameToRank;
    }

    public Integer getNumberOfMatchesPerSampleToRank() {
        return numberOfMatchesPerSampleToRank == null ? 0 : numberOfMatchesPerSampleToRank;
    }

    public void setNumberOfMatchesPerSampleToRank(Integer numberOfMatchesPerSampleToRank) {
        this.numberOfMatchesPerSampleToRank = numberOfMatchesPerSampleToRank;
    }
}
