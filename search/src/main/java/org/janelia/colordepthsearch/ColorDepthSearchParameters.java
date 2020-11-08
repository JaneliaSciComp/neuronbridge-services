package org.janelia.colordepthsearch;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Search parameters for a color depth search with multiple masks.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorDepthSearchParameters {

    final static int DEFAULT_MASK_THRESHOLD = 100;

    private String libraryBucket;
    private List<String> libraries;
    private List<String> gradientsFolders;
    private List<String> zgapMasksFolders;
    private String searchBucket;
    private List<String> maskKeys;
    private List<Integer> maskThresholds;
    private Integer dataThreshold = 100;
    private Double pixColorFluctuation = 2.0;
    private Integer xyShift = 0;
    private boolean mirrorMask = false;
    private Double minMatchingPixRatio = 2.;
    private Integer negativeRadius = 20;
    private boolean withGradientScores = false;

    public String getLibraryBucket() {
        return libraryBucket;
    }

    public void setLibraryBucket(String libraryBucket) {
        this.libraryBucket = libraryBucket;
    }

    public List<String> getLibraries() {
        return libraries;
    }

    public List<String> getGradientsFolders() {
        return gradientsFolders;
    }

    public void setGradientsFolders(List<String> gradientsFolders) {
        this.gradientsFolders = gradientsFolders;
    }

    public List<String> getZgapMasksFolders() {
        return zgapMasksFolders;
    }

    public void setZgapMasksFolders(List<String> zgapMasksFolders) {
        this.zgapMasksFolders = zgapMasksFolders;
    }

    public void setLibraries(List<String> libraries) {
        this.libraries = libraries;
    }

    public String getSearchBucket() {
        return searchBucket;
    }

    public void setSearchBucket(String searchBucket) {
        this.searchBucket = searchBucket;
    }

    public List<String> getMaskKeys() {
        return maskKeys;
    }

    /**
     * Keys of masks to search with, relative to the maskPrefix.
     * @param maskKeys
     */
    public void setMaskKeys(List<String> maskKeys) {
        this.maskKeys = maskKeys;
    }

    public Integer getDataThreshold() {
        return dataThreshold;
    }

    /**
     * Intensity threshold for ignoring pixels in the data.
     * @param dataThreshold
     */
    public void setDataThreshold(Integer dataThreshold) {
        this.dataThreshold = dataThreshold;
    }

    public List<Integer> getMaskThresholds() {
        return maskThresholds;
    }

    /**
     * Intensity threshold for ignoring pixels in the mask.
     * @param maskThresholds
     */
    public void setMaskThresholds(List<Integer> maskThresholds) {
        this.maskThresholds = maskThresholds;
    }

    public Double getPixColorFluctuation() {
        return pixColorFluctuation;
    }

    /**
     * Set how much color depth (i.e. Z distance) to search.
     * @param pixColorFluctuation
     */
    public void setPixColorFluctuation(Double pixColorFluctuation) {
        this.pixColorFluctuation = pixColorFluctuation;
    }

    public Integer getXyShift() {
        return xyShift;
    }

    /**
     * Set how much to shift the mask in the XY plane when comparing against the search library.
     * @param xyShift
     */
    public void setXyShift(Integer xyShift) {
        this.xyShift = xyShift;
    }

    public boolean isMirrorMask() {
        return mirrorMask;
    }

    /**
     * Set whether or not to mirror the mask across the Y axis.
     * @param mirrorMask
     */
    public void setMirrorMask(boolean mirrorMask) {
        this.mirrorMask = mirrorMask;
    }


    public Double getMinMatchingPixRatio() {
        return minMatchingPixRatio;
    }

    public void setMinMatchingPixRatio(Double minMatchingPixRatio) {
        this.minMatchingPixRatio = minMatchingPixRatio;
    }

    public Integer getNegativeRadius() {
        return negativeRadius;
    }

    public void setNegativeRadius(Integer negativeRadius) {
        this.negativeRadius = negativeRadius;
    }

    public boolean isWithGradientScores() {
        return withGradientScores;
    }

    public void setWithGradientScores(boolean withGradientScores) {
        this.withGradientScores = withGradientScores;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("libraryBucket", libraryBucket)
                .append("libraries", libraries)
                .append("gradientsFolders", gradientsFolders)
                .append("zgapMasksFolders", zgapMasksFolders)
                .append("searchBucket", searchBucket)
                .append("maskKeys", maskKeys)
                .append("maskThresholds", maskThresholds)
                .append("dataThreshold", dataThreshold)
                .append("pixColorFluctuation", pixColorFluctuation)
                .append("xyShift", xyShift)
                .append("mirrorMask", mirrorMask)
                .append("minMatchingPixRatio", minMatchingPixRatio)
                .append("negativeRadius", negativeRadius)
                .append("withGradientScores", withGradientScores)
                .toString();
    }
}
