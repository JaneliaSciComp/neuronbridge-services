package org.janelia.colordepthsearch;

import java.util.List;

/**
 * Search parameters for a color depth search with multiple masks.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorDepthSearchParameters {

    private String libraryBucket;
    private List<String> libraries;
    private String searchBucket;
    private List<String> maskKeys;
    private Integer dataThreshold = 100;
    private List<Integer> maskThresholds;
    private Double pixColorFluctuation = 2.0;
    private Integer xyShift = 0;
    private boolean mirrorMask = false;
    private Double minMatchingPixRatio = 2.;

    public String getLibraryBucket() {
        return libraryBucket;
    }

    public void setLibraryBucket(String libraryBucket) {
        this.libraryBucket = libraryBucket;
    }

    public List<String> getLibraries() {
        return libraries;
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
}
