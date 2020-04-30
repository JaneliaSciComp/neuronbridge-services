package org.janelia.colordepthsearch;

import java.util.List;

/**
 * Search parameters for a color depth search with multiple masks and libraries.
 *
 * To be executed in parallel by many batch search lambdas.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ParallelSearchParameters {

    private List<String> libraries;
    private List<String> maskKeys;
    private Integer dataThreshold = 100;
    private List<Integer> maskThresholds;
    private Double pixColorFluctuation = 2.0;
    private Integer xyShift = 0;
    private boolean mirrorMask = false;

    public List<String> getLibraries() {
        return libraries;
    }

    /**
     * Set the keys of the color depth libraries to search.
     * @param libraries
     */
    public void setLibraries(List<String> libraries) {
        this.libraries = libraries;
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
}
