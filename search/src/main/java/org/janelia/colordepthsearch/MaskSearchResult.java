package org.janelia.colordepthsearch;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MaskSearchResult {

    private final String filepath;
    private final int maskIndex;
    private final int matchingSlices;
    private final double matchingSlicesPct;

    public MaskSearchResult(String filepath, int maskIndex, int matchingSlices, double matchingSlicesPct) {
        this.filepath = filepath;
        this.maskIndex = maskIndex;
        this.matchingSlices = matchingSlices;
        this.matchingSlicesPct = matchingSlicesPct;
    }

    public String getFilepath() {
        return filepath;
    }

    public int getMaskIndex() {
        return maskIndex;
    }

    public int getMatchingSlices() {
        return matchingSlices;
    }

    public double getMatchingSlicesPct() {
        return matchingSlicesPct;
    }
}
