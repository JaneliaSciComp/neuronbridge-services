package org.janelia.colordepthsearch;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MaskSearchResult {

    private final String filepath;
    private final int matchingSlices;
    private final double matchingSlicesPct;

    public MaskSearchResult(String filepath, int matchingSlices, double matchingSlicesPct) {
        this.filepath = filepath;
        this.matchingSlices = matchingSlices;
        this.matchingSlicesPct = matchingSlicesPct;
    }

    public String getFilepath() {
        return filepath;
    }

    public int getMatchingSlices() {
        return matchingSlices;
    }

    public double getMatchingSlicesPct() {
        return matchingSlicesPct;
    }
}
