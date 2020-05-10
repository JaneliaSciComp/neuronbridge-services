package org.janelia.colordepthsearch;

/**
 * The result of comparing a single search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MaskSearchResult {

    private final String filepath;
    private final int maskIndex;
    private final double score;

    public MaskSearchResult(String filepath, int maskIndex, double score) {
        this.filepath = filepath;
        this.maskIndex = maskIndex;
        this.score = score;
    }

    public String getFilepath() {
        return filepath;
    }

    public int getMaskIndex() {
        return maskIndex;
    }

    public double getScore() {
        return score;
    }
}
