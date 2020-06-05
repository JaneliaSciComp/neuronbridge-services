package org.janelia.colordepthsearch;

/**
 * The result of comparing a single search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MaskSearchResult {

    private final String filepath;
    private final double score;

    public MaskSearchResult(String filepath, double score) {
        this.filepath = filepath;
        this.score = score;
    }

    public String getFilepath() {
        return filepath;
    }

    public double getScore() {
        return score;
    }
}
