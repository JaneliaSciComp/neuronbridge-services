package org.janelia.colordepthsearch;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SearchMetadata {

    private ParallelSearchParameters parameters;
    private int partitions;

    public ParallelSearchParameters getParameters() {
        return parameters;
    }

    public void setParameters(ParallelSearchParameters parameters) {
        this.parameters = parameters;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }
}
