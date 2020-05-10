package org.janelia.colordepthsearch;

import java.util.Date;

/**
 * Metadata about a ParallelSearch invocation, including the search parameters that were used and how the search
 * was distributed amongst lambda functions.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SearchMetadata {

    private Date startTime;
    private ParallelSearchParameters parameters;
    private int partitions;

    public SearchMetadata(ParallelSearchParameters parameters, int partitions) {
        this.startTime = new Date();
        this.parameters = parameters;
        this.partitions = partitions;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

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
