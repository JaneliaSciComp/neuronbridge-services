package org.janelia.colordepthsearch;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Search parameters for a burst parallel search.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchSearchParameters {

    private String tasksTableName;
    private String jobId;
    private Integer batchId;
    private Integer startIndex;
    private Integer endIndex;

    private ColorDepthSearchParameters jobParameters = new ColorDepthSearchParameters();

    public String getTasksTableName() {
        return tasksTableName;
    }

    public void setTasksTableName(String tasksTableName) {
        this.tasksTableName = tasksTableName;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Integer getBatchId() {
        return batchId;
    }

    public void setBatchId(Integer batchId) {
        this.batchId = batchId;
    }


    public Integer getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(Integer startIndex) {
        this.startIndex = startIndex;
    }

    public Integer getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(Integer endIndex) {
        this.endIndex = endIndex;
    }

    public ColorDepthSearchParameters getJobParameters() {
        return jobParameters;
    }

    public void setJobParameters(ColorDepthSearchParameters jobParameters) {
        this.jobParameters = jobParameters;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tasksTableName", tasksTableName)
                .append("jobId", jobId)
                .append("batchId", batchId)
                .append("startIndex", startIndex)
                .append("endIndex", endIndex)
                .append("jobParameters", jobParameters)
                .toString();
    }
}
