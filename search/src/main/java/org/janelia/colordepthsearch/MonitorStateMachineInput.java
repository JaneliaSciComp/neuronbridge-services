package org.janelia.colordepthsearch;

/**
 * Initial state of the monitor state machine which describes the search that should be monitored.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MonitorStateMachineInput {

    private String bucket;
    private String prefix;

    public MonitorStateMachineInput(String bucket, String prefix) {
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
