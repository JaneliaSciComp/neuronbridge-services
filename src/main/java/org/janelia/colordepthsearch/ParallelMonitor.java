package org.janelia.colordepthsearch;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * Monitors a parallel search and notifies the client when all batches are done.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ParallelMonitor implements RequestHandler<ParallelSearchParameters, Long> {

    @Override
    public Long handleRequest(ParallelSearchParameters params, Context context) {

        // TBD

        return null;
    }
}
