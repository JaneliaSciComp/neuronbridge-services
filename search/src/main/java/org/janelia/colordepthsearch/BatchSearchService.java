package org.janelia.colordepthsearch;

import com.amazonaws.services.lambda.invoke.LambdaFunction;
import com.amazonaws.services.lambda.model.InvocationType;

/**
 * Interface for invoking the batch search lambda from the parallel search lambda.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface BatchSearchService {

    @LambdaFunction(
            // This function name is actually ignored later because we have to inject a
            // generated name, but it's necessary for it to be defined.
            functionName="search",
            // This makes the function async
            invocationType=InvocationType.Event)
    Void search(BatchSearchParameters parameters);

}
