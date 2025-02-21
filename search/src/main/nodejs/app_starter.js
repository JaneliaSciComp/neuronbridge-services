import { BatchClient, SubmitJobCommand } from "@aws-sdk/client-batch";
import { Jimp } from 'jimp';
import { getSearchKey, getSearchMaskId}  from './searchutils';
import {
    getS3ContentAsByteBufferWithRetry,
    getS3ContentMetadata,
    putS3Content,
    startStepFunction,
} from './utils';
import {
    ALIGNMENT_JOB_SUBMITTED,
    ALIGNMENT_JOB_COMPLETED,
    SEARCH_IN_PROGRESS,
    getSearchMetadata,
    lookupSearchMetadata,
    updateSearchMetadata
} from './awsappsyncutils';
import { generateMIPs } from './mockMIPGeneration';
import { cdsStarter } from './cds_starter';

const brainAlignJobDefinition = process.env.BRAIN_ALIGN_JOB_DEFINITION;
const vncAlignJobDefinition = process.env.VNC_ALIGN_JOB_DEFINITION;
const jobQueue = process.env.JOB_QUEUE;
const alignMonitorStateMachineArn = process.env.ALIGN_JOB_STATE_MACHINE_ARN;

const batchClient = new BatchClient();

export const appStarter = async (event) => {
    console.log(event);
    let sourceIsHttpApiGateway;
    let eventBody;
    if (event.body) {
        eventBody = JSON.parse(event.body);
        console.log('Parsed body', eventBody);
        sourceIsHttpApiGateway = true;
    } else {
        eventBody = event;
        sourceIsHttpApiGateway = false;
    }
    const newRecords = await getNewRecords(eventBody);
    const searchPromises = await newRecords
        .filter(r => !!r)
        .map(async r => {
            if (r.step < ALIGNMENT_JOB_COMPLETED) {
                console.log('Start alignment for', r);
                return await startAlignment(r);
            } else if (r.step >= ALIGNMENT_JOB_COMPLETED) {
                console.log('Start color depth search for', r);
                return await startColorDepthSearch(r);
            } else {
                // do nothing
                console.log('No processing for', r);
                return r;
            }
        });
    const results = await Promise.all(searchPromises);
    if (sourceIsHttpApiGateway) {
        console.log('Returned results:', results);
        const anErrorResult = results.find(r => !!r.errorMessage || !!r.alignmentErrorMessage);
        let httpStatusCode;
        let returnedResults;
        if (anErrorResult) {
            console.log('Errors found in results list:', results);
            httpStatusCode = anErrorResult.statusCode || 404; // send invalid request instead of internal server error
            if (anErrorResult.alignmentErrorMessage) {
                returnedResults = {
                    errorMessage: anErrorResult.alignmentErrorMessage,
                    submissionResults: results
                };
            } else {
                returnedResults = {
                    errorMessage: anErrorResult.errorMessage,
                    submissionResults: results
                };
            }
        } else {
            httpStatusCode = 200;
            returnedResults = results;
        }

        return {
            statusCode: httpStatusCode,
            isBase64Encoded: false,
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(returnedResults)
        };
    } else {
        return results;
    }
};

const getNewRecords = async (e) => {
    if (e.Records) {
        const newRecordsPromises = await e.Records
            .filter(r => r.eventName === 'INSERT')
            .map(r => r.dynamodb)
            .map(r => r.Keys.id.S)
            .map(async searchId => await getSearchMetadata(searchId));
        return await Promise.all(newRecordsPromises);
    } else if (e.submittedSearches) {
        // this branch retrieves the searches from the DB
        // but if some fields are not yet set because of DynamoDB's eventual consistency
        // it sets those fields from the submittedSearch instead
        const newSearchesPromises = await e.submittedSearches
            .map(async submittedSearch => {
                let searchMetadata = await getSearchMetadata(submittedSearch.id || submittedSearch.searchId);
                Object.entries(submittedSearch)
                    .forEach(([key, value]) => {
                        if (value !== null && (searchMetadata[key] === null || searchMetadata[key] === undefined)) {
                            console.log(`Field ${key} not set`, searchMetadata, 'expected to be', value);
                            searchMetadata[key] = value;
                        }
                    });
                return searchMetadata;
            });
        return await Promise.all(newSearchesPromises);
    } else if (e.searches) {
        return e.searches;
    } else {
        return [];
    }
};

const startColorDepthSearch = async (searchParams) => {
    const { concurrentColorDepthSearchLimits, perDayColorDepthSearchLimits, } = getLimits();
    const limitsMessage = await checkLimits(
        searchParams,
        concurrentColorDepthSearchLimits,
        perDayColorDepthSearchLimits,
        {
            singular: 'search',
            plural: 'searches'
        },
        s => s.step === SEARCH_IN_PROGRESS);
    if (limitsMessage) {
        console.log(`No color depth search started because ${limitsMessage}`, searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Color depth search was not started because ${limitsMessage}`
        });
        return {
            statusCode: 403,
            errorMessage: `Color depth search was not started because ${limitsMessage}`
        };
    } else {
        console.log('Start ColorDepthSearch', searchParams);
        const searchInputName = searchParams.searchMask
            ? searchParams.searchMask
            : searchParams.searchInputName;
        const currentSearchBucket = getCurrentSearchBucket();

        searchParams.displayableMask = await createDisplayableMask(currentSearchBucket, searchParams.searchInputFolder, searchInputName);
        if (searchParams.displayableMask) {
            await updateSearchMetadata({
                id: searchParams.id || searchParams.searchId,
                displayableMask: searchParams.displayableMask,
            });
        }
        searchParams.searchBucket = currentSearchBucket;
        const cdsInvocationResult = await cdsStarter(searchParams);
        console.log('Started ColorDepthSearch', cdsInvocationResult);
        return cdsInvocationResult;
    }
};

const createDisplayableMask = async (bucket, prefix, key) => {
    if (/\.(tiff?|gif|jpe?g|bmp)$/.test(key)) {
        const fullKey = `${prefix}/${key}`;
        try {
            console.log(`Convert ${bucket}:${key} to PNG`);
            const imageContent = await getS3ContentAsByteBufferWithRetry(bucket, fullKey);
            const pngMime = "image/png";
            const pngExt = ".png";
            const image = await Jimp.fromBuffer(imageContent);
            const imageBuffer = await image.getBuffer(pngMime);
            const pngImageName = getSearchKey(fullKey, pngExt);
            console.log(`Upload displayable mask to ${bucket}:${pngImageName}`, imageBuffer);
            await putS3Content(bucket, pngImageName, pngMime, imageBuffer);
            console.info(`${fullKey} converted to png successfully`);
            return getSearchMaskId(pngImageName, pngExt);
        } catch (convertError) {
            console.error(`Error converting ${bucket}:${prefix}/${key} to PNG`, convertError);
            return null;
        }
    } else {
        return key;
    }
};

const startAlignment = async (searchParams) => {
    const { concurrentAlignmentLimits, perDayAlignmentLimits, } = getLimits();
    const limitsMessage = await checkLimits(
        searchParams,
        concurrentAlignmentLimits,
        perDayAlignmentLimits,
        {
            singular: 'alignment',
            plural: 'alignments'
        },
        s => s.step === ALIGNMENT_JOB_SUBMITTED);
    if (limitsMessage) {
        console.log(`No job invoked because ${limitsMessage}`, searchParams);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            errorMessage: `Your alignment could not be started because you are already running ${limitsMessage} `
        });
        return {
            statusCode: 403,
            errorMessage: `Alignment was not started because ${limitsMessage}`
        };
    } else {
        if (searchParams.simulateMIPGeneration) {
            return await generateMIPs(searchParams);
        } else {
            console.log('Prepare to submit alignment for', searchParams);
            return await submitAlignmentJob(searchParams);
        }
    }
};

const checkLimits = async (searchParams, concurrentSearches, perDayLimits, limitChecked, searchesFilter) => {
    if (concurrentSearches < 0 && perDayLimits < 0) {
        // no limits
        return null;
    }
    const searches = await lookupSearchMetadata({
        currentSearchId: searchParams.id,
        identityId: searchParams.identityId,
        owner: searchParams.owner,
        withNoErrorsOnly: true,
        lastUpdated: new Date()
    });
    if (perDayLimits >= 0 && searches.length >= perDayLimits) {
        return `you already reached the daily limits`;
    }
    const currentSearches =  searches.filter(searchesFilter);
    if (concurrentSearches >= 0 && currentSearches.length >=  concurrentSearches) {
        const limitMessage = currentSearches.length < 2
            ? limitChecked.singular
            : limitChecked.plural;
        return `you are already running ${currentSearches.length} ${limitMessage} - the maximum number of allowed concurrent ${limitChecked.plural}`;
    }
    return null;
};

const submitAlignmentJob = async (searchParams) => {
    const fullSearchInputImage = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
    const searchInputMetadata = await getS3ContentMetadata(getCurrentSearchBucket(), fullSearchInputImage);
    console.log('Search input metadata', searchInputMetadata);
    const searchInputSize = searchInputMetadata.ContentLength;
    const searchInputContentType = searchInputMetadata.ContentType;
    let estimatedMemory; // estimated memory in MB
    if (searchInputContentType === 'application/zip') {
        estimatedMemory = searchInputSize / (1024.0 * 1024.0) * 4 * 8;
        console.log(`Estimate memory for zip files to ${estimatedMemory}`);
    } else if (searchParams.searchInputName.toLowerCase().endsWith('.h5j')) {
        // for h5j we consider a compression factor of "only" 36
        // so in some cases this may result in OOM because there are situations when
        // the compression factor may be ~200x
        estimatedMemory = searchInputSize / (1024.0 * 1024.0) * 4 * 36;
        console.log(`Estimate memory for h5j files to ${estimatedMemory}`);
    } else {
        estimatedMemory = searchInputSize / (1024.0 * 1024.0) * 4;
    }
    const computeResources = selectComputeResources(estimatedMemory);
    console.log(`Estimated memory ${estimatedMemory} -> cpus: ${computeResources.cpus}, mem: ${computeResources.mem}`);
    const params = setAlignmentJobParams(searchParams, computeResources);
    console.log('Job parameters', params);
    try {
        // submit batch job
        const job = await batchClient.send(new SubmitJobCommand(params));
        const now = new Date();
        console.log('Submitted', job);
        console.log(`Job ${job.jobName} launched with id ${job.jobId}`, job);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            step: ALIGNMENT_JOB_SUBMITTED,
            alignStarted: now.toISOString(),
            alignmentSize: Math.ceil(estimatedMemory/1024.0)
        });
        if (alignMonitorStateMachineArn != null) {
            // start the state machine
            const timestamp = now.getTime();
            await startStepFunction(
                `Align_${job.jobId}_${timestamp}`,
                {
                    searchId: searchParams.id || null,
                    jobId: job.jobId,
                    startTime: timestamp
                },
                alignMonitorStateMachineArn
            );
        }
        return job;
    } catch (submitError) {
        console.error('Error submitting job with parameters', params, submitError);
        await updateSearchMetadata({
            id: searchParams.id || searchParams.searchId,
            step: ALIGNMENT_JOB_SUBMITTED,
            errorMessage: `Error submitting alignment job for ${searchParams.id}:${fullSearchInputImage} - ${submitError.message}`
        });
        return {
            statusCode: 404,
            errorMessage: `Error submitting alignment job: ${submitError.message}`
        };
    }
};

const selectComputeResources = estimatedMemory => {
    // it appears that AWS is allocating a machine
    // with the same # of cores as request but with 2x the requested memory
    if (estimatedMemory < 16 * 1024) {
        // m4.2xlarge (8cores/32G)
        return {
            mem: 16*1024,
            cpus: 8
        };
    } else if (estimatedMemory < 32 * 1024) {
        // m4.4xlarge (16cores/64G)
        return {
            mem: 32*1024,
            cpus: 16
        };
    } else if (estimatedMemory < 64 * 1024) {
        // r4.4xlarge (16cores/122G)
        return {
            mem: 64*1024,
            cpus: 16
        };
    } else {
        // m4.10xlarge (40cores/160G)
        return {
            mem: 100*1024,
            cpus: 40
        };
    }
};

const getCurrentSearchBucket = () => {
    return process.env.SEARCH_BUCKET;
};

const getLimits = () => {
    return {
        concurrentColorDepthSearchLimits: process.env.MAX_ALLOWED_CONCURRENT_SEARCHES || 1,
        perDayColorDepthSearchLimits: process.env.MAX_SEARCHES_PER_DAY || 1,
        concurrentAlignmentLimits: process.env.MAX_ALLOWED_CONCURRENT_ALIGNMENTS || 1,
        perDayAlignmentLimits: process.env.MAX_ALIGNMENTS_PER_DAY || 1,
    };
};

const setAlignmentJobParams = (searchParams, computeResources) => {
    console.log('Set alignment job parameters', searchParams);
    if (!searchParams.anatomicalRegion || searchParams.anatomicalRegion.toLowerCase() === 'brain') {
        return setBrainAlignmentJobParams(searchParams, computeResources);
    } else if (searchParams.anatomicalRegion.toLowerCase() === 'vnc') {
        return setVNCAlignmentJobParams(searchParams, computeResources);
    } else {
        throw Error("Unsupported alignment JOB for ", searchParams);
    }
};

const setBrainAlignmentJobParams = (searchParams, computeResources) => {
    const jobName = `align-brain-${searchParams.id}`;
    const alignmentInput = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
    const comparisonAlgorithm = searchParams.algorithm === 'avg' ? 'Median' : 'Max';
    let jobParameters = {
        search_id: searchParams.id,
        input_filename: alignmentInput,
        output_folder: searchParams.searchInputFolder,
        comparison_alg: comparisonAlgorithm,
        nslots: computeResources.cpus + ''
    };
    if (searchParams.userDefinedImageParams) {
        const xyRes = searchParams.voxelX ? searchParams.voxelX + '' : '0';
        const zRes = searchParams.voxelZ ? searchParams.voxelZ + '' : '0';
        const refChannel = searchParams.referenceChannel;
        // resolution values must be set
        jobParameters.force_voxel_size = searchParams.voxelX ? 'true' : 'false';
        jobParameters.xy_resolution = xyRes;
        jobParameters.z_resolution = zRes;
        jobParameters.reference_channel = refChannel;
    }
    return {
        jobDefinition: brainAlignJobDefinition,
        jobQueue: jobQueue,
        jobName: jobName,
        containerOverrides: {
            'vcpus': computeResources.cpus,
            'memory': computeResources.mem,
            'environment': [{
                name: 'ALIGNMENT_MEMORY',
                value: computeResources.mem + 'M'
            }],
        },
        parameters: jobParameters,
    };
};

const setVNCAlignmentJobParams = (searchParams, computeResources) => {
    const jobName = `align-vnc-${searchParams.id}`;
    const alignmentInput = `${searchParams.searchInputFolder}/${searchParams.searchInputName}`;
    let jobParameters = {
        search_id: searchParams.id,
        input_filename: alignmentInput,
        output_folder: searchParams.searchInputFolder,
        nslots: computeResources.cpus + ''
    };
    return {
        jobDefinition: vncAlignJobDefinition,
        jobQueue: jobQueue,
        jobName: jobName,
        containerOverrides: {
            'vcpus': computeResources.cpus,
            'memory': computeResources.mem,
            'environment': [{
                name: 'ALIGNMENT_MEMORY',
                value: computeResources.mem + 'M'
            }],
        },
        parameters: jobParameters,
    };
};
