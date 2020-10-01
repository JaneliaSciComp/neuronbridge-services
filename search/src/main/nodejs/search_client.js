// Run a test search and analyze its performance.
//
// Usage:
// npm run-script search <stackName> <inputfile> [batchSize] [numLevels]
// npm run-script search <stackName> report [monitorName]
//
'use strict'
const util = require('util')
var fs = require('fs')
const readFile = util.promisify(fs.readFile)
const sleep = util.promisify(setTimeout)
const AWS = require('aws-sdk')
const cwLogs = new AWS.CloudWatchLogs()
const { mean, median, min, max, std } = require('mathjs')
const { invokeFunction } = require('./utils')

// Wait until the given monitor is done, then return the final execution
async function waitForMonitor(stateMachineName, monitorUniqueName) {

  // Find the state machine
  console.log('Finding ' + monitorUniqueName)
  const stepFunctions = new AWS.StepFunctions()
  const stateMachinesResponse = await stepFunctions.listStateMachines().promise()
  const stateMachine = stateMachinesResponse.stateMachines.find((machine) => machine.name === stateMachineName)

  if (!stateMachine) {
    console.log(`No state machine with name ${stateMachineName} found`)
    console.log(stateMachinesResponse)
    return
  }

  // Wait for the monitor state machine to finish, and then analyze its step history
  console.log('Waiting on ' + stateMachine.stateMachineArn)
  let execution = null
  let status = ''
  do {
    const executionsResponse = await stepFunctions.listExecutions({ stateMachineArn: stateMachine.stateMachineArn }).promise()
    execution = executionsResponse.executions.find((execution) => execution.name === monitorUniqueName)
    if (!execution) {
      console.log(`No execution with name ${monitorUniqueName} found`)
      console.log(executionsResponse)
      return
    }
    status = execution.status
    await sleep(2000) // try to avoid rate limiting
  } while (status === 'RUNNING')

  return execution
}

// Iterate over the  history for the given state machine execution, and run the provided function for each event.
async function forExecutionHistory(executionArn, func) {
  const stepFunctions = new AWS.StepFunctions()
  const historyParams = { executionArn: executionArn }
  do {
    const historyResponse = await stepFunctions.getExecutionHistory(historyParams).promise()
    historyParams.nextToken = historyResponse.nextToken
    for (const event of historyResponse.events) {
      if (!func(event)) {
        historyParams.nextToken = null
        break
      }
    }
    await sleep(200) // try to avoid rate limiting
  } while (historyParams.nextToken)
}

// Fetch and return all the CloudWatch log streams for the given Lambda, with time constraints.
async function getStreams(functionName, startTime, endTime) {
  const streamsParams = {
    logGroupName: `/aws/lambda/${functionName}`,
    orderBy: 'LastEventTime',
    descending: true
  }
  const logStreams = []
  var streamsResponse
  do {
    streamsResponse = await cwLogs.describeLogStreams(streamsParams).promise()
    for (const logStream of streamsResponse.logStreams) {
      if (logStream.lastEventTimestamp < startTime) {
        // Nothing returned after this event is relevant, so let's stop before we get rate limited
        streamsResponse.nextToken = null
        break
      }
      if (logStream.firstEventTimestamp >= startTime && logStream.firstEventTimestamp < endTime) {
        logStreams.push(logStream)
      }
    }
    streamsParams.nextToken = streamsResponse.nextToken
    await sleep(50) // try to avoid rate limiting
  } while (streamsParams.nextToken)
  return logStreams
}

// Parse the given worker log and extract its true start time
async function getWorkerStartTime(logGroupName, logStreamName) {
  const logEventsParams = {
    logGroupName: logGroupName,
    logStreamName: logStreamName,
  };
  //console.log(`Getting events for ${logStreamName}`)
  const eventsResponse = await cwLogs.getLogEvents(logEventsParams).promise()
  for(const logEvent of eventsResponse.events) {
    const batchIdSplit = logEvent.message.split('Batch Id: ');
    if (batchIdSplit.length>1) {
      const batchStr = batchIdSplit[1].trim()
      const batchId = parseInt(batchStr);
      return [batchId,logEvent.timestamp];
    }
  }
  // It's much slower to do filter on the server side (?!)
  // const logEventsParams = {
  //   logGroupName: logGroupName,
  //   logStreamNames: [logStreamName],
  //   filterPattern: "Batch Id"
  // };
  // const filterResponse = await cwLogs.filterLogEvents(logEventsParams).promise()
  // for(const logEvent of filterResponse.events) {
  //   const batchIdSplit = logEvent.message.split('Batch Id: ');
  //   const batchStr = batchIdSplit[1].trim()
  //   const batchId = parseInt(batchStr);
  //   return [batchId,logEvent.timestamp];
  // }
  // return [null,null];
}

// Generate a complete performance report for a burst-parallel execution
async function report(dispatchFunction, searchFunction, stateMachineName, monitorUniqueName) {

  const execution = await waitForMonitor(stateMachineName, monitorUniqueName)

  if (execution.status!=='SUCCEEDED') {
    console.log(`Search status is ${execution.status}`)
    return
  }

  // State machine (and thus search) is now complete
  console.log('Reporting on ' + monitorUniqueName)

  const stages = []

  let searchStarted = null;
  let stateMachineStarted = null
  let stateMachineEnded = null
  let reduceStarted = null
  let reduceEnded = null
  let totalTasks = null
  let currState = null
  let monitorStarted = null
  let monitorName = null

  // Analyze state machine step history

  console.log("Step function history:");
  await forExecutionHistory(execution.executionArn, event => {
    if (event.type === 'ExecutionStarted') {
      stateMachineStarted = event.timestamp
    } else if (event.type === 'ExecutionSucceeded') {
      stateMachineEnded = event.timestamp
    } else if (event.type === 'LambdaFunctionScheduled') {
      if (currState=='Monitor') {
        monitorStarted = event.timestamp
      }
    } else if (event.type === 'LambdaFunctionSucceeded') {
      if (currState=='Monitor') {
        const output = JSON.parse(event.lambdaFunctionSucceededEventDetails.output)
        monitorName = `Monitor (${output.numRemaining})`
        stages.push({
          category: "Overview",
          name: monitorName,
          start: monitorStarted,
          end: event.timestamp
        })
      }
      if (reduceStarted && !reduceEnded) {
        reduceEnded = event.timestamp
      } else {
        const output = JSON.parse(event.lambdaFunctionSucceededEventDetails.output)
        if (searchStarted==null) {
          searchStarted = new Date(output.startTime);
        }
        console.log(`${event.timestamp.toISOString()} ${output.numRemaining}/${output.numBatches} remaining (${output.elapsedSecs} secs elapsed)`)
        if (!totalTasks) {
          totalTasks = output.numBatches
        }
      }
    } else if (event.type === 'TaskStateEntered') {
      currState = event.stateEnteredEventDetails.name
      if (currState === 'Reduce') {
        reduceStarted = event.timestamp
      }
    }
    return true
  })

  stages.push({
    category: "Overview",
    name: "State Machine",
    start: stateMachineStarted,
    end: stateMachineEnded
  })

  stages.push({
    category: "Overview",
    name: "Reducer",
    start: reduceStarted,
    end: reduceEnded
  })

  console.log('Fetching dispatcher log streams...')
  const dispatcherLogStreams = await getStreams(dispatchFunction, stateMachineStarted, stateMachineEnded)

  console.log('Fetching dispatcher log events...')
  let firstDispatcherTime = Number.MAX_SAFE_INTEGER
  let lastDispatcherTime = 0
  const dispatchTimes = {}
  const dispatchElapsedTimes = []
  for (const logStream of dispatcherLogStreams) {
    
    if (logStream.firstEventTimestamp < firstDispatcherTime) {
      firstDispatcherTime = logStream.firstEventTimestamp
    }
    if (logStream.lastEventTimestamp > lastDispatcherTime) {
      lastDispatcherTime = logStream.lastEventTimestamp
    }
    
    const elapsedMs = logStream.lastEventTimestamp - logStream.firstEventTimestamp
    dispatchElapsedTimes.push(elapsedMs)

    const logGroupName = `/aws/lambda/${dispatchFunction}`
    const logEventsParams = {
      logGroupName: logGroupName,
      logStreamNames: [logStream.logStreamName],
      filterPattern: "Dispatching Batch Id"
    }

    do {
      const filterResponse = await cwLogs.filterLogEvents(logEventsParams).promise()
      logEventsParams.nextToken = filterResponse.nextToken
      for(const logEvent of filterResponse.events) {
        const dispatchSplit = logEvent.message.split('Batch Id: ');
        const batchStr = dispatchSplit[1].trim()
        const batchId = parseInt(batchStr);
        dispatchTimes[batchId] = logEvent.timestamp
      }
      await sleep(200) // try to avoid rate limiting
    } while (logEventsParams.nextToken)

    stages.push({
      category: "Dispatchers",
      name: "Dispatcher",
      logGroupName: logGroupName,
      logStreamName: logStream.logStreamName,
      start: new Date(logStream.firstEventTimestamp),
      end: new Date(logStream.lastEventTimestamp)
    })

  }

  console.log(`Search ran with ${totalTasks} workers`)
  console.log(`Parsed ${Object.keys(dispatchTimes).length} dispatch times`)

  console.log('Fetching worker log streams...')
  const workerLogStreams = await getStreams(searchFunction, stateMachineStarted, stateMachineEnded)
  
  if (totalTasks !== workerLogStreams.length) {
    console.log(`WARNING: Number of worker logs (${workerLogStreams.length}) does not match number of workers (${totalTasks}). Subsequent analysis will not be valid.`)
  }

  console.log('Fetching worker log events...')
  let firstWorkerTime = Number.MAX_SAFE_INTEGER
  let lastWorkerTime = 0
  const workerElapsedTimes = []
  const workerStartTimes = []
  for (const logStream of workerLogStreams) {
    
    const elapsedMs = logStream.lastEventTimestamp - logStream.firstEventTimestamp
    workerElapsedTimes.push(elapsedMs)

    const logGroupName = `/aws/lambda/${searchFunction}`
    const logEventsParams = {
      logGroupName: logGroupName,
      logStreamName: logStream.logStreamName,
      startFromHead: true
    };
    let batchId = null
    let batchStartTime = null
    let firstEventTime = Number.MAX_SAFE_INTEGER
    let lastEventTime = 0
    
    const eventsResponse = await cwLogs.getLogEvents(logEventsParams).promise()
    for(const logEvent of eventsResponse.events) {
      if (logEvent.timestamp < firstEventTime) {
        firstEventTime = logEvent.timestamp
      }
      if (logEvent.timestamp > lastEventTime) {
        lastEventTime = logEvent.timestamp
      }
      batchStartTime = logEvent.timestamp
      const batchIdSplit = logEvent.message.split('Batch Id: ');
      if (batchIdSplit.length>1) {
        const batchStr = batchIdSplit[1].trim()
        batchId = parseInt(batchStr);
      }
    }

    if (firstEventTime < firstWorkerTime) {
      firstWorkerTime = firstEventTime
    }
    if (lastEventTime > lastWorkerTime) {
      lastWorkerTime = lastEventTime
    }

    if (!batchId || !batchStartTime) {
      console.log("WARNING: could not find metadata in log for "+logStream.logStreamName);
    }
    else if (dispatchTimes[batchId]) {
      const startupTime = batchStartTime - dispatchTimes[batchId]
      workerStartTimes.push(startupTime)
    }
    else {
      console.log("WARNING: missing dispatch time for batch "+batchId);
    }

    stages.push({
      category: "Workers",
      name: "Worker "+batchId,
      logGroupName: logGroupName,
      logStreamName: logStream.logStreamName,
      start: new Date(logStream.firstEventTimestamp),
      end: new Date(logStream.lastEventTimestamp)
    })
  }

  const totalElapsed = stateMachineEnded.getTime() - searchStarted.getTime()
  const totalStateMachineElapsed = stateMachineEnded.getTime() - stateMachineStarted.getTime()
  const totalSearchElapsed = reduceStarted.getTime() - firstDispatcherTime
  const totalReducerElapsed = reduceEnded.getTime() - reduceStarted.getTime()
  const dispatcherElapsed = lastDispatcherTime - firstDispatcherTime
  const searchWorkerElapsed = lastWorkerTime - firstWorkerTime
  const totalReducerDelay = reduceStarted.getTime() - lastWorkerTime
  const userTimeElapsed = reduceEnded.getTime() - stateMachineStarted.getTime()

  stages.push({
    category: "Overview",
    name: "Dispatchers",
    start: new Date(firstDispatcherTime),
    end: new Date(lastDispatcherTime),
    stats: {
      mean: mean(dispatchElapsedTimes),
      median: median(dispatchElapsedTimes),
      std: std(dispatchElapsedTimes),
      min: min(dispatchElapsedTimes),
      max: max(dispatchElapsedTimes),
    }
  })

  stages.push({
    category: "Overview",
    name: "Workers",
    start: new Date(firstWorkerTime),
    end: new Date(lastWorkerTime),
    stats: {
      mean: mean(workerElapsedTimes),
      median: median(workerElapsedTimes),
      std: std(workerElapsedTimes),
      min: min(workerElapsedTimes),
      max: max(workerElapsedTimes),
    }
  })

  if (totalReducerDelay > 0) {
    stages.push({
      category: "Overview",
      name: "Reducer Delay",
      start: new Date(lastWorkerTime),
      end: reduceStarted
    })
  }

  function pad (n) {
    return Math.round(n).toString().padStart(7, ' ') + ' ms'
  }

  console.log(`totalElapsed:                 ${pad(totalElapsed)}`)
  console.log(`  totalStateMachineElapsed:   ${pad(totalStateMachineElapsed)}`)
  console.log(`    userTimeElapsed:          ${pad(userTimeElapsed)}`)
  console.log(`      totalSearchElapsed:     ${pad(totalSearchElapsed)}`)
  console.log(`        dispatcherElapsed:    ${pad(dispatcherElapsed)}`)
  console.log(``)
  console.log(`        workerStartMean:      ${pad(mean(workerStartTimes))}`)
  console.log(`        workerStartMedian:    ${pad(median(workerStartTimes))}`)
  console.log(`        workerStartStd:       ${pad(std(workerStartTimes))}`)
  console.log(`        workerStartMin:       ${pad(min(workerStartTimes))}`)
  console.log(`        workerStartMax:       ${pad(max(workerStartTimes))}`)
  console.log(``)
  console.log(`        searchWorkerElapsed:  ${pad(searchWorkerElapsed)}`)
  console.log(`          searchWorkerMean:   ${pad(mean(workerElapsedTimes))}`)
  console.log(`          searchWorkerMedian: ${pad(median(workerElapsedTimes))}`)
  console.log(`          searchWorkerStd:    ${pad(std(workerElapsedTimes))}`)
  console.log(`          searchWorkerMin:    ${pad(min(workerElapsedTimes))}`)
  console.log(`          searchWorkerMax:    ${pad(max(workerElapsedTimes))}`)
  console.log(``)
  if (totalReducerDelay > 0) {
    console.log(`        totalReducerDelay:    ${pad(totalReducerDelay)}`)
  }
  console.log(`      totalReducerElapsed:    ${pad(totalReducerElapsed)}`)
  
  return {
    aggregate: ["Dispatcher", "Workers"],
    stages: stages
  }
}

async function main () {
  const args = process.argv.slice(2)
  const identifier = args[0]
  const infile = args[1]

  const dispatchFunction = identifier + '-searchDispatch'
  const searchFunction = identifier + '-search'
  const stateMachineName = 'searchMonitorStateMachine-' + identifier

  if (infile==="report") {
    
    const monitorName = args[2]
    const reportObj = await report(dispatchFunction, searchFunction, stateMachineName, monitorName)
    let reportStr = JSON.stringify(reportObj, null, 2)
    
    const outfile = monitorName+".json"
    fs.writeFileSync(outfile, reportStr)
    console.log(`Wrote report to ${outfile}`)
    console.log(`To analyze, open timeline.html and load the JSON data file.`)
  }
  else {
    const searchParamsJson = await readFile(infile, 'utf8')
    const searchParams = JSON.parse(searchParamsJson)

    if (args.length==4) {
      searchParams["batchSize"] = Number.parseInt(args[2])
      searchParams["numLevels"] = Number.parseInt(args[3])
    }

    const cdsInvocationResult = await invokeFunction(dispatchFunction, searchParams)

    if (cdsInvocationResult.FunctionError) {
      console.log('Error:', cdsInvocationResult.FunctionError)
      console.log(JSON.parse(cdsInvocationResult.Payload))
      return
    }
    else {
      console.log("Search running...")
    }

    if (cdsInvocationResult.LogResult) {
      const buff = Buffer.from(cdsInvocationResult.LogResult, 'base64')
      const logLines = buff.toString('ascii')
      console.log(logLines)
    }

    const response = JSON.parse(cdsInvocationResult.Payload)
    const execution = await waitForMonitor(stateMachineName, response.monitorUniqueName)
      
    if (execution.status==='SUCCEEDED') {
      console.log("Search complete.")
      console.log("Results may lag due to eventual consistency. To attempt analysis, run:")
      console.log(`npm run-script search ${identifier} report ${response.monitorUniqueName}`)
    }
    else {
      // Search failed, try to find out why...
      console.log(`Search status is ${execution.status}`)
      await forExecutionHistory(execution.executionArn, event => {
        if (event.type === 'ExecutionFailed') {
          console.log(event)
          return false
        }
        return true
      })
    }
  }
}

main()
  .catch(err => {
    console.log(err.stack)
  })
