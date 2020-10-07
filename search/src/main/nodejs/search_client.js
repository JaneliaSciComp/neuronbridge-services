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

const TRACE = false
const DEBUG = false

// This is a guess about how long the first dispatcher took in order to start the state machine. 
// If this is not large enough, we may not find all the dispatchers. 
const STATE_MACHINE_START_TIME_ESTIMATE = 30000

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
    await sleep(1000) // try to avoid rate limiting
  } while (status === 'RUNNING')

  return execution
}

// Iterate over the  history for the given state machine execution, and run the provided function for each event.
// If func returns false then the for loop exits with a break
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
        if (DEBUG) console.log('DEBUG: Next stream has last event before start time: ', new Date(logStream.lastEventTimestamp))
        break
      }
      if (logStream.firstEventTimestamp >= startTime || logStream.firstEventTimestamp < endTime) {
        if (DEBUG) console.log(`DEBUG: Including ${logStream.logStreamName}`)
        logStreams.push(logStream)
      }
      else {
        if (DEBUG) console.log(`DEBUG: Omitting ${logStream.logStreamName} because last event out of range:`, new Date(logStream.lastEventTimestamp))
      }
    }
    streamsParams.nextToken = streamsResponse.nextToken
    await sleep(50) // try to avoid rate limiting
  } while (streamsParams.nextToken)
  return logStreams
}

function isStart(logEvent) {
  return logEvent.message.startsWith('START ')
}

function isEnd(logEvent) {
  return logEvent.message.startsWith('END ')
}

function isReport(logEvent) {
  return logEvent.message.startsWith('REPORT ')
}

function getRequestId(logEvent, requests) {
  // First check for a START message where the request id is easily parsed
  //START RequestId: 3e7c1b57-4e64-4b3d-ac59-9fc8fb0882cf Version: $LATEST 
  const match = logEvent.message.match(/START RequestId: (\S+) /);
  if (match) {
    return match[1]
  }
  else if (requests) {
    // Are any of the known request ids present in the log line?
    for(let requestId of Object.keys(requests)) {
      if (logEvent.message.indexOf(requestId) > 0) {
        return requestId
      }
    }
  }
  else {
    console.log('WARNING: no valid request id found in:', logEvent)
    return null
  }
}

function parseReport(logEvent) {
  //REPORT RequestId: 3e7c1b57-4e64-4b3d-ac59-9fc8fb0882cf Duration: 28823.03 ms Billed Duration: 28900 ms Memory Size: 384 MB Max Memory Used: 181 MB Init Duration: 601.37 ms 
  const match = logEvent.message.match(/.*?Duration: ([\d.]+ \w+)\s+Billed Duration: ([\d.]+ \w+)\s+Memory Size: (\d+ \w+)\s+Max Memory Used: (\d+ \w+)(\s+Init Duration: ([\d.]+ \w+))?/);
  if (!match) {
    console.log('Could not parse report: ', logEvent.message)
    return {}
  }
  const report = {
    duration: match[1],
    billedBuration: match[2],
    memorySize: match[3],
    maxMemoryUsed: match[4]
  }
  if (match.length > 6) {
    report.initDuration = match[6]
  }
  return report
}

async function getRequestLogs(logGroupName, logStreamName) {
  const requests = {}
  const logEventsParams = {
    logGroupName: logGroupName,
    logStreamName: logStreamName,
    startFromHead: true
  }
  do {
    const eventsResponse = await cwLogs.getLogEvents(logEventsParams).promise()
    logEventsParams.nextToken = eventsResponse.nextToken
    for(const logEvent of eventsResponse.events) {
      if (isStart(logEvent)) {
        const requestId = getRequestId(logEvent)
        requests[requestId] = {
          firstEventTime: Number.MAX_SAFE_INTEGER,
          lastEventTime: 0,
          logEvents: []
        }
      }
      else if (isEnd(logEvent)) {
        // Ignore
      }
      else if (isReport(logEvent)) {
        const requestId = getRequestId(logEvent, requests)
        const report = parseReport(logEvent)
        requests[requestId].report = report
      }
      else {
        const requestId = getRequestId(logEvent, requests)
        const r = requests[requestId]
        if (r) {
          if (logEvent.timestamp < r.firstEventTime) {
            r.firstEventTime = logEvent.timestamp
          }
          if (logEvent.timestamp > r.lastEventTime) {
            r.lastEventTime = logEvent.timestamp
          }
          r.logEvents.push(logEvent)
        }
        else {
          if (DEBUG) console.log(`No such request: ${requestId}`)
        }
      }
    }
    await sleep(50) // try to avoid rate limiting
  } while (logEventsParams.nextToken)

  return requests
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

  let input = null
  const stages = []

  // Analyze state machine step history
  let searchStarted = null
  let stateMachineStarted = null
  let stateMachineEnded = null
  let reduceStarted = null
  let reduceEnded = null
  let totalTasks = null
  let currState = null
  let monitorStarted = null
  let monitorName = null
  let reducerLogGroupName = null
  let monitorLogGroupName = null
  console.log("Step function history:");

  await forExecutionHistory(execution.executionArn, event => {
    //console.log(event)
    if (event.type === 'ExecutionStarted') {
      stateMachineStarted = event.timestamp
    } else if (event.type === 'ExecutionSucceeded') {
      stateMachineEnded = event.timestamp
    } else if (event.type === 'TaskScheduled') {
      if (currState=='Monitor') {
        monitorStarted = event.timestamp
        const parameters = JSON.parse(event.taskScheduledEventDetails.parameters)
        monitorLogGroupName = '/aws/lambda/'+parameters.FunctionName
      }
      else if (currState=='Reduce') {
        const parameters = JSON.parse(event.taskScheduledEventDetails.parameters)
        reducerLogGroupName = '/aws/lambda/'+parameters.FunctionName
      }
    } else if (event.type === 'TaskSucceeded') {
      const output = JSON.parse(event.taskSucceededEventDetails.output).Payload
      if (currState=='Monitor') {
        monitorName = `Monitor (${output.numRemaining})`
        stages.push({
          category: "Overview",
          name: monitorName,
          logGroupName: monitorLogGroupName,
          start: monitorStarted,
          end: event.timestamp
        })
      }
      if (reduceStarted && !reduceEnded) {
        reduceEnded = event.timestamp
      } else {
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
    logGroupName: reducerLogGroupName,
    start: reduceStarted,
    end: reduceEnded
  })
  

  console.log('Fetching dispatcher log streams...')
  const dispatcherLogStreams = await getStreams(dispatchFunction, new Date(stateMachineStarted.getTime() - STATE_MACHINE_START_TIME_ESTIMATE), stateMachineEnded)

  console.log('Fetching dispatcher log events...')
  let firstDispatcherTime = Number.MAX_SAFE_INTEGER
  let lastDispatcherTime = 0
  const dispatchTimes = {}
  const dispatchElapsedTimes = []
  for (const logStream of dispatcherLogStreams) {
    
    const logGroupName = `/aws/lambda/${dispatchFunction}`
    const logStreamName = logStream.logStreamName
    const requests = await getRequestLogs(logGroupName, logStreamName)

    for(const requestId of Object.keys(requests)) {
      const r = requests[requestId]

      let rootDispatcher = false
      let inputEvent = null
      let monitorId = null

      for(const logEvent of r.logEvents) {

        if (logEvent.message.indexOf('Root Dispatcher')>0) {
          rootDispatcher = true
        }

        const monitorSplit = logEvent.message.split('Monitor: ');
        if (monitorSplit.length>1) {
          monitorId = monitorSplit[1].trim()
        }

        const dispatchSplit = logEvent.message.split('Dispatching Batch Id: ');
        if (dispatchSplit.length>1) {
          const batchStr = dispatchSplit[1].trim()
          const batchId = parseInt(batchStr);
          dispatchTimes[batchId] = logEvent.timestamp
        }
        const eventSplit = logEvent.message.split('Input event: ');
        if (eventSplit.length>1) {
          inputEvent = eventSplit[1].trim()
        }
      }

      if (monitorId) {
        if (monitorId!==monitorUniqueName) {
          if (TRACE) console.log(`DEBUG: wrong monitor name ${monitorId} in ${logStreamName}/${requestId}`)
          continue
        }
      }
      else {
        console.log(`No monitor id found in ${logStreamName}/${requestId}`)
      }

      if (r.firstEventTime < firstDispatcherTime) {
        firstDispatcherTime = r.firstEventTime
      }
      if (r.lastEventTime > lastDispatcherTime) {
        lastDispatcherTime = r.lastEventTime
      }
  
      if (rootDispatcher && inputEvent) {
        input = JSON.parse(inputEvent)
      }

      const elapsedMs = r.lastEventTime - r.firstEventTime
      dispatchElapsedTimes.push(elapsedMs)

      stages.push({
        category: "Dispatchers",
        name: rootDispatcher ? "Root Dispatcher" : "Dispatcher",
        logGroupName: logGroupName,
        logStreamName: logStreamName,
        start: new Date(r.firstEventTime),
        end: new Date(r.lastEventTime),
        report: r.report
      })
    }
  }

  if (!input) {
    console.log('WARNING: Could not find original input event in root dispatcher')
  }

  console.log(`Search ran with ${totalTasks} workers`)
  console.log(`Parsed ${Object.keys(dispatchTimes).length} dispatch times`)

  console.log('Fetching worker log streams...')
  const workerLogStreams = await getStreams(searchFunction, firstDispatcherTime, stateMachineEnded)
  
  if (totalTasks !== workerLogStreams.length) {
    console.log(`WARNING: Number of worker logs (${workerLogStreams.length}) does not match number of workers (${totalTasks}).`)
  }

  console.log('Fetching worker log events...')
  let firstWorkerTime = Number.MAX_SAFE_INTEGER
  let lastWorkerTime = 0
  const workerElapsedTimes = []
  const workerStartTimes = []

  for (const logStream of workerLogStreams) {

    const logGroupName = `/aws/lambda/${searchFunction}`
    const logStreamName = logStream.logStreamName
    const requests = await getRequestLogs(logGroupName, logStreamName)

    let batchId = null
    let monitorId = null

    for(const requestId of Object.keys(requests)) {
      const r = requests[requestId]

      for(const logEvent of r.logEvents) {

        const monitorSplit = logEvent.message.split('Monitor: ');
        if (monitorSplit.length>1) {
          monitorId = monitorSplit[1].trim()
        }

        const batchIdSplit = logEvent.message.split('Batch Id: ');
        if (batchIdSplit.length>1) {
          const batchStr = batchIdSplit[1].trim()
          batchId = parseInt(batchStr);
        }
      }

      if (monitorId) {
        if (monitorId!==monitorUniqueName) {
          if (TRACE) console.log(`DEBUG: wrong monitor name ${monitorId} in ${logStreamName}/${requestId}`)
          continue
        }
      }
      else {
        console.log(`No monitor id found in ${logStreamName}/${requestId}`)
      }


      if (r.firstEventTime < firstWorkerTime) {
        firstWorkerTime = r.firstEventTime
      }
      if (r.lastEventTime > lastWorkerTime) {
        lastWorkerTime = r.lastEventTime
      }

      if (batchId==null) {
        console.log(`WARNING: could not find batch id in ${logStreamName}/${requestId}`);
      }
      else if (dispatchTimes[batchId]) {
        const startupTime = r.firstEventTime - dispatchTimes[batchId]
        workerStartTimes.push(startupTime)
      }
      else {
        console.log(`WARNING: missing dispatch time for batch ${batchId}`);
      }

      const elapsedMs = r.lastEventTime - r.firstEventTime
      workerElapsedTimes.push(elapsedMs)

      stages.push({
        category: "Workers",
        name: "Worker "+batchId,
        logGroupName: logGroupName,
        logStreamName: logStream.logStreamName,
        start: new Date(r.firstEventTime),
        end: new Date(r.lastEventTime),
        report: r.report
      })
    }
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
    name: "Process",
    start: new Date(firstDispatcherTime),
    end: stateMachineEnded
  })

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
    input: input,
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
    if (reportObj) {
      const outfile = monitorName+".json"
      fs.writeFileSync(outfile, JSON.stringify(reportObj, null, 2))
      console.log(`Wrote report to ${outfile}`)
      console.log(`To analyze, open timeline.html and load the JSON data file.`)
    }
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
