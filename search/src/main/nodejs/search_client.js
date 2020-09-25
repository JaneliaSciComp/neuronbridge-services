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
const { invokeSync } = require('./utils')

async function waitFor(stateMachineName, monitorUniqueName) {

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

async function report(searchFunction, stateMachineName, monitorUniqueName) {

  const execution = await waitFor(stateMachineName, monitorUniqueName)

  if (execution.status!=='SUCCEEDED') {
    console.log(`Search status is ${execution.status}`)
    return
  }

  // State machine (and thus search) is now complete
  console.log('Reporting on ' + monitorUniqueName)

  let searchStarted = null;
  let stateMachineStarted = null
  let stateMachineEnded = null
  let reduceStarted = null
  let reduceEnded = null
  let totalTasks = null
  
  // Analyze state machine step history

  const stepFunctions = new AWS.StepFunctions()
  const historyParams = { executionArn: execution.executionArn }
  do {
    const historyResponse = await stepFunctions.getExecutionHistory(historyParams).promise()
    for (const event of historyResponse.events) {
      if (event.type === 'ExecutionStarted') {
        stateMachineStarted = event.timestamp
      } else if (event.type === 'ExecutionSucceeded') {
        stateMachineEnded = event.timestamp
      } else if (event.type === 'LambdaFunctionSucceeded') {
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
        if (event.stateEnteredEventDetails.name === 'Reduce') {
          reduceStarted = event.timestamp
        }
      }
    }
    historyParams.nextToken = historyResponse.nextToken
    await sleep(200) // try to avoid rate limiting
  } while (historyParams.nextToken)

  const totalElapsed = stateMachineEnded.getTime() - searchStarted.getTime()
  const totalStateMachineElapsed = stateMachineEnded.getTime() - stateMachineStarted.getTime()
  const totalSearchElapsed = reduceStarted.getTime() - stateMachineStarted.getTime()
  const totalReducerElapsed = reduceEnded.getTime() - reduceStarted.getTime()

  console.log('Fetching search worker log streams...')

  const streamsParams = {
    logGroupName: `/aws/lambda/${searchFunction}`,
    orderBy: 'LastEventTime',
    descending: true
  }

  const logStreams = []
  var streamsResponse
  do {
    //console.log("---------------------------------------------------------------------")
    streamsResponse = await cwLogs.describeLogStreams(streamsParams).promise()
    for (const logStream of streamsResponse.logStreams) {
      //let startDate = new Date(logStream.firstEventTimestamp);
      //let endDate = new Date(logStream.lastEventTimestamp);

      if (logStream.lastEventTimestamp < stateMachineStarted) {
        // Nothing returned after this event is relevant, so let's stop before we get rate limited
        //break
        //console.log(`<<<<< ${startDate.toISOString()} - ${endDate.toISOString()}`)
        streamsResponse.nextToken = null
      }
      else {
        //console.log(`      ${startDate.toISOString()} - ${endDate.toISOString()}`)
      }
      if (logStream.firstEventTimestamp >= stateMachineStarted) {
        logStreams.push(logStream)
      }
    }
    streamsParams.nextToken = streamsResponse.nextToken
    await sleep(100) // try to avoid rate limiting
  } while (streamsParams.nextToken)

  let minTime = Number.MAX_SAFE_INTEGER
  let maxTime = 0

  if (totalTasks !== logStreams.length) {
    console.log(`WARNING: Number of worker logs (${logStreams.length}) does not match number of workers (${totalTasks}). Subsequent analysis will not be valid.`)
  }

  console.log(`Search ran with ${totalTasks} workers`)

  const workerTimes = []
  for (const logStream of logStreams) {
    if (logStream.firstEventTimestamp < minTime) {
      minTime = logStream.firstEventTimestamp
    }
    if (logStream.lastEventTimestamp > maxTime) {
      maxTime = logStream.lastEventTimestamp
    }
    const elapsedMs = logStream.lastEventTimestamp - logStream.firstEventTimestamp
    workerTimes.push(elapsedMs)
    // console.log(`${logStream.logStreamName} - ${elapsedMs} ms`);
  }

  const searchWorkerElapsed = maxTime - minTime
  const totalReducerDelay = reduceStarted.getTime() - maxTime
  const userTimeElapsed = reduceEnded.getTime() - stateMachineStarted.getTime()

  function pad (n) {
    return Math.round(n).toString().padStart(6, ' ') + ' ms'
  }

  console.log(`totalElapsed:                  ${pad(totalElapsed)}`)
  console.log(`  totalStateMachineElapsed:    ${pad(totalStateMachineElapsed)}`)
  console.log(`    userTimeElapsed:           ${pad(userTimeElapsed)}`)
  console.log(`      totalSearchElapsed:      ${pad(totalSearchElapsed)}`)
  console.log(`        searchWorkerElapsed:   ${pad(searchWorkerElapsed)}`)
  console.log(`          searchWorkerMean:    ${pad(mean(workerTimes))}`)
  console.log(`          searchWorkerMedian:  ${pad(median(workerTimes))}`)
  console.log(`          searchWorkerStd:     ${pad(std(workerTimes))}`)
  console.log(`          searchWorkerMin:     ${pad(min(workerTimes))}`)
  console.log(`          searchWorkerMax:     ${pad(max(workerTimes))}`)
  console.log(`        totalReducerDelay:     ${pad(totalReducerDelay)}`)
  console.log(`      totalReducerElapsed:     ${pad(totalReducerElapsed)}`)
}

async function main () {
  const args = process.argv.slice(2)
  const identifier = args[0]
  const infile = args[1]

  const dispatchFunction = identifier + '-searchDispatch'
  const searchFunction = identifier + '-search'
  const stateMachineName = 'searchMonitorStateMachine-' + identifier

  if (infile==="report") {
    await report(searchFunction, stateMachineName, args[2])
  }
  else {
    const searchParamsJson = await readFile(infile, 'utf8')
    const searchParams = JSON.parse(searchParamsJson)

    if (args.length==4) {
      const batchSize = args[2]
      const numLevels = args[3]
      searchParams["batchSize"] = batchSize
      searchParams["numLevels"] = numLevels
    }

    const cdsInvocationResult = await invokeSync(dispatchFunction, searchParams)

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
    const execution = await waitFor(stateMachineName, response.monitorUniqueName)
      
    if (execution.status!=='SUCCEEDED') {
      console.log(`Search status is ${execution.status}`)
      return    
    }
    else {
      console.log("Search complete.")
      console.log("Results may lag due to eventual consistency. To attempt analysis, run:")
      console.log(`npm run-script search ${identifier} report ${response.monitorUniqueName}`)
    }
  }
}

main()
  .catch(err => {
    console.log(err.stack)
  })
