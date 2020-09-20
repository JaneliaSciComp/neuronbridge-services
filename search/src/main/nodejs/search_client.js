'use strict'
const AWS = require('aws-sdk')
const { invokeSync } = require('./utils')
var fs = require('fs')
const util = require('util')
const sleep = util.promisify(setTimeout)
const { mean, median, min, max, std } = require('mathjs')
const cwLogs = new AWS.CloudWatchLogs()

async function report (searchFunction, stateMachineName, monitorUniqueName) {
  console.log('Reporting on ' + monitorUniqueName)

  // Find the state machine

  const stepFunctions = new AWS.StepFunctions()
  const stateMachinesResponse = await stepFunctions.listStateMachines().promise()
  const stateMachine = stateMachinesResponse.stateMachines.find((machine) => machine.name === stateMachineName)

  if (!stateMachine) {
    console.log(`No state machine with name ${stateMachineName} found`)
    console.log(stateMachinesResponse)
    return
  }

  // First, wait for the monitor state machine to finish, and then analyze its step history

  let execution = null
  let status = ''
  while (status !== 'SUCCEEDED') {
    const executionsResponse = await stepFunctions.listExecutions({ stateMachineArn: stateMachine.stateMachineArn }).promise()
    execution = executionsResponse.executions.find((execution) => execution.name === monitorUniqueName)
    if (!execution) {
      console.log(`No execution with name ${monitorUniqueName} found`)
      console.log(executionsResponse)
      return
    }
    status = execution.status
  }

  // State machine (and thus search) is now complete

  let stateMachineStarted = null
  let stateMachineEnded = null
  let reduceStarted = null
  let reduceEnded = null
  let totalTasks = null

  // Analyze its step history

  const historyResponse = await stepFunctions.getExecutionHistory({ executionArn: execution.executionArn }).promise()
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
        console.log(`${event.timestamp.toISOString()} elapsed=${output.elapsedSecs} remaining=${output.numRemaining}`)
        if (!totalTasks) {
          totalTasks = output.numRemaining
        }
      }
    } else if (event.type === 'TaskStateEntered') {
      if (event.stateEnteredEventDetails.name === 'Reduce') {
        reduceStarted = event.timestamp
      }
    }
  }

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
    streamsResponse = await cwLogs.describeLogStreams(streamsParams).promise()
    if (streamsResponse.logStreams[0].firstEventTimestamp < stateMachineStarted) {
      // Nothing returned after this event is relevant, so let's stop before we get rate limited
      break
    }
    for (const logStream of streamsResponse.logStreams) {
      if (logStream.firstEventTimestamp > stateMachineStarted) {
        logStreams.push(logStream)
      }
    }
    streamsParams.nextToken = streamsResponse.nextToken
    await sleep(100) // try to avoid rate limiting
  } while (streamsResponse.nextToken)

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

  console.log(`totalStateMachineElapsed:    ${pad(totalStateMachineElapsed)}`)
  console.log(`  userTimeElapsed:           ${pad(userTimeElapsed)}`)
  console.log(`    totalSearchElapsed:      ${pad(totalSearchElapsed)}`)
  console.log(`      searchWorkerElapsed:   ${pad(searchWorkerElapsed)}`)
  console.log(`        searchWorkerMean:    ${pad(mean(workerTimes))}`)
  console.log(`        searchWorkerMedian:  ${pad(median(workerTimes))}`)
  console.log(`        searchWorkerStd:     ${pad(std(workerTimes))}`)
  console.log(`        searchWorkerMin:     ${pad(min(workerTimes))}`)
  console.log(`        searchWorkerMax:     ${pad(max(workerTimes))}`)
  console.log(`      totalReducerDelay:     ${pad(totalReducerDelay)}`)
  console.log(`    totalReducerElapsed:     ${pad(totalReducerElapsed)}`)
}

async function main () {
  const args = process.argv.slice(2)
  const identifier = args[0]
  const infile = args[1]

  const dispatchFunction = identifier + '-searchDispatch'
  const searchFunction = identifier + '-search'
  const stateMachineName = 'searchMonitorStateMachine-' + identifier

  const readFile = util.promisify(fs.readFile)
  const searchParamsJson = await readFile(infile, 'utf8')
  const searchParams = JSON.parse(searchParamsJson)

  const cdsInvocationResult = await invokeSync(dispatchFunction, searchParams)
  console.log(`Started ColorDepthSearch with exit status: ${cdsInvocationResult.StatusCode}`)

  if (cdsInvocationResult.FunctionError) {
    console.log('Error:', cdsInvocationResult.FunctionError)
    console.log(JSON.parse(cdsInvocationResult.Payload))
    return
  }

  const buff = Buffer.from(cdsInvocationResult.LogResult, 'base64')
  const logLines = buff.toString('ascii')
  console.log(logLines)

  const response = JSON.parse(cdsInvocationResult.Payload)

  // report(searchFunction, stateMachineName, "ColorDepthSearch_6f015370-f7c4-11ea-92a6-5f7925e8a762_1600223363623");
  report(searchFunction, stateMachineName, response.monitorUniqueName)
}

main()
  .catch(err => {
    console.log(err.stack)
  })
