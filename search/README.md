# NeuronBridge Color Depth Search

The color depth search is run in parallel using the [burst compute](https://github.com/JaneliaSciComp/burst-compute) framework.

## Deploy to AWS

Deploy to a given stage, e.g. dev or prod:
```
npm run sls -- deploy -s <stage>
```

## Linting

Run the linter to detect problems:
```
npx eslint src/main/nodejs
```

## Unit testing

Run unit tests:
```
npm test
```

## Integration testing

This runs a local client which invokes the search dispatch, waits for the search to run, and then analyzes the search performance and produces an HTML report.

```
npm run search janelia-neuronbridge-cds-dev src/test/resources/test1.json
npm run search janelia-neuronbridge-cds-dev report <jobId>
open timeline.html
```

You can then open the report json using [burst-compute's timeline.html](https://github.com/JaneliaSciComp/burst-compute/blob/master/timeline.html).

## Test Event for Color Depth Search Dispatch:

```
{
  "libraryAlignmentSpace": "JRC2018_Unisex_20x_HR",
  "searchableMIPSFolder": "searchable_neurons",
  "libraries": [
    "FlyEM_Hemibrain_v1.1"
  ],
  "searchInputName": "mask1417367048598452966.png",
  "searchInputFolder": "colorDepthTestData/test1",
  "dataThreshold": 100,
  "maskThreshold": 100,
  "pixColorFluctuation": 2,
  "xyShift": 2,
  "mirrorMask": true
}
```

Note that libraryAlignmentSpace and searchableMIPSFolder, if specified will apply to all libraries from the list.
It is also possible to simply invoke it without specifying the libraryAlignmentSpace and/or the searchableMIPFolder
as below:
```
{
  "libraries": [
    "JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.1/searchable_neurons"
  ],
  "searchInputName": "mask3900813784977233932.png",
  "searchInputFolder": "colorDepthTestData/test2",
  "dataThreshold": 100,
  "maskThreshold": 100,
  "pixColorFluctuation": 2,
  "xyShift": 2,
  "mirrorMask": true
}
```

