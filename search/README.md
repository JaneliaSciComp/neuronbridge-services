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

## Unit testing

Simply run:
```
npm test
```



