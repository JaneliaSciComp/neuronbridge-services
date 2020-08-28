## Test Event for Color Depth Search Dispatch:

```
{
  "libraryAlignmentSpace": "JRC2018_Unisex_20x_HR",
  "searchableMIPSFolder": "searchable_neurons",
  "libraries": [
    "FlyEM_Hemibrain_v1.1"
  ],
  "searchInputName": "1110173824_TC_18U.tif",
  "searchInputFolder": "private/us-east-1:853b7e81-c739-4434-99dd-aafeed3265e3/64d12160-e8a0-11ea-afe0-0be3a81cc1a1",
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
  "searchInputName": "1110173824_TC_18U.tif",
  "searchInputFolder": "private/us-east-1:853b7e81-c739-4434-99dd-aafeed3265e3/64d12160-e8a0-11ea-afe0-0be3a81cc1a1",
  "dataThreshold": 100,
  "maskThreshold": 100,
  "pixColorFluctuation": 2,
  "xyShift": 2,
  "mirrorMask": true
}
```
