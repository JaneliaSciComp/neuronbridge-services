# Metadata format examples

These are examples of how metadata is formatted on S3 for use by the NeuronBridge website and other clients of the NeuronBridge API. 

## Metadata for an LM line name

s3://janelia-neuronbridge-data-prod/by_line/JHS_K_85321.json
```javascript
{
    results: [
        {
            id: "2711777432590876683",
            publishedName: "LH1046",
            sampleRef: "Sample#2146798879260016738",
            imagePath: "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/LH1046-20150508_43_E4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.png",
            thumbnailPath: "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/LH1046-20150508_43_E4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.jpg",
            slideCode: "20150508_43_E4",
            gender: "f",
            mountingProtocol: "DPX PBS Mounting",
            anatomicalArea: "Brain",
            alignmentSpace: "JRC2018_Unisex_20x_HR",
            objective: "20x",
            library: "flylight_splitgal4_drivers",
            channel: "1"
            
        },
    ]
}
```

To construct the full path to an image, one needs to take the prefix from the constants file and prepend it to the URL, e.g.
https://color-depth-mips.s3.us-east-1.amazonaws.com/JRC2018_Unisex_20x_HR/flylight_splitgal4_drivers/JHS_K_85321-20141222_80_A3-f-20x-brain-JRC2018_Unisex_20x_HR-color_depth_1.png

## Metadata for an EM skeleton

s3://janelia-neuronbridge-data-prod/by_body/360677632.json
```javascript
{
    results: [
        {
            id: "2757945518448443403",
            library: "flyem_hemibrain"
            publishedName: "1001453586",
            imagePath: "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1001453586-RT-JRC2018_Unisex_20x_HR-CDM.png",
            thumbnailPath: "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1001453586-RT-JRC2018_Unisex_20x_HR-CDM.jpg",
        }
    ]
}
```

## Precomputed matches (EM->LM or LM->EM)

The same metadata that's available above is also denormalized in the match files for rapid access.

s3://janelia-neuronbridge-data-prod/cdsresults/2711777432142086155.json
```javascript
{
    results: [
        {
            {
                id: "2711776846424309771",
                publishedName: "R50G08",
                libraryName: "flylight_gen1_mcfo_case_1",
                matchedId: "2757945572890509323",
                matchedImageName: "5813021675_L_18U.tif",
                normalizedScore: 26724.11541976759,
                image_path: "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/5813021675-L-JRC2018_Unisex_20x_HR-CDM.png",
                thumbnail_path: "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/5813021675-L-JRC2018_Unisex_20x_HR-CDM.jpg",
                attrs: {
                    "Body Id": "5813021675",
                    "Library": "flyem_hemibrain",
                    "Matched pixels": "56",
                    "Score": "0.1009009009009009",
                    "ArtificialShapeScore": "26724.11541976759"
                }
            },
        }
        
    ]
}
```

https://janelia-neuronbridge-data-prod.s3.amazonaws.com/metadata/cdsresults/2757945537360560139.json
```javascript
{
    results: [
        {
            {
                id: "2757945537360560139",
                matchedId: "2711777430204317707",
                imagePath: "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/MB043B-20121212_32_A4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.png",
                thumbnailPath: "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/MB043B-20121212_32_A4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.jpg",
                slideCode: "20121212_32_A4",
                publishedName: "MB043B",
                gender: "f",
                mountingProtocol: "DPX PBS Mounting",
                anatomicalArea: "Brain",
                alignmentSpace: "JRC2018_Unisex_20x_HR",
                objective: "20x",
                library: "flylight_splitgal4_drivers",
                channel: "1",
                score: "50000"
            },
        }
    ]
}
```