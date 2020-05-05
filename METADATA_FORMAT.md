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
            libraryName: "flylight_splitgal4_drivers",
            internalName: "JRC_IS16786-20150508_43_E4-20x-Brain-JRC2018_Unisex_20x_HR-2146798879260016738-CH1_CDM.png",
            line: "JRC_IS16786",
            sampleRef: "Sample#2146798879260016738",
            image_path: "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/LH1046-20150508_43_E4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.png",
            thumbnail_path: "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/LH1046-20150508_43_E4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.jpg",
            attrs: {
                "Slide Code": "20150508_43_E4",
                "Published Name": "LH1046",
                "Gender": "f",
                "Genotype": "41G09-ZpGDBD in attP2",
                "Mounting Protocol": "DPX PBS Mounting",
                "Anatomical Area": "Brain",
                "Alignment Space": "JRC2018_Unisex_20x_HR",
                "Objective": "20x",
                "Library": "flylight_splitgal4_drivers",
                "Channel": "1"
            }

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
            publishedName: "1001453586",
            libraryName: "flyem_hemibrain",
            internalName: "1001453586_RT_18U.tif",
            line: null,
            sampleRef: null,
            image_path: "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1001453586-RT-JRC2018_Unisex_20x_HR-CDM.png",
            thumbnail_path: "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyEM_Hemibrain_v1.0/1001453586-RT-JRC2018_Unisex_20x_HR-CDM.jpg",
            attrs: {
                "Body Id": "1001453586",
                "Library": "flyem_hemibrain"
            }
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
                publishedName: "1077847238",
                libraryName: "flyem_hemibrain",
                matchedId: "2711777430204317707",
                matchedImageName: "GMR_MB043B-20121212_32_A4-20x-Brain-JRC2018_Unisex_20x_HR-1858873382750126178-CH1_CDM.png",
                normalizedScore: 50000.0,
                image_path: "https://s3.amazonaws.com/janelia-flylight-color-depth/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/MB043B-20121212_32_A4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.png",
                thumbnail_path: "https://s3.amazonaws.com/janelia-flylight-color-depth-thumbnails/JRC2018_Unisex_20x_HR/FlyLight_Split-GAL4_Drivers/MB043B-20121212_32_A4-Split_GAL4-f-20x-brain-JRC2018_Unisex_20x_HR-CDM_1.jpg",
                attrs: {
                    "Slide Code": "20121212_32_A4",
                    "Published Name": "MB043B",
                    "Gender": "f",
                    "Genotype": "GMR_MB043B MB043B",
                    "Mounting Protocol": "DPX PBS Mounting",
                    "Anatomical Area": "Brain",
                    "Alignment Space": "JRC2018_Unisex_20x_HR",
                    "Objective": "20x",
                    "Library": "flylight_splitgal4_drivers",
                    "Channel": "1",
                    "PublishedName": "MB043B",
                    "Matched pixels": "561",
                    "Score": "0.1346938775510204",
                    "ArtificialShapeScore": "50000.0"
                }
            },
        }
    ]
}
```