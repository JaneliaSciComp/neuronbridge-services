# Metadata format examples

These are examples of how metadata is formatted on S3 for use by the NeuronBridge website and other clients of the Color Depth Cloud API. 

## Constants

This is a static configuration file which defines constants used through-out the results so that they can be easily updated. It also allows us to minimize the size of the other files by extracting long constants. 

s3://color-depth-metadata/params.json
```javascript
{
    image_url_prefix: "https://color-depth-mips.s3.us-east-1.amazonaws.com/JRC2018_Unisex_20x_HR/",
    thumbnail_url_prefix: "https://color-depth-thumbnails.s3.us-east-1.amazonaws.com/JRC2018_Unisex_20x_HR/",
    default_line_name: "SS12345",
    default_body_id: "360677632"

}
```

## Metadata for an LM line name

s3://color-depth-metadata/by_line/JHS_K_85321.json
```javascript
{
    results: [
        {
            id: "2711777432142086155",
            image_path: "flylight_splitgal4_drivers/JHS_K_85321-20141222_80_A3-f-20x-brain-JRC2018_Unisex_20x_HR-color_depth_1.png",
            thumbnail_path: "flylight_splitgal4_drivers/JHS_K_85321-20141222_80_A3-f-20x-brain-JRC2018_Unisex_20x_HR-color_depth_1_sm.png",
            attrs: {
                "Line": "JHS_K_85321",
                "Slide Code": "20141222_80_A3",
                "Library": "FlyLight SplitGAL4 Drivers",
                "Channel": "1"
            }
        },
        {
            id: "..."
        }
    ]
}
```

To construct the full path to an image, one needs to take the prefix from the constants file and prepend it to the URL, e.g.
https://color-depth-mips.s3.us-east-1.amazonaws.com/JRC2018_Unisex_20x_HR/flylight_splitgal4_drivers/JHS_K_85321-20141222_80_A3-f-20x-brain-JRC2018_Unisex_20x_HR-color_depth_1.png

## Metadata for an EM skeleton

s3://color-depth-metadata/by_body/360677632.json
```javascript
{
    results: [
        {
            id: "2757945442397323275",
            image_path: "flyem_hemibrain_v1/360677632_RT_18U.png",
            thumbnail_path: "flyem_hemibrain_v1/360677632_RT_18U_sm.png",
            attrs: {
                "Body Id": "360677632",
                "Library": "FlyEM Hemibrain v1.0",
            }
        },
        {
            id: "..."
        }
    ]
}
```

## Precomputed matches (EM->LM or LM->EM)

The same metadata that's available above is also denormalized in the match files for rapid access.

s3://color-depth-metadata/precomputed_emlm_matches/2711777432142086155.json
```javascript
{
    results: [
        {
            id: "2757945442397323275",
            image_path: "flyem_hemibrain_v1/360677632_RT_18U.png",
            thumbnail_path: "flyem_hemibrain_v1/360677632_RT_18U_sm.png",
            attrs: {
                "Body Id": "360677632",
                "Library": "FlyEM Hemibrain v1.0",
                "Score": "34"
            }
        },
        {
            id: "..."
        }
    ]
}
```

