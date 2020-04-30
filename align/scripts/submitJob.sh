aws batch submit-job \
    --job-name testalign \
    --job-queue align-spot-dev \
    --job-definition AlignStackAndGenerateMIPs:14 \
    --container-overrides "vcpus=16,memory=8192" \
    --parameters '{
            "gender": "f",
            "area": "Brain",
            "shape": "Unknown",
            "objective": "20x",
            "mounting_protocol": "DPX Ethanol Mounting",
            "image_size": "1024x1024x125",
            "voxel_size": "0.92x0.92x1.00",
			"nchannels": "2",
            "reference_channel": "2",
            "templates_bucket": "janelia.hhmi.org-templates-dev",
            "inputs_bucket": "janelia.hhmi.org-unalignedstacks-dev",
            "outputs_bucket": "janelia.hhmi.org-alignedmips-dev",
            "input_filename": "/20x/s1/merge/tile-2614294591452483624.v3draw",
            "output_folder": "/20x/s1/align",
            "nslots": "16",
            "iam_role": "auto",
            "debug_flag": "true"
    }' \
    --timeout "attemptDurationSeconds=15000"
