# neuronbridge-services
AWS Services for NeuronBridge

## Deployment
As a prerequisite, you need to have the [AWS CLI](https://aws.amazon.com/cli/) installed and configured with credentials.

This command will compile and deploy the code to AWS:
```./deploy.sh```



## Tailing the logs
sls logs -f search -t
sls logs -f parallelSearch -t



# Generating Color Depths MIPS.

Color depth MIPS are generated using AWS batch service which takes an imnage uploaded by the user, aligns it using CMTK and generates color depth MIPS that can be masked and searched against a list of selected color depth libraries.

AWS batch service requires a compute environment, a job queue and a mechanism to submit jobs to the queue. The compute environment and the job queue(s) need to be generated only once. Jobs will be submitted to the queue when the user uploads data that need to be searched to S3.

The generation of the batch environment is based on the [AWS batch genomics sample blog](https://aws.amazon.com/blogs/compute/building-high-throughput-genomics-batch-workflows-on-aws-introduction-part-1-of-4/)

## Requirements

The setup of the batch environment requires aws-cli. On MacOS you can install this using: 

`brew install aws-sdk`

## Creating the batch environment

There are two basic configurations - one that uses an EC2 provisioned instance (compute-env-ec2.json) and one that uses a SPOT provisioned instance (conpute-env-spot.json). SPOT provisioned instances requires a role that has permissions to bid for spot instances - 'arn:aws:iam::777794738451:role/AmazonEC2SpotFleetRole' in our case

```
compute-env-ec2.json:

{
    "computeEnvironmentName": "compute-alignment-dev",
    "type": "MANAGED",
    "state": "ENABLED",
    "computeResources": {
        "type": "EC2",
        "allocationStrategy": "BEST_FIT_PROGRESSIVE",
        "minvCpus": 0,
        "desiredvCpus": 0,
        "maxvCpus": 64,
        "instanceTypes": [
            "optimal"
        ],
        "imageId": "ami-0b99f97b0f8e3252b",
        "subnets": [
            "subnet-5e11c814"
        ],
        "securityGroupIds": [
            "sg-963ee8df"
        ],
        "ec2KeyPair": "ec2_batch",
        "instanceRole": "arn:aws:iam::777794738451:instance-profile/ecsInstanceRole",
        "tags": {
            "Name": "align_compute",
            "Project": "CDCS",
            "Stage": "dev"
        }
    },
    "serviceRole": "arn:aws:iam::777794738451:role/service-role/AWSBatchServiceRole"
}

```

```
compute-env-spot.json:

{
    "computeEnvironmentName": "compute-alignment-spot-dev",
    "type": "MANAGED",
    "state": "ENABLED",
    "computeResources": {
        "type": "SPOT",
        "bidPercentage": 50,
        "spotIamFleetRole": "arn:aws:iam::777794738451:role/AmazonEC2SpotFleetRole",
        "allocationStrategy": "BEST_FIT_PROGRESSIVE",
        "minvCpus": 0,
        "desiredvCpus": 0,
        "maxvCpus": 64,
        "instanceTypes": [
            "optimal"
        ],
        "imageId": "ami-0b99f97b0f8e3252b",
        "subnets": [
            "subnet-5e11c814"
        ],
        "securityGroupIds": [
            "sg-963ee8df"
        ],
        "ec2KeyPair": "ec2_batch",
        "instanceRole": "arn:aws:iam::777794738451:instance-profile/ecsInstanceRole",
        "tags": {
            "Name": "align_compute",
            "Project": "CDCS",
            "Stage": "dev"
        }
    },
    "serviceRole": "arn:aws:iam::777794738451:role/service-role/AWSBatchServiceRole"
}
```

Notice that in the above configuration we use a custom AWS image that has EC2 installed as well as volumes mounted in a way that the batch job expects them.

To create the AMI you can start with an [Amazon ECS-optimized Amazon Linux AMI](https://aws.amazon.com/marketplace/search/results?x=0&y=0&searchTerms=Amazon+ECS-Optimized+Amazon+Linux+AMI&page=1&ref_=nav_search_box). After you start the EC2 instance run the commands from automount-scratch.sh and then save an image from the EC2 instance:
```
sudo yum -y update
sudo yum install -y fuse-devel
sudo mkfs -t ext4 /dev/xvdb
sudo mkdir /scratch_volume
sudo echo -e '/dev/xvdb\t/scratch_volume\text4\tdefaults\t0\t0' | sudo tee -a /etc/fstab
sudo mount –a
sudo stop ecs
sudo rm -rf /var/lib/ecs/data/ecs_agent_data.json
```


To create the batch environment use aws-cli with one of the above configuration as bellow:

`aws batch create-compute-environment --cli-input-json file://compute-env-ec2.json`
or 
`aws batch create-compute-environment --cli-input-json file://compute-env-spot.json`


## Creating the batch queue

An aws batch queue is typically associated with one compute environment. One possibility is to create a queue with a lower priority that uses spot instances and one with a higher priority that use a dedicated EC2 instance.

To create the queue simply run:

`aws batch create-job-queue --job-queue-name align-spot-dev --compute-environment-order order=0,computeEnvironment=compute-alignment-spot-dev  --priority 100 --state ENABLED`

## Triggering the batch job.

A color depth MIPs job is triggerred by the user uploading two files to the inputs bucket: a 3D image file (TIFF, LSM, VAA3D) and a metadata file with the same name but a .json extension. The upload of the metadata file triggers an AWS lambda which extracts the job information and submits it to the queuue. The triggering lambda is deployed using serverless framework:

`sls deploy`
