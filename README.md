# NeuronBridge backend services

[![DOI](https://zenodo.org/badge/260239328.svg)](https://zenodo.org/badge/latestdoi/260239328)
[![CircleCI](https://circleci.com/gh/JaneliaSciComp/neuronbridge-services.svg?style=svg)](https://circleci.com/gh/JaneliaSciComp/neuronbridge-services)

## Deployment

As a prerequisite, you need to have the [AWS CLI](https://aws.amazon.com/cli/) installed and configured with proper AWS credentials.

### Deploy NeuronBridge shared VPC
The VPC deployment is a one time deployment for ALL deploy environments, no matter whether they are DEV, PROD, TEST, VAL and so on. This will create a VPC that will be shared by all alignment jobs so be careful not to remove it because if you do you will have to redeploy all align environments.
To deploy:
```
cd vpc
npm install
npm run sls -- deploy
```
Notice that stage is not needed because it will deploy to a 'shared' stage ('janelia-neuronbridge-vpc-shared')

### Deploy Burst Compute framework

First, follow the instructions at [JaneliaSciComp/burst-compute](https://github.com/JaneliaSciComp/burst-compute) to deploy the framework. 

### Deploy Alignment Service

The NeuronBridge alignment service requires an AMI instance preconfigured with ECS and with all required volumes mounted as expected by the alignment batch job.

To create the AMI use these steps:
* Start an [Amazon ECS-optimized Amazon Linux AMI](https://aws.amazon.com/marketplace/search/results?x=0&y=0&searchTerms=Amazon+ECS-Optimized+Amazon+Linux+AMI&page=1&ref_=nav_search_box).
* When you launch the EC2 instance for creating the AMI you can use a small or even a micro instance,
but if the EBS volume is relatively small ~10G, attach an additional EBS volume of about 30GB.
* For generating the AMI the VPC, the security group and the selected key pair should allow you to ssh into the EC2 instance.
* Once the EC2 instance is up run the following commands that mount the expected volumes:

```
sudo yum -y update
sudo yum install -y fuse-devel
sudo mkdir /scratch_volume
```
If you attached to volumes to the EC2 instance use the second volume as scratch. In order to do that run:
```
sudo mkfs -t ext4 /dev/xvdb
sudo echo -e '/dev/xvdb\t/scratch_volume\text4\tdefaults\t0\t0' | sudo tee -a /etc/fstab
sudo mount –a
```
* Now the scratch volume is ready so stop ecs and clear the ecs_data:
```
sudo stop ecs
sudo rm -rf /var/lib/ecs/data/ecs_agent_data.json
```
* Then save an image from the running EC2 instance. Tag the AMI instance with the snapshot together. The tags that we
typically use are:

PROJECT=NeuronBridge
DEVELOPER=<username>
STAGE=prod

* Once the AMI instance ID is available the EC2 instance is no longer needed so you can terminate it.

* Use the new AMI instance ID in align/serverless.yml.

To deploy:
```
cd align
npm install
npm run sls -- deploy -s dev
```

The command above will create the compute environment, the job definition and the job queue.

### Deploy Color Depth Search Service

In order to create the color depth search lambdas run:

```
cd search
npm install
npm run sls -- deploy -s dev
```

To deploy with different search limits:
```
PER_DAY_SEARCH_LIMITS=2 CONCURRENT_SEARCH_LIMITS=2 npm run sls -- deploy -s cgdev
```
Note: a negative value for a limit means unlimited.

To update a single function, once you have a deployed stack

```
npm run sls -- deploy function -f <function_name> -s dev
```

### Alignment Parameters
* force_voxel_size - if true it uses resolution parameters provided by the user
* xy_resolution
* z_resolution
* reference_channel
* number of slots (not exposed to the user)


### Color Depth Search Parameters:

* dataThreshold - default: 100
* maskThreshold - default: 100
* pixColorFluctuation (zSliceRange) - default: 2
* xyShift - default: 2
* mirrorMask - default: true
* minMatchingPixRatio - default: 2
