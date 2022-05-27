# NeuronBridge backend services

[![DOI](https://zenodo.org/badge/260239328.svg)](https://zenodo.org/badge/latestdoi/260239328)
[![CircleCI](https://circleci.com/gh/JaneliaSciComp/neuronbridge-services.svg?style=svg)](https://circleci.com/gh/JaneliaSciComp/neuronbridge-services)

## Deployment

As a prerequisite, you need to have the [AWS CLI](https://aws.amazon.com/cli/) installed and configured with proper AWS credentials.

### Deploy NeuronBridge compute alignment stack

NeuronBridge compute alignment requires an AMI instance preconfigured with ECS and with all required volumes mounted as expected by the alignment batch job.

To create the AMI use these steps:
* start an [Amazon ECS-optimized Amazon Linux AMI](https://aws.amazon.com/marketplace/search/results?x=0&y=0&searchTerms=Amazon+ECS-Optimized+Amazon+Linux+AMI&page=1&ref_=nav_search_box).

* start the EC2 instance
* run the following commands that mount the expected volumes:

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
* save an image from the running EC2 instance

Once the AMI instance ID is available make sure you set the proper AMI instance in align/serverless.yml.
To deploy:
```
cd align
npm install
npm run sls -- deploy -s dev
```

The command above will create the compute environment, the job definition and the job queue.

### Deploy NeuronBridge¸ color depth search stack

Make sure you have built all the java packages with jdk 1.8 and maven:

```
mvnw clean package
```


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
