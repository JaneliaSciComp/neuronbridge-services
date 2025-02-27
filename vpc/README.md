## Deployment

Due to a limited number of available VPCs, the VPC is shared for all our environments: dev, prod, validation, etc. So be careful if you have to make changes and redeploy this.

To deploy simply run:
```
npm run sls -- deploy
```

To deploy a VPC instance for a specific environment run:
```
npm run sls -- deploy -s <env>
```

## Removing the VPC instance

Once deployed, the VPC instance does not have to be touched again, so under normal circumstances removing the default VPC instance `janelia-neuronbridge-vpc-shared` is not needed. Removing the default VPC instance  will invalidate align service in all NeuronBridge environments so only remove it, if it's really necessary.
