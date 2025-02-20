
class CustomHooksPlugin {
    constructor(serverless, options) {
      this.serverless = serverless;
      this.options = options;
  
      this.hooks = {
        "before:deploy:deploy": this.afterDeploy.bind(this),
        "before:remove:remove": this.preventRemove.bind(this),
      };
    }
  
    afterDeploy() {
      this.serverless.cli.log("✅ Deployment Successful!");
    }
  
    preventRemove() {
      this.serverless.cli.log("❌ ERROR: Removing this stack is not allowed!");
      process.exit(1)
    }
  }
  
  module.exports = CustomHooksPlugin;
  