const AWS = require("aws-sdk");
const db = new AWS.DynamoDB.DocumentClient();

//fetch data from original dynamdb table
async function fetchData() {
  const params = {
    TableName: "janelia-neuronbridge-search-table-prod",
    FilterExpression: "#owner = :owner",
    ExpressionAttributeValues: {
      ":owner": "3a3bf91e-d063-4e3f-8cb8-aa6f07dc1c71"
    },
    ExpressionAttributeNames: {
      "#owner": "owner"
    }
  };

  try {
    const data = await db.scan(params).promise();
    return data;
  } catch (err) {
    return err;
  }
}
// save data to new dynamodb table
async function saveNewItem(item) {
  const params = {
    TableName: "janelia-neuronbridge-search-table-dev",
    Item: item
  };

  try {
    await db.put(params).promise();
  } catch (err) {
    console.log(err);
    return err;
  }
}

exports.handler = async event => {
  const originalData = await fetchData();
  // return originalData;

  // obtain new owner id
  // obtain new identityId

  // foreach data item, change the owner value and the identityId to the new
  // values and store in the new dynamoDB table.
  const updatedRecords = [];
  for (let record of originalData.Items) {
    const updatedRecord = { ...record, owner: "foo", identityId: "foo" };
    updatedRecords.push(updatedRecord);
    console.log("before save");
    try {
      await saveNewItem(updatedRecord);
    } catch (err) {
      console.log(err);
    }
    console.log("after save");
  }

  return updatedRecords;
};
