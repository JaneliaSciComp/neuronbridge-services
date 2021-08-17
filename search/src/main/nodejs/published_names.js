import AWS from "aws-sdk";

const db = new AWS.DynamoDB.DocumentClient();

const itemLimit = process.env.ITEM_LIMIT || 20;

export const publishedNames = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {

    // grab the search string from the URL query string
    const { q: query } = event.queryStringParameters;
    // check that the query string is >= 3 characters (and not using wildcards?)
    if (query.length < 3) {
      return {
        isBase64Encoded: false,
        statusCode: 400,
        body: JSON.stringify({error: 'Query string must be longer than 3 characters.'})
      };
    }

    // query the dynamoDB table for published names using the query string.
    const params = {
      TableName: process.env.NAMES_TABLE,
      FilterExpression: "contains(#key, :key)",
      ExpressionAttributeValues: {
        ":key": query
      },
      ExpressionAttributeNames: {
        "#key": "key"
      }
    };

    let lastEvaluatedKey;
    const foundItems = [];

    do {
      const data = await db.scan(params).promise();
      data.Items.forEach(item => foundItems.push(item));
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      lastEvaluatedKey = data.LastEvaluatedKey;
    } while (typeof lastEvaluatedKey !== "undefined");

    // return the top n entries
    returnBody.names = foundItems.slice(0,itemLimit);

  } catch (error) {
    console.log(error);
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
