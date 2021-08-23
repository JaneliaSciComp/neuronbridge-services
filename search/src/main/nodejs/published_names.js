import AWS from "aws-sdk";

const db = new AWS.DynamoDB.DocumentClient({
  maxRetries: 3,
  httpOptions: {
    timeout: 5000
  }
});

const itemLimit = process.env.ITEM_LIMIT || 20;

export const publishedNames = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {

    if (!event.queryStringParameters || !event.queryStringParameters.q) {
      return {
        isBase64Encoded: false,
        statusCode: 400,
        body: JSON.stringify({error: 'Missing query string. eg /published_names?q=1234'})
      };
    }

    // grab the search string from the URL query string
    const { q: query, f: filter } = event.queryStringParameters;
    // check that the query string is >= 3 characters (and not using wildcards?)
    if (query.length < 3) {
      return {
        isBase64Encoded: false,
        statusCode: 400,
        body: JSON.stringify({error: 'Query string must be longer than 3 characters.'})
      };
    }

    // This allows us to change the strategy that we use to search the
    // dynamoDB table, either with contains or begins_with. Begins with is more
    // appropriate for autcomplete requests.
    const filterExpression = (filter === 'start') ? "begins_with(searchKey, :key)" : "contains(searchKey, :key)";

    // query the dynamoDB table for published names using the query string.
    const params = {
      TableName: process.env.NAMES_TABLE,
      FilterExpression: filterExpression,
      ExpressionAttributeValues: {
        ":key": query.toLowerCase()
      },
      ReturnConsumedCapacity: 'TOTAL'
    };

    let lastEvaluatedKey;
    const foundItems = [];

    do {
      const data = await db.scan(params).promise();
      console.log({ConsumedCapacity: data.ConsumedCapacity, lastEvaluatedKey, params});
      data.Items.forEach(item => foundItems.push(item));
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      lastEvaluatedKey = data.LastEvaluatedKey;
    } while (typeof lastEvaluatedKey !== "undefined");

    // return the top n entries
    returnBody.names = foundItems.slice(0,itemLimit);

  } catch (error) {
    console.log(`Error: ${error}`);
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
