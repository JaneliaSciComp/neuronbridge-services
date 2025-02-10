import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

// https://stackoverflow.com/questions/3446170/escape-string-for-use-in-javascript-regex
// I removed '*' from the original answer as I want that to be replaced later with .*
function escapeRegExp(string) {
  return string.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
}

export function getQueryParams(query, filter) {

  // set the defaults for the query
  const params = {
    TableName: process.env.CURATED_TABLE,
    KeyConditionExpression: "entryType = :entryType",
    ExpressionAttributeValues: {
      ":entryType": "searchString",
    },
    ReturnConsumedCapacity: 'TOTAL'
  };

  // if query string ends with or contains a wildcard, then use the string
  // before the '*' to perform the initial search. Then filter the results
  // with the full query string. This allows us to use the begins_with
  // query and not need a scan.
  //
  //
  // aws dynamodb query --region us-east-1 --table-name published-test \
  // --key-condition-expression "entryType = :entryType and begins_with(searchKey, :search)" \
  // --filter-expression "contains(filterKey, :postfix) and contains(filterKey, :prefix)" \
  // --expression-attribute-values '{":entryType":{"S": "searchString"}, ":search":{"S":"r22"}, ":postfix": {"S": "1"}, ":prefix": {"S": "2"}}'
  //
  // if the querystring starts with a wildcard, then we have to scan using
  // the 'contains' function. This is way less efficient, but not sure how
  // else to do it. Once we have all the scanned results, we can filter them
  // with the full wild card string.

  if (!query.match(/\*/)) {
    // if query string has no '*', then exact match
    if (filter === 'start') {
      // if query string has no '*', then still look for begins with
      // to make autocomplete work
      params.KeyConditionExpression = "entryType = :entryType and begins_with(searchKey, :search)";
      params.ExpressionAttributeValues[':search'] = query.toLowerCase();
    } else {
      params.KeyConditionExpression = "entryType = :entryType and searchKey = :search";
      params.ExpressionAttributeValues[':search'] = query.toLowerCase();
    }
  } else if (query.match(/^\*[^*]*$/)) {
    // if query string contains only a wild card at the start
    params.ExpressionAttributeValues[':postfix'] = query.replace('*','').toLowerCase();
    params.FilterExpression = "contains(filterKey, :postfix)";
  } else if (query.match(/^[^*]*\*$/)) {
    // if query string contains only a wild card at the end
    params.ExpressionAttributeValues[':search'] = query.split('*')[0].toLowerCase();
    params.KeyConditionExpression = "entryType = :entryType and begins_with(searchKey, :search)";
  } else if(query.match(/^\*[^*]*\*$/)) {
    // if query string starts and ends with a wild card
    params.ExpressionAttributeValues[':search'] = query.replace(/\*/g,'').toLowerCase();
    params.FilterExpression = "contains(filterKey, :search)";
  } else if (query.match(/^[^*]*\*[^*]*$/)) {
    // if query string has wild card in the middle.
    params.ExpressionAttributeValues[':search'] = query.split('*')[0].toLowerCase();
    params.KeyConditionExpression = "entryType = :entryType and begins_with(searchKey, :search)";
    params.ExpressionAttributeValues[':postfix'] = query.split('*')[1].toLowerCase();
    params.FilterExpression = "contains(filterKey, :postfix)";
  } else if (query.match(/^\*[^*]+\*.+$/)) {
    // query string has a wild card at start, middle, and maybe at
    // the end.
    const searchTerms = query.toLowerCase().split('*');
    params.FilterExpression = searchTerms.filter(term => term !== '').map((term, count) => {
      params.ExpressionAttributeValues[`:term${count}`] = term;
      return `contains(filterKey, :term${count})`;
    }).join(' and ');
  }

  return params;

}

const dbClient = DynamoDBDocumentClient.from(new DynamoDBClient({
  maxRetries: 3,
  httpOptions: {
    timeout: 5000
  }
}));

export const curatedMatches = async event => {
  const returnObj = {
    isBase64Encoded: false,
    statusCode: 200,
    body: JSON.stringify({})
  };
  let returnBody = {};

  try {
    if (event.queryStringParameters && event.queryStringParameters.version) {
      return {
        isBase64Encoded: false,
        statusCode: 200,
        body: JSON.stringify({version: process.env.CURATED_TABLE})
      };
    }

    if (!event.queryStringParameters || !event.queryStringParameters.q) {
      return {
        isBase64Encoded: false,
        statusCode: 400,
        body: JSON.stringify({error: 'Missing query string. eg /curated_matches?q=1234'})
      };
    }

    // grab the search string from the URL query string
    const { q: query, f: filter } = event.queryStringParameters;
    // check that the query string is >= 3 characters (and not using wildcards?)
    if (query.length < 2) {
      return {
        isBase64Encoded: false,
        statusCode: 400,
        body: JSON.stringify({error: 'Query string must be longer than 2 characters.'})
      };
    }

    if (query.match(/\*(\*|\.)\*/)) {
      return {
        isBase64Encoded: false,
        statusCode: 400,
        body: JSON.stringify({error: 'Ha ha, nice try, Query string must not be all wildcards.'})
      };
    }

    // query the dynamoDB table for curated matches using the query string.
    const queryParams = getQueryParams(query, filter);

    let lastEvaluatedKey;
    const foundItems = [];

    do {
      const data = await dbClient.send(new QueryCommand(queryParams));
      console.log({ConsumedCapacity: data.ConsumedCapacity, lastEvaluatedKey});
      data.Items.forEach(item => foundItems.push(item));
      queryParams.ExclusiveStartKey = data.LastEvaluatedKey;
      lastEvaluatedKey = data.LastEvaluatedKey;
    } while (typeof lastEvaluatedKey !== "undefined");

    console.log(`Found ${foundItems.length} matches`);

    let matched = [];
    if (filter === 'start' && !query.match(/\*/)) {
      matched = foundItems;
    }
    else {
      // use the original search term to filter the returned results.
      const escapedQuery = escapeRegExp(query);
      const match = new RegExp(`^${escapedQuery.replace(/\*/g, ".*")}$`, "i");
      matched = foundItems.filter(item => {
        return item.name.match(match);
      });
    }

    console.log(`Returning ${matched.length} matches after filtering`);

    returnBody.matches = matched;
    returnBody.params = queryParams;

  } catch (error) {
    console.log(`Error: ${error}`);
    returnObj.statusCode = 500;
    returnBody.message = error.message;
  }

  returnObj.body = JSON.stringify(returnBody);

  return returnObj;
};
