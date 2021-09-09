import { getQueryParams } from "../../main/nodejs/published_names";

describe('testing query parameter generation', () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    jest.resetModules(); // Most important - it clears the cache
    process.env = { ...OLD_ENV, NAMES_TABLE: 'published-test', ...OLD_ENV }; // Make a copy
  });

  afterAll(() => {
    process.env = OLD_ENV; // Restore old environment
  });

  test('default search no wild cards', () => {
    expect(getQueryParams('test')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType and searchKey = :search",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "test"
      },
    });
  });

  test('default search with leading wild card', () => {
    expect(getQueryParams('*test')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":postfix": "test"
      },
      FilterExpression: "contains(filterKey, :postfix)"
    });
  });

  test('default search with trailing wild card', () => {
    expect(getQueryParams('test*')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType and begins_with(searchKey, :search)",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "test"
      }
    });
  });

  test('default search with wild card at start, middle and end', () => {
    expect(getQueryParams('*te*st*')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":term0": "te",
        ":term1": "st"
      },
      FilterExpression: "contains(filterKey, :term0) and contains(filterKey, :term1)"
    });
  });


  test('default search with leading & trailing wild card', () => {
    expect(getQueryParams('*test*')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "test"
      },
      FilterExpression: "contains(filterKey, :search)"
    });
  });

  test('default search with wild card in the middle', () => {
    expect(getQueryParams('te*st')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType and begins_with(searchKey, :search)",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "te",
        ":postfix": "st",
      },
      FilterExpression: "contains(filterKey, :postfix)"
    });
  });


  test('autocomplete search no wild cards', () => {
    expect(getQueryParams('test', 'start')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType and begins_with(searchKey, :search)",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "test"
      },
    });
  });

  test('autocomplete search with trailing wild card', () => {
    expect(getQueryParams('test*', 'start')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType and begins_with(searchKey, :search)",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "test"
      },
    });
  });

  test('autocomplete search with leading wild card', () => {
    expect(getQueryParams('*test', 'start')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":postfix": "test"
      },
      FilterExpression: "contains(filterKey, :postfix)"
    });
  });

  test('autocomplete search with leading & trailing wild cards', () => {
    expect(getQueryParams('*test*', 'start')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "test"
      },
      FilterExpression: "contains(filterKey, :search)"
    });
  });

  test('autocomplete search with wild card in the middle', () => {
    expect(getQueryParams('te*st', 'start')).toStrictEqual({
      TableName: "published-test",
      ReturnConsumedCapacity: 'TOTAL',
      KeyConditionExpression: "itemType = :itemType and begins_with(searchKey, :search)",
      ExpressionAttributeValues: {
        ":itemType": "searchString",
        ":search": "te",
        ":postfix": "st"
      },
      FilterExpression: "contains(filterKey, :postfix)"
    });
  });


});


