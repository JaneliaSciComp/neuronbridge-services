const searchutils = require('../../main/nodejs/searchutils');

test('getSearchKey', () => {
  expect(searchutils.getSearchKey('/path', '.png')).toBe('/path.png');
  expect(searchutils.getSearchKey('/path', '')).toBe('/path');
});

test('getSearchMaskId', () => {
  expect(searchutils.getSearchMaskId('path.png')).toBe('path');
  expect(searchutils.getSearchMaskId('folder/path.png')).toBe('path');
});

test('getSearchSubFolder', () => {
  expect(searchutils.getIntermediateSearchResultsPrefix('folder/path')).toBe('folder/results');
  expect(searchutils.getIntermediateSearchResultsPrefix('folder/path.png')).toBe('folder/results');
  expect(searchutils.getIntermediateSearchResultsPrefix('path')).toBe('results');
  expect(searchutils.getIntermediateSearchResultsPrefix('path.png')).toBe('results');
});
