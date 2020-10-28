import { getSearchKey, getSearchMaskId, getIntermediateSearchResultsPrefix } from '../../main/nodejs/searchutils';

test('getSearchKey', () => {
  expect(getSearchKey('/path', '.png')).toBe('/path.png');
  expect(getSearchKey('/path', '')).toBe('/path');
});

test('getSearchMaskId', () => {
  expect(getSearchMaskId('path.png')).toBe('path');
  expect(getSearchMaskId('folder/path.png')).toBe('path');
});

test('getSearchSubFolder', () => {
  expect(getIntermediateSearchResultsPrefix('folder/path')).toBe('folder/results');
  expect(getIntermediateSearchResultsPrefix('folder/path.png')).toBe('folder/results');
  expect(getIntermediateSearchResultsPrefix('path')).toBe('results');
  expect(getIntermediateSearchResultsPrefix('path.png')).toBe('results');
});
