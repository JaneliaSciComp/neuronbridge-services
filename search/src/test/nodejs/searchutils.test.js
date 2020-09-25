const searchutils = require('../../main/nodejs/searchutils');

test('getSearchKey', () => {
  expect(searchutils.getSearchKey('/path', '.png')).toBe('/path.png');
  expect(searchutils.getSearchKey('/path', '')).toBe('/path');
});



