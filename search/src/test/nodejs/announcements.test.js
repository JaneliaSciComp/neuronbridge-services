import { isUserAdmin } from "../../main/nodejs/announcements";

describe('testing user access', () => {
  test('call with no parameters should return false', () => {
    expect(isUserAdmin()).toBe(false);
  });
  test('call with a string should return false', () => {
    expect(isUserAdmin('test')).toBe(false);
  });
  test('call with an empty object should return false', () => {
    expect(isUserAdmin({})).toBe(false);
  });
  test('call with a token where user isn\'t admin should return false', () => {
    const token = {
      claims: {
        'cognito:groups': []
      }
    };
    expect(isUserAdmin(token)).toBe(false);
  });
  test('call with a token where cognito groups is missing should return false', () => {
    const token = {
      claims: {}
    };
    expect(isUserAdmin(token)).toBe(false);
  });
  test('call with a token where user is admin should return true', () => {
    const token = {
      claims: {
        'cognito:groups': ['neuronbridge-admins']
      }
    };
    expect(isUserAdmin(token)).toBe(true);
  });
  test('call with a token where user is admin and in other groups should return true', () => {
    const token = {
      claims: {
        'cognito:groups': ['neuronbridge-admins', 'general']
      }
    };
    expect(isUserAdmin(token)).toBe(true);
  });



});
