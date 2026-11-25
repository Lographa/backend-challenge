import { describe, expect, test } from 'bun:test';
import { canonicalHash, canonicalJson, PAYLOAD_HASH_PATTERN } from '../../src/common/hashing/canonical-hash';

describe('canonicalJson', () => {
  test('sorts keys recursively and drops undefined properties', () => {
    expect(canonicalJson({ b: 1, a: { d: [3, { z: true, y: null }], c: 'x' }, e: undefined })).toBe(
      '{"a":{"c":"x","d":[3,{"y":null,"z":true}]},"b":1}',
    );
  });

  test('keeps array order and turns undefined items into null', () => {
    expect(canonicalJson([2, undefined, 1])).toBe('[2,null,1]');
  });

  test.each([NaN, Infinity, -Infinity])('rejects the non-finite number %p', (value) => {
    expect(() => canonicalJson({ value })).toThrow(TypeError);
  });

  test.each([new Date(0), new Map(), 10n, () => 1, Symbol('s')])('rejects %p', (value) => {
    expect(() => canonicalJson({ value })).toThrow(TypeError);
  });
});

describe('canonicalHash', () => {
  test('is a lowercase SHA-256 hex digest', () => {
    expect(canonicalHash({ a: 1 })).toMatch(PAYLOAD_HASH_PATTERN);
  });

  test('does not depend on key order', () => {
    expect(canonicalHash({ amount: '10.00', currency: 'BRL' })).toBe(
      canonicalHash({ currency: 'BRL', amount: '10.00' }),
    );
  });

  test('changes with any value', () => {
    expect(canonicalHash({ amount: '10.00' })).not.toBe(canonicalHash({ amount: '10.01' }));
    expect(canonicalHash({ amount: '10.00' })).not.toBe(canonicalHash({ amount: '10' }));
  });
});
