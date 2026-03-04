import assert from 'node:assert/strict';
import { dedupeCategories, dedupeSupplierOptions, normalizeOptionLabel } from './dropdownOptions.js';

assert.equal(normalizeOptionLabel('  Proveedor   Uno '), 'Proveedor Uno');

const categories = dedupeCategories([
  { _id: 'cat-1', name: '  Materiales  Varios ' },
  { _id: 'cat-1', name: 'materiales varios' },
]);
assert.equal(categories.length, 1, 'Categories should dedupe by stable _id');
assert.equal(categories[0].name, 'Materiales Varios', 'Category labels should be normalized');

const suppliers = dedupeSupplierOptions([
  { value: 'C200', label: '  ACME   SA ' },
  { value: 'C200', label: 'acme sa' },
]);
assert.equal(suppliers.length, 1, 'Suppliers should dedupe by stable value first');

console.log('dropdownOptions mock tests passed');
