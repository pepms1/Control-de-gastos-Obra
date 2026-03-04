import assert from 'node:assert/strict';
import { dedupeCategories, dedupeSupplierOptions, normalizeOptionLabel } from './dropdownOptions.js';

assert.equal(normalizeOptionLabel('  Proveedor   Uno '), 'proveedor uno');

const categories = dedupeCategories([
  { id: '', name: '  Materiales  Varios ' },
  { id: null, name: 'materiales varios' },
]);
assert.equal(categories.length, 1, 'Categories should dedupe by normalized label when id is missing');

const suppliers = dedupeSupplierOptions([
  { value: 'C200', label: '  ACME   SA ' },
  { value: 'C200', label: 'acme sa' },
]);
assert.equal(suppliers.length, 1, 'Suppliers should dedupe by stable value first');

console.log('dropdownOptions mock tests passed');
