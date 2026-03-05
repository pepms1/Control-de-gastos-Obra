import assert from 'node:assert/strict';
import { normalizeTransaction } from './api.js';

const withManual = normalizeTransaction({
  categoryHintName: 'SAP Materiales',
  categoryHintCode: '5100',
  categoryManualName: 'Manual Materiales',
  categoryManualCode: 'MAN-01',
});

assert.equal(withManual.categoryEffectiveName, 'Manual Materiales');
assert.equal(withManual.categoryEffectiveCode, 'MAN-01');

const withHintOnly = normalizeTransaction({
  category_hint_name: 'SAP Servicios',
  category_hint_code: '5200',
});

assert.equal(withHintOnly.categoryEffectiveName, 'SAP Servicios');
assert.equal(withHintOnly.categoryEffectiveCode, '5200');
console.log('ok');
