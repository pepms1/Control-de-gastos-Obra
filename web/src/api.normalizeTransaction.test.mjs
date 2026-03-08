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


const sapSbo = normalizeTransaction({
  source: 'sap-sbo',
  fecha: '2026-01-10',
  monto: 1250.5,
  proveedor: 'Proveedor SBO',
  descripcion: 'Pago aplicado',
});

assert.equal(sapSbo.date, '2026-01-10');
assert.equal(sapSbo.amount, 1250.5);
assert.equal(sapSbo.supplierName, 'Proveedor SBO');
assert.equal(sapSbo.description, 'Pago aplicado');

console.log('ok');
