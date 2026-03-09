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


const sboWithInvoiceFields = normalizeTransaction({
  source: 'sap-sbo',
  amount: 1800,
  subtotal: 1551.72,
  iva: 248.28,
  totalFactura: 1800,
});

assert.equal(sboWithInvoiceFields.amount, 1800);
assert.equal(sboWithInvoiceFields.subtotal, 1551.72);
assert.equal(sboWithInvoiceFields.montoSinIva, 1551.72);
assert.equal(sboWithInvoiceFields.iva, 248.28);
assert.equal(sboWithInvoiceFields.montoIva, 248.28);
assert.equal(sboWithInvoiceFields.totalFactura, 1800);
assert.equal(sboWithInvoiceFields.tax.subtotal, 1551.72);
assert.equal(sboWithInvoiceFields.tax.iva, 248.28);
assert.equal(sboWithInvoiceFields.tax.totalFactura, 1800);

const sboWithSapFallbacks = normalizeTransaction({
  sourceDb: 'SBO_BANK',
  sap: {
    sourceSbo: 'OBRA_A',
    invoiceSubtotal: 100,
    invoiceIva: 16,
    invoiceTotal: 116,
  },
});

assert.equal(sboWithSapFallbacks.subtotal, 100);
assert.equal(sboWithSapFallbacks.iva, 16);
assert.equal(sboWithSapFallbacks.totalFactura, 116);
assert.equal(sboWithSapFallbacks.tax.subtotal, 100);
assert.equal(sboWithSapFallbacks.tax.iva, 16);
assert.equal(sboWithSapFallbacks.tax.totalFactura, 116);

console.log('ok');
