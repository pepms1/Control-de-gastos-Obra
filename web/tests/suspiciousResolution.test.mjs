import test from 'node:test';
import assert from 'node:assert/strict';

import {
  getSuspiciousTransactionId,
  getSuspiciousResolutionTarget,
  MISSING_TRANSACTION_ID_ERROR,
} from '../src/ui/suspiciousResolution.js';

test('uses explicit transactionId from suspicious list payload', () => {
  const row = {
    transactionId: '665f27f0ab12cd34ef56a789',
    id: 'legacy-id-should-not-win',
    _id: 'legacy-underscore-id-should-not-win',
    paymentNum: '12345',
  };

  assert.equal(getSuspiciousTransactionId(row), '665f27f0ab12cd34ef56a789');
});

test('falls back to id and _id when transactionId is absent', () => {
  assert.equal(getSuspiciousTransactionId({ id: 'abc' }), 'abc');
  assert.equal(getSuspiciousTransactionId({ _id: 'def' }), 'def');
});

test('client-side blocks resolve when transaction id is undefined/missing', () => {
  const target = getSuspiciousResolutionTarget({ paymentNum: '123', invoiceNum: '456' });

  assert.equal(target.canResolve, false);
  assert.equal(target.transactionId, '');
  assert.equal(target.error, MISSING_TRANSACTION_ID_ERROR);
});
