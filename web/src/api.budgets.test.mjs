import assert from 'node:assert/strict';
import test from 'node:test';
import { shouldInjectProjectId } from './api.js';

test('injecta projectId en /api/budgets listado', () => {
  assert.equal(shouldInjectProjectId('GET', '/api/budgets'), true);
});

test('no inyecta projectId en summary global de presupuestos', () => {
  assert.equal(shouldInjectProjectId('GET', '/api/budgets/summary-by-project'), false);
});

test('respeta rutas auth sin inyección', () => {
  assert.equal(shouldInjectProjectId('GET', '/auth/me'), false);
});
