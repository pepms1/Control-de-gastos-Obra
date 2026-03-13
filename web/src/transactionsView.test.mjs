import assert from 'node:assert/strict';
import { computeShownTransactions, TRACE_TRANSACTION_ID } from './ui/transactionsView.js';

const tx = {
  _id: TRACE_TRANSACTION_ID,
  description: 'Pago Calderón material',
  date: '2025-03-01',
  created_at: '2025-03-01T10:00:00Z',
};

const deps = {
  catMap: {},
  vendorMap: {},
  getTransactionCategoryLabel: () => 'Sin categoría',
  getCategoryHintCode: () => '',
};

const withoutFilters = computeShownTransactions({
  rows: [tx],
  categoryFilter: 'ALL',
  uncategorizedFilter: '__UNCATEGORIZED__',
  searchFilter: '',
  sortBy: 'date_desc',
  ...deps,
});
assert.equal(withoutFilters.shown.length, 1);
assert.equal(withoutFilters.shown[0]._id, TRACE_TRANSACTION_ID);

const withStaleSearch = computeShownTransactions({
  rows: [tx],
  categoryFilter: 'ALL',
  uncategorizedFilter: '__UNCATEGORIZED__',
  searchFilter: 'proveedor inexistente',
  sortBy: 'date_desc',
  ...deps,
});
assert.equal(withStaleSearch.afterCategory.length, 1);
assert.equal(withStaleSearch.afterSearch.length, 0);
assert.equal(withStaleSearch.shown.length, 0);

console.log('ok');
