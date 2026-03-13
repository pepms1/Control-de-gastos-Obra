export const TRACE_TRANSACTION_ID = '69ae6065aae96a6a5bd0529f';

export function getTransactionIdentity(transaction) {
  return String(transaction?._id || transaction?.id || transaction?.transactionId || '').trim();
}

export function computeShownTransactions({
  rows,
  categoryFilter,
  uncategorizedFilter,
  searchFilter,
  sortBy,
  catMap,
  vendorMap,
  getTransactionCategoryLabel,
  getCategoryHintCode,
}) {
  const allRows = Array.isArray(rows) ? rows : [];
  const afterCategory = allRows.filter((row) => {
    if (categoryFilter === 'ALL') return true;
    if (categoryFilter === uncategorizedFilter) return getTransactionCategoryLabel(row, catMap) === 'Sin categoría';
    return (row.categoryEffectiveCode || row.categoryEffectiveName || row.category_id || row.categoryId) === categoryFilter;
  });

  const query = searchFilter.trim().toLowerCase();
  const afterSearch = afterCategory.filter((row) => {
    if (!query) return true;
    const searchableFields = [
      row.description,
      row.concept,
      row.proveedorNombre,
      row.supplierName,
      row.proveedor?.name,
      getTransactionCategoryLabel(row, catMap),
      getCategoryHintCode(row),
    ];
    return searchableFields.some((field) => String(field || '').toLowerCase().includes(query));
  });

  const shown = [...afterSearch].sort((a, b) => {
    if (sortBy === 'created_desc') {
      const aCreatedAt = a.created_at || '';
      const bCreatedAt = b.created_at || '';
      if (aCreatedAt !== bCreatedAt) return bCreatedAt.localeCompare(aCreatedAt);
      if (a.date === b.date) return 0;
      return b.date.localeCompare(a.date);
    }

    if (sortBy === 'supplier_asc') {
      const aSupplier = (a.proveedorNombre || a.supplierName || vendorMap[a.vendor_id] || a.proveedor?.name || '').toLowerCase();
      const bSupplier = (b.proveedorNombre || b.supplierName || vendorMap[b.vendor_id] || b.proveedor?.name || '').toLowerCase();
      if (aSupplier !== bSupplier) return aSupplier.localeCompare(bSupplier, 'es');
    }

    if (a.date === b.date) {
      const aCreatedAt = a.created_at || '';
      const bCreatedAt = b.created_at || '';
      return bCreatedAt.localeCompare(aCreatedAt);
    }
    return b.date.localeCompare(a.date);
  });

  return { shown, afterCategory, afterSearch, allRows };
}
