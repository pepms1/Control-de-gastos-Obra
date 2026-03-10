function normalizeString(value) {
  if (value === null || value === undefined) return '';
  return String(value).trim();
}

export function parseNumberOrNull(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function pickNumber(...values) {
  for (const value of values) {
    const parsed = parseNumberOrNull(value);
    if (parsed !== null) return parsed;
  }
  return null;
}

export function deriveSourceSboFromSourceDb(sourceDb) {
  const normalized = normalizeString(sourceDb).toUpperCase();
  if (!normalized.startsWith('SBO_')) return null;
  return normalized;
}

export function resolveSourceSbo(transaction) {
  const fromRoot = normalizeString(transaction?.sourceSbo);
  if (fromRoot) return fromRoot;

  const fromSap = normalizeString(transaction?.sap?.sourceSbo);
  if (fromSap) return fromSap;

  return deriveSourceSboFromSourceDb(transaction?.sourceDb);
}

export function isSapSboTransaction(transaction) {
  const source = normalizeString(transaction?.source).toLowerCase();
  const sourceDb = normalizeString(transaction?.sourceDb).toUpperCase();
  const sourceSbo = resolveSourceSbo(transaction);
  return source === 'sap-sbo' || Boolean(sourceSbo) || sourceDb.startsWith('SBO_');
}

export function getSapBadgeLabel(transaction) {
  return isSapSboTransaction(transaction) ? 'SAP/SBO' : '';
}

export function resolveCategoryHintName(transaction) {
  const value = normalizeString(
    transaction?.categoryHintName
    || transaction?.category_hint_name
    || transaction?.CategoryHintName
    || transaction?.sap?.categoryHintName,
  );
  return value || null;
}

export function resolveCategoryHintCode(transaction) {
  const value = normalizeString(
    transaction?.categoryHintCode
    || transaction?.category_hint_code
    || transaction?.CategoryHintCode
    || transaction?.sap?.categoryHintCode,
  );
  return value || null;
}

export function resolveSupplierName(transaction) {
  const supplierName = normalizeString(transaction?.supplierName);
  if (supplierName) return supplierName;
  const businessPartner = normalizeString(transaction?.sap?.businessPartner);
  if (businessPartner) return businessPartner;
  return normalizeString(transaction?.proveedor || transaction?.proveedorNombre);
}

export function resolveTransactionType(transaction) {
  const explicitType = normalizeString(transaction?.type).toUpperCase();
  if (explicitType) return explicitType;

  const movementType = normalizeString(
    transaction?.movement_type || transaction?.movementType || transaction?.sap?.movementType,
  ).toLowerCase();
  if (movementType === 'egreso') return 'EXPENSE';
  if (movementType === 'ingreso') return 'INCOME';

  return null;
}

export function getTransactionTaxBreakdown(transaction) {
  return {
    subtotal: pickNumber(transaction?.subtotal, transaction?.montoSinIva, transaction?.tax?.subtotal, transaction?.sap?.invoiceSubtotal),
    iva: pickNumber(transaction?.iva, transaction?.montoIva, transaction?.tax?.iva, transaction?.sap?.invoiceIva),
    totalFactura: pickNumber(transaction?.totalFactura, transaction?.tax?.totalFactura, transaction?.sap?.invoiceTotal),
  };
}

export function getSapCategoryLabel(transaction) {
  return resolveCategoryHintName(transaction)
    || resolveCategoryHintCode(transaction)
    || '';
}
