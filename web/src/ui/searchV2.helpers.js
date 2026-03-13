const CATEGORY2_UNCLASSIFIED_ID = 'trabajos_especiales_unclassified';
const CATEGORY2_UNCLASSIFIED_NAME = 'Trabajos Especiales sin clasificar';

export function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim()
    .replace(/\s+/g, ' ');
}

export function tokenizeQuery(query) {
  return normalizeText(query).split(' ').filter(Boolean);
}

export function resolveSupplierIdentity(transaction) {
  const sap = transaction?.sap || {};
  const supplierId = String(transaction?.supplierId || transaction?.vendor_id || '').trim();
  const businessPartner = String(transaction?.sapMeta?.businessPartner || sap?.businessPartner || transaction?.businessPartner || '').trim();
  const cardCode = String(transaction?.sapMeta?.cardCode || sap?.cardCode || transaction?.supplierCardCode || '').trim();
  const name = String(
    transaction?.supplierName
    || transaction?.proveedorNombre
    || transaction?.proveedor
    || businessPartner
    || cardCode
    || supplierId
    || ''
  ).trim();

  const normalizedBusinessPartner = normalizeText(businessPartner);
  const normalizedCardCode = normalizeText(cardCode);
  const normalizedName = normalizeText(name);
  const normalizedIdentityName = normalizedBusinessPartner || normalizedName;

  if (normalizedIdentityName && normalizedCardCode) {
    return {
      key: `bpcc:${normalizedIdentityName}|${normalizedCardCode}`,
      supplierId,
      businessPartner,
      cardCode,
      name,
    };
  }
  if (normalizedCardCode) return { key: `card:${normalizedCardCode}`, supplierId, businessPartner, cardCode, name };
  if (normalizedBusinessPartner) return { key: `bp:${normalizedBusinessPartner}`, supplierId, businessPartner, cardCode, name };

  if (supplierId) return { key: `id:${supplierId}`, supplierId, businessPartner, cardCode, name };
  if (name) return { key: `name:${normalizeText(name)}`, supplierId: '', businessPartner, cardCode, name };
  return { key: '', supplierId: '', businessPartner: '', cardCode: '', name: '' };
}

export function resolveVendorIdentity(vendor) {
  const supplierId = vendor?._id || vendor?.id || vendor?.vendorId || vendor?.supplierId;
  const source = String(vendor?.source || '').trim().toLowerCase();
  const isSyntheticSapCatalog = source === 'sap-sbo' || String(supplierId || '').trim().toLowerCase().startsWith('sap-sbo:');
  const supplierName = vendor?.name || vendor?.displayName || vendor?.label;
  const businessPartner = vendor?.businessPartner || vendor?.externalIds?.sapBusinessPartner;

  return resolveSupplierIdentity({
    supplierId,
    supplierName,
    supplierCardCode: vendor?.supplierCardCode || vendor?.cardCode || vendor?.externalIds?.sapCardCode,
    businessPartner: businessPartner || (isSyntheticSapCatalog ? supplierName : ''),
  });
}

export function resolveCategory2(transaction, categoryMap = {}) {
  const resolvedName = String(transaction?.resolvedCategory2Name || transaction?.resolved_category2_name || '').trim();
  const resolvedId = String(transaction?.resolvedCategory2Id || transaction?.resolved_category2_id || '').trim();

  if (resolvedName || resolvedId) {
    return { id: resolvedId || resolvedName, name: resolvedName || categoryMap[resolvedId] || resolvedId };
  }

  const legacyName = String(
    transaction?.categoryEffectiveName
    || transaction?.categoryManualName
    || transaction?.categoryName
    || transaction?.category_hint_name
    || transaction?.category
    || ''
  ).trim();
  const legacyId = String(
    transaction?.categoryEffectiveCode
    || transaction?.categoryManualCode
    || transaction?.categoryCode
    || transaction?.category_hint_code
    || transaction?.categoryId
    || transaction?.category_id
    || ''
  ).trim();

  if (legacyName || legacyId) {
    return { id: legacyId || legacyName, name: legacyName || categoryMap[legacyId] || legacyId };
  }

  return { id: CATEGORY2_UNCLASSIFIED_ID, name: CATEGORY2_UNCLASSIFIED_NAME };
}

export function buildSearchHaystack(transaction, categoryMap) {
  const sap = transaction?.sap || {};
  const sapMeta = transaction?.sapMeta || {};
  const supplier = resolveSupplierIdentity(transaction);
  const category2 = resolveCategory2(transaction, categoryMap);

  const fields = [
    transaction?.description,
    transaction?.concept,
    transaction?.concepto,
    transaction?.descripcion,
    supplier?.name,
    supplier?.supplierId,
    supplier?.businessPartner,
    supplier?.cardCode,
    sapMeta?.businessPartner,
    sap?.businessPartner,
    sapMeta?.cardCode,
    sap?.cardCode,
    sapMeta?.invoiceNum,
    sap?.invoiceNum,
    sapMeta?.paymentNum,
    sap?.paymentNum,
    sapMeta?.externalDocNum,
    sap?.externalDocNum,
    sap?.paymentDocEntry,
    sap?.invoiceDocEntry,
    category2?.name,
    category2?.id,
    transaction?.resolvedCategory2Name,
    transaction?.resolvedCategory2Id,
    transaction?.categoryName,
    transaction?.categoryCode,
    transaction?.category_hint_name,
    transaction?.category_hint_code,
    transaction?.projectDisplayName,
    transaction?.projectName,
    transaction?.sourceSbo,
  ];

  return normalizeText(fields.filter(Boolean).join(' '));
}

export function matchesSearch(transaction, query, categoryMap) {
  const tokens = tokenizeQuery(query);
  if (!tokens.length) return true;
  const haystack = buildSearchHaystack(transaction, categoryMap);
  return tokens.every((token) => haystack.includes(token));
}

export function getTypeLabel(type) {
  if (type === 'INCOME') return 'Ingreso';
  if (type === 'EXPENSE') return 'Egreso';
  return '—';
}
