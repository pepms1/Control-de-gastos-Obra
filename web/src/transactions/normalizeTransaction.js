import {
  getSapBadgeLabel,
  isSapSboTransaction,
  pickNumber,
  resolveCategoryHintCode,
  resolveCategoryHintName,
  resolveSourceSbo,
  resolveSupplierName,
  resolveTransactionType,
} from './helpers.js';

function toNullableString(value) {
  if (value === null || value === undefined) return null;
  const normalized = String(value).trim();
  return normalized || null;
}

function buildSapMeta(sap) {
  if (!sap || typeof sap !== 'object') return null;

  return {
    movementType: toNullableString(sap.movementType),
    sourceType: toNullableString(sap.sourceType),
    paymentDocEntry: pickNumber(sap.paymentDocEntry),
    paymentNum: pickNumber(sap.paymentNum),
    invoiceDocEntry: pickNumber(sap.invoiceDocEntry),
    invoiceNum: pickNumber(sap.invoiceNum),
    externalDocNum: toNullableString(sap.externalDocNum),
    movementDate: toNullableString(sap.movementDate),
    invoiceDate: toNullableString(sap.invoiceDate),
    montoAplicado: pickNumber(sap.montoAplicado, sap.montoAplicadoCents !== undefined ? Number(sap.montoAplicadoCents) / 100 : null),
    invoiceSubtotal: pickNumber(sap.invoiceSubtotal),
    invoiceIva: pickNumber(sap.invoiceIva),
    invoiceTotal: pickNumber(sap.invoiceTotal),
    paymentCurrency: toNullableString(sap.paymentCurrency),
    invoiceCurrency: toNullableString(sap.invoiceCurrency),
    cardCode: toNullableString(sap.cardCode),
    businessPartner: toNullableString(sap.businessPartner),
    categoryHintCode: toNullableString(sap.categoryHintCode),
    categoryHintName: toNullableString(sap.categoryHintName),
    rawProjectCode: toNullableString(sap.rawProjectCode),
    rawProjectName: toNullableString(sap.rawProjectName),
    documentProjectCode: toNullableString(sap.documentProjectCode),
    documentProjectName: toNullableString(sap.documentProjectName),
    paymentProjectCode: toNullableString(sap.paymentProjectCode),
    paymentProjectName: toNullableString(sap.paymentProjectName),
    projectResolutionSource: toNullableString(sap.projectResolutionSource),
    normalizedProjectName: toNullableString(sap.normalizedProjectName),
    sourceDb: toNullableString(sap.sourceDb),
    sourceSbo: toNullableString(sap.sourceSbo),
    sourceSboMode: toNullableString(sap.sourceSboMode),
  };
}

export function normalizeTransaction(transaction) {
  if (!transaction || typeof transaction !== 'object') return transaction;

  const isSapSbo = isSapSboTransaction(transaction);
  const sourceSbo = resolveSourceSbo(transaction);
  const sapMeta = buildSapMeta(transaction.sap);

  const subtotal = pickNumber(transaction.subtotal, transaction.montoSinIva, transaction?.tax?.subtotal, transaction?.sap?.invoiceSubtotal);
  const iva = pickNumber(transaction.iva, transaction.montoIva, transaction?.tax?.iva, transaction?.sap?.invoiceIva);
  const totalFacturaRaw = pickNumber(transaction.totalFactura, transaction?.tax?.totalFactura, transaction?.sap?.invoiceTotal);
  const totalFactura = totalFacturaRaw ?? ((subtotal !== null && iva !== null) ? subtotal + iva : null);

  const amount = pickNumber(transaction.amount, transaction.monto, transaction?.sap?.montoAplicado, totalFactura, subtotal);
  const categoryName = resolveCategoryHintName(transaction);
  const categoryCode = resolveCategoryHintCode(transaction);
  const categoryManualName = toNullableString(transaction.categoryManualName);
  const categoryManualCode = toNullableString(transaction.categoryManualCode);
  const categoryEffectiveName = toNullableString(transaction.categoryEffectiveName) || categoryManualName || categoryName;
  const categoryEffectiveCode = toNullableString(transaction.categoryEffectiveCode) || categoryManualCode || categoryCode;
  const resolvedCategory2Id = toNullableString(transaction.resolvedCategory2Id);
  const resolvedCategory2Name = toNullableString(transaction.resolvedCategory2Name);
  const resolvedCategory2Source = toNullableString(transaction.resolvedCategory2Source);

  const normalizedType = resolveTransactionType(transaction);

  return {
    ...transaction,
    id: String(transaction.id || transaction._id || '').trim(),
    projectId: toNullableString(transaction.projectId),
    projectDisplayName: toNullableString(transaction.projectDisplayName)
      || toNullableString(transaction.projectName)
      || sapMeta?.normalizedProjectName
      || sapMeta?.rawProjectName
      || 'Sin proyecto',
    date: toNullableString(transaction.date)
      || toNullableString(transaction.fecha)
      || sapMeta?.movementDate
      || sapMeta?.invoiceDate,
    description: toNullableString(transaction.description)
      || toNullableString(transaction.descripcion)
      || '',
    supplierName: resolveSupplierName(transaction),
    type: normalizedType,
    amount,
    subtotal,
    montoSinIva: subtotal,
    iva,
    montoIva: iva,
    totalFactura,
    tax: {
      ...(transaction.tax && typeof transaction.tax === 'object' ? transaction.tax : {}),
      subtotal,
      iva,
      totalFactura,
    },
    categoryCode,
    categoryName,
    categoryHintCode: categoryCode,
    categoryHintName: categoryName,
    category_hint_code: toNullableString(transaction.category_hint_code) || categoryCode,
    category_hint_name: toNullableString(transaction.category_hint_name) || categoryName,
    categoryManualName,
    categoryManualCode,
    categoryEffectiveCode,
    categoryEffectiveName,
    resolvedCategory2Id,
    resolvedCategory2Name,
    resolvedCategory2Source,
    source: toNullableString(transaction.source) || '',
    sourceDb: toNullableString(transaction.sourceDb) || '',
    sourceSbo: sourceSbo || '',
    isSapSbo,
    sapBadgeLabel: getSapBadgeLabel(transaction),
    sapMeta,
  };
}
