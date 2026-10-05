// Utilidades compartidas por Presupuestos (captura de presupuestos por conceptos)
// y Estimaciones (avance, autorización y pago).

const moneyFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

export function formatCurrency(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '$0.00';
  return `$${moneyFormatter.format(amount)}`;
}

export function formatPct(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '0.00%';
  return `${amount.toFixed(2)}%`;
}

export function formatDate(value) {
  if (!value) return '—';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleDateString('es-MX');
}

export function normalizeTextForSupplierKey(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .trim()
    .replace(/\s+/g, ' ');
}

export function buildCanonicalSupplierKey({ supplierCardCode, businessPartner, supplierName }) {
  const cardCode = String(supplierCardCode || '').trim();
  const bp = String(businessPartner || '').trim();
  const name = String(supplierName || '').trim();
  if (bp && cardCode) return `bpcc:${normalizeTextForSupplierKey(bp)}|${normalizeTextForSupplierKey(cardCode)}`;
  if (bp) return `bp:${normalizeTextForSupplierKey(bp)}`;
  if (cardCode) return `cardcode:${normalizeTextForSupplierKey(cardCode)}`;
  if (name) return `name:${normalizeTextForSupplierKey(name)}`;
  return '';
}

export function generateId() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
  return `c_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

export function emptyConceptoRow() {
  return { id: generateId(), description: '', unit: '', quantity: '', unitPrice: '', group: '' };
}

export function computeLineItemAmount(row) {
  const quantity = Number(row.quantity) || 0;
  const unitPrice = Number(row.unitPrice) || 0;
  return quantity * unitPrice;
}

export function groupLabel(name) {
  return name || 'General';
}

// Grupos del presupuesto en el orden en que aparecen sus conceptos, con su importe.
export function listFormGroups(lineItems) {
  const groups = [];
  (lineItems || []).forEach((row) => {
    const name = String(row.group || '').trim();
    let group = groups.find((g) => g.name === name);
    if (!group) {
      group = { name, amount: 0, count: 0 };
      groups.push(group);
    }
    group.amount += computeLineItemAmount(row);
    group.count += 1;
  });
  return groups;
}

// Anticipo previsto por grupo: importe del grupo × su % de anticipo.
export function computeGroupAdvanceTotal(lineItems, groupAdvancePcts) {
  return listFormGroups(lineItems).reduce(
    (sum, group) => sum + (group.amount * (Number((groupAdvancePcts || {})[group.name]) || 0)) / 100,
    0,
  );
}

export function computeBudgetFormTotals(lineItems, advanceAmount, groupAdvancePcts) {
  const totalContractedAmount = (lineItems || []).reduce((sum, row) => sum + computeLineItemAmount(row), 0);
  const groupAdvance = computeGroupAdvanceTotal(lineItems, groupAdvancePcts);
  const advance = groupAdvance > 0 ? groupAdvance : Number(advanceAmount) || 0;
  const advancePct = totalContractedAmount > 0 ? (advance / totalContractedAmount) * 100 : 0;
  return { totalContractedAmount, advancePct, advanceAmount: advance, usesGroupAdvance: groupAdvance > 0 };
}

export function emptyBudgetForm(projectId) {
  return {
    projectId: projectId || '',
    supplierKey: '',
    supplierName: '',
    supplierCardCode: '',
    businessPartner: '',
    vendorId: '',
    name: '',
    currency: 'MXN',
    notes: '',
    retentionPct: '0',
    advanceAmortizationEnabled: false,
    advanceAmount: '0',
    groupAdvancePcts: {},
    isActive: true,
    lineItems: [emptyConceptoRow()],
  };
}

// KPI de un conjunto de presupuestos (todos, los de un proveedor o uno solo).
export function summarizeBudgets(budgetRows) {
  const totals = (budgetRows || []).reduce(
    (acc, row) => {
      acc.contracted += Number(row.totalContractedAmount) || 0;
      acc.paid += Number(row.paidAmount) || 0;
      acc.retained += Number(row.totalRetainedToDate) || 0;
      acc.advanceBalance += Number(row.remainingAdvanceBalance) || 0;
      acc.progressAmount += Number(row.approvedProgressAmount) || 0;
      acc.estimations += Number(row.estimationsCount) || 0;
      return acc;
    },
    { contracted: 0, paid: 0, retained: 0, advanceBalance: 0, progressAmount: 0, estimations: 0 },
  );
  return {
    ...totals,
    balance: totals.contracted - totals.paid,
    paidPct: totals.contracted > 0 ? (totals.paid / totals.contracted) * 100 : 0,
    progressPct: totals.contracted > 0 ? (totals.progressAmount / totals.contracted) * 100 : 0,
    count: (budgetRows || []).length,
    activeCount: (budgetRows || []).filter((row) => row.isActive !== false).length,
  };
}

export function isBlankConceptoRow(row) {
  return !String(row.description || '').trim() && !String(row.quantity || '').trim() && !String(row.unitPrice || '').trim();
}
