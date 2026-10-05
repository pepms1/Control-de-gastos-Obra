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

const escapeHtml = (value) => String(value ?? '')
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

// Hoja de autorización lista para imprimir / guardar como PDF (sin dependencias).
// Solo incluye la hoja de estimación hacia abajo; el monto autorizado y quién autorizó van en grande.
export function buildAuthorizedSheetHtml(estimation, budget = {}) {
  const sheet = estimation.groupBreakdown || [];
  const sum = (key) => sheet.reduce((total, row) => total + (Number(row[key]) || 0), 0);
  const totalBudget = sum('budgetAmount');
  const advanceGiven = budget.advanceAmortizationEnabled ? Number(budget.advanceAmount) || 0 : 0;
  const showPaidLines = Number(estimation.priorPaidApplied) > 0;
  const paidToDate = (Number(estimation.priorPaidApplied) || 0) + advanceGiven;
  const money = formatCurrency;
  const hasGroups = sheet.some((row) => row.group);
  const requested = estimation.requestedAmount != null ? Number(estimation.requestedAmount) : null;

  const rows = sheet.map((row) => `<tr>
    <td>${escapeHtml(groupLabel(row.group))}${row.isExtra ? ' <em>(extra)</em>' : ''}</td>
    <td class="n">${money(row.budgetAmount)}</td>
    <td class="n">${Number(row.advanceAmount) > 0 ? money(row.advanceAmount) : '—'}</td>
    <td class="n">${Number(row.advancePct) > 0 ? formatPct(row.advancePct) : '—'}</td>
    <td class="n">${money(row.cumulativeAmount)}</td>
    <td class="n">${formatPct(row.cumulativePct)}</td>
    <td class="n">${Number(row.cumulativeAmortization) > 0 ? money(row.cumulativeAmortization) : '—'}</td>
    <td class="n">${money(row.netAmount)}</td></tr>`).join('');

  const conceptRows = (estimation.lineItems || []).filter((li) => Number(li.periodAmount) !== 0).map((li) => `<tr>
    <td>${escapeHtml(li.description)}</td><td>${escapeHtml(li.unit || '—')}</td>
    <td class="n">${formatPct(li.previousProgressPct)}</td><td class="n">${formatPct(li.progressPct)}</td>
    <td class="n">${money(li.periodAmount)}</td></tr>`).join('');

  const totals = [
    ['avance acumulado +', money(sum('cumulativeAmount'))],
    ['amortización de anticipos −', money(sum('cumulativeAmortization'))],
    ['saldo acumulado', money(sum('netAmount'))],
    Number(estimation.retentionAmount) > 0 ? ['retención −', money(estimation.retentionAmount)] : null,
    showPaidLines && advanceGiven > 0 ? ['anticipo +', money(advanceGiven)] : null,
    showPaidLines ? ['pagado a la fecha −', money(paidToDate)] : null,
    ['saldo total (a liberar)', money(estimation.totalToPay)],
  ].filter(Boolean).map(([label, value]) => `<div>${label} <strong>${value}</strong></div>`).join('');

  return `<!doctype html><html lang="es"><head><meta charset="utf-8">
<title>Estimación ${escapeHtml(estimation.folio)} autorizada</title>
<style>
  body{font-family:Arial,Helvetica,sans-serif;color:#111;margin:28px;font-size:12px}
  h1{font-size:18px;margin:0 0 2px} .sub{color:#555;margin-bottom:14px}
  .auth{border:2px solid #166534;background:#f0fdf4;border-radius:8px;padding:14px 18px;margin:12px 0 18px}
  .auth .lbl{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#166534}
  .auth .amt{font-size:34px;font-weight:700;color:#14532d;margin:2px 0}
  .auth .who{font-size:15px;font-weight:600}
  table{width:100%;border-collapse:collapse;margin:8px 0} th,td{border:1px solid #ccc;padding:4px 6px;text-align:left}
  th{background:#f3f4f6} td.n{text-align:right} tr.tot td{font-weight:700;background:#fafafa}
  h2{font-size:13px;margin:16px 0 4px} .totals{text-align:right;line-height:1.6;margin-top:6px}
  .sign{display:flex;gap:40px;margin-top:48px;page-break-inside:avoid}
  .sign div{flex:1;text-align:center}
  .sign .line{border-top:1px solid #111;height:70px;margin-bottom:4px}
  .sign small{color:#555}
  @media print{body{margin:12mm}}
</style></head><body>
<h1>Estimación #${escapeHtml(estimation.folio)} · ${escapeHtml(budget.supplierNameSnapshot || estimation.supplierName || '')}</h1>
<div class="sub">${escapeHtml(budget.name || estimation.budgetName || '')} · Periodo ${formatDate(estimation.periodStart)} – ${formatDate(estimation.periodEnd)}</div>
<div class="auth">
  <div class="lbl">Monto autorizado</div>
  <div class="amt">${money(estimation.authorizedAmount)}</div>
  <div class="who">Autorizó: ${escapeHtml(estimation.approvedBy || '—')} · ${formatDate(estimation.approvedAt)}</div>
  ${requested !== null ? `<div>Solicitado por el contratista: ${money(requested)}</div>` : ''}
  <div>Avance calculado (a liberar): ${money(estimation.totalToPay)}</div>
  ${estimation.authorizationNote ? `<div>Motivo: ${escapeHtml(estimation.authorizationNote)}</div>` : ''}
</div>
<h2>Hoja de estimación${hasGroups ? ' por grupo (acumulado a la fecha)' : ''}</h2>
${hasGroups ? `<table><thead><tr><th>Grupo</th><th>Presupuesto</th><th>Anticipo</th><th>%</th><th>Avance $</th><th>Avance %</th><th>Amortización</th><th>Saldo</th></tr></thead><tbody>${rows}
<tr class="tot"><td>Total</td><td class="n">${money(totalBudget)}</td><td class="n">${money(sum('advanceAmount'))}</td><td></td><td class="n">${money(sum('cumulativeAmount'))}</td><td class="n">${formatPct(totalBudget > 0 ? (sum('cumulativeAmount') / totalBudget) * 100 : 0)}</td><td class="n">${money(sum('cumulativeAmortization'))}</td><td class="n">${money(sum('netAmount'))}</td></tr></tbody></table>`
: `<table><thead><tr><th>Concepto</th><th>Unidad</th><th>Avance previo</th><th>Avance acumulado</th><th>Importe periodo</th></tr></thead><tbody>${conceptRows}</tbody></table>`}
<div class="totals">${totals}</div>
<div class="sign">
  <div><div class="line"></div>Firma de autorización<br><small>${escapeHtml(estimation.approvedBy || '')}</small></div>
  <div><div class="line"></div>Fecha<br><small>&nbsp;</small></div>
</div>
</body></html>`;
}

export function openAuthorizedSheet(estimation, budget) {
  const win = window.open('', '_blank');
  if (!win) return false;
  win.document.open();
  win.document.write(buildAuthorizedSheetHtml(estimation, budget));
  win.document.close();
  win.focus();
  setTimeout(() => win.print(), 300);
  return true;
}
