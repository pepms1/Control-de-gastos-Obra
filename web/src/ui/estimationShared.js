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

// Descuento del presupuesto: `discountPct` se aplica a los precios de lista (los extras no llevan descuento).
export function netUnitPrice(row, discountPct = 0) {
  const unitPrice = Number(row.unitPrice) || 0;
  return row.isExtra || !(discountPct > 0) ? unitPrice : unitPrice * (1 - discountPct / 100);
}

export function computeLineItemAmount(row, discountPct = 0) {
  const quantity = Number(row.quantity) || 0;
  return quantity * netUnitPrice(row, discountPct);
}

// % de descuento resultante del formulario ({ discountEnabled, discountMode: 'pct'|'amount', discountValue }).
export function computeFormDiscount(lineItems, form) {
  const subtotal = (lineItems || []).reduce(
    (sum, row) => sum + (row.isExtra ? 0 : (Number(row.quantity) || 0) * (Number(row.unitPrice) || 0)),
    0,
  );
  const value = Number(form?.discountValue);
  if (!form?.discountEnabled || !Number.isFinite(value) || value <= 0 || subtotal <= 0) {
    return { applies: false, pct: 0, amount: 0, subtotal, valid: !form?.discountEnabled };
  }
  const pct = form.discountMode === 'amount' ? (value / subtotal) * 100 : value;
  const valid = pct > 0 && pct < 100;
  const effectivePct = valid ? pct : 0;
  return { applies: valid, pct: effectivePct, amount: (subtotal * effectivePct) / 100, subtotal, valid };
}

export function groupLabel(name) {
  return name || 'General';
}

// Grupos del presupuesto en el orden en que aparecen sus conceptos, con su importe.
export function listFormGroups(lineItems, discountPct = 0) {
  const groups = [];
  (lineItems || []).forEach((row) => {
    const name = String(row.group || '').trim();
    let group = groups.find((g) => g.name === name);
    if (!group) {
      group = { name, amount: 0, count: 0 };
      groups.push(group);
    }
    group.amount += computeLineItemAmount(row, discountPct);
    group.count += 1;
  });
  return groups;
}

// Anticipo previsto por grupo: importe del grupo × su % de anticipo.
export function computeGroupAdvanceTotal(lineItems, groupAdvancePcts, discountPct = 0) {
  return listFormGroups(lineItems, discountPct).reduce(
    (sum, group) => sum + (group.amount * (Number((groupAdvancePcts || {})[group.name]) || 0)) / 100,
    0,
  );
}

// El anticipo (solo sirve para amortizar) se captura en $ o como % del presupuesto: advanceInput = { mode, pct }.
export function computeBudgetFormTotals(lineItems, advanceAmount, groupAdvancePcts, discountPct = 0, advanceInput = null) {
  const totalContractedAmount = (lineItems || []).reduce((sum, row) => sum + computeLineItemAmount(row, discountPct), 0);
  const groupAdvance = computeGroupAdvanceTotal(lineItems, groupAdvancePcts, discountPct);
  const byPct = advanceInput?.mode === 'pct';
  const advance = groupAdvance > 0
    ? groupAdvance
    : byPct
      ? (totalContractedAmount * Math.min(Math.max(Number(advanceInput.pct) || 0, 0), 100)) / 100
      : Number(advanceAmount) || 0;
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
    advanceAmortizationEnabled: true,
    advanceMode: 'amount',
    advancePctInput: '',
    discountEnabled: false,
    discountMode: 'pct',
    discountValue: '',
    advanceAmount: '0',
    groupAdvancePcts: {},
    isActive: true,
    lineItems: [emptyConceptoRow()],
  };
}

// KPI de un conjunto de presupuestos (todos, los de un proveedor o uno solo).
// Con { bySupplier: true } (filas de un mismo proveedor) el pagado es el del PROVEEDOR: todos sus pagos menos los
// desasignados, no la suma de lo asignado a cada presupuesto.
export function summarizeBudgets(budgetRows, { bySupplier = false } = {}) {
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
  if (bySupplier) {
    const withSupplierPaid = (budgetRows || []).find((row) => row.supplierPaidAmount != null);
    if (withSupplierPaid) totals.paid = Number(withSupplierPaid.supplierPaidAmount) || 0;
  }
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

// Conceptos de un grupo de extras con su avance acumulado, para desglosarlos en la hoja.
export function extraConceptsOfGroup(group, estimation, budget) {
  const progressById = {};
  (estimation?.lineItems || []).forEach((li) => { progressById[li.conceptoId] = li; });
  return (budget?.lineItems || [])
    .filter((c) => c.isExtra && (c.group || '') === (group || ''))
    .map((c) => {
      const li = progressById[c.id] || {};
      const unitPrice = Number(c.unitPrice) || 0;
      const budgetAmount = Number(c.amount) || (Number(c.quantity) || 0) * unitPrice;
      const cumulativeAmount = (Number(li.cumulativeQuantity) || 0) * unitPrice;
      return {
        id: c.id,
        description: c.description,
        unit: c.unit,
        quantity: c.quantity,
        budgetAmount,
        cumulativeAmount,
        cumulativePct: budgetAmount > 0 ? (cumulativeAmount / budgetAmount) * 100 : 0,
      };
    });
}

const escapeHtml = (value) => String(value ?? '')
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

// Hoja de autorización lista para imprimir / guardar como PDF (sin dependencias).
// Solo incluye la hoja de estimación hacia abajo; el monto autorizado y quién autorizó van en grande.
// Hoja de estimación (por grupo o por concepto) de UN presupuesto, con sus totales.
function sheetSectionHtml(estimation, budget = {}) {
  const sheet = estimation.groupBreakdown || [];
  const sum = (key) => sheet.reduce((total, row) => total + (Number(row[key]) || 0), 0);
  const totalBudget = sum('budgetAmount');
  const advanceGiven = budget.advanceAmortizationEnabled ? Number(budget.advanceAmount) || 0 : 0;
  const showPaidLines = Number(estimation.priorPaidApplied) > 0;
  const paidToDate = (Number(estimation.priorPaidApplied) || 0) + advanceGiven;
  const money = formatCurrency;
  const hasGroups = sheet.some((row) => row.group);

  const extraRows = (row) => (row.isExtra ? extraConceptsOfGroup(row.group, estimation, budget) : []).map((c) => `<tr class="sub">
    <td>&nbsp;&nbsp;↳ ${escapeHtml(c.description)}${c.unit ? ` (${escapeHtml(c.quantity)} ${escapeHtml(c.unit)})` : ''}</td>
    <td class="n">${money(c.budgetAmount)}</td><td class="n">—</td><td class="n">—</td>
    <td class="n">${money(c.cumulativeAmount)}</td><td class="n">${formatPct(c.cumulativePct)}</td><td class="n">—</td>
    <td class="n">${money(c.cumulativeAmount)}</td></tr>`).join('');

  const rows = sheet.map((row) => `<tr>
    <td>${escapeHtml(groupLabel(row.group))}${row.isExtra ? ' <em>(extra)</em>' : ''}</td>
    <td class="n">${money(row.budgetAmount)}</td>
    <td class="n">${Number(row.advanceAmount) > 0 ? money(row.advanceAmount) : '—'}</td>
    <td class="n">${Number(row.advancePct) > 0 ? formatPct(row.advancePct) : '—'}</td>
    <td class="n">${money(row.cumulativeAmount)}</td>
    <td class="n">${formatPct(row.cumulativePct)}</td>
    <td class="n">${Number(row.cumulativeAmortization) > 0 ? money(row.cumulativeAmortization) : '—'}</td>
    <td class="n">${money(row.netAmount)}</td></tr>${extraRows(row)}`).join('');

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
    Number(estimation.advanceGivenAmount) > 0 ? ['anticipo a entregar +', money(estimation.advanceGivenAmount)] : null,
    ['saldo total (a liberar)', money(estimation.totalToPay)],
  ].filter(Boolean).map(([label, value]) => `<div>${label} <strong>${value}</strong></div>`).join('');

  return `<h2>Hoja de estimación${hasGroups ? ' por grupo (acumulado a la fecha)' : ''}</h2>
${hasGroups ? `<table><thead><tr><th>Grupo</th><th>Presupuesto</th><th>Anticipo</th><th>%</th><th>Avance $</th><th>Avance %</th><th>Amortización</th><th>Saldo</th></tr></thead><tbody>${rows}
<tr class="tot"><td>Total</td><td class="n">${money(totalBudget)}</td><td class="n">${money(sum('advanceAmount'))}</td><td></td><td class="n">${money(sum('cumulativeAmount'))}</td><td class="n">${formatPct(totalBudget > 0 ? (sum('cumulativeAmount') / totalBudget) * 100 : 0)}</td><td class="n">${money(sum('cumulativeAmortization'))}</td><td class="n">${money(sum('netAmount'))}</td></tr></tbody></table>`
: `<table><thead><tr><th>Concepto</th><th>Unidad</th><th>Avance previo</th><th>Avance acumulado</th><th>Importe periodo</th></tr></thead><tbody>${conceptRows}</tbody></table>`}
<div class="totals">${totals}</div>`;
}

const PDF_STYLE = `
  body{font-family:Arial,Helvetica,sans-serif;color:#111;margin:28px;font-size:12px}
  .obra{font-size:20px;font-weight:700;margin-bottom:6px}
  h1{font-size:18px;margin:0 0 2px} .sub{color:#555;margin-bottom:14px}
  .auth{border:2px solid #166534;background:#f0fdf4;border-radius:8px;padding:14px 18px;margin:12px 0 18px}
  .auth .lbl{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#166534}
  .auth .amt{font-size:34px;font-weight:700;color:#14532d;margin:2px 0}
  .auth .who{font-size:15px;font-weight:600}
  table{width:100%;border-collapse:collapse;margin:8px 0} th,td{border:1px solid #ccc;padding:4px 6px;text-align:left}
  th{background:#f3f4f6} td.n{text-align:right} tr.tot td{font-weight:700;background:#fafafa} tr.sub td{font-size:11px;color:#444;background:#fcfcfc}
  h2{font-size:13px;margin:16px 0 4px} .totals{text-align:right;line-height:1.6;margin-top:6px}
  .budget{font-size:15px;font-weight:700;margin:22px 0 0;border-bottom:2px solid #111;padding-bottom:2px}
  .summary{border-top:2px solid #111;margin-top:18px;padding-top:6px}
  .sign{display:flex;gap:40px;margin-top:48px;page-break-inside:avoid}
  .sign div{flex:1;text-align:center}
  .sign .line{border-top:1px solid #111;height:70px;margin-bottom:4px}
  .sign small{color:#555}
  @media print{body{margin:12mm}}`;

function authorizationBoxHtml(estimation) {
  const money = formatCurrency;
  const requested = estimation.requestedAmount != null ? Number(estimation.requestedAmount) : null;
  return `<div class="auth">
  <div class="lbl">Monto autorizado</div>
  <div class="amt">${money(estimation.authorizedAmount)}</div>
  <div class="who">Autorizó: ${escapeHtml(estimation.approvedBy || '—')} · ${formatDate(estimation.approvedAt)}</div>
  ${requested !== null ? `<div>Solicitado por el contratista: ${money(requested)}</div>` : ''}
  <div>Avance calculado (a liberar): ${money(estimation.totalToPay)}</div>
  ${estimation.authorizationNote ? `<div>Motivo: ${escapeHtml(estimation.authorizationNote)}</div>` : ''}
</div>`;
}

function signatureHtml(estimation) {
  return `<div class="sign">
  <div><div class="line"></div>Firma de autorización<br><small>${escapeHtml(estimation.approvedBy || '')}</small></div>
</div>`;
}

// Hoja de autorización de UN presupuesto (estimaciones anteriores al flujo por proveedor).
export function buildAuthorizedSheetHtml(estimation, budget = {}, projectName = '') {
  return `<!doctype html><html lang="es"><head><meta charset="utf-8">
<title>Estimación ${escapeHtml(estimation.folio)} autorizada</title>
<style>${PDF_STYLE}</style></head><body>
${projectName ? `<div class="obra">${escapeHtml(projectName)}</div>` : ''}
<h1>Estimación #${escapeHtml(estimation.folio)} · ${escapeHtml(budget.supplierNameSnapshot || estimation.supplierName || '')}</h1>
<div class="sub">${escapeHtml(budget.name || estimation.budgetName || '')} · Periodo ${formatDate(estimation.periodStart)} – ${formatDate(estimation.periodEnd)}</div>
${authorizationBoxHtml(estimation)}
${sheetSectionHtml(estimation, budget)}
${signatureHtml(estimation)}
</body></html>`;
}

// Hoja de autorización de la estimación del PROVEEDOR: una hoja por presupuesto (departamento)
// y el monto autorizado total arriba.
export function buildAuthorizedBatchHtml(batch, budgetsById = {}, projectName = '') {
  const money = formatCurrency;
  const parts = batch.parts || [];
  const sections = parts.map((part) => {
    const budget = budgetsById[part.estimationBudgetId] || {};
    const discountNote = Number(budget.discountPct) > 0 ? ` <small>(precios con ${formatPct(budget.discountPct)} de descuento)</small>` : '';
    return `<div class="budget">Presupuesto: ${escapeHtml(budget.name || part.budgetName || '')}${discountNote}</div>
${sheetSectionHtml(part, budget)}`;
  }).join('');
  const summary = (parts.length > 1 || Number(batch.advanceGivenAmount) > 0) ? `<div class="summary"><h2>Resumen de la estimación</h2><div class="totals">
  <div>subtotal del periodo <strong>${money(batch.periodSubtotal)}</strong></div>
  <div>retención − <strong>${money(batch.retentionAmount)}</strong></div>
  ${Number(batch.advanceAmortizationAmount) > 0 ? `<div>amortización de anticipos − <strong>${money(batch.advanceAmortizationAmount)}</strong></div>` : ''}
  ${Number(batch.priorPaidApplied) > 0 ? `<div>pagos previos − <strong>${money(batch.priorPaidApplied)}</strong></div>` : ''}
  ${Number(batch.advanceGivenAmount) > 0 ? `<div>anticipo a entregar + <strong>${money(batch.advanceGivenAmount)}</strong></div>` : ''}
  <div>total a liberar <strong>${money(batch.totalToPay)}</strong></div></div></div>` : '';
  return `<!doctype html><html lang="es"><head><meta charset="utf-8">
<title>Estimación ${escapeHtml(batch.folio)} autorizada</title>
<style>${PDF_STYLE}</style></head><body>
${projectName ? `<div class="obra">${escapeHtml(projectName)}</div>` : ''}
<h1>Estimación #${escapeHtml(batch.folio)} · ${escapeHtml(batch.supplierName || '')}</h1>
<div class="sub">${parts.length > 1 ? `${parts.length} presupuestos · ` : `${escapeHtml(batch.budgetName || '')} · `}Periodo ${formatDate(batch.periodStart)} – ${formatDate(batch.periodEnd)}</div>
${authorizationBoxHtml(batch)}
${sections}
${summary}
${signatureHtml(batch)}
</body></html>`;
}

function openPrintWindow(html) {
  const win = window.open('', '_blank');
  if (!win) return false;
  win.document.open();
  win.document.write(html);
  win.document.close();
  win.focus();
  setTimeout(() => win.print(), 300);
  return true;
}

export function openAuthorizedSheet(estimation, budget, projectName = '') {
  return openPrintWindow(buildAuthorizedSheetHtml(estimation, budget, projectName));
}

export function openAuthorizedBatchSheet(batch, budgetsById, projectName = '') {
  return openPrintWindow(buildAuthorizedBatchHtml(batch, budgetsById, projectName));
}


// Unidades más comunes en presupuestos de obra (valor guardado, etiqueta del desplegable).
export const COMMON_UNITS = [
  { value: 'm2', label: 'm² · metro cuadrado' },
  { value: 'ml', label: 'ml · metro lineal' },
  { value: 'm', label: 'm · metro' },
  { value: 'm3', label: 'm³ · metro cúbico' },
  { value: 'pza', label: 'pza · pieza' },
  { value: 'lote', label: 'lote' },
  { value: 'jgo', label: 'jgo · juego' },
  { value: 'salida', label: 'salida' },
  { value: 'punto', label: 'punto' },
  { value: 'servicio', label: 'servicio' },
  { value: 'kg', label: 'kg · kilogramo' },
  { value: 'ton', label: 'ton · tonelada' },
  { value: 'lt', label: 'lt · litro' },
  { value: 'saco', label: 'saco' },
  { value: 'caja', label: 'caja' },
  { value: 'rollo', label: 'rollo' },
  { value: 'viaje', label: 'viaje' },
  { value: 'día', label: 'día · jornada' },
  { value: 'hora', label: 'hora' },
];

const UNIT_ALIASES = {
  'm²': 'm2', mt2: 'm2', mts2: 'm2', m2: 'm2',
  'm³': 'm3', mt3: 'm3', mts3: 'm3', m3: 'm3',
  ml: 'ml', mlineal: 'ml', 'mts lineales': 'ml', 'metro lineal': 'ml', 'metros lineales': 'ml',
  m: 'm', mt: 'm', mts: 'm', metro: 'm', metros: 'm',
  pza: 'pza', pzas: 'pza', pz: 'pza', pieza: 'pza', piezas: 'pza',
  lote: 'lote', lotes: 'lote',
  jgo: 'jgo', jgos: 'jgo', juego: 'jgo', juegos: 'jgo',
  salida: 'salida', salidas: 'salida', punto: 'punto', puntos: 'punto',
  servicio: 'servicio', servicios: 'servicio',
  kg: 'kg', kgs: 'kg', kilo: 'kg', kilos: 'kg', ton: 'ton', tons: 'ton', tonelada: 'ton', toneladas: 'ton',
  lt: 'lt', lts: 'lt', litro: 'lt', litros: 'lt',
  saco: 'saco', sacos: 'saco', caja: 'caja', cajas: 'caja', rollo: 'rollo', rollos: 'rollo',
  viaje: 'viaje', viajes: 'viaje', dia: 'día', dias: 'día', 'día': 'día', 'días': 'día', jornal: 'día', jornales: 'día',
  hora: 'hora', horas: 'hora', hr: 'hora', hrs: 'hora',
};

// Unifica las unidades que vienen de archivos o de texto pegado («m2.», «PZAS», «m²»…) con las del desplegable.
export function normalizeUnit(raw) {
  const text = String(raw || '').trim().toLowerCase().replace(/\.+$/, '').trim();
  if (!text) return '';
  return UNIT_ALIASES[text] || String(raw).trim().replace(/\.+$/, '').trim();
}
