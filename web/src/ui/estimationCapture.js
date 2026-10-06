// Cálculos de captura de avance (solo para la vista previa; el servidor recalcula al guardar).

export function todayIsoDate() {
  const now = new Date();
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 10);
}

export const round2 = (value) => Math.round((Number(value) || 0) * 100) / 100;

export function pctOf(quantity, contracted) {
  const base = Number(contracted) || 0;
  return base > 0 ? Math.round(((Number(quantity) || 0) / base) * 10000) / 100 : 0;
}

// Misma matemática que resolve_period_quantities del servidor.
export function computePeriodQuantity(mode, li, globalPct, groupPcts) {
  if (mode === 'quantity') return Number(li.periodQuantity) || 0;
  let pctRaw;
  if (mode === 'global') pctRaw = globalPct;
  else if (mode === 'group') pctRaw = (groupPcts || {})[li.group || ''];
  else pctRaw = li.progressPct;
  if (pctRaw === '' || pctRaw === null || pctRaw === undefined) return 0;
  const pct = Number(pctRaw) || 0;
  const target = ((Number(li.contractedQuantity) || 0) * pct) / 100;
  return Math.max(target - (Number(li.previousCumulativeQuantity) || 0), 0);
}

// Espejo de compute_estimation_money_fields del servidor.
export function computeEstimationPreview(budgetDetail, lineItemInputs, remainingBalanceOverride, mode = 'quantity', globalPct = '', remainingOpeningOverride, groupPcts) {
  const periodAmountOf = (li) => computePeriodQuantity(mode, li, globalPct, groupPcts) * (Number(li.unitPrice) || 0);
  const periodSubtotal = (lineItemInputs || []).reduce((sum, li) => sum + periodAmountOf(li), 0);
  const retentionPct = Number(budgetDetail?.retentionPct) || 0;
  const retentionAmount = (periodSubtotal * retentionPct) / 100;

  let advanceAmortizationAmount = 0;
  if (budgetDetail?.advanceAmortizationEnabled) {
    // Cada grupo amortiza su propio % de anticipo; sin anticipo por grupo, el % general.
    const groupRates = Object.fromEntries((budgetDetail?.groups || []).map((g) => [g.name, Number(g.advancePct) || 0]));
    const hasGroupRates = Object.values(groupRates).some((rate) => rate > 0);
    const uniformRate = Number(budgetDetail?.advancePct) || 0;
    const rawAmortization = (lineItemInputs || []).reduce(
      (sum, li) => sum + (periodAmountOf(li) * (hasGroupRates ? groupRates[li.group || ''] || 0 : uniformRate)) / 100,
      0,
    );
    const remainingBalance = Number(remainingBalanceOverride ?? budgetDetail?.remainingAdvanceBalance) || 0;
    advanceAmortizationAmount = Math.max(0, Math.min(rawAmortization, remainingBalance));
  }

  const netBeforePriorPayments = periodSubtotal - retentionAmount - advanceAmortizationAmount;
  // Pagos previos (saldo inicial) ya entregados: se descuentan de lo que se libera.
  const remainingOpening = Number(remainingOpeningOverride ?? budgetDetail?.remainingOpeningPaidBalance) || 0;
  const priorPaidApplied = Math.min(Math.max(netBeforePriorPayments, 0), Math.max(remainingOpening, 0));
  const totalToPay = netBeforePriorPayments - priorPaidApplied;
  return { periodSubtotal, retentionAmount, advanceAmortizationAmount, priorPaidApplied, totalToPay };
}

// Avance acumulado por concepto de un presupuesto según las estimaciones del proveedor
// (sin contar la que se está editando).
export function previousCumulativeForBudget(budgetId, batches, excludeBatchId = null) {
  const totals = {};
  (batches || []).forEach((batch) => {
    if (excludeBatchId && batch.id === excludeBatchId) return;
    (batch.parts || []).forEach((part) => {
      if (String(part.estimationBudgetId) !== String(budgetId)) return;
      (part.lineItems || []).forEach((li) => {
        totals[li.conceptoId] = (Number(totals[li.conceptoId]) || 0) + (Number(li.periodQuantity) || 0);
      });
    });
  });
  return totals;
}

// Un renglón por grupo del presupuesto, con lo que ya llevaba de avance.
export function buildGroupForm(budget, previousQtyByConceptoId, overlayProgress, previousAmountByGroup) {
  const concepts = budget?.lineItems || [];
  const groupNames = (budget?.groups || []).map((g) => g.name);
  const names = groupNames.length ? groupNames : Array.from(new Set(concepts.map((c) => c.group || '')));
  return names.map((name) => {
    const members = concepts.filter((c) => (c.group || '') === name);
    const meta = (budget?.groups || []).find((g) => g.name === name) || {};
    const budgetAmount = meta.budgetAmount ?? round2(members.reduce((sum, c) => sum + (Number(c.amount) || 0), 0));
    const previousAmount = previousAmountByGroup?.[name] ?? round2(
      members.reduce((sum, c) => sum + (Number(previousQtyByConceptoId?.[c.id]) || 0) * (Number(c.unitPrice) || 0), 0),
    );
    const previousPct = budgetAmount > 0 ? (previousAmount / budgetAmount) * 100 : 0;
    const overlay = (overlayProgress || []).find((entry) => (entry.group || '') === name);
    const pctExact = overlay ? Number(overlay.progressPct) || 0 : previousPct;
    return {
      name,
      budgetAmount,
      advancePct: Number(meta.advancePct) || 0,
      isExtra: Boolean(meta.isExtra),
      previousAmount,
      previousPct,
      pctExact,
      pct: String(Math.round(pctExact * 10000) / 10000),
      amount: ((budgetAmount * pctExact) / 100).toFixed(2),
      source: 'pct',
      touched: Boolean(overlay),
    };
  });
}

export function hasNamedGroups(budget) {
  return (budget?.groups || []).some((g) => g.name);
}

// Formulario de captura de UN presupuesto. Con `savedPart` (borrador existente) parte de lo ya capturado.
export function buildCaptureForm(budget, previousCumulativeByConceptoId, savedPart = null) {
  const concepts = budget?.lineItems || [];
  const base = (item) => ({
    conceptoId: item.id,
    description: item.description,
    unit: item.unit,
    group: item.group || '',
    unitPrice: item.unitPrice,
    contractedQuantity: item.quantity,
  });
  if (!savedPart) {
    return {
      captureMode: hasNamedGroups(budget) ? 'group' : 'global',
      globalProgressPct: '',
      groups: buildGroupForm(budget, previousCumulativeByConceptoId, null, null),
      lineItems: concepts.map((item) => {
        const previous = Number(previousCumulativeByConceptoId?.[item.id]) || 0;
        const previousPct = String(pctOf(previous, item.quantity));
        return { ...base(item), previousCumulativeQuantity: previous, previousProgressPct: previousPct, progressPct: previousPct, periodQuantity: '' };
      }),
    };
  }
  const mode = ['global', 'concept', 'quantity', 'group'].includes(savedPart.captureMode) ? savedPart.captureMode : 'quantity';
  const previousQty = {};
  (savedPart.lineItems || []).forEach((li) => { previousQty[li.conceptoId] = li.previousCumulativeQuantity; });
  const previousAmountByGroup = {};
  (savedPart.groupBreakdown || []).forEach((entry) => { previousAmountByGroup[entry.group || ''] = entry.previousAmount; });
  const savedById = new Map((savedPart.lineItems || []).map((li) => [li.conceptoId, li]));
  return {
    captureMode: mode,
    globalProgressPct: savedPart.globalProgressPct != null ? String(savedPart.globalProgressPct) : '',
    groups: buildGroupForm(budget, previousQty, savedPart.groupProgress, Object.keys(previousAmountByGroup).length ? previousAmountByGroup : null),
    // Se arma con los conceptos ACTUALES del presupuesto (con el avance que ya traía el borrador):
    // así los extras agregados después de crear el borrador también cuentan.
    lineItems: concepts.map((concept) => {
      const li = savedById.get(concept.id);
      if (!li) {
        return { ...base(concept), previousCumulativeQuantity: Number(previousCumulativeByConceptoId?.[concept.id]) || 0, previousProgressPct: '0', progressPct: '0', periodQuantity: '' };
      }
      return {
        ...base(concept),
        previousCumulativeQuantity: li.previousCumulativeQuantity,
        previousProgressPct: String(li.previousProgressPct ?? pctOf(li.previousCumulativeQuantity, li.contractedQuantity)),
        progressPct: String(li.progressPct ?? pctOf(li.cumulativeQuantity, li.contractedQuantity)),
        periodQuantity: String(li.periodQuantity ?? ''),
      };
    }),
  };
}

// Lo que se manda al servidor para este presupuesto (solo lo que el usuario movió).
export function buildCapturePayload(budgetId, form) {
  const payload = { estimationBudgetId: budgetId, captureMode: form.captureMode };
  if (form.captureMode === 'global') {
    payload.globalProgressPct = Number(form.globalProgressPct) || 0;
  } else if (form.captureMode === 'group') {
    payload.groupProgress = (form.groups || [])
      .filter((group) => group.touched)
      .map((group) => ({ group: group.name, progressPct: Number(group.pct) || 0 }));
  } else if (form.captureMode === 'concept') {
    payload.lineItems = form.lineItems
      .filter((li) => String(li.progressPct) !== String(li.previousProgressPct))
      .map((li) => ({ conceptoId: li.conceptoId, progressPct: Number(li.progressPct) || 0 }));
  } else {
    payload.lineItems = form.lineItems.map((li) => ({ conceptoId: li.conceptoId, periodQuantity: Number(li.periodQuantity) || 0 }));
  }
  return payload;
}
