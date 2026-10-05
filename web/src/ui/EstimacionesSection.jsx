import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';
import { ExtrasPanel } from './ExtrasPanel.jsx';
import { formatCurrency, formatDate, formatPct, groupLabel, openAuthorizedSheet, extraConceptsOfGroup } from './estimationShared.js';

const WORKFLOW_LABELS = {
  BORRADOR: 'Borrador',
  ENVIADA: 'Por autorizar',
  APROBADA: 'Aprobada',
  REGISTRADA: 'Registrada',
};

function describeEstimationStatus(estimation) {
  const workflow = estimation?.workflowStatus || 'REGISTRADA';
  if (workflow === 'APROBADA') {
    return estimation?.paymentStatus === 'PAGADA' ? 'Pagada' : 'Aprobada · Por pagar';
  }
  return WORKFLOW_LABELS[workflow] || workflow;
}

function statusBadgeStyle(estimation) {
  const workflow = estimation?.workflowStatus || 'REGISTRADA';
  if (workflow === 'APROBADA') {
    return estimation?.paymentStatus === 'PAGADA'
      ? { background: '#dcfce7', color: '#166534' }
      : { background: '#dbeafe', color: '#1e40af' };
  }
  if (workflow === 'ENVIADA') return { background: '#fef3c7', color: '#92400e' };
  if (workflow === 'BORRADOR') return { background: '#e5e7eb', color: '#374151' };
  return { background: '#f3f4f6', color: '#4b5563' };
}

function StatusBadge({ estimation }) {
  return (
    <span className="badge" style={statusBadgeStyle(estimation)}>
      {describeEstimationStatus(estimation)}
    </span>
  );
}

// Same math as the backend's resolve_period_quantities, for live preview only.
function computePeriodQuantity(mode, li, globalPct, groupPcts) {
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

function todayIsoDate() {
  const now = new Date();
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 10);
}

// Mirrors the backend's compute_estimation_money_fields — used only for
// live preview while typing; the authoritative values come back from the
// server response on save.
function computeEstimationPreview(budgetDetail, lineItemInputs, remainingBalanceOverride, mode = 'quantity', globalPct = '', remainingOpeningOverride, groupPcts) {
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

export function EstimacionesSection({ projects, selectedProjectId, isReviewer = false, initialBudgetId = null, onInitialBudgetConsumed, onOpenBudgets, onWorkflowChange, approvalProjectIds = null }) {
  const [view, setView] = useState('list');
  // Un admin puede tener asignadas solo algunas obras para autorizar (null = todas).
  const canApproveProject = (projectId) =>
    isReviewer && (approvalProjectIds === null || approvalProjectIds.includes(String(projectId || '')));
  const canApproveSelectedProject = canApproveProject(selectedProjectId);
  const [section, setSection] = useState('budgets');
  const [extrasOpen, setExtrasOpen] = useState(false);
  const [queueRows, setQueueRows] = useState([]);
  const [queueLoading, setQueueLoading] = useState(false);
  const [viewingEstimation, setViewingEstimation] = useState(null);
  const [authorizedAmount, setAuthorizedAmount] = useState('');
  const [authorizationNote, setAuthorizationNote] = useState('');
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [supplierFilter, setSupplierFilter] = useState('');
  const [includeInactive, setIncludeInactive] = useState(false);
  const [saving, setSaving] = useState(false);


  const [selectedBudgetId, setSelectedBudgetId] = useState(null);
  const [budgetDetail, setBudgetDetail] = useState(null);
  const [estimationsList, setEstimationsList] = useState([]);
  const [detailLoading, setDetailLoading] = useState(false);

  const [showEstimationForm, setShowEstimationForm] = useState(false);
  const [editingEstimation, setEditingEstimation] = useState(null);
  const [estimationForm, setEstimationForm] = useState(null);



  async function loadEstimationBudgets() {
    setLoading(true);
    setError('');
    try {
      const data = await api.estimationBudgets({
        projectId: selectedProjectId,
        supplier: supplierFilter,
        includeInactive: includeInactive ? 'true' : 'false',
      });
      setRows(Array.isArray(data) ? data : []);
    } catch (e) {
      setRows([]);
      setError(e.message || 'No se pudieron cargar los presupuestos de estimaciones');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!selectedProjectId) return;
    loadEstimationBudgets();
  }, [selectedProjectId, includeInactive]);

  useEffect(() => {
    setView('list');
    setSelectedBudgetId(null);
    setBudgetDetail(null);
    setEstimationsList([]);
    setShowEstimationForm(false);
    setEditingEstimation(null);
    setViewingEstimation(null);
  }, [selectedProjectId]);

  // Llegada desde Presupuestos ("Estimaciones →"): abre ese presupuesto directo.
  useEffect(() => {
    if (!initialBudgetId || !selectedProjectId) return;
    setSection('budgets');
    openBudgetDetail({ id: initialBudgetId });
    if (onInitialBudgetConsumed) onInitialBudgetConsumed();
  }, [initialBudgetId, selectedProjectId]);

  const projectsById = useMemo(
    () => new Map((Array.isArray(projects) ? projects : []).map((project) => [String(project?._id || ''), project])),
    [projects],
  );

  const listTotals = useMemo(
    () =>
      rows.reduce(
        (acc, row) => {
          acc.totalContractedAmount += Number(row.totalContractedAmount) || 0;
          acc.totalRetainedToDate += Number(row.totalRetainedToDate) || 0;
          acc.remainingAdvanceBalance += Number(row.remainingAdvanceBalance) || 0;
          acc.paidAmount += Number(row.paidAmount) || 0;
          acc.approvedProgressAmount += Number(row.approvedProgressAmount) || 0;
          return acc;
        },
        { totalContractedAmount: 0, totalRetainedToDate: 0, remainingAdvanceBalance: 0, paidAmount: 0, approvedProgressAmount: 0 },
      ),
    [rows],
  );

  const listProgressPct = listTotals.totalContractedAmount > 0
    ? (listTotals.approvedProgressAmount / listTotals.totalContractedAmount) * 100
    : 0;
  const listPaidPct = listTotals.totalContractedAmount > 0
    ? (listTotals.paidAmount / listTotals.totalContractedAmount) * 100
    : 0;
  const activeBudgetsCount = rows.filter((row) => row.isActive !== false).length;

  async function loadBudgetDetail(budgetId) {
    setDetailLoading(true);
    setError('');
    try {
      const [detail, estimations] = await Promise.all([api.getEstimationBudget(budgetId), api.estimations(budgetId)]);
      setBudgetDetail(detail);
      setEstimationsList(Array.isArray(estimations) ? estimations : []);
    } catch (e) {
      setError(e.message || 'No se pudo cargar el presupuesto');
    } finally {
      setDetailLoading(false);
    }
  }

  async function openBudgetDetail(row) {
    setSelectedBudgetId(row.id);
    setView('detail');
    setShowEstimationForm(false);
    setEditingEstimation(null);
    await loadBudgetDetail(row.id);
  }

  function backToList() {
    setExtrasOpen(false);
    setView('list');
    setSelectedBudgetId(null);
    setBudgetDetail(null);
    setEstimationsList([]);
    setShowEstimationForm(false);
    setEditingEstimation(null);
  }

  function pctOf(quantity, contracted) {
    const base = Number(contracted) || 0;
    return base > 0 ? Math.round(((Number(quantity) || 0) / base) * 10000) / 100 : 0;
  }

  const round2 = (value) => Math.round((Number(value) || 0) * 100) / 100;

  // Un renglón por grupo del presupuesto, con lo que ya llevaba de avance.
  function buildGroupForm(previousQtyByConceptoId, overlayProgress, previousAmountByGroup) {
    const concepts = budgetDetail?.lineItems || [];
    const groupNames = (budgetDetail?.groups || []).map((g) => g.name);
    const names = groupNames.length ? groupNames : Array.from(new Set(concepts.map((c) => c.group || '')));
    return names.map((name) => {
      const members = concepts.filter((c) => (c.group || '') === name);
      const meta = (budgetDetail?.groups || []).find((g) => g.name === name) || {};
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

  // El avance por grupo se captura solo en %; el monto es una consecuencia que se muestra.
  function updateEstimationGroup(name, value) {
    setEstimationForm((prev) => ({
      ...prev,
      groups: (prev.groups || []).map((group) => {
        if (group.name !== name) return group;
        const budget = Number(group.budgetAmount) || 0;
        const pct = Number(value);
        const exact = Number.isFinite(pct) ? pct : 0;
        return { ...group, pct: value, pctExact: exact, amount: value === '' ? '' : ((budget * exact) / 100).toFixed(2), source: 'pct', touched: true };
      }),
    }));
  }

  const hasNamedGroups = (budgetDetail?.groups || []).some((g) => g.name);

  function buildEstimationFormFromBudget(previousCumulativeByConceptoId) {
    const today = todayIsoDate();
    return {
      periodStart: today,
      periodEnd: today,
      notes: '',
      requestedAmount: '',
      captureMode: hasNamedGroups ? 'group' : 'global',
      globalProgressPct: '',
      groups: buildGroupForm(previousCumulativeByConceptoId, null, null),
      lineItems: (budgetDetail?.lineItems || []).map((item) => {
        const previous = Number(previousCumulativeByConceptoId?.[item.id]) || 0;
        const previousPct = String(pctOf(previous, item.quantity));
        return {
          conceptoId: item.id,
          description: item.description,
          unit: item.unit,
          group: item.group || '',
          unitPrice: item.unitPrice,
          contractedQuantity: item.quantity,
          previousCumulativeQuantity: previous,
          previousProgressPct: previousPct,
          progressPct: previousPct,
          periodQuantity: '',
        };
      }),
    };
  }

  function startCreateEstimation() {
    const latest = estimationsList[estimationsList.length - 1];
    const previousCumulativeByConceptoId = {};
    if (latest) {
      (latest.lineItems || []).forEach((li) => {
        previousCumulativeByConceptoId[li.conceptoId] = li.cumulativeQuantity;
      });
    }
    setViewingEstimation(null);
    setEditingEstimation(null);
    setEstimationForm(buildEstimationFormFromBudget(previousCumulativeByConceptoId));
    setShowEstimationForm(true);
  }

  function startEditEstimation(estimation) {
    const mode = ['global', 'concept', 'quantity', 'group'].includes(estimation.captureMode) ? estimation.captureMode : 'quantity';
    const previousQty = {};
    (estimation.lineItems || []).forEach((li) => { previousQty[li.conceptoId] = li.previousCumulativeQuantity; });
    const previousAmountByGroup = {};
    (estimation.groupBreakdown || []).forEach((entry) => { previousAmountByGroup[entry.group || ''] = entry.previousAmount; });
    setViewingEstimation(null);
    setEditingEstimation(estimation);
    setEstimationForm({
      periodStart: estimation.periodStart || '',
      periodEnd: estimation.periodEnd || '',
      notes: estimation.notes || '',
      requestedAmount: estimation.requestedAmount != null ? String(estimation.requestedAmount) : '',
      captureMode: mode,
      globalProgressPct: estimation.globalProgressPct != null ? String(estimation.globalProgressPct) : '',
      groups: buildGroupForm(previousQty, estimation.groupProgress, Object.keys(previousAmountByGroup).length ? previousAmountByGroup : null),
      // Se arma con los conceptos ACTUALES del presupuesto (con el avance que ya traía el
      // borrador): así los extras agregados después de crear el borrador también cuentan.
      lineItems: (() => {
        const savedById = new Map((estimation.lineItems || []).map((li) => [li.conceptoId, li]));
        const concepts = budgetDetail?.lineItems || [];
        if (!concepts.length) {
          return (estimation.lineItems || []).map((li) => ({
            conceptoId: li.conceptoId,
            description: li.description,
            unit: li.unit,
            group: li.group || '',
            unitPrice: li.unitPrice,
            contractedQuantity: li.contractedQuantity,
            previousCumulativeQuantity: li.previousCumulativeQuantity,
            previousProgressPct: String(li.previousProgressPct ?? pctOf(li.previousCumulativeQuantity, li.contractedQuantity)),
            progressPct: String(li.progressPct ?? pctOf(li.cumulativeQuantity, li.contractedQuantity)),
            periodQuantity: String(li.periodQuantity ?? ''),
          }));
        }
        return concepts.map((concept) => {
          const li = savedById.get(concept.id);
          const base = {
            conceptoId: concept.id,
            description: concept.description,
            unit: concept.unit,
            group: concept.group || '',
            unitPrice: concept.unitPrice,
            contractedQuantity: concept.quantity,
          };
          if (!li) {
            // concepto agregado después (extra): sin historial en este borrador
            return { ...base, previousCumulativeQuantity: 0, previousProgressPct: '0', progressPct: '0', periodQuantity: '' };
          }
          return {
            ...base,
            previousCumulativeQuantity: li.previousCumulativeQuantity,
            previousProgressPct: String(li.previousProgressPct ?? pctOf(li.previousCumulativeQuantity, li.contractedQuantity)),
            progressPct: String(li.progressPct ?? pctOf(li.cumulativeQuantity, li.contractedQuantity)),
            periodQuantity: String(li.periodQuantity ?? ''),
          };
        });
      })(),
    });
    setShowEstimationForm(true);
  }

  function resetEstimationForm() {
    setShowEstimationForm(false);
    setEditingEstimation(null);
    setEstimationForm(null);
  }

  function updateEstimationLine(conceptoId, field, value) {
    setEstimationForm((prev) => ({
      ...prev,
      lineItems: prev.lineItems.map((li) => (li.conceptoId === conceptoId ? { ...li, [field]: value } : li)),
    }));
  }

  const estimationPreview = useMemo(() => {
    if (!estimationForm || !budgetDetail) return null;
    const remainingOverride = editingEstimation
      ? (Number(budgetDetail.remainingAdvanceBalance) || 0) + (Number(editingEstimation.advanceAmortizationAmount) || 0)
      : undefined;
    const remainingOpeningOverride = editingEstimation
      ? (Number(budgetDetail.remainingOpeningPaidBalance) || 0) + (Number(editingEstimation.priorPaidApplied) || 0)
      : undefined;
    const groupPcts = Object.fromEntries((estimationForm.groups || []).map((group) => [group.name, group.pctExact]));
    return computeEstimationPreview(
      budgetDetail,
      estimationForm.lineItems,
      remainingOverride,
      estimationForm.captureMode,
      estimationForm.globalProgressPct,
      remainingOpeningOverride,
      groupPcts,
    );
  }, [estimationForm, budgetDetail, editingEstimation]);

  function buildEstimationPayload() {
    const payload = {
      periodStart: estimationForm.periodStart,
      periodEnd: estimationForm.periodEnd,
      notes: estimationForm.notes,
      requestedAmount: estimationForm.requestedAmount === '' ? null : Number(estimationForm.requestedAmount),
      captureMode: estimationForm.captureMode,
    };
    if (estimationForm.captureMode === 'global') {
      payload.globalProgressPct = Number(estimationForm.globalProgressPct) || 0;
    } else if (estimationForm.captureMode === 'group') {
      // Solo los grupos que el usuario movió, siempre en % (no se captura por monto).
      payload.groupProgress = (estimationForm.groups || [])
        .filter((group) => group.touched)
        .map((group) => ({ group: group.name, progressPct: Number(group.pct) || 0 }));
    } else if (estimationForm.captureMode === 'concept') {
      // Solo se mandan los conceptos que el usuario movio; el resto no avanza.
      payload.lineItems = estimationForm.lineItems
        .filter((li) => String(li.progressPct) !== String(li.previousProgressPct))
        .map((li) => ({ conceptoId: li.conceptoId, progressPct: Number(li.progressPct) || 0 }));
    } else {
      payload.lineItems = estimationForm.lineItems.map((li) => ({
        conceptoId: li.conceptoId,
        periodQuantity: Number(li.periodQuantity) || 0,
      }));
    }
    return payload;
  }

  async function submitEstimationForm(event, { sendForReview = false } = {}) {
    event?.preventDefault?.();
    if (!selectedBudgetId || !estimationForm) return;
    if (estimationForm.captureMode === 'concept' && !buildEstimationPayload().lineItems.length) {
      setError('Captura el avance de al menos un concepto.');
      return;
    }
    if (estimationForm.captureMode === 'group' && !buildEstimationPayload().groupProgress.length) {
      setError('Captura el avance de al menos un grupo.');
      return;
    }
    setSaving(true);
    setError('');
    try {
      const payload = buildEstimationPayload();
      if (editingEstimation) {
        await api.updateEstimation(selectedBudgetId, editingEstimation.id, payload);
        if (sendForReview) await api.submitEstimation(selectedBudgetId, editingEstimation.id);
      } else {
        await api.createEstimation(selectedBudgetId, { ...payload, submit: sendForReview });
      }
      await loadBudgetDetail(selectedBudgetId);
      await loadEstimationBudgets();
      await loadQueue();
      if (onWorkflowChange) onWorkflowChange();
      resetEstimationForm();
    } catch (e) {
      setError(e.message || 'No se pudo guardar la estimación');
    } finally {
      setSaving(false);
    }
  }

  async function runEstimationAction(action, estimation, ...args) {
    setSaving(true);
    setError('');
    try {
      await action(estimation.estimationBudgetId || selectedBudgetId, estimation.id, ...args);
      if (selectedBudgetId) await loadBudgetDetail(selectedBudgetId);
      await loadEstimationBudgets();
      await loadQueue();
      if (onWorkflowChange) onWorkflowChange();
      setViewingEstimation(null);
    } catch (e) {
      setError(e.message || 'No se pudo completar la acción');
    } finally {
      setSaving(false);
    }
  }

  function sendDraftForReview(estimation) {
    if (!window.confirm(`¿Enviar la estimación #${estimation.folio} a revisión? Ya no podrás editarla.`)) return;
    runEstimationAction(api.submitEstimation, estimation);
  }

  function approveViewingEstimation() {
    const estimation = viewingEstimation;
    if (!estimation) return;
    const calculated = Number(estimation.totalToPay) || 0;
    const requested = estimation.requestedAmount != null ? Number(estimation.requestedAmount) : null;
    const baseline = requested ?? calculated;
    const amount = authorizedAmount === '' ? baseline : Number(authorizedAmount);
    if (!Number.isFinite(amount) || amount < 0) {
      setError('El monto autorizado no es válido.');
      return;
    }
    if (requested !== null && amount > requested + 0.004) {
      setError('El monto autorizado no puede exceder lo solicitado por el contratista.');
      return;
    }
    if ((Math.abs(amount - baseline) >= 0.01 || amount - calculated >= 0.01) && !authorizationNote.trim()) {
      setError(
        amount < baseline
          ? 'Indica el motivo por el que se autoriza menos de lo solicitado.'
          : 'Indica el motivo por el que se autoriza más de lo que marca el avance.',
      );
      return;
    }
    runEstimationAction(api.approveEstimation, estimation, {
      authorizedAmount: amount,
      authorizationNote: authorizationNote.trim(),
    });
  }

  function returnViewingEstimation() {
    const estimation = viewingEstimation;
    if (!estimation) return;
    const reason = window.prompt('Motivo de la devolución (se le mostrará a quien capturó):');
    if (!reason || !reason.trim()) return;
    runEstimationAction(api.returnEstimation, estimation, { reason: reason.trim() });
  }

  function openEstimationView(estimation) {
    setShowEstimationForm(false);
    setEditingEstimation(null);
    setViewingEstimation(estimation);
    setAuthorizedAmount(String(estimation.authorizedAmount ?? estimation.requestedAmount ?? estimation.totalToPay ?? ''));
    setAuthorizationNote(estimation.authorizationNote || '');
  }

  async function loadQueue() {
    if (!selectedProjectId) return;
    setQueueLoading(true);
    try {
      let params;
      if (section === 'review') params = { projectId: selectedProjectId, status: 'ENVIADA' };
      else if (section === 'payable') params = { projectId: selectedProjectId, status: 'APROBADA', paymentStatus: 'POR_PAGAR' };
      else if (section === 'drafts') params = { projectId: selectedProjectId, status: 'BORRADOR,ENVIADA' };
      else {
        setQueueRows([]);
        return;
      }
      const data = await api.estimationsQueue(params);
      setQueueRows(Array.isArray(data?.items) ? data.items : []);
    } catch (e) {
      setQueueRows([]);
      setError(e.message || 'No se pudo cargar la bandeja de estimaciones');
    } finally {
      setQueueLoading(false);
    }
  }

  useEffect(() => {
    loadQueue();
  }, [section, selectedProjectId]);

  // Contador del tab "Por autorizar" aunque se este en otra seccion.
  const [pendingReviewCount, setPendingReviewCount] = useState(0);
  useEffect(() => {
    if (!isReviewer || !selectedProjectId || !canApproveSelectedProject) {
      setPendingReviewCount(0);
      return;
    }
    api.estimationsQueue({ projectId: selectedProjectId, status: 'ENVIADA' })
      .then((data) => setPendingReviewCount(Array.isArray(data?.items) ? data.items.length : 0))
      .catch(() => setPendingReviewCount(0));
  }, [isReviewer, selectedProjectId, canApproveSelectedProject, estimationsList, section]);

  async function openEstimationFromQueue(row) {
    setSection('budgets');
    setSelectedBudgetId(row.estimationBudgetId);
    setView('detail');
    setShowEstimationForm(false);
    setEditingEstimation(null);
    await loadBudgetDetail(row.estimationBudgetId);
    openEstimationView(row);
  }

  function changeEstimationFolio(estimation) {
    const raw = window.prompt(
      `Número de la estimación #${estimation.folio}. Las siguientes continuarán a partir del número más alto.`,
      String(estimation.folio),
    );
    if (raw === null || raw.trim() === '' || Number(raw) === Number(estimation.folio)) return;
    runEstimationAction(api.setEstimationFolio, estimation, Number(raw));
  }

  // Abre la hoja de autorización (imprimir / guardar como PDF). Desde la cola se carga primero el detalle.
  async function printAuthorized(estimation) {
    setError('');
    try {
      let budget = budgetDetail && budgetDetail.id === estimation.estimationBudgetId ? budgetDetail : null;
      let full = estimation.groupBreakdown ? estimation : null;
      if (!budget) budget = await api.getEstimationBudget(estimation.estimationBudgetId);
      if (!full) {
        const list = await api.estimations(estimation.estimationBudgetId);
        full = (Array.isArray(list) ? list : list?.items || []).find((e) => e.id === estimation.id) || estimation;
      }
      if (!openAuthorizedSheet({ ...estimation, ...full }, budget, (() => { const proj = (projects || []).find((p) => String(p._id) === String(selectedProjectId)); return proj?.displayName || proj?.name || ''; })())) {
        setError('El navegador bloqueó la ventana. Permite ventanas emergentes para generar el PDF.');
      }
    } catch (err) {
      setError(err?.message || 'No se pudo generar el PDF.');
    }
  }

  async function deleteEstimationRow(estimation) {
    const confirmed = window.confirm(`¿Eliminar la estimación #${estimation.folio}? Esta acción no se puede deshacer.`);
    if (!confirmed) return;
    setSaving(true);
    setError('');
    try {
      await api.deleteEstimation(selectedBudgetId, estimation.id);
      await loadBudgetDetail(selectedBudgetId);
      await loadEstimationBudgets();
      await loadQueue();
    } catch (e) {
      setError(e.message || 'No se pudo eliminar la estimación');
    } finally {
      setSaving(false);
    }
  }

  const budgetPending = budgetDetail?.approvalStatus === 'PENDIENTE';
  const hasOpenEstimation = estimationsList.some((row) => row.workflowStatus === 'BORRADOR' || row.workflowStatus === 'ENVIADA');

  return (
    <div style={{ display: 'grid', gap: 14 }}>
      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}

      <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
        <button type="button" className={section === 'budgets' ? '' : 'secondary'} onClick={() => setSection('budgets')}>
          Presupuestos
        </button>
        {isReviewer ? (
          <>
            <button type="button" className={section === 'review' ? '' : 'secondary'} onClick={() => setSection('review')}>
              Por autorizar{pendingReviewCount > 0 ? ` (${pendingReviewCount})` : ''}
            </button>
            <button type="button" className={section === 'payable' ? '' : 'secondary'} onClick={() => setSection('payable')}>
              Por pagar
            </button>
          </>
        ) : (
          <button type="button" className={section === 'drafts' ? '' : 'secondary'} onClick={() => setSection('drafts')}>
            Mis estimaciones abiertas
          </button>
        )}
      </div>

      {section !== 'budgets' ? (
        <div className="card" style={{ overflow: 'hidden' }}>
          <div className="card-header">
            <strong>
              {section === 'review' && 'Estimaciones por autorizar'}
              {section === 'payable' && 'Estimaciones aprobadas por pagar'}
              {section === 'drafts' && 'Borradores y estimaciones en revisión'}
            </strong>
            <div style={{ flex: 1 }} />
            <button type="button" className="secondary" onClick={loadQueue}>Actualizar</button>
          </div>
          {section === 'review' && !canApproveSelectedProject && (
            <div className="small" style={{ padding: 12, background: '#fef3c7', color: '#92400e' }}>
              Las estimaciones de esta obra las autoriza otro admin: aquí puedes verlas, pero no aprobarlas ni devolverlas.
            </div>
          )}
          {section === 'payable' && (
            <div className="small" style={{ padding: 12, color: '#475569' }}>
              Se marcan como pagadas solas cuando se importa el pago del proveedor y cubre el monto autorizado.
            </div>
          )}
          {queueLoading ? (
            <div className="small" style={{ padding: 16 }}>Cargando...</div>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table>
                <thead>
                  <tr>
                    <th>Proveedor</th>
                    <th>Presupuesto</th>
                    <th>Folio</th>
                    <th>Periodo</th>
                    <th>Avance acum.</th>
                    <th>Total calculado</th>
                    <th>Solicitado</th>
                    {section === 'payable' && <th>Autorizado</th>}
                    <th>Estatus</th>
                    <th>Acciones</th>
                  </tr>
                </thead>
                <tbody>
                  {queueRows.map((row) => (
                    <tr key={row.id}>
                      <td>{row.supplierName || '—'}</td>
                      <td>{row.budgetName || '—'}</td>
                      <td>#{row.folio}</td>
                      <td>{formatDate(row.periodStart)} – {formatDate(row.periodEnd)}</td>
                      <td>{formatPct(row.cumulativeProgressPct)}</td>
                      <td>{formatCurrency(row.totalToPay)}</td>
                      <td>{row.requestedAmount != null ? formatCurrency(row.requestedAmount) : '—'}</td>
                      {section === 'payable' && <td><strong>{formatCurrency(row.authorizedAmount)}</strong></td>}
                      <td><StatusBadge estimation={row} /></td>
                      <td>
                        <div className="row" style={{ gap: 6 }}>
                          <button type="button" onClick={() => openEstimationFromQueue(row)}>
                            {section === 'review' && canApproveProject(row.projectId) ? 'Revisar' : section === 'review' ? 'Ver' : 'Abrir'}
                          </button>
                          {section === 'payable' && (
                            <button type="button" className="secondary" onClick={() => printAuthorized(row)}>
                              PDF autorizado
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                  {!queueRows.length && (
                    <tr>
                      <td colSpan={section === 'payable' ? 10 : 9} className="small" style={{ textAlign: 'center' }}>
                        {section === 'review' && 'No hay estimaciones esperando autorización.'}
                        {section === 'payable' && 'No hay estimaciones aprobadas pendientes de pago.'}
                        {section === 'drafts' && 'No hay borradores ni estimaciones en revisión.'}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ) : view === 'list' ? (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div>
                <div className="kpi-label">Total contratado</div>
                <div className="kpi-value">{formatCurrency(listTotals.totalContractedAmount)}</div>
                <div className="kpi-sub">en presupuestos por conceptos</div>
              </div>
            </div>
            {isReviewer && (
              <div className="kpi-card">
                <div>
                  <div className="kpi-label">Total pagado</div>
                  <div className="kpi-value">{formatCurrency(listTotals.paidAmount)}</div>
                  <div className="kpi-sub">egresos ligados a estos presupuestos</div>
                </div>
              </div>
            )}
            {isReviewer && (
              <div className="kpi-card">
                <div>
                  <div className="kpi-label">% pagado</div>
                  <div className="kpi-value">{formatPct(listPaidPct)}</div>
                  <div className="kpi-sub">pagado / contratado, sin importar el avance estimado</div>
                </div>
              </div>
            )}
            {isReviewer && (
              <div className="kpi-card">
                <div>
                  <div className="kpi-label">Saldo por pagar</div>
                  <div className="kpi-value">{formatCurrency(listTotals.totalContractedAmount - listTotals.paidAmount)}</div>
                  <div className="kpi-sub">contratado − pagado</div>
                </div>
              </div>
            )}
            <div className="kpi-card">
              <div>
                <div className="kpi-label">% de avance estimado</div>
                <div className="kpi-value">{formatPct(listProgressPct)}</div>
                <div className="kpi-sub">{formatCurrency(listTotals.approvedProgressAmount)} en estimaciones aprobadas</div>
              </div>
            </div>
            <div className="kpi-card">
              <div>
                <div className="kpi-label">Retenido a la fecha</div>
                <div className="kpi-value">{formatCurrency(listTotals.totalRetainedToDate)}</div>
                <div className="kpi-sub">fondo de garantía acumulado</div>
              </div>
            </div>
            <div className="kpi-card">
              <div>
                <div className="kpi-label">Anticipo pendiente</div>
                <div className="kpi-value">{formatCurrency(listTotals.remainingAdvanceBalance)}</div>
                <div className="kpi-sub">saldo por amortizar</div>
              </div>
            </div>
            <div className="kpi-card">
              <div>
                <div className="kpi-label">Número de presupuestos</div>
                <div className="kpi-value">{rows.length}</div>
                <div className="kpi-sub">{includeInactive ? `${activeBudgetsCount} activos · ${rows.length - activeBudgetsCount} inactivos` : 'activos'}</div>
              </div>
            </div>
          </div>

          <div className="card" style={{ overflow: 'hidden' }}>
            <div className="card-header">
              <div className="search-input-wrap" style={{ maxWidth: 360 }}>
                <input
                  className="search-input"
                  value={supplierFilter}
                  onChange={(e) => setSupplierFilter(e.target.value)}
                  placeholder="Filtrar por proveedor"
                />
              </div>
              <button type="button" className="secondary" onClick={loadEstimationBudgets}>Buscar</button>
              <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
                <input type="checkbox" checked={includeInactive} onChange={(e) => setIncludeInactive(e.target.checked)} />
                Mostrar inactivos
              </label>
              <div style={{ flex: 1 }} />
              {isReviewer && onOpenBudgets && (
                <button type="button" className="secondary" onClick={onOpenBudgets} style={{ fontSize: 13 }}>
                  Capturar presupuestos →
                </button>
              )}
            </div>

            {loading ? (
              <div className="small" style={{ padding: 16 }}>Cargando presupuestos...</div>
            ) : (
              <div style={{ overflowX: 'auto' }}>
                <table>
                  <thead>
                    <tr>
                      <th>Proveedor</th>
                      <th>Presupuesto</th>
                      <th>Total contratado</th>
                      {isReviewer && <th>Pagado</th>}
                      <th>Anticipo</th>
                      <th>% Retención</th>
                      <th># Estimaciones</th>
                      <th>Estado</th>
                      <th>Acciones</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => (
                      <tr key={row.id}>
                        <td>{row.supplierNameSnapshot || row.supplierKey}</td>
                        <td>{row.name || '—'}</td>
                        <td>{formatCurrency(row.totalContractedAmount)}</td>
                        {isReviewer && <td>{formatCurrency(row.paidAmount)}</td>}
                        <td>{formatCurrency(row.advanceAmount)}</td>
                        <td>{formatPct(row.retentionPct)}</td>
                        <td>{row.estimationsCount}</td>
                        <td>{row.isActive === false ? 'Inactivo' : 'Activo'}</td>
                        <td>
                          <div className="row" style={{ gap: 6 }}>
                            <button type="button" onClick={() => openBudgetDetail(row)}>Estimar / ver</button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!rows.length && (
                      <tr>
                        <td colSpan={9} className="small" style={{ textAlign: 'center' }}>
                          No hay presupuestos para los filtros seleccionados. Los presupuestos se capturan en la pestaña Presupuestos.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      ) : (
        <>
          <div>
            <button type="button" className="secondary" onClick={backToList}>← Volver a presupuestos</button>
          </div>

          {detailLoading || !budgetDetail ? (
            <div className="small">Cargando presupuesto...</div>
          ) : (
            <>
              <div className="kpi-grid">
                <div className="kpi-card">
                  <div>
                    <div className="kpi-label">{budgetDetail.name || budgetDetail.supplierNameSnapshot}</div>
                    <div className="kpi-value">{formatCurrency(budgetDetail.totalContractedAmount)}</div>
                    <div className="kpi-sub">total contratado</div>
                  </div>
                </div>
                {isReviewer && (
                  <div className="kpi-card">
                    <div>
                      <div className="kpi-label">Pagado</div>
                      <div className="kpi-value">{formatCurrency(budgetDetail.paidAmount)}</div>
                      <div className="kpi-sub">saldo: {formatCurrency(budgetDetail.remainingToPayAmount)}</div>
                    </div>
                  </div>
                )}
                <div className="kpi-card">
                  <div>
                    <div className="kpi-label">Retenido a la fecha</div>
                    <div className="kpi-value">{formatCurrency(budgetDetail.totalRetainedToDate)}</div>
                    <div className="kpi-sub">{formatPct(budgetDetail.retentionPct)} por estimación</div>
                  </div>
                </div>
                <div className="kpi-card">
                  <div>
                    <div className="kpi-label">Saldo de anticipo</div>
                    <div className="kpi-value">{formatCurrency(budgetDetail.remainingAdvanceBalance)}</div>
                    <div className="kpi-sub">
                      {budgetDetail.advanceAmortizationEnabled ? `de ${formatCurrency(budgetDetail.advanceAmount)}` : 'amortización desactivada'}
                    </div>
                  </div>
                </div>
                <div className="kpi-card">
                  <div>
                    <div className="kpi-label">Estimaciones</div>
                    <div className="kpi-value">{budgetDetail.estimationsCount}</div>
                    <div className="kpi-sub">{budgetDetail.isActive === false ? 'presupuesto inactivo' : 'presupuesto activo'}</div>
                  </div>
                </div>
              </div>

              {isReviewer && !hasNamedGroups && (budgetDetail.lineItems || []).length > 1 && estimationsList.length === 0 && (
                <div className="card small" style={{ padding: 12, background: '#fef3c7', color: '#92400e' }}>
                  Este presupuesto no tiene <strong>grupos</strong> de conceptos (tuberías, bajadas, cuarto de bombas...), por eso solo se puede estimar por
                  concepto o en global. Para estimar por grupo, agrúpalos en Presupuestos → Editar (puedes marcar conceptos y asignarles el grupo, o
                  usar «Agrupar desde archivo» con el Word/Excel del contratista).
                  {onOpenBudgets && (
                    <> <button type="button" className="secondary" onClick={onOpenBudgets} style={{ marginLeft: 8 }}>Ir a Presupuestos →</button></>
                  )}
                </div>
              )}

              {extrasOpen && isReviewer && (
                <ExtrasPanel
                  budget={budgetDetail}
                  onClose={() => setExtrasOpen(false)}
                  onSaved={async () => {
                    setExtrasOpen(false);
                    await loadBudgetDetail(budgetDetail.id);
                    await loadEstimationBudgets();
                  }}
                />
              )}

              <div className="card" style={{ overflow: 'hidden' }}>
                <div className="card-header">
                  <strong>{budgetDetail.supplierNameSnapshot}</strong>
                  <div style={{ flex: 1 }} />
                  {isReviewer && (
                    <button type="button" className="secondary" onClick={() => setExtrasOpen((open) => !open)} title="Agregar conceptos extra o un presupuesto adicional">
                      + Extras
                    </button>
                  )}
                  {isReviewer && onOpenBudgets && (
                    <button type="button" className="secondary" onClick={onOpenBudgets} title="Editar el presupuesto, asignar pagos o registrar el saldo inicial">
                      Administrar en Presupuestos
                    </button>
                  )}
                  {!showEstimationForm && (
                    <button
                      type="button"
                      onClick={startCreateEstimation}
                      disabled={budgetDetail.isActive === false || hasOpenEstimation || budgetPending}
                      title={budgetPending ? 'El presupuesto está pendiente de autorización' : hasOpenEstimation ? 'Hay una estimación abierta; ciérrala (aprobada) antes de crear otra' : undefined}
                    >
                      + Nueva estimación
                    </button>
                  )}
                </div>
                {budgetPending && (
                  <div className="small" style={{ background: '#fef3c7', color: '#92400e', borderRadius: 6, padding: 10 }}>
                    Este presupuesto está pendiente de autorización{budgetDetail.reauthRequired ? ' (se modificó después de autorizado)' : ''}. Un admin debe autorizarlo en Presupuestos antes de estimar.
                  </div>
                )}

                <div style={{ overflowX: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Folio</th>
                        <th>Periodo</th>
                        <th>Avance acum.</th>
                        <th>Subtotal</th>
                        <th>Retención</th>
                        <th>Amortización anticipo</th>
                        <th>Total calculado</th>
                        <th>Solicitado</th>
                        <th>Autorizado</th>
                        <th>Estatus</th>
                        <th>Acciones</th>
                      </tr>
                    </thead>
                    <tbody>
                      {estimationsList.map((estimation) => {
                        const workflow = estimation.workflowStatus || 'REGISTRADA';
                        const canEdit = workflow === 'BORRADOR' || (workflow === 'REGISTRADA' && isReviewer && estimation.isLatest);
                        const canDelete = (workflow === 'BORRADOR' || (workflow === 'REGISTRADA' && isReviewer)) && estimation.isLatest;
                        return (
                          <tr key={estimation.id}>
                            <td>#{estimation.folio}</td>
                            <td>{formatDate(estimation.periodStart)} – {formatDate(estimation.periodEnd)}</td>
                            <td>{estimation.cumulativeProgressPct != null ? formatPct(estimation.cumulativeProgressPct) : '—'}</td>
                            <td>{formatCurrency(estimation.periodSubtotal)}</td>
                            <td>{formatCurrency(estimation.retentionAmount)}</td>
                            <td>{formatCurrency(estimation.advanceAmortizationAmount)}</td>
                            <td>{formatCurrency(estimation.totalToPay)}</td>
                            <td>{estimation.requestedAmount != null ? formatCurrency(estimation.requestedAmount) : '—'}</td>
                            <td>
                              {workflow === 'APROBADA' ? <strong>{formatCurrency(estimation.authorizedAmount)}</strong> : '—'}
                            </td>
                            <td><StatusBadge estimation={estimation} /></td>
                            <td>
                              <div className="row" style={{ gap: 6, flexWrap: 'wrap' }}>
                                <button type="button" className="secondary" onClick={() => openEstimationView(estimation)}>
                                  {workflow === 'ENVIADA' && canApproveProject(estimation.projectId || budgetDetail?.projectId) ? 'Revisar' : 'Ver'}
                                </button>
                                {isReviewer && (
                                  <button type="button" className="secondary" onClick={() => changeEstimationFolio(estimation)}>
                                    Cambiar nº
                                  </button>
                                )}
                                {workflow === 'APROBADA' && (
                                  <button type="button" className="secondary" onClick={() => printAuthorized(estimation)}>
                                    PDF autorizado
                                  </button>
                                )}
                                {canEdit && (
                                  <button type="button" className="secondary" onClick={() => startEditEstimation(estimation)}>
                                    Editar
                                  </button>
                                )}
                                {workflow === 'BORRADOR' && (
                                  <button type="button" onClick={() => sendDraftForReview(estimation)} disabled={saving}>
                                    Enviar a revisión
                                  </button>
                                )}
                                {canDelete && (
                                  <button
                                    type="button"
                                    className="secondary"
                                    onClick={() => deleteEstimationRow(estimation)}
                                    style={{ color: '#b91c1c' }}
                                  >
                                    Eliminar
                                  </button>
                                )}
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                      {!estimationsList.length && (
                        <tr>
                          <td colSpan={11} className="small" style={{ textAlign: 'center' }}>
                            Aún no hay estimaciones registradas para este presupuesto.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              {viewingEstimation && !showEstimationForm && (
                <div className="card" style={{ display: 'grid', gap: 10, padding: 16 }}>
                  <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
                    <div className="row" style={{ gap: 8, alignItems: 'center' }}>
                      <strong>Estimación #{viewingEstimation.folio}</strong>
                      <StatusBadge estimation={viewingEstimation} />
                      <span className="small">
                        {formatDate(viewingEstimation.periodStart)} – {formatDate(viewingEstimation.periodEnd)}
                      </span>
                    </div>
                    <div className="row" style={{ gap: 6 }}>
                      {viewingEstimation.workflowStatus === 'APROBADA' && (
                        <button type="button" onClick={() => printAuthorized(viewingEstimation)}>PDF autorizado</button>
                      )}
                      <button type="button" className="secondary" onClick={() => setViewingEstimation(null)}>✕ Cerrar</button>
                    </div>
                  </div>

                  {viewingEstimation.returnReason && viewingEstimation.workflowStatus === 'BORRADOR' && (
                    <div className="small" style={{ color: '#92400e' }}>
                      Devuelta por {viewingEstimation.returnedBy || 'un admin'}: {viewingEstimation.returnReason}
                    </div>
                  )}
                  {viewingEstimation.notes && <div className="small">Notas: {viewingEstimation.notes}</div>}

                  <div style={{ overflowX: 'auto' }}>
                    <table>
                      <thead>
                        <tr>
                          <th>Concepto</th>
                          <th>Unidad</th>
                          <th>Avance previo</th>
                          <th>Este periodo</th>
                          <th>Avance acumulado</th>
                          <th>Importe periodo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(viewingEstimation.lineItems || []).map((li) => (
                          <tr key={li.conceptoId}>
                            <td>{li.description}</td>
                            <td>{li.unit || '—'}</td>
                            <td>{formatPct(li.previousProgressPct)}</td>
                            <td>{li.periodQuantity} {li.unit}</td>
                            <td>{formatPct(li.progressPct)}</td>
                            <td>{formatCurrency(li.periodAmount)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {(() => {
                    const sheet = viewingEstimation.groupBreakdown || [];
                    if (!sheet.some((row) => row.group)) return null;
                    const sum = (key) => sheet.reduce((total, row) => total + (Number(row[key]) || 0), 0);
                    const totalBudget = sum('budgetAmount');
                    const advanceGiven = budgetDetail?.advanceAmortizationEnabled ? Number(budgetDetail?.advanceAmount) || 0 : 0;
                    const showPaidLines = Number(viewingEstimation.priorPaidApplied) > 0;
                    const paidToDate = (Number(viewingEstimation.priorPaidApplied) || 0) + advanceGiven;
                    return (
                      <div style={{ display: 'grid', gap: 8 }}>
                        <strong style={{ fontSize: 13 }}>Hoja de estimación por grupo (acumulado a la fecha)</strong>
                        <div style={{ overflowX: 'auto' }}>
                          <table>
                            <thead>
                              <tr>
                                <th>Grupo</th>
                                <th>Presupuesto</th>
                                <th>Anticipo</th>
                                <th>%</th>
                                <th>Avance $</th>
                                <th>Avance %</th>
                                <th>Amortización</th>
                                <th>Saldo</th>
                              </tr>
                            </thead>
                            <tbody>
                              {sheet.flatMap((row) => [(
                                <tr key={row.group || '__general__'}>
                                  <td>
                                    {groupLabel(row.group)}
                                    {row.isExtra && <span className="small" style={{ marginLeft: 6, color: '#92400e' }}>extra</span>}
                                  </td>
                                  <td>{formatCurrency(row.budgetAmount)}</td>
                                  <td>{Number(row.advanceAmount) > 0 ? formatCurrency(row.advanceAmount) : '—'}</td>
                                  <td>{Number(row.advancePct) > 0 ? formatPct(row.advancePct) : '—'}</td>
                                  <td>{formatCurrency(row.cumulativeAmount)}</td>
                                  <td>{formatPct(row.cumulativePct)}</td>
                                  <td>{Number(row.cumulativeAmortization) > 0 ? formatCurrency(row.cumulativeAmortization) : '—'}</td>
                                  <td>{formatCurrency(row.netAmount)}</td>
                                </tr>
                              ), ...(row.isExtra ? extraConceptsOfGroup(row.group, viewingEstimation, budgetDetail) : []).map((c) => (
                                <tr key={`${row.group}-${c.id}`} style={{ fontSize: 12, color: '#475569' }}>
                                  <td style={{ paddingLeft: 24 }}>↳ {c.description}{c.unit ? ` (${c.quantity} ${c.unit})` : ''}</td>
                                  <td>{formatCurrency(c.budgetAmount)}</td>
                                  <td>—</td>
                                  <td>—</td>
                                  <td>{formatCurrency(c.cumulativeAmount)}</td>
                                  <td>{formatPct(c.cumulativePct)}</td>
                                  <td>—</td>
                                  <td>{formatCurrency(c.cumulativeAmount)}</td>
                                </tr>
                              ))])}
                              <tr style={{ fontWeight: 600 }}>
                                <td>Total</td>
                                <td>{formatCurrency(totalBudget)}</td>
                                <td>{formatCurrency(sum('advanceAmount'))}</td>
                                <td></td>
                                <td>{formatCurrency(sum('cumulativeAmount'))}</td>
                                <td>{formatPct(totalBudget > 0 ? (sum('cumulativeAmount') / totalBudget) * 100 : 0)}</td>
                                <td>{formatCurrency(sum('cumulativeAmortization'))}</td>
                                <td>{formatCurrency(sum('netAmount'))}</td>
                              </tr>
                            </tbody>
                          </table>
                        </div>
                        <div className="small" style={{ display: 'grid', gap: 2, justifyContent: 'end', textAlign: 'right' }}>
                          <div>avance acumulado + <strong>{formatCurrency(sum('cumulativeAmount'))}</strong></div>
                          <div>amortización de anticipos − <strong>{formatCurrency(sum('cumulativeAmortization'))}</strong></div>
                          <div>saldo acumulado <strong>{formatCurrency(sum('netAmount'))}</strong></div>
                          {Number(viewingEstimation.retentionAmount) > 0 && (
                            <div>retención − <strong>{formatCurrency(viewingEstimation.retentionAmount)}</strong></div>
                          )}
                          {showPaidLines && advanceGiven > 0 && <div>anticipo + <strong>{formatCurrency(advanceGiven)}</strong></div>}
                          {showPaidLines && <div>pagado a la fecha − <strong>{formatCurrency(paidToDate)}</strong></div>}
                          <div style={{ fontSize: 13 }}>saldo total (a liberar) <strong>{formatCurrency(viewingEstimation.totalToPay)}</strong></div>
                        </div>
                      </div>
                    );
                  })()}

                  <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13 }}>
                    <div><strong>Avance acumulado del contrato:</strong> {formatPct(viewingEstimation.cumulativeProgressPct)}</div>
                    <div><strong>Subtotal:</strong> {formatCurrency(viewingEstimation.periodSubtotal)}</div>
                    <div><strong>Retención:</strong> {formatCurrency(viewingEstimation.retentionAmount)}</div>
                    <div><strong>Amortización anticipo:</strong> {formatCurrency(viewingEstimation.advanceAmortizationAmount)}</div>
                    {Number(viewingEstimation.priorPaidApplied) > 0 && (
                      <div><strong>Pagos previos reconocidos:</strong> −{formatCurrency(viewingEstimation.priorPaidApplied)}</div>
                    )}
                    <div><strong>Total calculado (a liberar):</strong> {formatCurrency(viewingEstimation.totalToPay)}</div>
                  </div>

                  {viewingEstimation.workflowStatus === 'APROBADA' && (
                    <div className="small" style={{ display: 'grid', gap: 2 }}>
                      <div>
                        <strong>Autorizado: {formatCurrency(viewingEstimation.authorizedAmount)}</strong>
                        {viewingEstimation.requestedAmount != null && (
                          <> · Solicitado por el contratista: {formatCurrency(viewingEstimation.requestedAmount)}
                            {Math.abs(Number(viewingEstimation.authorizedVsRequested) || 0) >= 0.01 && (
                              <> ({formatCurrency(viewingEstimation.authorizedVsRequested)} vs. solicitado)</>
                            )}
                          </>
                        )}
                        {Math.abs(Number(viewingEstimation.authorizedDifference) || 0) >= 0.01 && (
                          <> · {Number(viewingEstimation.authorizedDifference) > 0 ? '+' : ''}{formatCurrency(viewingEstimation.authorizedDifference)} vs. avance calculado</>
                        )}
                      </div>
                      {viewingEstimation.authorizationNote && <div>Motivo: {viewingEstimation.authorizationNote}</div>}
                      <div>Aprobada por {viewingEstimation.approvedBy} · {formatDate(viewingEstimation.approvedAt)}</div>
                    </div>
                  )}

                  {isReviewer && viewingEstimation.workflowStatus === 'ENVIADA' && !canApproveProject(viewingEstimation.projectId || budgetDetail?.projectId || selectedProjectId) && (
                    <div className="small" style={{ background: '#fef3c7', color: '#92400e', borderRadius: 6, padding: 10 }}>
                      Esta estimación espera autorización, pero esta obra no está asignada a tu usuario para autorizar. La aprueba otro admin.
                    </div>
                  )}

                  {isReviewer && viewingEstimation.workflowStatus === 'ENVIADA' && canApproveProject(viewingEstimation.projectId || budgetDetail?.projectId || selectedProjectId) && (
                    <div style={{ display: 'grid', gap: 8, borderTop: '1px solid var(--border, #e5e7eb)', paddingTop: 10 }}>
                      <strong>Autorización</strong>
                      {(() => {
                        const calculated = Number(viewingEstimation.totalToPay) || 0;
                        const requested = viewingEstimation.requestedAmount != null ? Number(viewingEstimation.requestedAmount) : null;
                        const baseline = requested ?? calculated;
                        const amount = authorizedAmount === '' ? baseline : Number(authorizedAmount) || 0;
                        const needsNote = Math.abs(amount - baseline) >= 0.01 || amount - calculated >= 0.01;
                        return (
                          <>
                            <div className="kpi-grid">
                              <div className="kpi-card">
                                <div>
                                  <div className="kpi-label">Avance reportado (a liberar)</div>
                                  <div className="kpi-value">{formatCurrency(calculated)}</div>
                                  <div className="kpi-sub">calculado con el avance capturado</div>
                                </div>
                              </div>
                              <div className="kpi-card">
                                <div>
                                  <div className="kpi-label">Solicitado por el contratista</div>
                                  <div className="kpi-value">{requested !== null ? formatCurrency(requested) : '—'}</div>
                                  <div className="kpi-sub">
                                    {requested === null
                                      ? 'sin capturar: se parte del avance'
                                      : Math.abs(requested - calculated) < 0.01
                                        ? 'igual al avance'
                                        : `${formatCurrency(Math.abs(requested - calculated))} ${requested > calculated ? 'más' : 'menos'} que el avance`}
                                  </div>
                                </div>
                              </div>
                              <div className="kpi-card">
                                <div>
                                  <div className="kpi-label">A autorizar</div>
                                  <div className="kpi-value">{formatCurrency(amount)}</div>
                                  <div className="kpi-sub">
                                    {requested !== null && Math.abs(amount - requested) >= 0.01
                                      ? `${formatCurrency(Math.abs(requested - amount))} ${amount < requested ? 'menos' : 'más'} que lo solicitado`
                                      : 'lo solicitado'}
                                  </div>
                                </div>
                              </div>
                            </div>
                            <div className="row" style={{ gap: 8, flexWrap: 'wrap', alignItems: 'flex-end' }}>
                              <div>
                                <label>Monto autorizado{requested !== null ? ' (máximo: lo solicitado)' : ''}</label>
                                <input
                                  type="number"
                                  min="0"
                                  max={requested !== null ? requested : undefined}
                                  step="0.01"
                                  value={authorizedAmount}
                                  onChange={(e) => setAuthorizedAmount(e.target.value)}
                                  style={{ width: 180, borderColor: requested !== null && amount > requested + 0.004 ? '#b91c1c' : undefined }}
                                />
                              </div>
                              <div style={{ flex: 1, minWidth: 220 }}>
                                <label>
                                  Motivo{needsNote
                                    ? (amount < baseline ? ' (obligatorio: se autoriza menos de lo solicitado)' : ' (obligatorio: se paga más de lo que marca el avance)')
                                    : ' (opcional)'}
                                </label>
                                <input value={authorizationNote} onChange={(e) => setAuthorizationNote(e.target.value)} />
                              </div>
                            </div>
                          </>
                        );
                      })()}
                      <div className="row" style={{ gap: 8 }}>
                        <button type="button" onClick={approveViewingEstimation} disabled={saving}>
                          {saving ? 'Procesando...' : 'Aprobar y pasar a pago'}
                        </button>
                        <button type="button" className="secondary" onClick={returnViewingEstimation} disabled={saving}>
                          Devolver a captura
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              )}

              {showEstimationForm && estimationForm && (
                <form className="card" style={{ display: 'grid', gap: 10, padding: 16 }} onSubmit={submitEstimationForm}>
                  <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                    <strong>{editingEstimation ? `Editar estimación #${editingEstimation.folio}` : 'Nueva estimación'}</strong>
                    <button type="button" className="secondary" onClick={resetEstimationForm}>✕ Cancelar</button>
                  </div>

                  <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
                    <div>
                      <label>Periodo desde</label>
                      <input
                        type="date"
                        value={estimationForm.periodStart}
                        onChange={(e) => setEstimationForm((prev) => ({ ...prev, periodStart: e.target.value }))}
                        required
                      />
                    </div>
                    <div>
                      <label>Periodo hasta</label>
                      <input
                        type="date"
                        value={estimationForm.periodEnd}
                        onChange={(e) => setEstimationForm((prev) => ({ ...prev, periodEnd: e.target.value }))}
                        required
                      />
                    </div>
                    <div>
                      <label>Monto solicitado por el contratista</label>
                      <input
                        type="number"
                        min="0"
                        step="0.01"
                        value={estimationForm.requestedAmount}
                        onChange={(e) => setEstimationForm((prev) => ({ ...prev, requestedAmount: e.target.value }))}
                        placeholder="Igual al avance"
                        style={{ width: 190 }}
                      />
                    </div>
                    <div style={{ flex: 1, minWidth: 200 }}>
                      <label>Notas</label>
                      <input
                        value={estimationForm.notes}
                        onChange={(e) => setEstimationForm((prev) => ({ ...prev, notes: e.target.value }))}
                      />
                    </div>
                  </div>

                  {Number(budgetDetail.recognizedPaidAmount) > 0 && (
                    <div className="small" style={{ background: 'var(--gray-100)', borderRadius: 6, padding: 8, display: 'grid', gap: 4 }}>
                      <div>
                        Pagado a la fecha al contratista: <strong>{formatCurrency(budgetDetail.recognizedPaidAmount)}</strong>
                        {' '}= {formatPct(budgetDetail.recognizedPaidPct)} del presupuesto.
                        {budgetDetail.openingMode === 'auto' && ' Se toman todos los pagos asignados a este presupuesto (puedes quitar los que no correspondan en «Asignar pagos»).'}
                        {' '}Lo ya pagado se descuenta de lo que se libera en esta estimación.
                      </div>
                      {!editingEstimation && (
                        <div>
                          <button
                            type="button"
                            className="secondary"
                            onClick={() => setEstimationForm((prev) => ({
                              ...prev,
                              captureMode: 'global',
                              globalProgressPct: String(Math.min(100, Number(budgetDetail.recognizedPaidPct) || 0)),
                            }))}
                          >
                            Usar el % pagado como avance global
                          </button>
                        </div>
                      )}
                    </div>
                  )}

                  <div className="row" style={{ gap: 14, flexWrap: 'wrap', alignItems: 'center' }}>
                    <strong style={{ fontSize: 13 }}>¿Cómo capturas el avance?</strong>
                    {[
                      ...(hasNamedGroups ? [['group', 'Avance por grupo']] : []),
                      ['global', 'Avance global (%)'],
                      ['concept', 'Avance por concepto (%)'],
                      ['quantity', 'Por unidad (m², pzas…)'],
                    ].map(([value, label]) => (
                      <label key={value} className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
                        <input
                          type="radio"
                          name="estimation-capture-mode"
                          checked={estimationForm.captureMode === value}
                          onChange={() => setEstimationForm((prev) => ({ ...prev, captureMode: value }))}
                        />
                        {label}
                      </label>
                    ))}
                  </div>

                  {estimationForm.captureMode === 'global' && (
                    <div>
                      <label>Avance acumulado del presupuesto (%)</label>
                      <input
                        type="number"
                        min="0"
                        max="100"
                        step="0.01"
                        value={estimationForm.globalProgressPct}
                        onChange={(e) => setEstimationForm((prev) => ({ ...prev, globalProgressPct: e.target.value }))}
                        style={{ width: 140 }}
                        required
                      />
                      <div className="small">
                        Se aplica a todos los conceptos. Un concepto que ya va más adelante no baja. Es el avance total a la fecha, no solo el de esta semana.
                      </div>
                    </div>
                  )}
                  {estimationForm.captureMode === 'quantity' && (
                    <div className="small">
                      Escribe las unidades que avanzaron <strong>en este periodo</strong> (por ejemplo, los m² de mármol colocados). Se multiplican por el precio unitario y se
                      muestra el avance acumulado en % de cada concepto.
                    </div>
                  )}
                  {estimationForm.captureMode === 'concept' && (
                    <div className="small">
                      Escribe el avance acumulado (%) de cada concepto que avanzó. Los que no cambies no avanzan en esta estimación.
                    </div>
                  )}

                  {estimationForm.captureMode === 'group' && (() => {
                    const groups = estimationForm.groups || [];
                    const rows = groups.map((group) => {
                      const target = (group.budgetAmount * group.pctExact) / 100;
                      const period = Math.max(target - group.previousAmount, 0);
                      return { group, period, rawAmortization: (period * group.advancePct) / 100 };
                    });
                    const rawTotal = rows.reduce((sum, row) => sum + row.rawAmortization, 0);
                    const cappedTotal = estimationPreview?.advanceAmortizationAmount ?? rawTotal;
                    const scale = rawTotal > 0 ? Math.min(cappedTotal / rawTotal, 1) : 0;
                    const totals = rows.reduce(
                      (acc, row) => ({
                        budget: acc.budget + row.group.budgetAmount,
                        previous: acc.previous + row.group.previousAmount,
                        period: acc.period + row.period,
                        amortization: acc.amortization + row.rawAmortization * scale,
                      }),
                      { budget: 0, previous: 0, period: 0, amortization: 0 },
                    );
                    return (
                      <>
                        <div className="small">
                          Escribe el avance <strong>acumulado</strong> de cada grupo en %. Se aplica a todos los conceptos del grupo; los grupos que no muevas no avanzan en esta
                          estimación. Si avanzaron unidades (m², piezas…), usa «Por unidad».
                        </div>
                        <div style={{ overflowX: 'auto' }}>
                          <table>
                            <thead>
                              <tr>
                                <th>Grupo</th>
                                <th>Presupuesto</th>
                                <th>Anticipo</th>
                                <th>Avance previo</th>
                                <th>Avance acumulado %</th>
                                <th>Avance acumulado $</th>
                                <th>Este periodo</th>
                                <th>Amortización</th>
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map(({ group, period, rawAmortization }) => {
                                const below = group.touched && group.pctExact < group.previousPct - 0.005;
                                const over = group.pctExact > 100.0001;
                                return (
                                  <tr key={group.name || '__general__'}>
                                    <td>
                                      {groupLabel(group.name)}
                                      {group.isExtra && <span className="small" style={{ marginLeft: 6, color: '#92400e' }}>extra</span>}
                                    </td>
                                    <td>{formatCurrency(group.budgetAmount)}</td>
                                    <td>{group.advancePct > 0 ? formatPct(group.advancePct) : '—'}</td>
                                    <td>{formatCurrency(group.previousAmount)} ({formatPct(group.previousPct)})</td>
                                    <td>
                                      <input
                                        type="number"
                                        min="0"
                                        max="100"
                                        step="0.01"
                                        value={group.pct}
                                        onChange={(e) => updateEstimationGroup(group.name, e.target.value)}
                                        style={{ width: 90, borderColor: below || over ? '#b91c1c' : undefined }}
                                      />
                                    </td>
                                    <td>{formatCurrency(group.amount === '' ? 0 : group.amount)}</td>
                                    <td>{formatCurrency(period)}</td>
                                    <td>{formatCurrency(rawAmortization * scale)}</td>
                                  </tr>
                                );
                              })}
                              <tr style={{ fontWeight: 600 }}>
                                <td>Total</td>
                                <td>{formatCurrency(totals.budget)}</td>
                                <td></td>
                                <td>{formatCurrency(totals.previous)}</td>
                                <td></td>
                                <td></td>
                                <td>{formatCurrency(totals.period)}</td>
                                <td>{formatCurrency(totals.amortization)}</td>
                              </tr>
                            </tbody>
                          </table>
                        </div>
                      </>
                    );
                  })()}

                  {estimationForm.captureMode !== 'group' && (
                  <div style={{ overflowX: 'auto' }}>
                    <table>
                      <thead>
                        <tr>
                          <th>Concepto</th>
                          <th>Unidad</th>
                          <th>Cant. contratada</th>
                          <th>Avance previo</th>
                          <th>{estimationForm.captureMode === 'quantity' ? 'Cantidad de este periodo' : 'Avance este periodo'}</th>
                          <th>Avance acumulado</th>
                          <th>Importe periodo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {estimationForm.lineItems.map((li, liIndex) => {
                          const periodQuantity = computePeriodQuantity(estimationForm.captureMode, li, estimationForm.globalProgressPct);
                          const showGroupHeader = hasNamedGroups && (liIndex === 0 || (estimationForm.lineItems[liIndex - 1].group || '') !== (li.group || ''));
                          const cumulativeQuantity = li.previousCumulativeQuantity + periodQuantity;
                          const cumulativePct = pctOf(cumulativeQuantity, li.contractedQuantity);
                          const periodAmount = periodQuantity * (Number(li.unitPrice) || 0);
                          const overContracted = cumulativeQuantity > li.contractedQuantity + 0.0001;
                          const belowPrevious =
                            estimationForm.captureMode === 'concept' &&
                            (Number(li.progressPct) || 0) < (Number(li.previousProgressPct) || 0) - 0.005;
                          return (
                            <React.Fragment key={li.conceptoId}>
                            {showGroupHeader && (
                              <tr>
                                <td colSpan={7} style={{ fontWeight: 600, background: 'var(--gray-100)' }}>{groupLabel(li.group)}</td>
                              </tr>
                            )}
                            <tr>
                              <td>{li.description}</td>
                              <td>{li.unit || '—'}</td>
                              <td>{li.contractedQuantity}</td>
                              <td>{li.previousCumulativeQuantity} ({formatPct(li.previousProgressPct)})</td>
                              <td>
                                {estimationForm.captureMode === 'quantity' && (
                                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                                    <input
                                      type="number"
                                      min="0"
                                      step="0.01"
                                      value={li.periodQuantity}
                                      onChange={(e) => updateEstimationLine(li.conceptoId, 'periodQuantity', e.target.value)}
                                      style={{ width: 100 }}
                                    />
                                    {li.unit}
                                  </span>
                                )}
                                {estimationForm.captureMode === 'concept' && (
                                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                                    <input
                                      type="number"
                                      min="0"
                                      max="100"
                                      step="0.01"
                                      value={li.progressPct}
                                      onChange={(e) => updateEstimationLine(li.conceptoId, 'progressPct', e.target.value)}
                                      style={{ width: 90, borderColor: belowPrevious ? '#b91c1c' : undefined }}
                                    />
                                    %
                                  </span>
                                )}
                                {estimationForm.captureMode === 'global' && <span>{Math.round(periodQuantity * 10000) / 10000}</span>}
                                {estimationForm.captureMode !== 'quantity' && (
                                  <div className="small">{Math.round(periodQuantity * 10000) / 10000} {li.unit}</div>
                                )}
                              </td>
                              <td style={{ color: overContracted || belowPrevious ? 'var(--danger-text, #b91c1c)' : undefined }}>
                                {Math.round(cumulativeQuantity * 10000) / 10000} ({formatPct(cumulativePct)})
                              </td>
                              <td>{formatCurrency(periodAmount)}</td>
                            </tr>
                            </React.Fragment>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                  )}

                  {estimationPreview && (
                    <div className="kpi-grid">
                      <div className="kpi-card">
                        <div>
                          <div className="kpi-label">Subtotal del periodo</div>
                          <div className="kpi-value">{formatCurrency(estimationPreview.periodSubtotal)}</div>
                        </div>
                      </div>
                      <div className="kpi-card">
                        <div>
                          <div className="kpi-label">Retención</div>
                          <div className="kpi-value">−{formatCurrency(estimationPreview.retentionAmount)}</div>
                        </div>
                      </div>
                      <div className="kpi-card">
                        <div>
                          <div className="kpi-label">Amortización anticipo</div>
                          <div className="kpi-value">−{formatCurrency(estimationPreview.advanceAmortizationAmount)}</div>
                        </div>
                      </div>
                      <div className="kpi-card">
                        <div>
                          <div className="kpi-label">Pagos previos reconocidos</div>
                          <div className="kpi-value">−{formatCurrency(estimationPreview.priorPaidApplied)}</div>
                          <div className="kpi-sub">ya pagado al contratista</div>
                        </div>
                      </div>
                      <div className="kpi-card">
                        <div>
                          <div className="kpi-label">A liberar (monto a autorizar)</div>
                          <div className="kpi-value">{formatCurrency(estimationPreview.totalToPay)}</div>
                          <div className="kpi-sub">total calculado de esta estimación</div>
                        </div>
                      </div>
                      <div className="kpi-card">
                        <div>
                          <div className="kpi-label">Solicitado por el contratista</div>
                          <div className="kpi-value">
                            {estimationForm.requestedAmount === '' ? '—' : formatCurrency(Number(estimationForm.requestedAmount) || 0)}
                          </div>
                          <div className="kpi-sub">
                            {estimationForm.requestedAmount === ''
                              ? 'sin capturar: se toma el avance'
                              : (() => {
                                  const diff = (Number(estimationForm.requestedAmount) || 0) - estimationPreview.totalToPay;
                                  if (Math.abs(diff) < 0.01) return 'igual al avance reportado';
                                  return `${formatCurrency(Math.abs(diff))} ${diff > 0 ? 'más' : 'menos'} que el avance`;
                                })()}
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  <div className="row" style={{ gap: 8 }}>
                    {editingEstimation?.workflowStatus === 'REGISTRADA' ? (
                      <button type="submit" disabled={saving}>{saving ? 'Guardando...' : 'Guardar estimación'}</button>
                    ) : (
                      <>
                        <button type="submit" className="secondary" disabled={saving}>
                          {saving ? 'Guardando...' : 'Guardar borrador'}
                        </button>
                        <button type="button" disabled={saving} onClick={(e) => submitEstimationForm(e, { sendForReview: true })}>
                          Guardar y enviar a revisión
                        </button>
                      </>
                    )}
                    <button type="button" className="secondary" onClick={resetEstimationForm}>Cancelar</button>
                  </div>
                </form>
              )}
            </>
          )}
        </>
      )}
    </div>
  );
}
