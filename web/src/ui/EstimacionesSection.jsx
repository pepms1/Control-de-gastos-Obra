import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';

const moneyFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatCurrency(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '$0.00';
  return `$${moneyFormatter.format(amount)}`;
}

function formatPct(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '0.00%';
  return `${amount.toFixed(2)}%`;
}

function formatDate(value) {
  if (!value) return '—';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleDateString('es-MX');
}

function normalizeTextForSupplierKey(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .trim()
    .replace(/\s+/g, ' ');
}

function buildCanonicalSupplierKey({ supplierCardCode, businessPartner, supplierName }) {
  const cardCode = String(supplierCardCode || '').trim();
  const bp = String(businessPartner || '').trim();
  const name = String(supplierName || '').trim();
  if (bp && cardCode) return `bpcc:${normalizeTextForSupplierKey(bp)}|${normalizeTextForSupplierKey(cardCode)}`;
  if (bp) return `bp:${normalizeTextForSupplierKey(bp)}`;
  if (cardCode) return `cardcode:${normalizeTextForSupplierKey(cardCode)}`;
  if (name) return `name:${normalizeTextForSupplierKey(name)}`;
  return '';
}

function generateId() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
  return `c_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

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
function computePeriodQuantity(mode, li, globalPct) {
  if (mode === 'quantity') return Number(li.periodQuantity) || 0;
  const pctRaw = mode === 'global' ? globalPct : li.progressPct;
  if (pctRaw === '' || pctRaw === null || pctRaw === undefined) return 0;
  const pct = Number(pctRaw) || 0;
  const target = ((Number(li.contractedQuantity) || 0) * pct) / 100;
  return Math.max(target - (Number(li.previousCumulativeQuantity) || 0), 0);
}

function emptyConceptoRow() {
  return { id: generateId(), description: '', unit: '', quantity: '', unitPrice: '' };
}

function computeLineItemAmount(row) {
  const quantity = Number(row.quantity) || 0;
  const unitPrice = Number(row.unitPrice) || 0;
  return quantity * unitPrice;
}

function computeBudgetFormTotals(lineItems, advanceAmount) {
  const totalContractedAmount = (lineItems || []).reduce((sum, row) => sum + computeLineItemAmount(row), 0);
  const advance = Number(advanceAmount) || 0;
  const advancePct = totalContractedAmount > 0 ? (advance / totalContractedAmount) * 100 : 0;
  return { totalContractedAmount, advancePct };
}

// Mirrors the backend's compute_estimation_money_fields — used only for
// live preview while typing; the authoritative values come back from the
// server response on save.
function computeEstimationPreview(budgetDetail, lineItemInputs, remainingBalanceOverride, mode = 'quantity', globalPct = '') {
  const periodSubtotal = (lineItemInputs || []).reduce(
    (sum, li) => sum + computePeriodQuantity(mode, li, globalPct) * (Number(li.unitPrice) || 0),
    0,
  );
  const retentionPct = Number(budgetDetail?.retentionPct) || 0;
  const retentionAmount = (periodSubtotal * retentionPct) / 100;

  let advanceAmortizationAmount = 0;
  if (budgetDetail?.advanceAmortizationEnabled) {
    const advancePct = Number(budgetDetail?.advancePct) || 0;
    const remainingBalance = Number(remainingBalanceOverride ?? budgetDetail?.remainingAdvanceBalance) || 0;
    advanceAmortizationAmount = Math.max(0, Math.min((periodSubtotal * advancePct) / 100, remainingBalance));
  }

  const totalToPay = periodSubtotal - retentionAmount - advanceAmortizationAmount;
  return { periodSubtotal, retentionAmount, advanceAmortizationAmount, totalToPay };
}

function emptyBudgetForm(projectId) {
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
    isActive: true,
    lineItems: [emptyConceptoRow()],
  };
}

export function EstimacionesSection({ projects, selectedProjectId, isReviewer = false }) {
  const [view, setView] = useState('list');
  const [section, setSection] = useState('budgets');
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
  const [supplierOptions, setSupplierOptions] = useState([]);
  const [saving, setSaving] = useState(false);

  const [showForm, setShowForm] = useState(false);
  const [editingBudgetRow, setEditingBudgetRow] = useState(null);
  const [form, setForm] = useState(emptyBudgetForm(selectedProjectId));

  const [selectedBudgetId, setSelectedBudgetId] = useState(null);
  const [budgetDetail, setBudgetDetail] = useState(null);
  const [estimationsList, setEstimationsList] = useState([]);
  const [detailLoading, setDetailLoading] = useState(false);

  const [showEstimationForm, setShowEstimationForm] = useState(false);
  const [editingEstimation, setEditingEstimation] = useState(null);
  const [estimationForm, setEstimationForm] = useState(null);

  const [importingConceptos, setImportingConceptos] = useState(false);
  const [importWarnings, setImportWarnings] = useState([]);
  const importFileInputRef = useRef(null);

  const [assigningBudget, setAssigningBudget] = useState(null);
  const [candidateTransactions, setCandidateTransactions] = useState([]);
  const [selectedTransactionIds, setSelectedTransactionIds] = useState(new Set());
  const [transactionSearch, setTransactionSearch] = useState('');
  const [loadingTransactions, setLoadingTransactions] = useState(false);

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
    setShowForm(false);
    setEditingBudgetRow(null);
    setAssigningBudget(null);
    setCandidateTransactions([]);
    setSelectedTransactionIds(new Set());
    setTransactionSearch('');
  }, [selectedProjectId]);

  useEffect(() => {
    if (!selectedProjectId || !isReviewer) return;
    let active = true;
    const normalizeRows = (payload) => {
      if (Array.isArray(payload)) return payload;
      if (Array.isArray(payload?.items)) return payload.items;
      if (Array.isArray(payload?.rows)) return payload.rows;
      if (Array.isArray(payload?.data)) return payload.data;
      return [];
    };
    Promise.allSettled([api.expensesSummaryBySupplier(), api.suppliers()])
      .then(([summaryResult, suppliersResult]) => {
        if (!active) return;

        const optionsByKey = new Map();
        const summaryRows = summaryResult.status === 'fulfilled' ? normalizeRows(summaryResult.value) : [];
        const supplierCatalogRows = suppliersResult.status === 'fulfilled' ? normalizeRows(suppliersResult.value) : [];

        if (summaryRows.length) {
          summaryRows.forEach((row) => {
            const key = String(row?.supplierKey || '').trim();
            if (!key) return;
            optionsByKey.set(key, {
              supplierKey: key,
              supplierName: row?.supplierName || key,
              sapCardCode: row?.sapCardCode || '',
              sapBusinessPartner: row?.sapBusinessPartner || '',
              vendorId: row?.vendorId || '',
            });
          });
        }

        if (supplierCatalogRows.length) {
          supplierCatalogRows.forEach((supplier) => {
            const supplierName = String(supplier?.name || '').trim();
            const sapCardCode = String(supplier?.cardCode || '').trim();
            const key = buildCanonicalSupplierKey({ supplierCardCode: sapCardCode, businessPartner: '', supplierName });
            if (!key) return;
            if (!optionsByKey.has(key)) {
              optionsByKey.set(key, {
                supplierKey: key,
                supplierName: supplierName || sapCardCode || key,
                sapCardCode,
                sapBusinessPartner: '',
                vendorId: '',
              });
            }
          });
        }

        setSupplierOptions(
          Array.from(optionsByKey.values()).sort((a, b) => (a.supplierName || '').localeCompare(b.supplierName || '', 'es')),
        );
      })
      .catch(() => {
        if (active) setSupplierOptions([]);
      });
    return () => {
      active = false;
    };
  }, [selectedProjectId]);

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
          return acc;
        },
        { totalContractedAmount: 0, totalRetainedToDate: 0, remainingAdvanceBalance: 0, paidAmount: 0 },
      ),
    [rows],
  );

  const conceptoIdsWithHistory = useMemo(
    () => new Set(editingBudgetRow?.conceptoIdsWithHistory || []),
    [editingBudgetRow],
  );

  const formTotals = useMemo(
    () => computeBudgetFormTotals(form.lineItems, form.advanceAmount),
    [form.lineItems, form.advanceAmount],
  );

  function resetBudgetForm() {
    setEditingBudgetRow(null);
    setShowForm(false);
    setForm(emptyBudgetForm(selectedProjectId));
    setImportWarnings([]);
  }

  function startCreateBudget() {
    setEditingBudgetRow(null);
    setForm(emptyBudgetForm(selectedProjectId));
    setImportWarnings([]);
    setShowForm(true);
  }

  function startEditBudget(row) {
    setEditingBudgetRow(row);
    setForm({
      projectId: row.projectId || selectedProjectId || '',
      supplierKey: row.supplierKey || '',
      supplierName: row.supplierNameSnapshot || '',
      supplierCardCode: row.supplierCardCode || '',
      businessPartner: row.businessPartner || '',
      vendorId: row.vendorId || '',
      name: row.name || '',
      currency: row.currency || 'MXN',
      notes: row.notes || '',
      retentionPct: String(row.retentionPct ?? 0),
      advanceAmortizationEnabled: Boolean(row.advanceAmortizationEnabled),
      advanceAmount: String(row.advanceAmount ?? 0),
      isActive: row.isActive !== false,
      lineItems: (row.lineItems && row.lineItems.length ? row.lineItems : [emptyConceptoRow()]).map((item) => ({
        id: item.id,
        description: item.description || '',
        unit: item.unit || '',
        quantity: String(item.quantity ?? ''),
        unitPrice: String(item.unitPrice ?? ''),
      })),
    });
    setImportWarnings([]);
    setShowForm(true);
  }

  function updateConceptoRow(index, patch) {
    setForm((prev) => ({
      ...prev,
      lineItems: prev.lineItems.map((row, i) => (i === index ? { ...row, ...patch } : row)),
    }));
  }

  function addConceptoRow() {
    setForm((prev) => ({ ...prev, lineItems: [...prev.lineItems, emptyConceptoRow()] }));
  }

  function removeConceptoRow(index) {
    setForm((prev) => ({ ...prev, lineItems: prev.lineItems.filter((_, i) => i !== index) }));
  }

  function isBlankConceptoRow(row) {
    return !String(row.description || '').trim() && !String(row.quantity || '').trim() && !String(row.unitPrice || '').trim();
  }

  async function handleImportConceptosFile(event) {
    const file = event.target.files?.[0];
    if (event.target) event.target.value = '';
    if (!file) return;

    setImportingConceptos(true);
    setImportWarnings([]);
    setError('');
    try {
      const result = await api.importEstimationConceptos(file);
      const importedRows = (Array.isArray(result?.items) ? result.items : []).map((item) => ({
        id: generateId(),
        description: item.description || '',
        unit: item.unit || '',
        quantity: String(item.quantity ?? ''),
        unitPrice: String(item.unitPrice ?? ''),
      }));
      if (!importedRows.length) {
        setError('El archivo no arrojó conceptos importables.');
        return;
      }
      setForm((prev) => ({
        ...prev,
        lineItems:
          prev.lineItems.length === 1 && isBlankConceptoRow(prev.lineItems[0])
            ? importedRows
            : [...prev.lineItems, ...importedRows],
      }));
      setImportWarnings(Array.isArray(result?.warnings) ? result.warnings : []);
    } catch (e) {
      setError(e.message || 'No se pudo importar el archivo');
    } finally {
      setImportingConceptos(false);
    }
  }

  async function submitBudgetForm(event) {
    event.preventDefault();
    setSaving(true);
    setError('');
    try {
      const lineItemsPayload = form.lineItems
        .filter((row) => String(row.description || '').trim())
        .map((row) => ({
          id: row.id,
          description: row.description,
          unit: row.unit,
          quantity: Number(String(row.quantity).replace(/,/g, '').trim()),
          unitPrice: Number(String(row.unitPrice).replace(/,/g, '').trim()),
        }));

      const advanceAmount = Number(String(form.advanceAmount).replace(/,/g, '').trim()) || 0;

      if (editingBudgetRow) {
        await api.updateEstimationBudget(editingBudgetRow.id, {
          name: form.name,
          notes: form.notes,
          isActive: Boolean(form.isActive),
          currency: form.currency,
          retentionPct: Number(form.retentionPct) || 0,
          advanceAmortizationEnabled: Boolean(form.advanceAmortizationEnabled),
          advanceAmount,
          lineItems: lineItemsPayload,
        });
      } else {
        await api.createEstimationBudget({
          projectId: form.projectId,
          supplierKey: form.supplierKey,
          supplierName: form.supplierName,
          supplierCardCode: form.supplierCardCode,
          businessPartner: form.businessPartner,
          vendorId: form.vendorId,
          name: form.name,
          currency: form.currency,
          notes: form.notes,
          retentionPct: Number(form.retentionPct) || 0,
          advanceAmortizationEnabled: Boolean(form.advanceAmortizationEnabled),
          advanceAmount,
          lineItems: lineItemsPayload,
        });
      }

      await loadEstimationBudgets();
      if (view === 'detail' && selectedBudgetId) await loadBudgetDetail(selectedBudgetId);
      resetBudgetForm();
    } catch (e) {
      setError(e.message || 'No se pudo guardar el presupuesto');
    } finally {
      setSaving(false);
    }
  }

  async function deleteCurrentBudget() {
    if (!editingBudgetRow?.id) return;
    const confirmed = window.confirm('¿Seguro que quieres eliminar este presupuesto? Esta acción no se puede deshacer.');
    if (!confirmed) return;

    setSaving(true);
    setError('');
    try {
      await api.deleteEstimationBudget(editingBudgetRow.id);
      await loadEstimationBudgets();
      resetBudgetForm();
      if (view === 'detail') backToList();
    } catch (e) {
      setError(e.message || 'No se pudo eliminar el presupuesto');
    } finally {
      setSaving(false);
    }
  }

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
    setView('list');
    setSelectedBudgetId(null);
    setBudgetDetail(null);
    setEstimationsList([]);
    setShowEstimationForm(false);
    setEditingEstimation(null);
    closeAssignPayments();
  }

  async function loadBudgetPaymentTransactions(budgetId, search = '') {
    if (!budgetId) return;
    setLoadingTransactions(true);
    setError('');
    try {
      const payload = await api.estimationBudgetTransactions(budgetId, search ? { search } : {});
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setCandidateTransactions(items);
      setSelectedTransactionIds(new Set(items.filter((item) => item.isAssignedToCurrentBudget).map((item) => item.id)));
    } catch (e) {
      setCandidateTransactions([]);
      setSelectedTransactionIds(new Set());
      setError(e.message || 'No se pudieron cargar las transacciones del presupuesto');
    } finally {
      setLoadingTransactions(false);
    }
  }

  function startAssignPayments(row) {
    setAssigningBudget(row);
    setTransactionSearch('');
    loadBudgetPaymentTransactions(row.id);
  }

  function closeAssignPayments() {
    setAssigningBudget(null);
    setCandidateTransactions([]);
    setSelectedTransactionIds(new Set());
    setTransactionSearch('');
  }

  async function saveAssignedPayments() {
    if (!assigningBudget?.id) return;
    setSaving(true);
    setError('');
    try {
      await api.saveEstimationBudgetTransactionLinks(assigningBudget.id, {
        selectedTransactionIds: Array.from(selectedTransactionIds),
      });
      await loadEstimationBudgets();
      if (selectedBudgetId === assigningBudget.id) await loadBudgetDetail(assigningBudget.id);
      await loadBudgetPaymentTransactions(assigningBudget.id, transactionSearch);
    } catch (e) {
      setError(e.message || 'No se pudieron guardar las asignaciones');
    } finally {
      setSaving(false);
    }
  }

  function pctOf(quantity, contracted) {
    const base = Number(contracted) || 0;
    return base > 0 ? Math.round(((Number(quantity) || 0) / base) * 10000) / 100 : 0;
  }

  function buildEstimationFormFromBudget(previousCumulativeByConceptoId) {
    return {
      periodStart: '',
      periodEnd: '',
      notes: '',
      captureMode: 'global',
      globalProgressPct: '',
      lineItems: (budgetDetail?.lineItems || []).map((item) => {
        const previous = Number(previousCumulativeByConceptoId?.[item.id]) || 0;
        const previousPct = String(pctOf(previous, item.quantity));
        return {
          conceptoId: item.id,
          description: item.description,
          unit: item.unit,
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
    const mode = ['global', 'concept', 'quantity'].includes(estimation.captureMode) ? estimation.captureMode : 'quantity';
    setViewingEstimation(null);
    setEditingEstimation(estimation);
    setEstimationForm({
      periodStart: estimation.periodStart || '',
      periodEnd: estimation.periodEnd || '',
      notes: estimation.notes || '',
      captureMode: mode,
      globalProgressPct: estimation.globalProgressPct != null ? String(estimation.globalProgressPct) : '',
      lineItems: (estimation.lineItems || []).map((li) => ({
        conceptoId: li.conceptoId,
        description: li.description,
        unit: li.unit,
        unitPrice: li.unitPrice,
        contractedQuantity: li.contractedQuantity,
        previousCumulativeQuantity: li.previousCumulativeQuantity,
        previousProgressPct: String(li.previousProgressPct ?? pctOf(li.previousCumulativeQuantity, li.contractedQuantity)),
        progressPct: String(li.progressPct ?? pctOf(li.cumulativeQuantity, li.contractedQuantity)),
        periodQuantity: String(li.periodQuantity ?? ''),
      })),
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
    return computeEstimationPreview(
      budgetDetail,
      estimationForm.lineItems,
      remainingOverride,
      estimationForm.captureMode,
      estimationForm.globalProgressPct,
    );
  }, [estimationForm, budgetDetail, editingEstimation]);

  function buildEstimationPayload() {
    const payload = {
      periodStart: estimationForm.periodStart,
      periodEnd: estimationForm.periodEnd,
      notes: estimationForm.notes,
      captureMode: estimationForm.captureMode,
    };
    if (estimationForm.captureMode === 'global') {
      payload.globalProgressPct = Number(estimationForm.globalProgressPct) || 0;
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
    const amount = authorizedAmount === '' ? calculated : Number(authorizedAmount);
    if (!Number.isFinite(amount) || amount < 0) {
      setError('El monto autorizado no es válido.');
      return;
    }
    if (Math.abs(amount - calculated) >= 0.01 && !authorizationNote.trim()) {
      setError('Indica el motivo cuando el monto autorizado es distinto al calculado.');
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

  function markEstimationPaid(estimation) {
    if (!window.confirm(`¿Marcar la estimación #${estimation.folio} como pagada?`)) return;
    runEstimationAction(api.markEstimationPaid, estimation, {});
  }

  function openEstimationView(estimation) {
    setShowEstimationForm(false);
    setEditingEstimation(null);
    setViewingEstimation(estimation);
    setAuthorizedAmount(String(estimation.authorizedAmount ?? estimation.totalToPay ?? ''));
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
    if (!isReviewer || !selectedProjectId) return;
    api.estimationsQueue({ projectId: selectedProjectId, status: 'ENVIADA' })
      .then((data) => setPendingReviewCount(Array.isArray(data?.items) ? data.items.length : 0))
      .catch(() => setPendingReviewCount(0));
  }, [isReviewer, selectedProjectId, estimationsList, section]);

  async function openEstimationFromQueue(row) {
    setSection('budgets');
    setSelectedBudgetId(row.estimationBudgetId);
    setView('detail');
    setShowEstimationForm(false);
    setEditingEstimation(null);
    await loadBudgetDetail(row.estimationBudgetId);
    openEstimationView(row);
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

  const hasOpenEstimation = estimationsList.some((row) => row.workflowStatus === 'BORRADOR' || row.workflowStatus === 'ENVIADA');

  return (
    <div style={{ display: 'grid', gap: 14 }}>
      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}

      {showForm && (
        <form className="card" style={{ display: 'grid', gap: 10, padding: 16 }} onSubmit={submitBudgetForm}>
          <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
            <strong>{editingBudgetRow ? 'Editar presupuesto' : 'Nuevo presupuesto'}</strong>
            <button type="button" className="secondary" onClick={resetBudgetForm}>✕ Cancelar</button>
          </div>

          <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
            <div>
              <label>Obra</label>
              <select
                value={form.projectId}
                onChange={(e) => setForm((prev) => ({ ...prev, projectId: e.target.value }))}
                disabled={Boolean(editingBudgetRow)}
                required
              >
                <option value="">Selecciona obra</option>
                {(projects || []).map((project) => (
                  <option key={project._id} value={project._id}>{project.displayName || project.name}</option>
                ))}
              </select>
            </div>
            <div>
              <label>Proveedor</label>
              <select
                value={form.supplierKey}
                onChange={(e) => {
                  const nextKey = e.target.value;
                  const option = supplierOptions.find((row) => row.supplierKey === nextKey);
                  setForm((prev) => ({
                    ...prev,
                    supplierKey: nextKey,
                    supplierName: option?.supplierName || prev.supplierName,
                    supplierCardCode: option?.sapCardCode || prev.supplierCardCode,
                    businessPartner: option?.sapBusinessPartner || prev.businessPartner,
                    vendorId: option?.vendorId || prev.vendorId,
                  }));
                }}
                disabled={Boolean(editingBudgetRow)}
                required
              >
                <option value="">Selecciona proveedor</option>
                {supplierOptions.map((row) => (
                  <option key={row.supplierKey} value={row.supplierKey}>{row.supplierName || row.supplierKey}</option>
                ))}
              </select>
            </div>
            <div>
              <label>Nombre del presupuesto</label>
              <input
                value={form.name}
                onChange={(e) => setForm((prev) => ({ ...prev, name: e.target.value }))}
                placeholder="Ej. Instalación hidrosanitaria"
              />
            </div>
            <div>
              <label>% Retención (fondo de garantía)</label>
              <input
                value={form.retentionPct}
                onChange={(e) => setForm((prev) => ({ ...prev, retentionPct: e.target.value }))}
                placeholder="0"
                style={{ width: 90 }}
              />
            </div>
            <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
              <input
                type="checkbox"
                checked={Boolean(form.advanceAmortizationEnabled)}
                onChange={(e) => setForm((prev) => ({ ...prev, advanceAmortizationEnabled: e.target.checked }))}
              />
              Amortizar anticipo
            </label>
            <div>
              <label>Monto de anticipo</label>
              <input
                value={form.advanceAmount}
                onChange={(e) => setForm((prev) => ({ ...prev, advanceAmount: e.target.value }))}
                placeholder="0.00"
                style={{ width: 120 }}
              />
            </div>
            <div>
              <label>Nota (opcional)</label>
              <input value={form.notes} onChange={(e) => setForm((prev) => ({ ...prev, notes: e.target.value }))} />
            </div>
            {editingBudgetRow && (
              <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
                <input
                  type="checkbox"
                  checked={Boolean(form.isActive)}
                  onChange={(e) => setForm((prev) => ({ ...prev, isActive: e.target.checked }))}
                />
                Presupuesto activo
              </label>
            )}
          </div>

          <div>
            <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
              <label>Conceptos</label>
              <div className="row" style={{ gap: 6 }}>
                <input
                  ref={importFileInputRef}
                  type="file"
                  accept=".xlsx,.csv,.pdf"
                  onChange={handleImportConceptosFile}
                  style={{ display: 'none' }}
                />
                <button
                  type="button"
                  className="secondary"
                  onClick={() => importFileInputRef.current?.click()}
                  disabled={importingConceptos}
                >
                  {importingConceptos ? 'Importando...' : '⭱ Importar Excel/CSV/PDF'}
                </button>
                <button type="button" className="secondary" onClick={addConceptoRow}>+ Agregar concepto</button>
              </div>
            </div>
            {importWarnings.length > 0 && (
              <div className="small" style={{ color: 'var(--gray-600)', background: 'var(--gray-100)', borderRadius: 6, padding: 8, marginBottom: 6 }}>
                {importWarnings.map((warning, idx) => (
                  <div key={idx}>⚠ {warning}</div>
                ))}
              </div>
            )}
            <div style={{ overflowX: 'auto' }}>
              <table>
                <thead>
                  <tr>
                    <th>Descripción</th>
                    <th>Unidad</th>
                    <th>Cantidad</th>
                    <th>Precio unitario</th>
                    <th>Importe</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {form.lineItems.map((row, index) => {
                    const hasHistory = conceptoIdsWithHistory.has(row.id);
                    return (
                      <tr key={row.id}>
                        <td>
                          <input
                            value={row.description}
                            onChange={(e) => updateConceptoRow(index, { description: e.target.value })}
                            required
                          />
                        </td>
                        <td>
                          <input
                            value={row.unit}
                            onChange={(e) => updateConceptoRow(index, { unit: e.target.value })}
                            placeholder="m2, pza, lote..."
                            style={{ width: 90 }}
                          />
                        </td>
                        <td>
                          <input
                            type="number"
                            min="0"
                            step="0.01"
                            value={row.quantity}
                            onChange={(e) => updateConceptoRow(index, { quantity: e.target.value })}
                            style={{ width: 100 }}
                            required
                          />
                        </td>
                        <td>
                          <input
                            type="number"
                            min="0"
                            step="0.01"
                            value={row.unitPrice}
                            onChange={(e) => updateConceptoRow(index, { unitPrice: e.target.value })}
                            style={{ width: 110 }}
                            required
                          />
                        </td>
                        <td>{formatCurrency(computeLineItemAmount(row))}</td>
                        <td>
                          <button
                            type="button"
                            className="secondary"
                            onClick={() => removeConceptoRow(index)}
                            disabled={hasHistory || form.lineItems.length <= 1}
                            title={hasHistory ? 'No se puede quitar: ya tiene avance registrado en alguna estimación' : undefined}
                          >
                            ✕
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <div className="row" style={{ gap: 16, fontSize: 13, marginTop: 4 }}>
              <div><strong>Total contratado:</strong> {formatCurrency(formTotals.totalContractedAmount)}</div>
              {Boolean(form.advanceAmortizationEnabled) && (
                <div><strong>% Anticipo (calculado):</strong> {formatPct(formTotals.advancePct)}</div>
              )}
            </div>
          </div>

          <div className="row" style={{ gap: 8 }}>
            <button type="submit" disabled={saving}>
              {saving ? 'Guardando...' : editingBudgetRow ? 'Guardar cambios' : 'Crear presupuesto'}
            </button>
            {editingBudgetRow && (
              <button type="button" className="secondary" onClick={deleteCurrentBudget} disabled={saving} style={{ color: '#b91c1c' }}>
                Eliminar
              </button>
            )}
            <button type="button" className="secondary" onClick={resetBudgetForm}>Cancelar</button>
          </div>
        </form>
      )}

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
                      {section === 'payable' && <td><strong>{formatCurrency(row.authorizedAmount)}</strong></td>}
                      <td><StatusBadge estimation={row} /></td>
                      <td>
                        <div className="row" style={{ gap: 6 }}>
                          <button type="button" onClick={() => openEstimationFromQueue(row)}>
                            {section === 'review' ? 'Revisar' : 'Abrir'}
                          </button>
                          {section === 'payable' && (
                            <button type="button" className="secondary" onClick={() => markEstimationPaid(row)} disabled={saving}>
                              Marcar pagada
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                  {!queueRows.length && (
                    <tr>
                      <td colSpan={section === 'payable' ? 9 : 8} className="small" style={{ textAlign: 'center' }}>
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
                <div className="kpi-label">Presupuestos</div>
                <div className="kpi-value">{rows.length}</div>
                <div className="kpi-sub">{includeInactive ? 'incluyendo inactivos' : 'activos'}</div>
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
              {isReviewer && (
                <button type="button" onClick={showForm ? resetBudgetForm : startCreateBudget} style={{ fontSize: 13 }}>
                  {showForm ? '✕ Cancelar' : '+ Nuevo presupuesto'}
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
                            <button type="button" className="secondary" onClick={() => openBudgetDetail(row)}>Ver</button>
                            {isReviewer && <button type="button" className="secondary" onClick={() => startEditBudget(row)}>Editar</button>}
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!rows.length && (
                      <tr>
                        <td colSpan={9} className="small" style={{ textAlign: 'center' }}>
                          No hay presupuestos para los filtros seleccionados.
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

              <div className="card" style={{ overflow: 'hidden' }}>
                <div className="card-header">
                  <strong>{budgetDetail.supplierNameSnapshot}</strong>
                  <div style={{ flex: 1 }} />
                  {isReviewer && (
                    <>
                      <button type="button" className="secondary" onClick={() => startEditBudget(budgetDetail)}>Editar presupuesto</button>
                      <button type="button" className="secondary" onClick={() => startAssignPayments(budgetDetail)}>Asignar pagos</button>
                    </>
                  )}
                  {!showEstimationForm && (
                    <button
                      type="button"
                      onClick={startCreateEstimation}
                      disabled={budgetDetail.isActive === false || hasOpenEstimation}
                      title={hasOpenEstimation ? 'Hay una estimación abierta; ciérrala (aprobada) antes de crear otra' : undefined}
                    >
                      + Nueva estimación
                    </button>
                  )}
                </div>

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
                            <td>
                              {workflow === 'APROBADA' ? <strong>{formatCurrency(estimation.authorizedAmount)}</strong> : '—'}
                            </td>
                            <td><StatusBadge estimation={estimation} /></td>
                            <td>
                              <div className="row" style={{ gap: 6, flexWrap: 'wrap' }}>
                                <button type="button" className="secondary" onClick={() => openEstimationView(estimation)}>
                                  {workflow === 'ENVIADA' && isReviewer ? 'Revisar' : 'Ver'}
                                </button>
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
                                {isReviewer && workflow === 'APROBADA' && estimation.paymentStatus === 'POR_PAGAR' && (
                                  <button type="button" className="secondary" onClick={() => markEstimationPaid(estimation)} disabled={saving}>
                                    Marcar pagada
                                  </button>
                                )}
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                      {!estimationsList.length && (
                        <tr>
                          <td colSpan={10} className="small" style={{ textAlign: 'center' }}>
                            Aún no hay estimaciones registradas para este presupuesto.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              {assigningBudget && (
                <div className="grid budgets-assignment-panel" style={{ gap: 8, borderRadius: 10, padding: 12 }}>
                  <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                    <strong>Asignar pagos · {assigningBudget.supplierNameSnapshot || assigningBudget.supplierKey}</strong>
                    <button type="button" className="secondary" onClick={closeAssignPayments}>Cerrar</button>
                  </div>
                  <div className="small">
                    Por defecto, si este es el único presupuesto activo del proveedor, se le atribuyen automáticamente todos sus
                    egresos. En cuanto exista más de un presupuesto activo para el mismo proveedor, asigna aquí manualmente qué
                    pagos corresponden a cada uno.
                  </div>
                  <div className="row" style={{ gap: 8 }}>
                    <input
                      value={transactionSearch}
                      onChange={(e) => setTransactionSearch(e.target.value)}
                      placeholder="Buscar por descripción / concepto"
                      style={{ minWidth: 260 }}
                    />
                    <button
                      type="button"
                      className="secondary"
                      onClick={() => loadBudgetPaymentTransactions(assigningBudget.id, transactionSearch)}
                      disabled={loadingTransactions}
                    >
                      Filtrar
                    </button>
                    <button type="button" onClick={saveAssignedPayments} disabled={saving || loadingTransactions}>
                      {saving ? 'Guardando...' : 'Guardar asignación'}
                    </button>
                  </div>
                  {loadingTransactions ? (
                    <div className="small">Cargando transacciones...</div>
                  ) : (
                    <div style={{ overflowX: 'auto', maxHeight: 320 }}>
                      <table>
                        <thead>
                          <tr>
                            <th></th>
                            <th>Fecha</th>
                            <th>Descripción</th>
                            <th>Monto</th>
                            <th>Estado</th>
                          </tr>
                        </thead>
                        <tbody>
                          {candidateTransactions.map((tx) => {
                            const disabled = tx.isAssignedToOtherBudget;
                            const checked = selectedTransactionIds.has(tx.id);
                            return (
                              <tr key={tx.id}>
                                <td>
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    disabled={disabled}
                                    onChange={(e) => {
                                      const next = new Set(selectedTransactionIds);
                                      if (e.target.checked) next.add(tx.id);
                                      else next.delete(tx.id);
                                      setSelectedTransactionIds(next);
                                    }}
                                  />
                                </td>
                                <td>{formatDate(tx.date)}</td>
                                <td>{tx.description || '—'}</td>
                                <td>{formatCurrency(tx.amountWithTax)}</td>
                                <td>{tx.isAssignedToOtherBudget ? 'Asignado a otro presupuesto' : (tx.isAssignedToCurrentBudget ? 'Asignado a este presupuesto' : 'Libre')}</td>
                              </tr>
                            );
                          })}
                          {!candidateTransactions.length && (
                            <tr>
                              <td colSpan={5} className="small" style={{ textAlign: 'center' }}>No hay transacciones disponibles.</td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}

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
                    <button type="button" className="secondary" onClick={() => setViewingEstimation(null)}>✕ Cerrar</button>
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

                  <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13 }}>
                    <div><strong>Avance acumulado del contrato:</strong> {formatPct(viewingEstimation.cumulativeProgressPct)}</div>
                    <div><strong>Subtotal:</strong> {formatCurrency(viewingEstimation.periodSubtotal)}</div>
                    <div><strong>Retención:</strong> {formatCurrency(viewingEstimation.retentionAmount)}</div>
                    <div><strong>Amortización anticipo:</strong> {formatCurrency(viewingEstimation.advanceAmortizationAmount)}</div>
                    <div><strong>Total calculado:</strong> {formatCurrency(viewingEstimation.totalToPay)}</div>
                  </div>

                  {viewingEstimation.workflowStatus === 'APROBADA' && (
                    <div className="small" style={{ display: 'grid', gap: 2 }}>
                      <div>
                        <strong>Autorizado: {formatCurrency(viewingEstimation.authorizedAmount)}</strong>
                        {Math.abs(Number(viewingEstimation.authorizedDifference) || 0) >= 0.01 && (
                          <> ({Number(viewingEstimation.authorizedDifference) > 0 ? '+' : ''}{formatCurrency(viewingEstimation.authorizedDifference)} vs. calculado)</>
                        )}
                      </div>
                      {viewingEstimation.authorizationNote && <div>Motivo: {viewingEstimation.authorizationNote}</div>}
                      <div>Aprobada por {viewingEstimation.approvedBy} · {formatDate(viewingEstimation.approvedAt)}</div>
                    </div>
                  )}

                  {isReviewer && viewingEstimation.workflowStatus === 'ENVIADA' && (
                    <div style={{ display: 'grid', gap: 8, borderTop: '1px solid var(--border, #e5e7eb)', paddingTop: 10 }}>
                      <strong>Autorización</strong>
                      <div className="row" style={{ gap: 8, flexWrap: 'wrap', alignItems: 'flex-end' }}>
                        <div>
                          <label>Monto autorizado</label>
                          <input
                            type="number"
                            min="0"
                            step="0.01"
                            value={authorizedAmount}
                            onChange={(e) => setAuthorizedAmount(e.target.value)}
                            style={{ width: 160 }}
                          />
                        </div>
                        <div style={{ flex: 1, minWidth: 220 }}>
                          <label>
                            Motivo{Math.abs((Number(authorizedAmount) || 0) - (Number(viewingEstimation.totalToPay) || 0)) >= 0.01 ? ' (obligatorio, el monto cambió)' : ' (opcional)'}
                          </label>
                          <input value={authorizationNote} onChange={(e) => setAuthorizationNote(e.target.value)} />
                        </div>
                      </div>
                      <div className="small">
                        Calculado: {formatCurrency(viewingEstimation.totalToPay)} · Diferencia:{' '}
                        {formatCurrency((Number(authorizedAmount === '' ? viewingEstimation.totalToPay : authorizedAmount) || 0) - (Number(viewingEstimation.totalToPay) || 0))}
                      </div>
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
                    <div style={{ flex: 1, minWidth: 200 }}>
                      <label>Notas</label>
                      <input
                        value={estimationForm.notes}
                        onChange={(e) => setEstimationForm((prev) => ({ ...prev, notes: e.target.value }))}
                      />
                    </div>
                  </div>

                  <div className="row" style={{ gap: 14, flexWrap: 'wrap', alignItems: 'center' }}>
                    <strong style={{ fontSize: 13 }}>¿Cómo capturas el avance?</strong>
                    {[
                      ['global', 'Avance global (%)'],
                      ['concept', 'Avance por concepto (%)'],
                      ['quantity', 'Por cantidad'],
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
                  {estimationForm.captureMode === 'concept' && (
                    <div className="small">
                      Escribe el avance acumulado (%) de cada concepto que avanzó. Los que no cambies no avanzan en esta estimación.
                    </div>
                  )}

                  <div style={{ overflowX: 'auto' }}>
                    <table>
                      <thead>
                        <tr>
                          <th>Concepto</th>
                          <th>Unidad</th>
                          <th>Cant. contratada</th>
                          <th>Avance previo</th>
                          <th>Avance este periodo</th>
                          <th>Avance acumulado</th>
                          <th>Importe periodo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {estimationForm.lineItems.map((li) => {
                          const periodQuantity = computePeriodQuantity(estimationForm.captureMode, li, estimationForm.globalProgressPct);
                          const cumulativeQuantity = li.previousCumulativeQuantity + periodQuantity;
                          const cumulativePct = pctOf(cumulativeQuantity, li.contractedQuantity);
                          const periodAmount = periodQuantity * (Number(li.unitPrice) || 0);
                          const overContracted = cumulativeQuantity > li.contractedQuantity + 0.0001;
                          const belowPrevious =
                            estimationForm.captureMode === 'concept' &&
                            (Number(li.progressPct) || 0) < (Number(li.previousProgressPct) || 0) - 0.005;
                          return (
                            <tr key={li.conceptoId}>
                              <td>{li.description}</td>
                              <td>{li.unit || '—'}</td>
                              <td>{li.contractedQuantity}</td>
                              <td>{li.previousCumulativeQuantity} ({formatPct(li.previousProgressPct)})</td>
                              <td>
                                {estimationForm.captureMode === 'quantity' && (
                                  <input
                                    type="number"
                                    min="0"
                                    step="0.01"
                                    value={li.periodQuantity}
                                    onChange={(e) => updateEstimationLine(li.conceptoId, 'periodQuantity', e.target.value)}
                                    style={{ width: 100 }}
                                  />
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
                          );
                        })}
                      </tbody>
                    </table>
                  </div>

                  {estimationPreview && (
                    <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13 }}>
                      <div><strong>Subtotal:</strong> {formatCurrency(estimationPreview.periodSubtotal)}</div>
                      <div><strong>Retención:</strong> {formatCurrency(estimationPreview.retentionAmount)}</div>
                      <div><strong>Amortización anticipo:</strong> {formatCurrency(estimationPreview.advanceAmortizationAmount)}</div>
                      <div><strong>Total calculado:</strong> {formatCurrency(estimationPreview.totalToPay)}</div>
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
