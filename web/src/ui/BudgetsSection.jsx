import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { ExtrasPanel } from './ExtrasPanel.jsx';
import {
  buildCanonicalSupplierKey,
  computeBudgetFormTotals,
  computeLineItemAmount,
  groupLabel,
  listFormGroups,
  emptyBudgetForm,
  emptyConceptoRow,
  formatCurrency,
  formatDate,
  formatPct,
  generateId,
  isBlankConceptoRow,
  summarizeBudgets,
} from './estimationShared.js';

// Presupuestos: aquí se capturan los presupuestos por conceptos (los mismos que
// usa el módulo de Estimaciones), se importan desde Excel/CSV/PDF/Word, se les
// asignan pagos y se registra su saldo inicial. Estimaciones se enfoca en
// estimar, autorizar y pagar.

function classifyBudgetStatus(paidPct) {
  const progress = Number(paidPct);
  if (!Number.isFinite(progress)) return { label: 'En presupuesto', className: 'in-budget' };
  if (progress > 100) return { label: 'Excedido', className: 'exceeded' };
  if (progress >= 100) return { label: 'Pagado', className: 'paid' };
  return { label: 'En presupuesto', className: 'in-budget' };
}

export function BudgetsSection({ projects, selectedProjectId, onOpenEstimations }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [supplierFilter, setSupplierFilter] = useState('');
  const [includeInactive, setIncludeInactive] = useState(false);
  const [supplierOptions, setSupplierOptions] = useState([]);
  const [saving, setSaving] = useState(false);
  const [editingAreaM2, setEditingAreaM2] = useState(false);
  const [areaM2Input, setAreaM2Input] = useState('');
  const [savingAreaM2, setSavingAreaM2] = useState(false);
  const [areaM2Error, setAreaM2Error] = useState('');
  const [localAreaM2Override, setLocalAreaM2Override] = useState(null);
  const [totalEgresosSinIva, setTotalEgresosSinIva] = useState(0);
  const [expandedSuppliers, setExpandedSuppliers] = useState(() => new Set());

  const [showForm, setShowForm] = useState(false);
  const [editingBudgetRow, setEditingBudgetRow] = useState(null);
  const [form, setForm] = useState(emptyBudgetForm(selectedProjectId));
  const [importingConceptos, setImportingConceptos] = useState(false);
  const [importWarnings, setImportWarnings] = useState([]);
  const importFileInputRef = useRef(null);

  const [assigningBudget, setAssigningBudget] = useState(null);
  const [candidateTransactions, setCandidateTransactions] = useState([]);
  const [selectedTransactionIds, setSelectedTransactionIds] = useState(new Set());
  const [transactionSearch, setTransactionSearch] = useState('');
  const [loadingTransactions, setLoadingTransactions] = useState(false);

  const [extrasBudget, setExtrasBudget] = useState(null);
  const [openingBudget, setOpeningBudget] = useState(null);
  const [openingLoading, setOpeningLoading] = useState(false);
  const [openingTransactions, setOpeningTransactions] = useState([]);
  const [openingRequiresAssignment, setOpeningRequiresAssignment] = useState(false);
  const [openingAssignments, setOpeningAssignments] = useState({});
  const [openingManualAdvance, setOpeningManualAdvance] = useState('');
  const [openingManualPrior, setOpeningManualPrior] = useState('');
  const [openingNote, setOpeningNote] = useState('');

  const projectsById = useMemo(
    () => new Map((Array.isArray(projects) ? projects : []).map((project) => [String(project?._id || ''), project])),
    [projects],
  );

  // ---- datos agrupados por proveedor (+ / − para expandir) ----
  const groupedRows = useMemo(() => {
    const groups = new Map();
    rows.forEach((row) => {
      const supplierKey = String(row?.supplierKey || '').trim();
      const supplierName = String(row?.supplierNameSnapshot || row?.supplierKey || 'Sin proveedor');
      const groupKey = supplierKey || `__name__:${supplierName}`;
      if (!groups.has(groupKey)) groups.set(groupKey, { key: groupKey, supplierName, items: [] });
      groups.get(groupKey).items.push(row);
    });
    return Array.from(groups.values())
      .map((group) => ({ ...group, totals: summarizeBudgets(group.items) }))
      .sort((a, b) => a.supplierName.localeCompare(b.supplierName, 'es'));
  }, [rows]);

  const grandTotals = useMemo(() => summarizeBudgets(rows), [rows]);

  const conceptoIdsWithHistory = useMemo(
    () => new Set(editingBudgetRow?.conceptoIdsWithHistory || []),
    [editingBudgetRow],
  );

  const formTotals = useMemo(
    () => computeBudgetFormTotals(form.lineItems, form.advanceAmount, form.groupAdvancePcts),
    [form.lineItems, form.advanceAmount, form.groupAdvancePcts],
  );

  function toggleSupplierExpand(groupKey) {
    setExpandedSuppliers((prev) => {
      const next = new Set(prev);
      if (next.has(groupKey)) next.delete(groupKey);
      else next.add(groupKey);
      return next;
    });
  }

  async function loadBudgets() {
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
      setError(e.message || 'No se pudieron cargar los presupuestos');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!selectedProjectId) return;
    loadBudgets();
  }, [selectedProjectId, includeInactive]);

  useEffect(() => {
    setLocalAreaM2Override(null);
    setEditingAreaM2(false);
    setTotalEgresosSinIva(0);
    setShowForm(false);
    setEditingBudgetRow(null);
    setAssigningBudget(null);
    setOpeningBudget(null);
    setExtrasBudget(null);
    setExpandedSuppliers(new Set());
  }, [selectedProjectId]);

  useEffect(() => {
    if (!selectedProjectId) return;
    let active = true;
    api.spendByCategory({ include_iva: 'false' })
      .then((data) => { if (active) setTotalEgresosSinIva(Number(data?.total_expenses) || 0); })
      .catch(() => {});
    return () => { active = false; };
  }, [selectedProjectId]);

  useEffect(() => {
    if (!selectedProjectId) return;
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
        supplierCatalogRows.forEach((supplier) => {
          const supplierName = String(supplier?.name || '').trim();
          const sapCardCode = String(supplier?.cardCode || '').trim();
          const key = buildCanonicalSupplierKey({ supplierCardCode: sapCardCode, businessPartner: '', supplierName });
          if (!key || optionsByKey.has(key)) return;
          optionsByKey.set(key, {
            supplierKey: key,
            supplierName: supplierName || sapCardCode || key,
            sapCardCode,
            sapBusinessPartner: '',
            vendorId: '',
          });
        });
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

  // ---- formulario de presupuesto ----
  function resetBudgetForm() {
    setEditingBudgetRow(null);
    setShowForm(false);
    setForm(emptyBudgetForm(selectedProjectId));
    setImportWarnings([]);
  }

  function startCreateBudget(prefillSupplierRow) {
    setEditingBudgetRow(null);
    const base = emptyBudgetForm(selectedProjectId);
    setForm(
      prefillSupplierRow?.supplierKey
        ? {
            ...base,
            supplierKey: prefillSupplierRow.supplierKey,
            supplierName: prefillSupplierRow.supplierNameSnapshot || '',
            supplierCardCode: prefillSupplierRow.supplierCardCode || '',
            businessPartner: prefillSupplierRow.businessPartner || '',
            vendorId: prefillSupplierRow.vendorId || '',
          }
        : base,
    );
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
      groupAdvancePcts: Object.fromEntries(Object.entries(row.groupAdvancePcts || {}).map(([name, pct]) => [name, String(pct)])),
      isActive: row.isActive !== false,
      lineItems: (row.lineItems && row.lineItems.length ? row.lineItems : [emptyConceptoRow()]).map((item) => ({
        id: item.id,
        description: item.description || '',
        unit: item.unit || '',
        quantity: String(item.quantity ?? ''),
        unitPrice: String(item.unitPrice ?? ''),
        group: item.group || '',
        ...(item.isExtra
          ? { isExtra: true, extraKind: item.extraKind, extraNote: item.extraNote, addedAt: item.addedAt, addedBy: item.addedBy }
          : {}),
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
        group: item.group || '',
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
          group: String(row.group || '').trim(),
          ...(row.isExtra
            ? { isExtra: true, extraKind: row.extraKind, extraNote: row.extraNote, addedAt: row.addedAt, addedBy: row.addedBy }
            : {}),
        }));
      // % de anticipo por grupo (solo grupos que existen y tienen anticipo).
      const groupNames = new Set(lineItemsPayload.map((row) => row.group));
      const groupAdvancePcts = Object.fromEntries(
        Object.entries(form.groupAdvancePcts || {})
          .filter(([name, pct]) => groupNames.has(name) && Number(pct) > 0)
          .map(([name, pct]) => [name, Number(pct)]),
      );
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
          groupAdvancePcts,
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
          groupAdvancePcts,
          lineItems: lineItemsPayload,
        });
        // el proveedor del presupuesto nuevo queda a la vista
        setExpandedSuppliers((prev) => new Set(prev).add(String(form.supplierKey || '')));
      }

      await loadBudgets();
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
      await loadBudgets();
      resetBudgetForm();
    } catch (e) {
      setError(e.message || 'No se pudo eliminar el presupuesto');
    } finally {
      setSaving(false);
    }
  }

  // ---- asignar pagos ----
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
    setOpeningBudget(null);
    setAssigningBudget(row);
    setTransactionSearch('');
    loadBudgetPaymentTransactions(row.id);
  }

  // Seleccionar/quitar todos los pagos visibles (los de otro presupuesto no se tocan).
  function setAllTransactionsSelected(selected) {
    setSelectedTransactionIds((prev) => {
      const next = new Set(prev);
      candidateTransactions.forEach((tx) => {
        if (tx.isAssignedToOtherBudget) return;
        if (selected) next.add(tx.id);
        else next.delete(tx.id);
      });
      return next;
    });
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
      await loadBudgets();
      await loadBudgetPaymentTransactions(assigningBudget.id, transactionSearch);
    } catch (e) {
      setError(e.message || 'No se pudieron guardar las asignaciones');
    } finally {
      setSaving(false);
    }
  }

  // ---- saldo inicial: pagos previos y anticipo ya entregado ----
  async function openOpeningPanel(row) {
    closeAssignPayments();
    setOpeningBudget(row);
    setOpeningLoading(true);
    setError('');
    try {
      const payload = await api.estimationBudgetTransactions(row.id);
      setOpeningTransactions(Array.isArray(payload?.items) ? payload.items : []);
      setOpeningRequiresAssignment(Boolean(payload?.supplierHasMultipleActiveBudgets));
      const assignments = {};
      (row.openingAdvanceTransactionIds || []).forEach((id) => { assignments[id] = 'advance'; });
      (row.openingPriorPaymentTransactionIds || []).forEach((id) => { assignments[id] = 'prior'; });
      setOpeningAssignments(assignments);
      setOpeningManualAdvance(row.openingManualAdvanceAmount ? String(row.openingManualAdvanceAmount) : '');
      setOpeningManualPrior(row.openingManualPriorPaidAmount ? String(row.openingManualPriorPaidAmount) : '');
      setOpeningNote(row.openingNote || '');
    } catch (e) {
      setError(e.message || 'No se pudieron cargar los pagos del proveedor');
    } finally {
      setOpeningLoading(false);
    }
  }

  const openingTotals = useMemo(() => {
    let advance = Number(openingManualAdvance) || 0;
    let prior = Number(openingManualPrior) || 0;
    openingTransactions.forEach((tx) => {
      if (openingAssignments[tx.id] === 'advance') advance += Number(tx.amountWithTax) || 0;
      if (openingAssignments[tx.id] === 'prior') prior += Number(tx.amountWithTax) || 0;
    });
    const total = Number(openingBudget?.totalContractedAmount) || 0;
    return { advance, prior, paid: advance + prior, paidPct: total > 0 ? ((advance + prior) / total) * 100 : 0 };
  }, [openingTransactions, openingAssignments, openingManualAdvance, openingManualPrior, openingBudget]);

  async function saveOpeningBalance() {
    if (!openingBudget) return;
    setSaving(true);
    setError('');
    try {
      const ids = (kind) => Object.entries(openingAssignments).filter(([, value]) => value === kind).map(([id]) => id);
      await api.saveEstimationOpeningBalance(openingBudget.id, {
        advanceTransactionIds: ids('advance'),
        priorPaymentTransactionIds: ids('prior'),
        manualAdvanceAmount: Number(openingManualAdvance) || 0,
        manualPriorPaidAmount: Number(openingManualPrior) || 0,
        note: openingNote,
      });
      setOpeningBudget(null);
      await loadBudgets();
    } catch (e) {
      setError(e.message || 'No se pudo guardar el saldo inicial');
    } finally {
      setSaving(false);
    }
  }

  // ---- costo / m² ----
  const selectedProject = projectsById.get(String(selectedProjectId || '')) || null;
  const areaM2 = localAreaM2Override ?? selectedProject?.areaM2 ?? null;
  const costoM2 = areaM2 && areaM2 > 0 ? totalEgresosSinIva / areaM2 : null;

  async function saveAreaM2() {
    const raw = areaM2Input.trim();
    if (!selectedProjectId) return;
    setSavingAreaM2(true);
    setAreaM2Error('');
    try {
      await api.updateAdminProjectAreaM2(selectedProjectId, raw);
      const parsed = raw === '' ? null : Number(raw);
      setLocalAreaM2Override(parsed);
      setEditingAreaM2(false);
    } catch (e) {
      setAreaM2Error(e.message || 'No se pudo guardar');
    } finally {
      setSavingAreaM2(false);
    }
  }

  function startEditAreaM2() {
    setAreaM2Input(areaM2 != null ? String(areaM2) : '');
    setAreaM2Error('');
    setEditingAreaM2(true);
  }

  const icon = (children, stroke = 'var(--primary)') => (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={stroke} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      {children}
    </svg>
  );

  const grandKpis = [
    {
      label: 'Presupuesto total',
      value: formatCurrency(grandTotals.contracted),
      sub: 'comprometido en presupuestos por conceptos',
      icon: icon(<><line x1="12" y1="1" x2="12" y2="23" /><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" /></>),
    },
    {
      label: 'Total pagado',
      value: formatCurrency(grandTotals.paid),
      sub: 'ejecutado',
      icon: icon(<><path d="M21 12V7H5a2 2 0 0 1 0-4h14v4" /><path d="M3 5v14a2 2 0 0 0 2 2h16v-5" /><path d="M18 12a2 2 0 0 0 0 4h4v-4z" /></>),
    },
    {
      label: '% pagado',
      value: formatPct(grandTotals.paidPct),
      sub: classifyBudgetStatus(grandTotals.paidPct).label,
      icon: icon(<><line x1="19" y1="5" x2="5" y2="19" /><circle cx="6.5" cy="6.5" r="2.5" /><circle cx="17.5" cy="17.5" r="2.5" /></>),
    },
    {
      label: 'Saldo disponible',
      value: formatCurrency(grandTotals.balance),
      sub: grandTotals.balance < 0 ? '⚠ excedido' : 'restante',
      icon: icon(<><polyline points="23 6 13.5 15.5 8.5 10.5 1 18" /><polyline points="17 6 23 6 23 12" /></>, grandTotals.balance < 0 ? 'var(--danger-text, #b91c1c)' : 'var(--primary)'),
      danger: grandTotals.balance < 0,
    },
    {
      label: '% de avance estimado',
      value: formatPct(grandTotals.progressPct),
      sub: `${formatCurrency(grandTotals.progressAmount)} en estimaciones aprobadas`,
      icon: icon(<><rect x="4" y="2" width="16" height="20" rx="2" /><path d="M9 22v-4h6v4M8 6h.01M16 6h.01M8 10h.01M16 10h.01M8 14h.01M16 14h.01" /></>),
    },
    {
      label: 'Número de presupuestos',
      value: grandTotals.count,
      sub: `${groupedRows.length} proveedor${groupedRows.length === 1 ? '' : 'es'}${includeInactive ? ` · ${grandTotals.activeCount} activos` : ''}`,
      icon: icon(<><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /></>),
    },
  ];

  return (
    <div style={{ display: 'grid', gap: 14 }}>
      {/* KPI bar */}
      <div className="kpi-grid">
        {grandKpis.map((k) => (
          <div className="kpi-card" key={k.label}>
            <div className="kpi-icon" style={k.danger ? { background: 'var(--danger-bg, #fee2e2)' } : undefined}>
              {k.icon}
            </div>
            <div>
              <div className="kpi-label">{k.label}</div>
              <div className="kpi-value" style={k.danger ? { color: 'var(--danger-text, #b91c1c)' } : undefined}>{k.value}</div>
              <div className="kpi-sub">{k.sub}</div>
            </div>
          </div>
        ))}

        {/* Costo / m² — editable inline */}
        <div className="kpi-card">
          <div className="kpi-icon">
            {icon(<><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" /><polyline points="9 22 9 12 15 12 15 22" /></>)}
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div className="kpi-label">Costo / m²</div>
            {editingAreaM2 ? (
              <form
                onSubmit={(e) => { e.preventDefault(); saveAreaM2(); }}
                style={{ display: 'flex', gap: 4, alignItems: 'center', marginTop: 2 }}
              >
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  placeholder="m² del proyecto"
                  value={areaM2Input}
                  onChange={(e) => setAreaM2Input(e.target.value)}
                  disabled={savingAreaM2}
                  autoFocus
                  style={{ width: 110, fontSize: 13, padding: '2px 6px' }}
                />
                <button type="submit" disabled={savingAreaM2} style={{ fontSize: 12, padding: '2px 8px' }}>
                  {savingAreaM2 ? '...' : 'OK'}
                </button>
                <button type="button" className="secondary" onClick={() => setEditingAreaM2(false)} disabled={savingAreaM2} style={{ fontSize: 12, padding: '2px 8px' }}>
                  ✕
                </button>
              </form>
            ) : (
              <div
                className="kpi-value"
                style={{ cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6 }}
                onClick={startEditAreaM2}
                title="Clic para configurar m²"
              >
                {costoM2 != null ? formatCurrency(costoM2) : '—'}
              </div>
            )}
            {areaM2Error && <div className="small" style={{ color: 'var(--danger-text, #b91c1c)', marginTop: 2 }}>{areaM2Error}</div>}
            <div className="kpi-sub">
              {costoM2 != null ? `${Number(areaM2).toLocaleString('es-MX')} m²` : (editingAreaM2 ? 'ingresa m² del proyecto' : 'clic para configurar')}
            </div>
          </div>
        </div>
      </div>

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
                value={formTotals.usesGroupAdvance ? formTotals.advanceAmount.toFixed(2) : form.advanceAmount}
                onChange={(e) => setForm((prev) => ({ ...prev, advanceAmount: e.target.value }))}
                placeholder="0.00"
                style={{ width: 120 }}
                disabled={formTotals.usesGroupAdvance}
                title={formTotals.usesGroupAdvance ? 'Sale de los % de anticipo por grupo' : undefined}
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
                  accept=".xlsx,.csv,.pdf,.docx"
                  onChange={handleImportConceptosFile}
                  style={{ display: 'none' }}
                />
                <button
                  type="button"
                  className="secondary"
                  onClick={() => importFileInputRef.current?.click()}
                  disabled={importingConceptos}
                >
                  {importingConceptos ? 'Importando...' : '⭱ Importar Excel/CSV/PDF/Word'}
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
                    <th>Grupo</th>
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
                            list="budget-group-options"
                            value={row.group || ''}
                            onChange={(e) => updateConceptoRow(index, { group: e.target.value })}
                            placeholder="Sin grupo"
                            style={{ width: 150 }}
                          />
                        </td>
                        <td>
                          <input
                            value={row.description}
                            onChange={(e) => updateConceptoRow(index, { description: e.target.value })}
                            required
                          />
                          {row.isExtra && (
                            <span className="small" style={{ marginLeft: 6, color: '#92400e' }} title={row.extraNote || undefined}>
                              {row.extraKind === 'adicional' ? 'adicional' : 'extra'}
                            </span>
                          )}
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
            <datalist id="budget-group-options">
              {listFormGroups(form.lineItems).filter((g) => g.name).map((g) => (
                <option key={g.name} value={g.name} />
              ))}
            </datalist>
            {listFormGroups(form.lineItems).some((g) => g.name) && (
              <div style={{ marginTop: 10 }}>
                <label>Anticipo por grupo</label>
                <div className="small" style={{ marginBottom: 4 }}>
                  Cada grupo amortiza su propio % de anticipo al estimar. Si lo dejas en 0 el grupo no amortiza (por ejemplo, los extras).
                </div>
                <div style={{ overflowX: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Grupo</th>
                        <th>Conceptos</th>
                        <th>Presupuesto</th>
                        <th>% anticipo</th>
                        <th>Anticipo</th>
                      </tr>
                    </thead>
                    <tbody>
                      {listFormGroups(form.lineItems).map((group) => {
                        const pct = Number((form.groupAdvancePcts || {})[group.name]) || 0;
                        return (
                          <tr key={group.name || '__general__'}>
                            <td>{groupLabel(group.name)}</td>
                            <td>{group.count}</td>
                            <td>{formatCurrency(group.amount)}</td>
                            <td>
                              <input
                                type="number"
                                min="0"
                                max="100"
                                step="0.01"
                                value={(form.groupAdvancePcts || {})[group.name] ?? ''}
                                onChange={(e) => setForm((prev) => ({
                                  ...prev,
                                  groupAdvancePcts: { ...(prev.groupAdvancePcts || {}), [group.name]: e.target.value },
                                }))}
                                style={{ width: 90 }}
                              />
                            </td>
                            <td>{formatCurrency((group.amount * pct) / 100)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
            <div className="row" style={{ gap: 16, fontSize: 13, marginTop: 4 }}>
              <div><strong>Total contratado:</strong> {formatCurrency(formTotals.totalContractedAmount)}</div>
              {(Boolean(form.advanceAmortizationEnabled) || formTotals.usesGroupAdvance) && (
                <div><strong>% Anticipo (calculado):</strong> {formatPct(formTotals.advancePct)}</div>
              )}
              {formTotals.usesGroupAdvance && (
                <div><strong>Anticipo previsto:</strong> {formatCurrency(formTotals.advanceAmount)}</div>
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

      <div className="card budgets-card" style={{ overflow: 'hidden' }}>
        {/* Toolbar */}
        <div className="card-header">
          <div className="search-input-wrap" style={{ maxWidth: 360 }}>
            <input
              className="search-input"
              value={supplierFilter}
              onChange={(e) => setSupplierFilter(e.target.value)}
              placeholder="Filtrar por proveedor"
            />
          </div>
          <button type="button" className="secondary" onClick={loadBudgets}>Buscar</button>
          <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
            <input type="checkbox" checked={includeInactive} onChange={(e) => setIncludeInactive(e.target.checked)} />
            Mostrar inactivos
          </label>
          <button
            type="button"
            className="secondary"
            onClick={() => setExpandedSuppliers(expandedSuppliers.size ? new Set() : new Set(groupedRows.map((group) => group.key)))}
            disabled={!groupedRows.length}
          >
            {expandedSuppliers.size ? 'Contraer todo' : 'Expandir todo'}
          </button>
          <div style={{ flex: 1 }} />
          <button type="button" onClick={() => (showForm ? resetBudgetForm() : startCreateBudget())} style={{ fontSize: 13 }}>
            {showForm ? '✕ Cancelar' : '+ Nuevo presupuesto'}
          </button>
        </div>

        {loading ? (
          <div className="small" style={{ padding: 16 }}>Cargando presupuestos...</div>
        ) : (
          <div className="budgets-table-shell" style={{ overflowX: 'auto' }}>
            <table className="budgets-table">
              <thead>
                <tr>
                  <th className="col-supplier">Proveedor</th>
                  <th className="col-count"># presupuestos</th>
                  <th className="col-money">Presupuesto total</th>
                  <th className="col-money">Pagado total</th>
                  <th className="col-money">Saldo total</th>
                  <th className="col-progress">% pagado</th>
                  <th className="col-count">% avance estimado</th>
                  <th className="col-status">Estado global</th>
                  <th className="col-detail">Detalle</th>
                </tr>
              </thead>
              <tbody>
                {groupedRows.map((group) => {
                  const status = classifyBudgetStatus(group.totals.paidPct);
                  const isExpanded = expandedSuppliers.has(group.key);
                  return (
                    <React.Fragment key={group.key}>
                      <tr className="budgets-group-row">
                        <td>
                          <button
                            type="button"
                            className="secondary budgets-expand-btn"
                            onClick={() => toggleSupplierExpand(group.key)}
                            style={{ marginRight: 8 }}
                            aria-expanded={isExpanded}
                            aria-label={isExpanded ? 'Colapsar proveedor' : 'Expandir proveedor'}
                          >
                            {isExpanded ? '−' : '+'}
                          </button>
                          <strong className="supplier-name">{group.supplierName || '—'}</strong>
                        </td>
                        <td>{group.items.length}</td>
                        <td>{formatCurrency(group.totals.contracted)}</td>
                        <td>{formatCurrency(group.totals.paid)}</td>
                        <td style={{ color: group.totals.balance < 0 ? 'var(--danger-text, #b91c1c)' : undefined }}>{formatCurrency(group.totals.balance)}</td>
                        <td>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                            <div style={{ flex: 1, height: 6, background: 'var(--gray-150)', borderRadius: 99, overflow: 'hidden', minWidth: 60 }}>
                              <div style={{ height: '100%', width: `${Math.min(group.totals.paidPct, 100)}%`, background: group.totals.paidPct > 100 ? 'var(--danger-text, #b91c1c)' : 'var(--primary)', borderRadius: 99, transition: 'width .4s' }} />
                            </div>
                            <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--gray-600)', whiteSpace: 'nowrap' }}>{Math.round(group.totals.paidPct)}%</span>
                          </div>
                        </td>
                        <td>{formatPct(group.totals.progressPct)}</td>
                        <td><span className={`budget-badge budget-status ${status.className}`}>{status.label}</span></td>
                        <td>
                          <button type="button" className="secondary" onClick={() => toggleSupplierExpand(group.key)}>
                            {isExpanded ? 'Ocultar' : 'Ver'}
                          </button>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr>
                          <td colSpan={9} style={{ padding: 0 }}>
                            <div className="budgets-table-shell" style={{ overflowX: 'auto' }}>
                              <table className="budgets-table budgets-table-nested">
                                <thead>
                                  <tr>
                                    <th>Obra</th>
                                    <th>Presupuesto</th>
                                    <th>Conceptos</th>
                                    <th>Contratado</th>
                                    <th>Pagado</th>
                                    <th>Saldo</th>
                                    <th>% pagado</th>
                                    <th>% avance estimado</th>
                                    <th>Estimaciones</th>
                                    <th>Estado</th>
                                    <th>Acciones</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {group.items.map((row) => {
                                    const project = projectsById.get(String(row.projectId || ''));
                                    const rowTotals = summarizeBudgets([row]);
                                    const childStatus = row.isActive === false
                                      ? { label: 'Inactivo', className: 'in-budget' }
                                      : classifyBudgetStatus(rowTotals.paidPct);
                                    const hasEstimations = Number(row.estimationsCount) > 0;
                                    return (
                                      <tr key={row.id}>
                                        <td>{project?.displayName || project?.name || row.projectId}</td>
                                        <td>{row.name || '—'}</td>
                                        <td>{(row.lineItems || []).length}</td>
                                        <td>
                                          {formatCurrency(row.totalContractedAmount)}
                                          {Number(row.extraAmount) > 0 && (
                                            <div className="small" style={{ color: '#92400e' }}>incluye {formatCurrency(row.extraAmount)} en extras</div>
                                          )}
                                        </td>
                                        <td>{formatCurrency(row.paidAmount)}</td>
                                        <td style={{ color: rowTotals.balance < 0 ? '#b91c1c' : undefined }}>{formatCurrency(rowTotals.balance)}</td>
                                        <td><span className={`budget-badge budget-progress ${childStatus.className}`}>{formatPct(rowTotals.paidPct)}</span></td>
                                        <td>{formatPct(rowTotals.progressPct)}</td>
                                        <td>{row.estimationsCount}</td>
                                        <td><span className={`budget-badge budget-status ${childStatus.className}`}>{childStatus.label}</span></td>
                                        <td>
                                          <div className="row" style={{ gap: 6, flexWrap: 'wrap' }}>
                                            <button type="button" className="secondary" onClick={() => startEditBudget(row)}>Editar</button>
                                            <button type="button" className="secondary" onClick={() => startAssignPayments(row)}>Asignar pagos</button>
                                            <button
                                              type="button"
                                              className="secondary"
                                              onClick={() => { closeAssignPayments(); setOpeningBudget(null); setExtrasBudget(row); }}
                                              title="Agregar conceptos extra o un presupuesto adicional"
                                            >
                                              + Extras
                                            </button>
                                            <button
                                              type="button"
                                              className="secondary"
                                              onClick={() => openOpeningPanel(row)}
                                              disabled={hasEstimations}
                                              title={hasEstimations ? 'El saldo inicial solo puede cambiarse antes de la primera estimación' : 'Anticipo y pagos previos ya entregados'}
                                            >
                                              Saldo inicial
                                            </button>
                                            {onOpenEstimations && (
                                              <button type="button" onClick={() => onOpenEstimations(row.id)}>Estimaciones →</button>
                                            )}
                                          </div>
                                        </td>
                                      </tr>
                                    );
                                  })}
                                </tbody>
                              </table>
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
                {!groupedRows.length && (
                  <tr>
                    <td colSpan={9} className="small" style={{ textAlign: 'center' }}>No hay presupuestos para los filtros seleccionados.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}

              {assigningBudget && (
                <div className="grid budgets-assignment-panel" style={{ gap: 8, borderRadius: 10, padding: 12 }}>
                  <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                    <strong>Asignar pagos · {assigningBudget.supplierNameSnapshot || assigningBudget.supplierKey}</strong>
                    <button type="button" className="secondary" onClick={closeAssignPayments}>Cerrar</button>
                  </div>
                  <div className="small">
                    Si este es el único presupuesto activo del proveedor, todos sus pagos cuentan automáticamente: quita aquí solo los que
                    sean de otro presupuesto o estén fuera de este (los pagos nuevos seguirán contando). Lo que dejes marcado se descuenta
                    de lo que se libera en la estimación. Con más de un presupuesto activo del mismo proveedor, marca manualmente qué
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
                    <button type="button" className="secondary" onClick={() => setAllTransactionsSelected(true)} disabled={loadingTransactions || !candidateTransactions.length}>
                      Seleccionar todos
                    </button>
                    <button type="button" className="secondary" onClick={() => setAllTransactionsSelected(false)} disabled={loadingTransactions || !candidateTransactions.length}>
                      Quitar todos
                    </button>
                    <button type="button" onClick={saveAssignedPayments} disabled={saving || loadingTransactions}>
                      {saving ? 'Guardando...' : 'Guardar asignación'}
                    </button>
                  </div>
                  {!loadingTransactions && candidateTransactions.length > 0 && (
                    <div className="small">
                      {selectedTransactionIds.size} de {candidateTransactions.filter((tx) => !tx.isAssignedToOtherBudget).length} pagos asignados a este presupuesto ·{' '}
                      {formatCurrency(
                        candidateTransactions
                          .filter((tx) => selectedTransactionIds.has(tx.id))
                          .reduce((sum, tx) => sum + (Number(tx.amountWithTax) || 0), 0),
                      )}
                    </div>
                  )}
                  {loadingTransactions ? (
                    <div className="small">Cargando transacciones...</div>
                  ) : (
                    <div style={{ overflowX: 'auto', maxHeight: 320 }}>
                      <table>
                        <thead>
                          <tr>
                            <th>
                              <input
                                type="checkbox"
                                title="Seleccionar / quitar todos"
                                checked={
                                  candidateTransactions.some((tx) => !tx.isAssignedToOtherBudget) &&
                                  candidateTransactions.filter((tx) => !tx.isAssignedToOtherBudget).every((tx) => selectedTransactionIds.has(tx.id))
                                }
                                onChange={(e) => setAllTransactionsSelected(e.target.checked)}
                              />
                            </th>
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

        {extrasBudget && (
          <div style={{ padding: 12 }}>
            <ExtrasPanel
              budget={extrasBudget}
              onClose={() => setExtrasBudget(null)}
              onSaved={async () => {
                setExtrasBudget(null);
                await loadBudgets();
              }}
            />
          </div>
        )}

        {openingBudget && (
          <div className="grid budgets-assignment-panel" style={{ gap: 10, borderRadius: 10, padding: 12 }}>
            <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
              <strong>Saldo inicial · {openingBudget.supplierNameSnapshot || openingBudget.supplierKey} · {openingBudget.name || 'Presupuesto'}</strong>
              <button type="button" className="secondary" onClick={() => setOpeningBudget(null)} disabled={saving}>Cerrar</button>
            </div>
            <div className="small">
              Para presupuestos que ya traen pagos al empezar a usar el módulo. El anticipo se amortiza solo en cada estimación y los pagos a cuenta
              se descuentan de lo que se libera. Si no registras nada, se toman todos los pagos asignados al presupuesto. Solo puede cambiarse antes de la primera estimación.
            </div>
            {openingLoading ? (
              <div className="small">Cargando pagos del proveedor...</div>
            ) : (
              <>
                {openingRequiresAssignment && (
                  <div className="small" style={{ color: '#92400e' }}>
                    Este proveedor tiene varios presupuestos activos: solo puedes elegir pagos ya asignados a este presupuesto (usa «Asignar pagos»). Los montos manuales sí funcionan.
                  </div>
                )}
                <div style={{ overflowX: 'auto', maxHeight: 260, overflowY: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Fecha</th>
                        <th>Descripción</th>
                        <th>Monto</th>
                        <th>Cómo se considera</th>
                      </tr>
                    </thead>
                    <tbody>
                      {openingTransactions.map((tx) => {
                        const blocked = tx.isAssignedToOtherBudget || (openingRequiresAssignment && !tx.isAssignedToCurrentBudget);
                        return (
                          <tr key={tx.id} style={blocked ? { opacity: 0.5 } : undefined}>
                            <td>{formatDate(tx.date)}</td>
                            <td>{tx.description || '—'}</td>
                            <td>{formatCurrency(tx.amountWithTax)}</td>
                            <td>
                              <select
                                value={openingAssignments[tx.id] || ''}
                                disabled={blocked}
                                onChange={(e) => setOpeningAssignments((prev) => {
                                  const next = { ...prev };
                                  if (e.target.value) next[tx.id] = e.target.value;
                                  else delete next[tx.id];
                                  return next;
                                })}
                              >
                                <option value="">No incluir</option>
                                <option value="advance">Anticipo</option>
                                <option value="prior">Pago a cuenta</option>
                              </select>
                              {tx.isAssignedToOtherBudget && <span className="small"> asignado a otro presupuesto</span>}
                            </td>
                          </tr>
                        );
                      })}
                      {!openingTransactions.length && (
                        <tr><td colSpan={4} className="small" style={{ textAlign: 'center' }}>Este proveedor no tiene pagos registrados.</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
                <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
                  <div>
                    <label>Anticipo manual (opcional)</label>
                    <input type="number" min="0" step="0.01" value={openingManualAdvance} onChange={(e) => setOpeningManualAdvance(e.target.value)} style={{ width: 170 }} />
                  </div>
                  <div>
                    <label>Otros pagos a cuenta manuales (opcional)</label>
                    <input type="number" min="0" step="0.01" value={openingManualPrior} onChange={(e) => setOpeningManualPrior(e.target.value)} style={{ width: 170 }} />
                  </div>
                  <div style={{ flex: 1, minWidth: 200 }}>
                    <label>Nota</label>
                    <input value={openingNote} onChange={(e) => setOpeningNote(e.target.value)} />
                  </div>
                </div>
                <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13 }}>
                  <div><strong>Anticipo:</strong> {formatCurrency(openingTotals.advance)}</div>
                  <div><strong>Pagos a cuenta:</strong> {formatCurrency(openingTotals.prior)}</div>
                  <div><strong>Pagado a la fecha:</strong> {formatCurrency(openingTotals.paid)} ({formatPct(openingTotals.paidPct)} del presupuesto)</div>
                </div>
                <div className="row" style={{ gap: 8 }}>
                  <button type="button" onClick={saveOpeningBalance} disabled={saving}>{saving ? 'Guardando...' : 'Guardar saldo inicial'}</button>
                  <button type="button" className="secondary" onClick={() => setOpeningBudget(null)} disabled={saving}>Cancelar</button>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
