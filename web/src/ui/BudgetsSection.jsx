import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { PasteTextImport } from './PasteTextImport.jsx';
import { OpeningBalancePanel } from './OpeningBalancePanel.jsx';
import { SupplierPaymentsPanel } from './SupplierPaymentsPanel.jsx';
import { UnitSelect } from './UnitSelect.jsx';
import { ExtrasPanel } from './ExtrasPanel.jsx';
import {
  buildCanonicalSupplierKey,
  computeBudgetFormTotals,
  normalizeTextForSupplierKey,
  computeFormDiscount,
  computeLineItemAmount,
  netUnitPrice,
  groupLabel,
  listFormGroups,
  emptyBudgetForm,
  emptyConceptoRow,
  formatCurrency,
  formatDate,
  formatPct,
  generateId,
  isBlankConceptoRow,
  normalizeUnit,
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

export function BudgetsSection({ projects, selectedProjectId, onOpenEstimations, isReviewer = false, approvalProjectIds = null, onApprovalChange }) {
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
  const [showPasteText, setShowPasteText] = useState(false);
  const [paymentsSupplier, setPaymentsSupplier] = useState(null);
  const budgetFormRef = useRef(null);

  // El formulario (nuevo o de edición) y los paneles abren debajo de la tabla de proveedores:
  // se desplaza la pantalla hasta ellos para que no pasen desapercibidos.
  useEffect(() => {
    if (!showForm) return;
    const timer = setTimeout(() => budgetFormRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 60);
    return () => clearTimeout(timer);
  }, [showForm, editingBudgetRow?.id]);
  const importFileInputRef = useRef(null);
  const groupFileInputRef = useRef(null);
  const [selectedConceptoIds, setSelectedConceptoIds] = useState(new Set());
  const [bulkGroupName, setBulkGroupName] = useState('');
  const [groupingFromFile, setGroupingFromFile] = useState(false);
  const [groupingMessage, setGroupingMessage] = useState('');

  const [assigningBudget, setAssigningBudget] = useState(null);
  const [candidateTransactions, setCandidateTransactions] = useState([]);
  const [selectedTransactionIds, setSelectedTransactionIds] = useState(new Set());
  const [transactionSearch, setTransactionSearch] = useState('');
  const [loadingTransactions, setLoadingTransactions] = useState(false);

  const [viewingBudget, setViewingBudget] = useState(null);
  const [extrasBudget, setExtrasBudget] = useState(null);
  const [openingBudget, setOpeningBudget] = useState(null);

  // Paneles de detalle / asignar pagos / extras / saldo inicial: abren abajo; se lleva la vista hasta ellos.
  useEffect(() => {
    if (!(viewingBudget || assigningBudget || extrasBudget || openingBudget)) return undefined;
    const timer = setTimeout(
      () => document.querySelector('.budgets-assignment-panel')?.scrollIntoView({ behavior: 'smooth', block: 'start' }),
      80,
    );
    return () => clearTimeout(timer);
  }, [viewingBudget?.id, assigningBudget?.id, extrasBudget?.id, openingBudget?.id]);

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
      .map((group) => ({ ...group, totals: summarizeBudgets(group.items, { bySupplier: true }) }))
      .sort((a, b) => a.supplierName.localeCompare(b.supplierName, 'es'));
  }, [rows]);

  const grandTotals = useMemo(() => summarizeBudgets(rows), [rows]);

  const conceptoIdsWithHistory = useMemo(
    () => new Set(editingBudgetRow?.conceptoIdsWithHistory || []),
    [editingBudgetRow],
  );

  const formDiscount = useMemo(
    () => computeFormDiscount(form.lineItems, form),
    [form.lineItems, form.discountEnabled, form.discountMode, form.discountValue],
  );
  const discountPct = formDiscount.applies ? formDiscount.pct : 0;
  const formTotals = useMemo(
    () => computeBudgetFormTotals(form.lineItems, form.advanceAmount, form.groupAdvancePcts, discountPct, { mode: form.advanceMode, pct: form.advancePctInput }),
    [form.lineItems, form.advanceAmount, form.groupAdvancePcts, discountPct, form.advanceMode, form.advancePctInput],
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
    setViewingBudget(null);
    setExpandedSuppliers(new Set());
  }, [selectedProjectId]);

  useEffect(() => {
    if (!selectedProjectId || !isReviewer) return;
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
  function resetGroupingTools() {
    setSelectedConceptoIds(new Set());
    setBulkGroupName('');
    setGroupingMessage('');
  }

  function resetBudgetForm() {
    resetGroupingTools();
    setEditingBudgetRow(null);
    setShowForm(false);
    setForm(emptyBudgetForm(selectedProjectId));
    setImportWarnings([]);
  }

  function startCreateBudget(prefillSupplierRow) {
    resetGroupingTools();
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
    setViewingBudget(null);
    resetGroupingTools();
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
      advanceMode: row.advanceMode === 'pct' ? 'pct' : 'amount',
      advancePctInput: row.advanceMode === 'pct' ? String(row.advancePctInput ?? '') : '',
      discountEnabled: Boolean(row.discountMode),
      discountMode: row.discountMode === 'amount' ? 'amount' : 'pct',
      discountValue: row.discountMode === 'amount' ? String(row.discountAmount ?? '') : row.discountMode === 'pct' ? String(row.discountPct ?? '') : '',
      groupAdvancePcts: Object.fromEntries(Object.entries(row.groupAdvancePcts || {}).map(([name, pct]) => [name, String(pct)])),
      isActive: row.isActive !== false,
      lineItems: (row.lineItems && row.lineItems.length ? row.lineItems : [emptyConceptoRow()]).map((item) => ({
        id: item.id,
        description: item.description || '',
        unit: item.unit || '',
        quantity: String(item.quantity ?? ''),
        // En el formulario se edita el precio de lista; el descuento se aplica aparte.
        unitPrice: String(item.listUnitPrice ?? item.unitPrice ?? ''),
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

  // ---- agrupar conceptos ya capturados ----
  function toggleSelectConcepto(id) {
    setSelectedConceptoIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function applyBulkGroup() {
    const name = bulkGroupName.trim();
    if (!selectedConceptoIds.size) {
      setGroupingMessage('Marca primero los conceptos que quieres agrupar.');
      return;
    }
    setForm((prev) => ({
      ...prev,
      lineItems: prev.lineItems.map((row) => (selectedConceptoIds.has(row.id) ? { ...row, group: name } : row)),
    }));
    setGroupingMessage(`${selectedConceptoIds.size} concepto(s) ${name ? `agrupados en «${name}»` : 'sin grupo'}.`);
    setSelectedConceptoIds(new Set());
  }

  // Lee un Excel/Word/PDF con los grupos y se los pone a los conceptos que YA existen
  // (por descripción); no agrega ni quita conceptos, así no se pierde historial.
  async function handleGroupFromFile(event) {
    const file = event.target.files?.[0];
    if (event.target) event.target.value = '';
    if (!file) return;
    setGroupingFromFile(true);
    setGroupingMessage('');
    setError('');
    try {
      const result = await api.importEstimationConceptos(file);
      const items = Array.isArray(result?.items) ? result.items : [];
      if (!items.some((item) => item.group)) {
        setGroupingMessage('El archivo no trae grupos (títulos de sección o columna «Grupo»), no se cambió nada.');
        return;
      }
      const keyOf = (text) => normalizeTextForSupplierKey(text).replace(/[^a-z0-9 ]/g, '').replace(/\s+/g, ' ').trim();
      const withoutParens = (text) => String(text || '').replace(/\([^)]*\)/g, ' ');
      const rowInfo = form.lineItems.map((row) => ({ id: row.id, full: keyOf(row.description), loose: keyOf(withoutParens(row.description)) }));
      const matched = new Map(); // id del concepto -> grupo
      const takeFrom = (field, key, candidates) => {
        const hit = candidates.find((info) => info[field] === key && !matched.has(info.id));
        return hit ? hit.id : null;
      };
      const unmatchedItems = [];
      // 1) exacto: la descripción tal cual, o con la sección entre paréntesis (como la
      //    primera versión del importador de Word nombraba los conceptos repetidos).
      items.forEach((item) => {
        const group = item.group || '';
        const id = takeFrom('full', keyOf(`${item.description} (${group})`), rowInfo) || takeFrom('full', keyOf(item.description), rowInfo);
        if (id) matched.set(id, group);
        else unmatchedItems.push(item);
      });
      // 2) flexible: ignorando lo que va entre paréntesis en cualquiera de los dos lados.
      const stillUnmatched = [];
      unmatchedItems.forEach((item) => {
        const id = takeFrom('loose', keyOf(withoutParens(item.description)), rowInfo);
        if (id) matched.set(id, item.group || '');
        else stillUnmatched.push(item);
      });
      setForm((prev) => ({
        ...prev,
        lineItems: prev.lineItems.map((row) => {
          if (!matched.has(row.id)) return row;
          const group = matched.get(row.id);
          // «W.C. (COLOCACION DE MUEBLES)» ya no necesita el sufijo: el grupo lo distingue.
          const suffix = ` (${group})`;
          const description = group && row.description.toLowerCase().endsWith(suffix.toLowerCase())
            ? row.description.slice(0, row.description.length - suffix.length).trim()
            : row.description;
          return { ...row, group, description };
        }),
      }));
      const sample = stillUnmatched.slice(0, 4).map((item) => `«${item.description}»`).join(', ');
      setGroupingMessage(
        `${matched.size} concepto(s) agrupados desde el archivo` +
        (stillUnmatched.length
          ? `; ${stillUnmatched.length} del archivo no coinciden con ningún concepto del presupuesto (no se agregaron): ${sample}${stillUnmatched.length > 4 ? '…' : ''}`
          : '') +
        `. Revisa y guarda el presupuesto.`,
      );
    } catch (e) {
      setError(e.message || 'No se pudo leer el archivo');
    } finally {
      setGroupingFromFile(false);
    }
  }

  async function handleImportConceptosFile(event) {
    const file = event.target.files?.[0];
    if (event.target) event.target.value = '';
    if (!file) return;

    setImportingConceptos(true);
    setImportWarnings([]);
    setError('');
    try {
      applyImportResult(await api.importEstimationConceptos(file));
    } catch (e) {
      setError(e.message || 'No se pudo importar el archivo');
    } finally {
      setImportingConceptos(false);
    }
  }

  // Agrega a la tabla los conceptos extraídos de un archivo o de texto pegado.
  function applyImportResult(result) {
    const importedRows = (Array.isArray(result?.items) ? result.items : []).map((item) => ({
      id: generateId(),
      description: item.description || '',
      unit: normalizeUnit(item.unit),
      quantity: String(item.quantity ?? ''),
      unitPrice: String(item.unitPrice ?? ''),
      group: item.group || '',
    }));
    if (!importedRows.length) {
      setError('No se obtuvieron conceptos importables.');
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
      if (form.discountEnabled && !formDiscount.applies) {
        setError('El descuento no es válido: debe ser mayor a 0 y menor al subtotal (o a 100 %).');
        setSaving(false);
        return;
      }
      const advancePayload = form.advanceMode === 'pct' && !formTotals.usesGroupAdvance
        ? { advanceMode: 'pct', advancePct: Number(form.advancePctInput) || 0 }
        : { advanceMode: 'amount' };
      const discountPayload = form.discountEnabled && formDiscount.applies
        ? { discountMode: form.discountMode, ...(form.discountMode === 'amount' ? { discountAmount: Number(form.discountValue) } : { discountPct: Number(form.discountValue) }) }
        : { discountMode: null };

      if (editingBudgetRow) {
        await api.updateEstimationBudget(editingBudgetRow.id, {
          name: form.name,
          notes: form.notes,
          isActive: Boolean(form.isActive),
          currency: form.currency,
          retentionPct: Number(form.retentionPct) || 0,
          advanceAmortizationEnabled: Boolean(form.advanceAmortizationEnabled),
          advanceAmount,
          ...advancePayload,
          groupAdvancePcts,
          ...discountPayload,
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
          ...advancePayload,
          groupAdvancePcts,
          ...discountPayload,
          lineItems: lineItemsPayload,
        });
        // el proveedor del presupuesto nuevo queda a la vista
        setExpandedSuppliers((prev) => new Set(prev).add(String(form.supplierKey || '')));
      }

      await loadBudgets();
      resetBudgetForm();
      if (onApprovalChange) onApprovalChange();
    } catch (e) {
      setError(e.message || 'No se pudo guardar el presupuesto');
    } finally {
      setSaving(false);
    }
  }

  const canAuthorizeProject = (projectId) =>
    isReviewer && (!Array.isArray(approvalProjectIds) || approvalProjectIds.includes(String(projectId)));

  async function authorizeBudget(row) {
    const confirmed = window.confirm(
      `¿Autorizar el presupuesto «${row.name || row.supplierNameSnapshot}»? Se bloquearán conceptos, precios y volúmenes, y ya se podrá estimar.`,
    );
    if (!confirmed) return;
    setSaving(true);
    setError('');
    try {
      await api.authorizeEstimationBudget(row.id);
      await loadBudgets();
      if (onApprovalChange) onApprovalChange();
    } catch (e) {
      setError(e.message || 'No se pudo autorizar el presupuesto');
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
    setViewingBudget(null);
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
  function openOpeningPanel(row) {
    setViewingBudget(null);
    closeAssignPayments();
    setError('');
    setOpeningBudget(row);
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

  // KPI de un proveedor: se muestran dentro del proveedor al expandirlo.
  const supplierKpis = (items) => {
    const sum = (key) => items.reduce((acc, row) => acc + (Number(row[key]) || 0), 0);
    const contracted = sum('totalContractedAmount');
    // Pagado al proveedor: todos sus pagos (menos los desasignados), no solo los ligados a un presupuesto.
    const paid = items.some((row) => row.supplierPaidAmount != null) ? Number(items.find((row) => row.supplierPaidAmount != null).supplierPaidAmount) || 0 : sum('paidAmount');
    const progress = sum('approvedProgressAmount');
    const delivered = items.reduce(
      (acc, row) => acc + ((row.openingMode === 'explicit' || row.openingMode === 'auto-frozen' ? Number(row.openingAdvanceAmount) || 0 : 0) + (Number(row.advanceGivenAmount) || 0)),
      0,
    );
    const list = [
      { label: 'Total contratado', value: formatCurrency(contracted), sub: `${items.length} presupuesto${items.length === 1 ? '' : 's'}${sum('extraAmount') > 0 ? ` · incluye ${formatCurrency(sum('extraAmount'))} en extras` : ''}` },
    ];
    if (isReviewer) {
      list.push(
        { label: 'Total pagado a la fecha', value: formatCurrency(paid), sub: `${formatPct(contracted > 0 ? (paid / contracted) * 100 : 0)} del contratado · todos los pagos del proveedor` },
        { label: 'Saldo por pagar', value: formatCurrency(contracted - paid), sub: 'contratado − pagado', danger: contracted - paid < 0 },
      );
    }
    list.push({ label: 'Avance estimado', value: formatPct(contracted > 0 ? (progress / contracted) * 100 : 0), sub: `${formatCurrency(progress)} en estimaciones aprobadas` });
    list.push({ label: 'Anticipo', value: formatCurrency(delivered), sub: `entregado · por amortizar ${formatCurrency(sum('remainingAdvanceBalance'))}` });
    list.push({ label: 'Retenido a la fecha', value: formatCurrency(sum('totalRetainedToDate')), sub: 'fondo de garantía' });
    list.push({ label: 'Al 100 %', value: `${items.filter((row) => row.isComplete).length} de ${items.length}`, sub: 'presupuestos completos' });
    return list;
  };

  // Quien solo captura presupuestos (sin rol admin) no ve pagos, saldos ni costos.
  // Los KPI de dinero se ven por proveedor (al expandirlo); arriba solo el costo por m² del proyecto.
  const visibleKpis = [];

  return (
    <div style={{ display: 'grid', gap: 14 }}>
      {/* KPI bar */}
      <div className="kpi-grid">
        {visibleKpis.map((k) => (
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
        {isReviewer && (
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
        )}
      </div>

      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}

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
                  {isReviewer && <th className="col-money">Pagado total</th>}
                  {isReviewer && <th className="col-money">Saldo total</th>}
                  {isReviewer && <th className="col-progress">% pagado</th>}
                  <th className="col-count">% avance estimado</th>
                  {isReviewer && <th className="col-status">Estado global</th>}
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
                        {isReviewer && <td>{formatCurrency(group.totals.paid)}</td>}
                        {isReviewer && <td style={{ color: group.totals.balance < 0 ? 'var(--danger-text, #b91c1c)' : undefined }}>{formatCurrency(group.totals.balance)}</td>}
                        {isReviewer && <td>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                            <div style={{ flex: 1, height: 6, background: 'var(--gray-150)', borderRadius: 99, overflow: 'hidden', minWidth: 60 }}>
                              <div style={{ height: '100%', width: `${Math.min(group.totals.paidPct, 100)}%`, background: group.totals.paidPct > 100 ? 'var(--danger-text, #b91c1c)' : 'var(--primary)', borderRadius: 99, transition: 'width .4s' }} />
                            </div>
                            <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--gray-600)', whiteSpace: 'nowrap' }}>{Math.round(group.totals.paidPct)}%</span>
                          </div>
                        </td>}
                        <td>{formatPct(group.totals.progressPct)}</td>
                        {isReviewer && <td><span className={`budget-badge budget-status ${status.className}`}>{status.label}</span></td>}
                        <td>
                          <button type="button" className="secondary" onClick={() => toggleSupplierExpand(group.key)}>
                            {isExpanded ? 'Ocultar' : 'Ver'}
                          </button>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr>
                          <td colSpan={9} style={{ padding: 0 }}>
                            <div className="kpi-grid" style={{ padding: 12 }}>
                              {supplierKpis(group.items).map((k) => (
                                <div className="kpi-card" key={k.label}>
                                  <div>
                                    <div className="kpi-label">{k.label}</div>
                                    <div className="kpi-value" style={k.danger ? { color: 'var(--danger-text, #b91c1c)' } : undefined}>{k.value}</div>
                                    <div className="kpi-sub">{k.sub}</div>
                                  </div>
                                </div>
                              ))}
                            </div>
                            {isReviewer && (
                              <div style={{ padding: '0 12px 8px', display: 'grid', gap: 8 }}>
                                <div>
                                  <button
                                    type="button"
                                    className="secondary"
                                    onClick={() => setPaymentsSupplier(paymentsSupplier?.key === group.key ? null : { key: group.key, name: group.supplierName })}
                                  >
                                    Pagos del proveedor (desasignar)
                                  </button>
                                </div>
                                {paymentsSupplier?.key === group.key && (
                                  <SupplierPaymentsPanel
                                    supplierKey={group.key}
                                    supplierName={group.supplierName || group.key}
                                    projectId={selectedProjectId}
                                    onClose={() => setPaymentsSupplier(null)}
                                    onSaved={async () => {
                                      setPaymentsSupplier(null);
                                      await loadBudgets();
                                    }}
                                  />
                                )}
                              </div>
                            )}
                            <div className="budgets-table-shell" style={{ overflowX: 'auto' }}>
                              <table className="budgets-table budgets-table-nested">
                                <thead>
                                  <tr>
                                    <th>Obra</th>
                                    <th>Presupuesto</th>
                                    <th>Conceptos</th>
                                    <th>Contratado</th>
                                    <th>% avance estimado</th>
                                    <th>Estimaciones</th>
                                    {isReviewer && <th>Estado</th>}
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
                                        <td>
                                          {row.name || '—'}
                                          {row.approvalStatus === 'PENDIENTE' && (
                                            <div className="small" style={{ color: '#92400e', fontWeight: 600 }}>
                                              {row.reauthRequired ? 'Por reautorizar' : 'Por autorizar'} · no se puede estimar
                                            </div>
                                          )}
                                        </td>
                                        <td>{(row.lineItems || []).length}</td>
                                        <td>
                                          {formatCurrency(row.totalContractedAmount)}
                                          {Number(row.extraAmount) > 0 && (
                                            <div className="small" style={{ color: '#92400e' }}>incluye {formatCurrency(row.extraAmount)} en extras</div>
                                          )}
                                        </td>
                                        <td>{formatPct(rowTotals.progressPct)}</td>
                                        <td>{row.estimationsCount}</td>
                                        {isReviewer && (
                                          <td>
                                            <span className={`budget-badge budget-status ${row.isActive === false ? 'in-budget' : row.isComplete ? 'paid' : 'in-budget'}`}>
                                              {row.isActive === false ? 'Inactivo' : row.isComplete ? 'Al 100 %' : row.approvalStatus === 'PENDIENTE' ? 'Por autorizar' : 'Activo'}
                                            </span>
                                          </td>
                                        )}
                                        <td>
                                          <div className="row" style={{ gap: 6, flexWrap: 'wrap' }}>
                                            <button
                                              type="button"
                                              className="secondary"
                                              onClick={() => { closeAssignPayments(); setOpeningBudget(null); setExtrasBudget(null); setViewingBudget(row); }}
                                              title="Ver el presupuesto: grupos, conceptos y totales"
                                            >
                                              Ver
                                            </button>
                                            <button type="button" className="secondary" onClick={() => startEditBudget(row)}>Editar</button>
                                            {row.approvalStatus === 'PENDIENTE' && canAuthorizeProject(row.projectId) && (
                                              <button type="button" onClick={() => authorizeBudget(row)} disabled={saving}>Autorizar</button>
                                            )}
                                            {isReviewer && <button type="button" className="secondary" onClick={() => startAssignPayments(row)}>Asignar pagos</button>}
                                            {isReviewer && <button
                                              type="button"
                                              className="secondary"
                                              onClick={() => { closeAssignPayments(); setOpeningBudget(null); setViewingBudget(null); setExtrasBudget(row); }}
                                              title="Agregar conceptos extra o un presupuesto adicional"
                                            >
                                              + Extras
                                            </button>}
                                            {isReviewer && <button
                                              type="button"
                                              className="secondary"
                                              onClick={() => openOpeningPanel(row)}
                                              disabled={hasEstimations}
                                              title={hasEstimations ? 'El saldo inicial solo puede cambiarse antes de la primera estimación' : 'Anticipo y pagos previos ya entregados'}
                                            >
                                              Saldo inicial
                                            </button>}
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

        {viewingBudget && (() => {
          const budget = rows.find((row) => row.id === viewingBudget.id) || viewingBudget;
          const concepts = budget.lineItems || [];
          const groups = budget.groups && budget.groups.length ? budget.groups : listFormGroups(concepts).map((g) => ({
            name: g.name, budgetAmount: g.amount, advancePct: 0, advanceAmount: 0, isExtra: false,
          }));
          const totals = summarizeBudgets([budget]);
          const discounted = Number(budget.discountPct) > 0;
          const cols = discounted ? 6 : 5;
          return (
            <div className="grid budgets-assignment-panel" style={{ gap: 10, borderRadius: 10, padding: 12 }}>
              <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
                <div>
                  <strong>{budget.supplierNameSnapshot || budget.supplierKey} · {budget.name || 'Presupuesto'}</strong>
                  <div className="small">
                    {concepts.length} concepto(s) en {groups.length} grupo(s) · retención {formatPct(budget.retentionPct)}
                    {Number(budget.advanceAmount) > 0 ? ` · anticipo ${formatCurrency(budget.advanceAmount)}` : ''}
                    {budget.notes ? ` · ${budget.notes}` : ''}
                  </div>
                </div>
                <div className="row" style={{ gap: 6 }}>
                  <button type="button" className="secondary" onClick={() => { startEditBudget(budget); }}>Editar</button>
                  {onOpenEstimations && <button type="button" onClick={() => onOpenEstimations(budget.id)}>Estimaciones →</button>}
                  <button type="button" className="secondary" onClick={() => setViewingBudget(null)}>✕ Cerrar</button>
                </div>
              </div>
              <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13 }}>
                {discounted && <div><strong>Subtotal:</strong> {formatCurrency(budget.listSubtotal)}</div>}
                {discounted && <div><strong>Descuento ({formatPct(budget.discountPct)}):</strong> −{formatCurrency(budget.discountAmount)}</div>}
                <div><strong>Contratado:</strong> {formatCurrency(budget.totalContractedAmount)}</div>
                {Number(budget.extraAmount) > 0 && <div><strong>Extras:</strong> {formatCurrency(budget.extraAmount)}</div>}
                {isReviewer && <div title="Solo los pagos asignados a este presupuesto; el pagado del proveedor está en sus KPI"><strong>Pagado asignado:</strong> {formatCurrency(budget.paidAmount)}</div>}
                <div><strong>Avance estimado:</strong> {formatPct(totals.progressPct)}</div>
              </div>
              <div style={{ overflowX: 'auto', maxHeight: 420, overflowY: 'auto' }}>
                <table>
                  <thead>
                    <tr>
                      <th>Concepto</th>
                      <th>Unidad</th>
                      <th>Cantidad</th>
                      <th>Precio unitario</th>
                      {discounted && <th>Precio con descuento</th>}
                      <th>Importe</th>
                    </tr>
                  </thead>
                  <tbody>
                    {groups.map((group) => (
                      <React.Fragment key={group.name || '__general__'}>
                        {(groups.length > 1 || group.name) && (
                          <tr>
                            <td colSpan={cols} style={{ fontWeight: 600, background: 'var(--gray-100)' }}>
                              {groupLabel(group.name)}
                              {group.isExtra && <> <span className="small" style={{ marginLeft: 6, color: '#92400e' }}>extra</span></>}
                              {' '}
                              <span className="small" style={{ marginLeft: 12, fontWeight: 400 }}>
                                {formatCurrency(group.budgetAmount)}
                                {Number(group.advancePct) > 0 ? ` · anticipo ${formatPct(group.advancePct)} (${formatCurrency(group.advanceAmount)})` : ''}
                              </span>
                            </td>
                          </tr>
                        )}
                        {concepts
                          .filter((concept) => (concept.group || '') === group.name)
                          .map((concept) => (
                            <tr key={concept.id}>
                              <td>
                                {concept.description}
                                {concept.isExtra && (
                                  <>
                                    {' '}
                                    <span className="small" style={{ marginLeft: 6, color: '#92400e' }} title={concept.extraNote || undefined}>
                                      {concept.extraKind === 'adicional' ? 'adicional' : 'extra'}
                                    </span>
                                  </>
                                )}
                              </td>
                              <td>{concept.unit || '—'}</td>
                              <td>{concept.quantity}</td>
                              <td>{formatCurrency(concept.listUnitPrice ?? concept.unitPrice)}</td>
                              {discounted && (
                                <td>{concept.listUnitPrice != null ? <strong>{formatCurrency(concept.unitPrice)}</strong> : <span className="small">sin descuento</span>}</td>
                              )}
                              <td>{formatCurrency(concept.amount)}</td>
                            </tr>
                          ))}
                      </React.Fragment>
                    ))}
                    <tr style={{ fontWeight: 600 }}>
                      <td colSpan={cols - 1}>Total contratado</td>
                      <td>{formatCurrency(budget.totalContractedAmount)}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          );
        })()}

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
          <OpeningBalancePanel
            budget={openingBudget}
            onClose={() => setOpeningBudget(null)}
            onSaved={async () => {
              setOpeningBudget(null);
              await loadBudgets();
            }}
          />
        )}
      </div>

      {showForm && (
        <form ref={budgetFormRef} className="card" style={{ display: 'grid', gap: 10, padding: 16 }} onSubmit={submitBudgetForm}>
          <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
            <strong>{editingBudgetRow ? 'Editar presupuesto' : 'Nuevo presupuesto'}</strong>
            <button type="button" className="secondary" onClick={resetBudgetForm}>✕ Cancelar</button>
          </div>
          {!isReviewer && !editingBudgetRow && (
            <div className="small" style={{ background: '#fef3c7', color: '#92400e', borderRadius: 6, padding: 10 }}>
              Al crearlo, el presupuesto queda <strong>por autorizar</strong>: un admin debe autorizarlo antes de poder estimar sobre él.
            </div>
          )}
          {!isReviewer && editingBudgetRow && editingBudgetRow.approvalStatus !== 'PENDIENTE' && (
            <div className="small" style={{ background: '#fef3c7', color: '#92400e', borderRadius: 6, padding: 10 }}>
              Este presupuesto ya está autorizado. Si cambias conceptos, precios unitarios, volúmenes, anticipo o retención, volverá a
              <strong> autorización</strong> y no se podrá estimar hasta que un admin lo autorice de nuevo.
            </div>
          )}
          {editingBudgetRow?.approvalStatus === 'PENDIENTE' && (
            <div className="small" style={{ background: '#fef3c7', color: '#92400e', borderRadius: 6, padding: 10 }}>
              Pendiente de autorización{editingBudgetRow.reauthRequired ? ' (modificado después de autorizado)' : ''}.
            </div>
          )}

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
              <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                Anticipo (para amortizar)
                <span className="small" style={{ display: 'inline-flex', gap: 8, fontWeight: 400 }}>
                  <label style={{ display: 'inline-flex', gap: 3, alignItems: 'center' }}>
                    <input type="radio" name="advance-mode" checked={form.advanceMode !== 'pct'} disabled={formTotals.usesGroupAdvance}
                      onChange={() => setForm((prev) => ({ ...prev, advanceMode: 'amount', advanceAmount: formTotals.advanceAmount ? formTotals.advanceAmount.toFixed(2) : prev.advanceAmount }))} />
                    $
                  </label>
                  <label style={{ display: 'inline-flex', gap: 3, alignItems: 'center' }}>
                    <input type="radio" name="advance-mode" checked={form.advanceMode === 'pct'} disabled={formTotals.usesGroupAdvance}
                      onChange={() => setForm((prev) => ({ ...prev, advanceMode: 'pct', advancePctInput: formTotals.advancePct ? String(Math.round(formTotals.advancePct * 100) / 100) : prev.advancePctInput }))} />
                    %
                  </label>
                </span>
              </label>
              {form.advanceMode === 'pct' && !formTotals.usesGroupAdvance ? (
                <input
                  type="number"
                  min="0"
                  max="100"
                  step="0.01"
                  value={form.advancePctInput}
                  onChange={(e) => setForm((prev) => ({ ...prev, advancePctInput: e.target.value }))}
                  placeholder="0"
                  style={{ width: 120 }}
                  aria-label="Anticipo en porcentaje"
                />
              ) : (
                <input
                  value={formTotals.usesGroupAdvance ? formTotals.advanceAmount.toFixed(2) : form.advanceAmount}
                  onChange={(e) => setForm((prev) => ({ ...prev, advanceAmount: e.target.value }))}
                  placeholder="0.00"
                  style={{ width: 120 }}
                  disabled={formTotals.usesGroupAdvance}
                  title={formTotals.usesGroupAdvance ? 'Sale de los % de anticipo por grupo' : undefined}
                  aria-label="Anticipo en cantidad"
                />
              )}
              <div className="small" style={{ color: 'var(--gray-600)' }}>
                {form.advanceMode === 'pct' && !formTotals.usesGroupAdvance
                  ? <>= {formatCurrency(formTotals.advanceAmount)}</>
                  : <>= {formatPct(formTotals.advancePct)} del presupuesto</>}
              </div>
              <div className="small" style={{ color: 'var(--gray-600)', maxWidth: 260 }}>
                Solo define cuánto se amortiza en cada estimación. Con varios presupuestos del proveedor se amortiza únicamente el anticipo que
                realmente se entregue (pago marcado como anticipo, o «Anticipo a entregar» en la estimación).
              </div>
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
            <div style={{ display: 'grid', gap: 6, marginBottom: 10 }}>
              <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
                <input
                  type="checkbox"
                  checked={Boolean(form.discountEnabled)}
                  onChange={(e) => setForm((prev) => ({ ...prev, discountEnabled: e.target.checked }))}
                />
                <strong>Agregar descuento al presupuesto</strong>
              </label>
              {form.discountEnabled && (
                <div className="row" style={{ gap: 12, flexWrap: 'wrap', alignItems: 'flex-end', background: 'var(--gray-50)', border: '1px solid var(--gray-200)', borderRadius: 8, padding: 10 }}>
                  <div className="row" style={{ gap: 12, alignItems: 'center' }}>
                    <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
                      <input type="radio" name="discount-mode" checked={form.discountMode === 'pct'} onChange={() => setForm((prev) => ({ ...prev, discountMode: 'pct', discountValue: '' }))} />
                      Porcentaje (%)
                    </label>
                    <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
                      <input type="radio" name="discount-mode" checked={form.discountMode === 'amount'} onChange={() => setForm((prev) => ({ ...prev, discountMode: 'amount', discountValue: '' }))} />
                      Cantidad cerrada ($)
                    </label>
                  </div>
                  <div>
                    <label>{form.discountMode === 'amount' ? 'Descuento ($)' : 'Descuento (%)'}</label>
                    <input
                      type="number"
                      min="0"
                      step="0.01"
                      value={form.discountValue}
                      onChange={(e) => setForm((prev) => ({ ...prev, discountValue: e.target.value }))}
                      placeholder={form.discountMode === 'amount' ? '0.00' : '0'}
                      style={{ width: 150, borderColor: form.discountValue !== '' && !formDiscount.applies ? '#b91c1c' : undefined }}
                    />
                  </div>
                  <div className="small" style={{ display: 'grid', gap: 2, minWidth: 260 }}>
                    <div>Subtotal sin descuento: <strong>{formatCurrency(formDiscount.subtotal)}</strong></div>
                    <div>
                      Descuento: <strong>{formatCurrency(formDiscount.amount)}</strong>
                      {formDiscount.applies && <> = <strong>{formatPct(formDiscount.pct)}</strong>{form.discountMode === 'amount' ? ' (se aplica a todos los precios)' : ''}</>}
                    </div>
                    <div>Total con descuento: <strong>{formatCurrency(formDiscount.subtotal - formDiscount.amount)}</strong></div>
                  </div>
                  {form.discountValue !== '' && !formDiscount.applies && (
                    <div className="small" style={{ color: '#b91c1c' }}>El descuento debe ser mayor a 0 y menor al subtotal (o a 100 %).</div>
                  )}
                  {editingBudgetRow && Number(editingBudgetRow.estimationsCount) > 0 && (
                    <div className="small" style={{ color: '#92400e', flexBasis: '100%' }}>
                      Este presupuesto ya tiene estimaciones: el descuento solo cambia los precios de las estimaciones nuevas; las ya capturadas conservan sus montos.
                    </div>
                  )}
                </div>
              )}
            </div>
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
                  className="btn-outline"
                  onClick={() => importFileInputRef.current?.click()}
                  disabled={importingConceptos}
                >
                  {importingConceptos ? 'Importando...' : '⭱ Importar Excel/CSV/PDF/Word'}
                </button>
                <button type="button" className="btn-outline" onClick={() => setShowPasteText((prev) => !prev)}>
                  📋 Pegar texto
                </button>
                <button type="button" onClick={addConceptoRow}>+ Agregar concepto</button>
              </div>
            </div>
            {showPasteText && (
              <div style={{ marginBottom: 6 }}>
                <PasteTextImport onResult={(result) => { setError(''); applyImportResult(result); }} onClose={() => setShowPasteText(false)} />
              </div>
            )}
            {importWarnings.length > 0 && (
              <div className="small" style={{ color: 'var(--gray-600)', background: 'var(--gray-100)', borderRadius: 6, padding: 8, marginBottom: 6 }}>
                {importWarnings.map((warning, idx) => (
                  <div key={idx}>⚠ {warning}</div>
                ))}
              </div>
            )}
            <div
              className="row"
              style={{ gap: 8, flexWrap: 'wrap', alignItems: 'flex-end', background: 'var(--gray-50)', border: '1px solid var(--gray-200)', borderRadius: 8, padding: 10, margin: '12px 0 8px' }}
            >
              <div>
                <label>Agrupar los conceptos marcados en</label>
                <input
                  list="budget-group-options"
                  value={bulkGroupName}
                  onChange={(e) => setBulkGroupName(e.target.value)}
                  placeholder="Ej. BAJADAS"
                  style={{ width: 200 }}
                />
              </div>
              <button type="button" className="btn-outline" onClick={applyBulkGroup}>Asignar grupo</button>
              <button
                type="button"
                className="btn-outline"
                onClick={() => setSelectedConceptoIds(
                  selectedConceptoIds.size === form.lineItems.length ? new Set() : new Set(form.lineItems.map((row) => row.id)),
                )}
              >
                {selectedConceptoIds.size === form.lineItems.length ? 'Quitar marcas' : 'Marcar todos'}
              </button>
              <div style={{ flex: 1 }} />
              <input ref={groupFileInputRef} type="file" accept=".xlsx,.csv,.pdf,.docx" onChange={handleGroupFromFile} style={{ display: 'none' }} />
              <button
                type="button"
                className="btn-outline"
                onClick={() => groupFileInputRef.current?.click()}
                disabled={groupingFromFile}
                title="Lee un Excel/Word con los grupos y se los pone a los conceptos que ya existen, sin agregar ni quitar conceptos"
              >
                {groupingFromFile ? 'Leyendo...' : '⭱ Agrupar desde archivo'}
              </button>
            </div>
            {groupingMessage && <div className="small" style={{ marginBottom: 6 }}>{groupingMessage}</div>}
            <div style={{ overflowX: 'auto' }}>
              <table>
                <thead>
                  <tr>
                    <th></th>
                    <th>Grupo</th>
                    <th>Descripción</th>
                    <th>Unidad</th>
                    <th>Cantidad</th>
                    <th>Precio unitario</th>
                    {discountPct > 0 && <th>Precio con descuento ({formatPct(discountPct)})</th>}
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
                            type="checkbox"
                            checked={selectedConceptoIds.has(row.id)}
                            onChange={() => toggleSelectConcepto(row.id)}
                            aria-label="Marcar concepto"
                          />
                        </td>
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
                          <UnitSelect value={row.unit} onChange={(unit) => updateConceptoRow(index, { unit })} width={130} />
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
                        {discountPct > 0 && (
                          <td>{row.isExtra ? <span className="small">sin descuento</span> : <strong>{formatCurrency(netUnitPrice(row, discountPct))}</strong>}</td>
                        )}
                        <td>{formatCurrency(computeLineItemAmount(row, discountPct))}</td>
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
              {listFormGroups(form.lineItems, discountPct).filter((g) => g.name).map((g) => (
                <option key={g.name} value={g.name} />
              ))}
            </datalist>
            {listFormGroups(form.lineItems, discountPct).some((g) => g.name) && (
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
                      {listFormGroups(form.lineItems, discountPct).map((group) => {
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
              {discountPct > 0 && (
                <>
                  <div><strong>Subtotal:</strong> {formatCurrency(formDiscount.subtotal)}</div>
                  <div><strong>Descuento ({formatPct(discountPct)}):</strong> −{formatCurrency(formDiscount.amount)}</div>
                </>
              )}
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
            {editingBudgetRow && isReviewer && (
              <button type="button" className="secondary" onClick={deleteCurrentBudget} disabled={saving} style={{ color: '#b91c1c' }}>
                Eliminar
              </button>
            )}
            <button type="button" className="secondary" onClick={resetBudgetForm}>Cancelar</button>
          </div>
        </form>
      )}
    </div>
  );
}
