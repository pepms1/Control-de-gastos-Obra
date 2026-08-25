import React, { useEffect, useMemo, useState } from 'react';
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
function computeEstimationPreview(budgetDetail, lineItemInputs, remainingBalanceOverride) {
  const periodSubtotal = (lineItemInputs || []).reduce(
    (sum, li) => sum + (Number(li.periodQuantity) || 0) * (Number(li.unitPrice) || 0),
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

export function EstimacionesSection({ projects, selectedProjectId }) {
  const [view, setView] = useState('list');
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
    setShowForm(false);
    setEditingBudgetRow(null);
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
          return acc;
        },
        { totalContractedAmount: 0, totalRetainedToDate: 0, remainingAdvanceBalance: 0 },
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
  }

  function startCreateBudget() {
    setEditingBudgetRow(null);
    setForm(emptyBudgetForm(selectedProjectId));
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
  }

  function buildEstimationFormFromBudget(previousCumulativeByConceptoId) {
    return {
      periodStart: '',
      periodEnd: '',
      notes: '',
      status: 'Registrada',
      lineItems: (budgetDetail?.lineItems || []).map((item) => ({
        conceptoId: item.id,
        description: item.description,
        unit: item.unit,
        unitPrice: item.unitPrice,
        contractedQuantity: item.quantity,
        previousCumulativeQuantity: Number(previousCumulativeByConceptoId?.[item.id]) || 0,
        periodQuantity: '',
      })),
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
    setEditingEstimation(null);
    setEstimationForm(buildEstimationFormFromBudget(previousCumulativeByConceptoId));
    setShowEstimationForm(true);
  }

  function startEditEstimation(estimation) {
    setEditingEstimation(estimation);
    setEstimationForm({
      periodStart: estimation.periodStart || '',
      periodEnd: estimation.periodEnd || '',
      notes: estimation.notes || '',
      status: estimation.status || 'Registrada',
      lineItems: (estimation.lineItems || []).map((li) => ({
        conceptoId: li.conceptoId,
        description: li.description,
        unit: li.unit,
        unitPrice: li.unitPrice,
        contractedQuantity: li.contractedQuantity,
        previousCumulativeQuantity: li.previousCumulativeQuantity,
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

  function updateEstimationQuantity(conceptoId, value) {
    setEstimationForm((prev) => ({
      ...prev,
      lineItems: prev.lineItems.map((li) => (li.conceptoId === conceptoId ? { ...li, periodQuantity: value } : li)),
    }));
  }

  const estimationPreview = useMemo(() => {
    if (!estimationForm || !budgetDetail) return null;
    const remainingOverride = editingEstimation
      ? (Number(budgetDetail.remainingAdvanceBalance) || 0) + (Number(editingEstimation.advanceAmortizationAmount) || 0)
      : undefined;
    return computeEstimationPreview(budgetDetail, estimationForm.lineItems, remainingOverride);
  }, [estimationForm, budgetDetail, editingEstimation]);

  async function submitEstimationForm(event) {
    event.preventDefault();
    if (!selectedBudgetId || !estimationForm) return;
    setSaving(true);
    setError('');
    try {
      const payload = {
        periodStart: estimationForm.periodStart,
        periodEnd: estimationForm.periodEnd,
        notes: estimationForm.notes,
        status: estimationForm.status,
        lineItems: estimationForm.lineItems.map((li) => ({
          conceptoId: li.conceptoId,
          periodQuantity: Number(li.periodQuantity) || 0,
        })),
      };
      if (editingEstimation) {
        await api.updateEstimation(selectedBudgetId, editingEstimation.id, payload);
      } else {
        await api.createEstimation(selectedBudgetId, payload);
      }
      await loadBudgetDetail(selectedBudgetId);
      await loadEstimationBudgets();
      resetEstimationForm();
    } catch (e) {
      setError(e.message || 'No se pudo guardar la estimación');
    } finally {
      setSaving(false);
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
    } catch (e) {
      setError(e.message || 'No se pudo eliminar la estimación');
    } finally {
      setSaving(false);
    }
  }

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
              <button type="button" className="secondary" onClick={addConceptoRow}>+ Agregar concepto</button>
            </div>
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

      {view === 'list' ? (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div>
                <div className="kpi-label">Total contratado</div>
                <div className="kpi-value">{formatCurrency(listTotals.totalContractedAmount)}</div>
                <div className="kpi-sub">en presupuestos por conceptos</div>
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
              <button type="button" onClick={showForm ? resetBudgetForm : startCreateBudget} style={{ fontSize: 13 }}>
                {showForm ? '✕ Cancelar' : '+ Nuevo presupuesto'}
              </button>
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
                        <td>{formatCurrency(row.advanceAmount)}</td>
                        <td>{formatPct(row.retentionPct)}</td>
                        <td>{row.estimationsCount}</td>
                        <td>{row.isActive === false ? 'Inactivo' : 'Activo'}</td>
                        <td>
                          <div className="row" style={{ gap: 6 }}>
                            <button type="button" className="secondary" onClick={() => openBudgetDetail(row)}>Ver</button>
                            <button type="button" className="secondary" onClick={() => startEditBudget(row)}>Editar</button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!rows.length && (
                      <tr>
                        <td colSpan={8} className="small" style={{ textAlign: 'center' }}>
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
                  <button type="button" className="secondary" onClick={() => startEditBudget(budgetDetail)}>Editar presupuesto</button>
                  {!showEstimationForm && (
                    <button type="button" onClick={startCreateEstimation} disabled={budgetDetail.isActive === false}>
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
                        <th>Subtotal</th>
                        <th>Retención</th>
                        <th>Amortización anticipo</th>
                        <th>Total a pagar</th>
                        <th>Estatus</th>
                        <th>Acciones</th>
                      </tr>
                    </thead>
                    <tbody>
                      {estimationsList.map((estimation) => (
                        <tr key={estimation.id}>
                          <td>#{estimation.folio}</td>
                          <td>{formatDate(estimation.periodStart)} – {formatDate(estimation.periodEnd)}</td>
                          <td>{formatCurrency(estimation.periodSubtotal)}</td>
                          <td>{formatCurrency(estimation.retentionAmount)}</td>
                          <td>{formatCurrency(estimation.advanceAmortizationAmount)}</td>
                          <td><strong>{formatCurrency(estimation.totalToPay)}</strong></td>
                          <td>{estimation.status}</td>
                          <td>
                            <div className="row" style={{ gap: 6 }}>
                              <button
                                type="button"
                                className="secondary"
                                onClick={() => startEditEstimation(estimation)}
                                disabled={!estimation.isLatest}
                                title={!estimation.isLatest ? 'Solo la última estimación puede editarse' : undefined}
                              >
                                Editar
                              </button>
                              <button
                                type="button"
                                className="secondary"
                                onClick={() => deleteEstimationRow(estimation)}
                                disabled={!estimation.isLatest}
                                title={!estimation.isLatest ? 'Solo la última estimación puede eliminarse' : undefined}
                                style={{ color: estimation.isLatest ? '#b91c1c' : undefined }}
                              >
                                Eliminar
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                      {!estimationsList.length && (
                        <tr>
                          <td colSpan={8} className="small" style={{ textAlign: 'center' }}>
                            Aún no hay estimaciones registradas para este presupuesto.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

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
                      <label>Estatus</label>
                      <input
                        value={estimationForm.status}
                        onChange={(e) => setEstimationForm((prev) => ({ ...prev, status: e.target.value }))}
                        placeholder="Registrada"
                        style={{ width: 140 }}
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
                          const periodQuantity = Number(li.periodQuantity) || 0;
                          const cumulativeQuantity = li.previousCumulativeQuantity + periodQuantity;
                          const periodAmount = periodQuantity * (Number(li.unitPrice) || 0);
                          const overContracted = cumulativeQuantity > li.contractedQuantity;
                          return (
                            <tr key={li.conceptoId}>
                              <td>{li.description}</td>
                              <td>{li.unit || '—'}</td>
                              <td>{li.contractedQuantity}</td>
                              <td>{li.previousCumulativeQuantity}</td>
                              <td>
                                <input
                                  type="number"
                                  min="0"
                                  step="0.01"
                                  value={li.periodQuantity}
                                  onChange={(e) => updateEstimationQuantity(li.conceptoId, e.target.value)}
                                  style={{ width: 100 }}
                                />
                              </td>
                              <td style={{ color: overContracted ? 'var(--danger-text, #b91c1c)' : undefined }}>
                                {cumulativeQuantity}
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
                      <div><strong>Total a pagar:</strong> {formatCurrency(estimationPreview.totalToPay)}</div>
                    </div>
                  )}

                  <div className="row" style={{ gap: 8 }}>
                    <button type="submit" disabled={saving}>{saving ? 'Guardando...' : 'Guardar estimación'}</button>
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
