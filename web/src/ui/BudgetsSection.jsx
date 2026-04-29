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

function classifyBudgetStatus(progressPct) {
  const progress = Number(progressPct);
  if (!Number.isFinite(progress)) return { label: 'En presupuesto', className: 'in-budget' };
  if (progress > 100) return { label: 'Excedido', className: 'exceeded' };
  if (progress === 100) return { label: 'Pagado', className: 'paid' };
  return { label: 'En presupuesto', className: 'in-budget' };
}

function normalizeTextForSupplierKey(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
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

export function BudgetsSection({ projects, selectedProjectId }) {
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
  const [editingBudget, setEditingBudget] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [assigningBudget, setAssigningBudget] = useState(null);
  const [candidateTransactions, setCandidateTransactions] = useState([]);
  const [selectedTransactionIds, setSelectedTransactionIds] = useState(new Set());
  const [transactionSearch, setTransactionSearch] = useState('');
  const [loadingTransactions, setLoadingTransactions] = useState(false);
  const [expandedSuppliers, setExpandedSuppliers] = useState(() => new Set());
  const [form, setForm] = useState({
    projectId: selectedProjectId || '',
    supplierKey: '',
    supplierName: '',
    supplierCardCode: '',
    businessPartner: '',
    vendorId: '',
    concept: '',
    budgetAmount: '',
    notes: '',
    budgetIncludesTax: true,
  });

  const projectsById = useMemo(
    () => new Map((Array.isArray(projects) ? projects : []).map((project) => [String(project?._id || ''), project])),
    [projects],
  );

  const groupedRows = useMemo(() => {
    const groups = new Map();
    rows.forEach((row) => {
      const supplierKey = String(row?.supplierKey || '').trim();
      const supplierName = String(row?.supplierNameSnapshot || row?.supplierKey || 'Sin proveedor');
      const groupKey = supplierKey || `__name__:${supplierName}`;
      if (!groups.has(groupKey)) {
        groups.set(groupKey, {
          key: groupKey,
          supplierName,
          items: [],
          totals: {
            budgetAmount: 0,
            paidAmount: 0,
            remainingAmount: 0,
            progressPct: 0,
          },
        });
      }
      const group = groups.get(groupKey);
      group.items.push(row);
      group.totals.budgetAmount += Number(row?.budgetAmount) || 0;
      group.totals.paidAmount += Number(row?.paidAmount) || 0;
      group.totals.remainingAmount += Number(row?.remainingAmount) || 0;
    });

    return Array.from(groups.values())
      .map((group) => ({
        ...group,
        totals: {
          ...group.totals,
          progressPct: group.totals.budgetAmount > 0
            ? (group.totals.paidAmount / group.totals.budgetAmount) * 100
            : 0,
        },
      }))
      .sort((a, b) => a.supplierName.localeCompare(b.supplierName, 'es'));
  }, [rows]);

  const grandTotals = useMemo(() => {
    const budgetAmount = groupedRows.reduce((a, g) => a + g.totals.budgetAmount, 0);
    const paidAmount = groupedRows.reduce((a, g) => a + g.totals.paidAmount, 0);
    const remainingAmount = budgetAmount - paidAmount;
    const progressPct = budgetAmount > 0 ? (paidAmount / budgetAmount) * 100 : 0;
    return { budgetAmount, paidAmount, remainingAmount, progressPct };
  }, [groupedRows]);

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
      const data = await api.budgets({
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
            const key = buildCanonicalSupplierKey({
              supplierCardCode: sapCardCode,
              businessPartner: '',
              supplierName,
            });
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

        setSupplierOptions(Array.from(optionsByKey.values()).sort((a, b) => (a.supplierName || '').localeCompare(b.supplierName || '', 'es')));
      })
      .catch(() => {
        if (!active) return;
        setSupplierOptions([]);
      });
    return () => {
      active = false;
    };
  }, [selectedProjectId]);

  function resetForm() {
    setEditingBudget(null);
    setShowForm(false);
    setForm({
      projectId: selectedProjectId || '',
      supplierKey: '',
      supplierName: '',
      supplierCardCode: '',
      businessPartner: '',
      vendorId: '',
      concept: '',
      budgetAmount: '',
      notes: '',
      budgetIncludesTax: true,
    });
  }

  function formatDate(value) {
    if (!value) return '—';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return String(value);
    return parsed.toLocaleDateString('es-MX');
  }

  async function loadBudgetTransactions(budgetId, search = '') {
    if (!budgetId) return;
    setLoadingTransactions(true);
    setError('');
    try {
      const payload = await api.budgetTransactions(budgetId, search ? { search } : {});
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
    loadBudgetTransactions(row.id);
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
      await api.saveBudgetTransactionLinks(assigningBudget.id, {
        selectedTransactionIds: Array.from(selectedTransactionIds),
      });
      await loadBudgets();
      await loadBudgetTransactions(assigningBudget.id, transactionSearch);
    } catch (e) {
      setError(e.message || 'No se pudieron guardar las asignaciones');
    } finally {
      setSaving(false);
    }
  }

  function startCreate() {
    resetForm();
    setShowForm(true);
  }

  function startEdit(row) {
    setEditingBudget(row);
    setShowForm(true);
    setForm({
      projectId: row.projectId || selectedProjectId || '',
      supplierKey: row.supplierKey || '',
      supplierName: row.supplierNameSnapshot || '',
      supplierCardCode: row.supplierCardCode || '',
      businessPartner: row.businessPartner || '',
      vendorId: row.vendorId || '',
      concept: row.concept || 'General',
      budgetAmount: String(row.budgetAmount ?? ''),
      notes: row.notes || '',
      isActive: row.isActive !== false,
      budgetIncludesTax: row.budgetIncludesTax !== false,
    });
  }

  async function submitForm(event) {
    event.preventDefault();
    setSaving(true);
    setError('');
    try {
      const payload = {
        projectId: form.projectId,
        supplierKey: form.supplierKey,
        supplierName: form.supplierName,
        supplierCardCode: form.supplierCardCode,
        businessPartner: form.businessPartner,
        vendorId: form.vendorId,
        concept: String(form.concept || '').trim(),
        budgetAmount: Number(String(form.budgetAmount).replace(/,/g, '').trim()),
        notes: form.notes,
        budgetIncludesTax: Boolean(form.budgetIncludesTax),
      };

      if (editingBudget) {
        await api.updateBudget(editingBudget.id, {
          budgetAmount: payload.budgetAmount,
          notes: payload.notes,
          isActive: Boolean(form.isActive),
          supplierNameSnapshot: payload.supplierName,
          concept: payload.concept,
          budgetIncludesTax: payload.budgetIncludesTax,
        });
      } else {
        await api.createBudget(payload);
      }

      await loadBudgets();
      resetForm();
    } catch (e) {
      setError(e.message || 'No se pudo guardar el presupuesto');
    } finally {
      setSaving(false);
    }
  }

  async function deleteCurrentBudget() {
    if (!editingBudget?.id) return;
    const confirmed = window.confirm('¿Seguro que quieres eliminar este presupuesto? Esta acción no se puede deshacer.');
    if (!confirmed) return;

    setSaving(true);
    setError('');
    try {
      await api.deleteBudget(editingBudget.id);
      await loadBudgets();
      resetForm();
    } catch (e) {
      setError(e.message || 'No se pudo eliminar el presupuesto');
    } finally {
      setSaving(false);
    }
  }

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

  const grandKpis = [
    {
      label: 'Presupuesto total',
      value: formatCurrency(grandTotals.budgetAmount),
      sub: 'comprometido',
      icon: (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>
        </svg>
      ),
      danger: false,
    },
    {
      label: 'Total pagado',
      value: formatCurrency(grandTotals.paidAmount),
      sub: 'ejecutado',
      icon: (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M21 12V7H5a2 2 0 0 1 0-4h14v4"/><path d="M3 5v14a2 2 0 0 0 2 2h16v-5"/><path d="M18 12a2 2 0 0 0 0 4h4v-4z"/>
        </svg>
      ),
      danger: false,
    },
    {
      label: 'Saldo disponible',
      value: formatCurrency(grandTotals.remainingAmount),
      sub: grandTotals.remainingAmount < 0 ? '⚠ excedido' : 'restante',
      icon: (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={grandTotals.remainingAmount < 0 ? 'var(--danger-text, #b91c1c)' : 'var(--primary)'} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/>
        </svg>
      ),
      danger: grandTotals.remainingAmount < 0,
    },
    {
      label: 'Avance global',
      value: `${Math.round(grandTotals.progressPct)}%`,
      sub: classifyBudgetStatus(grandTotals.progressPct).label,
      icon: (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <rect x="4" y="2" width="16" height="20" rx="2"/><path d="M9 22v-4h6v4M8 6h.01M16 6h.01M8 10h.01M16 10h.01M8 14h.01M16 14h.01"/>
        </svg>
      ),
      danger: false,
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
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--primary)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>
            </svg>
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
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.4, flexShrink: 0 }}>
                  <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                </svg>
              </div>
            )}
            {areaM2Error && <div className="small" style={{ color: 'var(--danger-text, #b91c1c)', marginTop: 2 }}>{areaM2Error}</div>}
            <div className="kpi-sub">
              {costoM2 != null ? `${Number(areaM2).toLocaleString('es-MX')} m²` : (editingAreaM2 ? 'ingresa m² del proyecto' : 'clic para configurar')}
            </div>
          </div>
        </div>
      </div>

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
          <div style={{ flex: 1 }} />
          <button type="button" onClick={showForm ? resetForm : startCreate} style={{ fontSize: 13 }}>
            {showForm ? '✕ Cancelar' : '+ Nuevo presupuesto'}
          </button>
        </div>

      {(showForm || !rows.length) && (
        <form className="grid" style={{ gap: 8, borderBottom: '1px solid var(--gray-100)', padding: 16 }} onSubmit={submitForm}>
          <div>
            <label>Obra</label>
            <select
              value={form.projectId}
              onChange={(e) => setForm((prev) => ({ ...prev, projectId: e.target.value }))}
              disabled={Boolean(editingBudget)}
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
              disabled={Boolean(editingBudget)}
              required
            >
              <option value="">Selecciona proveedor</option>
              {supplierOptions.map((row) => (
                <option key={row.supplierKey} value={row.supplierKey}>
                  {row.supplierName || row.supplierKey}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label>Concepto</label>
            <input
              value={form.concept}
              onChange={(e) => setForm((prev) => ({ ...prev, concept: e.target.value }))}
              placeholder="Ej. Carpintería"
              required
            />
          </div>
          <div>
            <label>Monto presupuesto</label>
            <input
              value={form.budgetAmount}
              onChange={(e) => setForm((prev) => ({ ...prev, budgetAmount: e.target.value }))}
              placeholder="0.00"
              required
            />
          </div>
          <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
            <input
              type="checkbox"
              checked={Boolean(form.budgetIncludesTax)}
              onChange={(e) => setForm((prev) => ({ ...prev, budgetIncludesTax: e.target.checked }))}
            />
            Incluye IVA
          </label>
          <div>
            <label>Nota (opcional)</label>
            <input
              value={form.notes}
              onChange={(e) => setForm((prev) => ({ ...prev, notes: e.target.value }))}
            />
          </div>
          {editingBudget && (
            <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
              <input
                type="checkbox"
                checked={Boolean(form.isActive)}
                onChange={(e) => setForm((prev) => ({ ...prev, isActive: e.target.checked }))}
              />
              Presupuesto activo
            </label>
          )}
          <div className="row" style={{ gap: 8 }}>
            <button type="submit" disabled={saving}>{saving ? 'Guardando...' : (editingBudget ? 'Guardar cambios' : 'Crear presupuesto')}</button>
            {editingBudget && (
              <button type="button" className="secondary" onClick={deleteCurrentBudget} disabled={saving} style={{ color: '#b91c1c' }}>
                Eliminar
              </button>
            )}
            {(showForm || editingBudget) && <button type="button" className="secondary" onClick={resetForm}>Cancelar</button>}
          </div>
        </form>
      )}

      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}

      {loading ? (
        <div className="small">Cargando presupuestos...</div>
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
                <th className="col-progress">Avance global</th>
                <th className="col-status">Estado global</th>
                <th className="col-detail">Detalle</th>
              </tr>
            </thead>
            <tbody>
              {groupedRows.map((group) => {
                const status = classifyBudgetStatus(group.totals.progressPct);
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
                      <td>{formatCurrency(group.totals.budgetAmount)}</td>
                      <td>{formatCurrency(group.totals.paidAmount)}</td>
                      <td style={{ color: group.totals.remainingAmount < 0 ? 'var(--danger-text, #b91c1c)' : undefined }}>{formatCurrency(group.totals.remainingAmount)}</td>
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          <div style={{ flex: 1, height: 6, background: 'var(--gray-150)', borderRadius: 99, overflow: 'hidden', minWidth: 60 }}>
                            <div style={{ height: '100%', width: `${Math.min(group.totals.progressPct, 100)}%`, background: group.totals.progressPct > 100 ? 'var(--danger-text, #b91c1c)' : 'var(--primary)', borderRadius: 99, transition: 'width .4s' }} />
                          </div>
                          <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--gray-600)', whiteSpace: 'nowrap' }}>{Math.round(group.totals.progressPct)}%</span>
                        </div>
                      </td>
                      <td><span className={`budget-badge budget-status ${status.className}`}>{status.label}</span></td>
                      <td>
                        <button type="button" className="secondary" onClick={() => toggleSupplierExpand(group.key)}>
                          {isExpanded ? 'Ocultar' : 'Ver'}
                        </button>
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={8} style={{ padding: 0 }}>
                          <div className="budgets-table-shell" style={{ overflowX: 'auto' }}>
                            <table className="budgets-table budgets-table-nested">
                              <thead>
                                <tr>
                                  <th>Obra</th>
                                  <th>Concepto</th>
                                  <th>Tipo</th>
                                  <th>Presupuesto</th>
                                  <th>Pagado</th>
                                  <th>Saldo</th>
                                  <th>Avance</th>
                                  <th>Estado</th>
                                  <th>Nota</th>
                                  <th>Acciones</th>
                                </tr>
                              </thead>
                              <tbody>
                                {group.items.map((row) => {
                                  const project = projectsById.get(String(row.projectId || ''));
                                  const childStatus = classifyBudgetStatus(row.progressPct);
                                  return (
                                    <tr key={row.id}>
                                      <td>{project?.displayName || project?.name || row.projectId}</td>
                                      <td>{row.concept || 'General'}</td>
                                      <td>{row.budgetIncludesTax === false ? 'Sin IVA' : 'Con IVA'}</td>
                                      <td>{formatCurrency(row.budgetAmount)}</td>
                                      <td>{formatCurrency(row.paidAmount)}</td>
                                      <td style={{ color: Number(row.remainingAmount) < 0 ? '#b91c1c' : undefined }}>{formatCurrency(row.remainingAmount)}</td>
                                      <td><span className={`budget-badge budget-progress ${childStatus.className}`}>{formatPct(row.progressPct)}</span></td>
                                      <td><span className={`budget-badge budget-status ${childStatus.className}`}>{childStatus.label}</span></td>
                                      <td>{row.notes || '—'}</td>
                                      <td>
                                        <div className="row" style={{ gap: 6 }}>
                                          <button type="button" className="secondary" onClick={() => startEdit(row)}>Editar</button>
                                          <button type="button" className="secondary" onClick={() => startAssignPayments(row)}>Asignar</button>
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
                  <td colSpan={8} className="small" style={{ textAlign: 'center' }}>No hay presupuestos para los filtros seleccionados.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {assigningBudget && (
        <div className="grid budgets-assignment-panel" style={{ gap: 8, borderRadius: 10, padding: 12 }}>
          <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
            <strong>Asignar · {assigningBudget.supplierNameSnapshot || assigningBudget.supplierKey}</strong>
            <button type="button" className="secondary" onClick={closeAssignPayments}>Cerrar</button>
          </div>
          <div className="small">
            Concepto: <strong>{assigningBudget.concept || 'General'}</strong> · Solo se usarán pagos asignados manualmente para este presupuesto.
          </div>
          <div className="row" style={{ gap: 8 }}>
            <input
              value={transactionSearch}
              onChange={(e) => setTransactionSearch(e.target.value)}
              placeholder="Buscar por descripción / concepto"
              style={{ minWidth: 260 }}
            />
            <button type="button" className="secondary" onClick={() => loadBudgetTransactions(assigningBudget.id, transactionSearch)} disabled={loadingTransactions}>
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
                    <th>Sin IVA</th>
                    <th>Con IVA</th>
                    <th>Tipo</th>
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
                        <td>{formatCurrency(tx.amountWithoutTax)}</td>
                        <td>{formatCurrency(tx.amountWithTax)}</td>
                        <td>{tx.type || 'EXPENSE'}</td>
                        <td>{tx.isAssignedToOtherBudget ? 'Asignado a otro presupuesto' : (tx.isAssignedToCurrentBudget ? 'Asignado a este presupuesto' : 'Libre')}</td>
                      </tr>
                    );
                  })}
                  {!candidateTransactions.length && (
                    <tr>
                      <td colSpan={7} className="small" style={{ textAlign: 'center' }}>No hay transacciones disponibles.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
    </div>
  );
}
