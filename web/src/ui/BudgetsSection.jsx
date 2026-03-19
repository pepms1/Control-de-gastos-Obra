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
  const [editingBudget, setEditingBudget] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({
    projectId: selectedProjectId || '',
    supplierKey: '',
    supplierName: '',
    supplierCardCode: '',
    businessPartner: '',
    vendorId: '',
    budgetAmount: '',
    notes: '',
    budgetIncludesTax: true,
  });

  const projectsById = useMemo(
    () => new Map((Array.isArray(projects) ? projects : []).map((project) => [String(project?._id || ''), project])),
    [projects],
  );

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
      budgetAmount: '',
      notes: '',
      budgetIncludesTax: true,
    });
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

  return (
    <div className="card" style={{ display: 'grid', gap: 12 }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
        <h2 style={{ margin: 0 }}>Presupuestos</h2>
        <button type="button" onClick={startCreate}>Nuevo presupuesto</button>
      </div>

      <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
        <input
          value={supplierFilter}
          onChange={(e) => setSupplierFilter(e.target.value)}
          placeholder="Filtrar por proveedor"
          style={{ minWidth: 220 }}
        />
        <button type="button" className="secondary" onClick={loadBudgets}>Buscar</button>
        <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
          <input type="checkbox" checked={includeInactive} onChange={(e) => setIncludeInactive(e.target.checked)} />
          Mostrar inactivos
        </label>
      </div>

      {(showForm || !rows.length) && (
        <form className="grid" style={{ gap: 8, border: '1px solid #e2e8f0', borderRadius: 10, padding: 12 }} onSubmit={submitForm}>
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
            {(showForm || editingBudget) && <button type="button" className="secondary" onClick={resetForm}>Cancelar</button>}
          </div>
        </form>
      )}

      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}

      {loading ? (
        <div className="small">Cargando presupuestos...</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table>
            <thead>
              <tr>
                <th>Obra</th>
                <th>Proveedor</th>
                <th>Presupuesto</th>
                <th>Tipo</th>
                <th>Pagado</th>
                <th>Saldo</th>
                <th>Avance %</th>
                <th>Estatus</th>
                <th>Nota</th>
                <th>Acciones</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const project = projectsById.get(String(row.projectId || ''));
                const status = classifyBudgetStatus(row.progressPct);
                return (
                  <tr key={row.id}>
                    <td>{project?.displayName || project?.name || row.projectId}</td>
                    <td>{row.supplierNameSnapshot || row.supplierKey || '—'}</td>
                    <td>{formatCurrency(row.budgetAmount)}</td>
                    <td>{row.budgetIncludesTax === false ? 'Sin IVA' : 'Con IVA'}</td>
                    <td>{formatCurrency(row.paidAmount)}</td>
                    <td style={{ color: Number(row.remainingAmount) < 0 ? '#b91c1c' : undefined }}>{formatCurrency(row.remainingAmount)}</td>
                    <td>{formatPct(row.progressPct)}</td>
                    <td><span className={`budget-status ${status.className}`}>{status.label}</span></td>
                    <td>{row.notes || '—'}</td>
                    <td><button type="button" className="secondary" onClick={() => startEdit(row)}>Editar</button></td>
                  </tr>
                );
              })}
              {!rows.length && (
                <tr>
                  <td colSpan={10} className="small" style={{ textAlign: 'center' }}>No hay presupuestos para los filtros seleccionados.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
