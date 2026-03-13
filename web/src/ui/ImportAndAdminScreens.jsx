import React, { useEffect, useMemo, useState } from 'react';
import { api, SELECTED_PROJECT_KEY } from '../api.js';
import {
  getSuspiciousResolutionTarget,
  getSuspiciousTransactionId,
  MISSING_TRANSACTION_ID_ERROR,
} from './suspiciousResolution.js';

function valueToArray(value) {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  return [value];
}

export function ImportSapScreen() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [summary, setSummary] = useState(null);
  const [errors, setErrors] = useState([]);
  const [message, setMessage] = useState('');
  const [mismatchError, setMismatchError] = useState(null);
  const [projects, setProjects] = useState([]);
  const [selectedProjectId, setSelectedProjectId] = useState(localStorage.getItem(SELECTED_PROJECT_KEY) || '');

  useEffect(() => {
    const nextProjectId = localStorage.getItem(SELECTED_PROJECT_KEY) || '';
    setSelectedProjectId(nextProjectId);

    async function loadProjects() {
      try {
        const response = await api.projects();
        setProjects(Array.isArray(response) ? response : []);
      } catch (err) {
        setProjects([]);
      }
    }

    loadProjects();
  }, []);

  const destinationProject = projects.find((project) => String(project?._id || '') === String(selectedProjectId || '')) || null;
  const canImport = Boolean(file && destinationProject?.name && !loading);

  async function onSubmit(e, options = {}) {
    e.preventDefault();
    if (!selectedProjectId || !destinationProject?.name) {
      setMessage('Selecciona un proyecto activo antes de importar.');
      return;
    }
    if (!file) {
      setMessage('Selecciona un archivo para importar.');
      return;
    }

    setLoading(true);
    setMessage('');
    setSummary(null);
    setErrors([]);
    setMismatchError(null);

    try {
      const response = await api.importSapPayments(file, destinationProject.name, selectedProjectId, Boolean(options.force));
      setSummary(response?.summary || response || null);
      setErrors(valueToArray(response?.errors));
      setMessage('Importación finalizada.');
    } catch (err) {
      const mismatchPayload = err?.detail?.error === 'PROJECT_MISMATCH' ? err.detail : null;
      if (mismatchPayload) {
        setMismatchError(mismatchPayload);
        setMessage(mismatchPayload.message || 'Archivo posiblemente de otro proyecto.');
      } else {
        setMessage(err.message || 'No se pudo importar el archivo.');
      }
    } finally {
      setLoading(false);
    }
  }



  async function forceImport() {
    if (!file || loading) return;
    const fakeEvent = { preventDefault() {} };
    await onSubmit(fakeEvent, { force: true });
  }

  return (
    <div className="container grid">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Importar pagos SAP</h2>
        <p className="small" style={{ marginTop: 0 }}>
          Proyecto destino: <strong>{destinationProject?.name || 'Sin proyecto seleccionado'}</strong>
        </p>
        <form className="grid" onSubmit={onSubmit}>
          <div>
            <label>Archivo</label>
            <input type="file" accept=".csv,.xlsx,.xls,.txt" onChange={(e) => setFile(e.target.files?.[0] || null)} />
          </div>
          <button type="submit" disabled={!canImport}>
            {loading ? 'Importando...' : 'Subir e importar'}
          </button>
        </form>
        {message && <p className="small" style={{ marginBottom: 0 }}>{message}</p>}

        {mismatchError && (
          <div className="card" style={{ border: '1px solid #f59e0b', marginTop: 12 }}>
            <h3 style={{ marginTop: 0 }}>Posible archivo de otro proyecto</h3>
            <p className="small">{mismatchError.message}</p>
            <pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(mismatchError.details || {}, null, 2)}</pre>
            <div style={{ display: 'flex', gap: 8 }}>
              <button type="button" onClick={() => setMismatchError(null)} disabled={loading}>Cancelar</button>
              <button type="button" onClick={forceImport} disabled={loading}>
                {loading ? 'Importando...' : 'Importar de todas formas'}
              </button>
            </div>
          </div>
        )}
      </div>

      {summary && (
        <div className="card">
          <h3 style={{ marginTop: 0 }}>Summary</h3>
          <pre style={{ whiteSpace: 'pre-wrap', marginBottom: 0 }}>{JSON.stringify(summary, null, 2)}</pre>
        </div>
      )}

      {errors.length > 0 && (
        <div className="card">
          <h3 style={{ marginTop: 0 }}>Errores</h3>
          <ul style={{ margin: 0, paddingLeft: 18 }}>
            {errors.map((error, idx) => (
              <li key={`${idx}-${String(error)}`}>{typeof error === 'string' ? error : JSON.stringify(error)}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function UnclassifiedSuppliersScreen() {
  const [suppliers, setSuppliers] = useState([]);
  const [categories, setCategories] = useState([]);
  const [selected, setSelected] = useState({});
  const [savingId, setSavingId] = useState(null);
  const [error, setError] = useState('');

  async function load() {
    setError('');
    try {
      const [supplierData, categoryData] = await Promise.all([api.unclassifiedSuppliers(), api.supplierCategories()]);
      setSuppliers(Array.isArray(supplierData) ? supplierData : []);
      setCategories(Array.isArray(categoryData) ? categoryData : []);
    } catch (err) {
      setError(err.message || 'No se pudieron cargar proveedores sin clasificar.');
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function saveCategory(supplierId) {
    const categoryId = selected[supplierId];
    if (!categoryId) return;

    setSavingId(supplierId);
    setError('');
    try {
      await api.updateSupplierCategory(supplierId, Number(categoryId));
      await load();
    } catch (err) {
      setError(err.message || 'No se pudo guardar la categoría.');
    } finally {
      setSavingId(null);
    }
  }

  return (
    <div className="container grid">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Proveedores sin clasificar</h2>
        {error && <p style={{ color: '#b91c1c' }}>{error}</p>}
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Proveedor</th>
              <th>Categoría</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {suppliers.map((supplier) => (
              <tr key={supplier.id}>
                <td>{supplier.id}</td>
                <td>{supplier.name || supplier.nombre || 'Sin nombre'}</td>
                <td>
                  <select
                    value={selected[supplier.id] || ''}
                    onChange={(e) => setSelected((prev) => ({ ...prev, [supplier.id]: e.target.value }))}
                  >
                    <option value="">Selecciona...</option>
                    {categories.map((category) => (
                      <option key={category.id} value={category.id}>
                        {category.name || category.nombre}
                      </option>
                    ))}
                  </select>
                </td>
                <td>
                  <button
                    type="button"
                    onClick={() => saveCategory(supplier.id)}
                    disabled={savingId === supplier.id || !selected[supplier.id]}
                  >
                    {savingId === supplier.id ? 'Guardando...' : 'Guardar'}
                  </button>
                </td>
              </tr>
            ))}
            {suppliers.length === 0 && (
              <tr>
                <td colSpan={4} className="small">
                  No hay proveedores sin clasificar.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SupplierCategoriesScreen() {
  const [categories, setCategories] = useState([]);
  const [name, setName] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  async function load() {
    setError('');
    try {
      const data = await api.supplierCategories();
      setCategories(Array.isArray(data) ? data : []);
    } catch (err) {
      setError(err.message || 'No se pudieron cargar categorías.');
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function createCategory(e) {
    e.preventDefault();
    if (!name.trim()) return;

    setSaving(true);
    setError('');
    try {
      await api.createSupplierCategory(name.trim());
      setName('');
      await load();
    } catch (err) {
      setError(err.message || 'No se pudo crear la categoría.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="container grid grid2">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Crear categoría</h2>
        <form className="grid" onSubmit={createCategory}>
          <div>
            <label>Nombre</label>
            <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Ej: Hormigón" />
          </div>
          <button type="submit" disabled={saving}>
            {saving ? 'Guardando...' : 'Crear categoría'}
          </button>
        </form>
        {error && <p style={{ color: '#b91c1c', marginBottom: 0 }}>{error}</p>}
      </div>

      <div className="card">
        <h2 style={{ marginTop: 0 }}>Categorías</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Nombre</th>
            </tr>
          </thead>
          <tbody>
            {categories.map((category) => (
              <tr key={category.id}>
                <td>{category.id}</td>
                <td>{category.name || category.nombre}</td>
              </tr>
            ))}
            {categories.length === 0 && (
              <tr>
                <td colSpan={2} className="small">
                  No hay categorías cargadas.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}


export function SuspiciousProjectResolutionScreen() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [filters, setFilters] = useState({
    status: 'pending',
    paymentEntity: '',
    documentProject: '',
    paymentProject: '',
    effectiveProject: '',
    supplier: '',
    dateFrom: '',
    dateTo: '',
    amountMin: '',
    amountMax: '',
    q: '',
  });
  const [reasonById, setReasonById] = useState({});
  const [toast, setToast] = useState('');
  const [selectedIds, setSelectedIds] = useState({});

  function getErrorMessage(err, fallback) {
    return err?.body?.detail?.message || err?.body?.detail || err?.message || fallback;
  }

  async function load() {
    setLoading(true);
    setError('');
    try {
      const params = Object.fromEntries(Object.entries(filters).filter(([, value]) => value !== '' && value !== null && value !== undefined));
      const response = await api.listSuspiciousProjectResolutions(params);
      setRows(Array.isArray(response?.items) ? response.items : []);
    } catch (err) {
      setError(err.message || 'No se pudo cargar la resolución de sospechosos.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [filters.status]);

  useEffect(() => {
    setSelectedIds({});
  }, [rows]);

  const visibleRows = useMemo(() => rows, [rows]);

  const groups = useMemo(() => {
    const map = new Map();
    visibleRows.forEach((row) => {
      const key = `${row.sourceSbo || row.sourceDb || '-'}|${row.documentProjectName || '-'}|${row.paymentProjectName || '-'}`;
      if (!map.has(key)) {
        map.set(key, {
          key,
          paymentEntity: row.sourceSbo || row.sourceDb || 'Sin entidad',
          documentProject: row.documentProjectName || 'Sin proyecto documento',
          paymentProject: row.paymentProjectName || 'Sin proyecto pago',
          count: 0,
        });
      }
      map.get(key).count += 1;
    });
    return Array.from(map.values()).sort((a, b) => b.count - a.count);
  }, [visibleRows]);

  const selectedTransactionIds = Object.keys(selectedIds).filter((id) => selectedIds[id]);

  function formatProjectBadge(label, value, tone) {
    return (
      <div style={{ background: tone, color: '#111827', borderRadius: 8, padding: '2px 8px', fontSize: 12 }}>
        <strong>{label}:</strong> {value || '—'}
      </div>
    );
  }

  function updateFilter(key, value) {
    setFilters((prev) => ({ ...prev, [key]: value }));
  }

  async function resolveRow(row, resolution) {
    const target = getSuspiciousResolutionTarget(row);
    if (!target.canResolve) {
      console.error('Missing transaction id for suspicious resolution', { row });
      setError(MISSING_TRANSACTION_ID_ERROR);
      return;
    }
    const { transactionId } = target;

    setToast('');
    try {
      const selectedProjectId = row?.currentAssignedProjectId || '';
      const selectedProjectCode = row?.currentAssignedProjectCode || '';
      const selectedProjectName = row?.currentAssignedProjectName || '';
      const reason = reasonById[transactionId] || '';

      console.info('Resolving suspicious transaction', {
        transactionId,
        resolution,
        paymentNum: row?.paymentNum,
        invoiceNum: row?.invoiceNum,
      });

      const response = await api.resolveSuspiciousProjectResolution(transactionId, {
        resolveTo: resolution,
        resolve_to: resolution,
        resolution,
        resolutionReason: reason,
        resolution_reason: reason,
        reason,
        projectId: selectedProjectId,
        project_id: selectedProjectId,
        projectCode: selectedProjectCode,
        project_code: selectedProjectCode,
        projectName: selectedProjectName,
        project_name: selectedProjectName,
        manualResolvedProjectId: selectedProjectId,
        manual_resolved_project_id: selectedProjectId,
        manualResolvedProjectCode: selectedProjectCode,
        manual_resolved_project_code: selectedProjectCode,
        manualResolvedProjectName: selectedProjectName,
        manual_resolved_project_name: selectedProjectName,
      });

      const persistedResolvedProjectId = String(response?.manualResolvedProjectId || '').trim();
      if (!persistedResolvedProjectId) {
        throw new Error('El backend devolvió una resolución incompleta: falta manualResolvedProjectId.');
      }

      await load();
      setToast('Resolución guardada correctamente.');
    } catch (err) {
      setError(getErrorMessage(err, 'No se pudo resolver la fila.'));
    }
  }

  async function bulkResolve(kind) {
    if (!selectedTransactionIds.length) return;
    setToast('');
    setError('');
    try {
      if (kind === 'document') {
        await api.bulkResolveSuspiciousProjectResolutionToDocument(selectedTransactionIds, 'resolucion_masiva_documento');
      } else {
        await api.bulkResolveSuspiciousProjectResolutionToPayment(selectedTransactionIds, 'resolucion_masiva_pago');
      }
      await load();
      setToast(`Resolución masiva aplicada (${selectedTransactionIds.length}).`);
    } catch (err) {
      setError(getErrorMessage(err, 'No se pudo aplicar la resolución masiva.'));
    }
  }

  return (
    <div className="container grid">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Resolución de sospechosos</h2>
        <div style={{ display: 'grid', gap: 8, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', marginBottom: 12 }}>
          <select value={filters.status} onChange={(e) => updateFilter('status', e.target.value)}>
            <option value="pending">Pendientes</option>
            <option value="resolved">Resueltos</option>
            <option value="all">Todos</option>
          </select>
          <input value={filters.paymentEntity} onChange={(e) => updateFilter('paymentEntity', e.target.value)} placeholder="Entidad que realizó el pago" />
          <input value={filters.documentProject} onChange={(e) => updateFilter('documentProject', e.target.value)} placeholder="Proyecto del documento" />
          <input value={filters.paymentProject} onChange={(e) => updateFilter('paymentProject', e.target.value)} placeholder="Proyecto del pago" />
          <input value={filters.effectiveProject} onChange={(e) => updateFilter('effectiveProject', e.target.value)} placeholder="Proyecto actual/efectivo" />
          <input value={filters.supplier} onChange={(e) => updateFilter('supplier', e.target.value)} placeholder="Proveedor o cardCode" />
          <input type="date" value={filters.dateFrom} onChange={(e) => updateFilter('dateFrom', e.target.value)} />
          <input type="date" value={filters.dateTo} onChange={(e) => updateFilter('dateTo', e.target.value)} />
          <input type="number" value={filters.amountMin} onChange={(e) => updateFilter('amountMin', e.target.value)} placeholder="Monto mínimo" />
          <input type="number" value={filters.amountMax} onChange={(e) => updateFilter('amountMax', e.target.value)} placeholder="Monto máximo" />
          <input
            value={filters.q}
            onChange={(e) => updateFilter('q', e.target.value)}
            placeholder="Búsqueda rápida: pago/factura/proveedor/comentarios"
          />
          <button type="button" onClick={load} disabled={loading}>{loading ? 'Cargando...' : 'Buscar'}</button>
        </div>
        <details style={{ marginBottom: 12 }}>
          <summary>Patrones agrupados (entidad + proyecto doc + proyecto pago)</summary>
          <div style={{ display: 'grid', gap: 6, marginTop: 8 }}>
            {groups.map((group) => (
              <button
                key={group.key}
                type="button"
                className="secondary"
                onClick={() => setFilters((prev) => ({
                  ...prev,
                  paymentEntity: group.paymentEntity,
                  documentProject: group.documentProject,
                  paymentProject: group.paymentProject,
                }))}
                style={{ textAlign: 'left' }}
              >
                {group.count} · {group.paymentEntity} · Doc: {group.documentProject} · Pago: {group.paymentProject}
              </button>
            ))}
            {groups.length === 0 && <span className="small">Sin grupos para los filtros actuales.</span>}
          </div>
        </details>
        <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
          <button type="button" onClick={() => bulkResolve('document')} disabled={!selectedTransactionIds.length}>Resolver seleccionadas a doc</button>
          <button type="button" onClick={() => bulkResolve('payment')} disabled={!selectedTransactionIds.length}>Resolver seleccionadas a pago</button>
          <span className="small">{selectedTransactionIds.length} seleccionadas</span>
        </div>
        {error && <p style={{ color: '#b91c1c' }}>{error}</p>}
        {toast && <p style={{ color: '#166534' }}>{toast}</p>}
        <table>
          <thead>
            <tr>
              <th></th><th>date</th><th>entidad pago</th><th>supplier</th><th>paymentNum</th><th>invoiceNum</th><th>amount</th>
              <th>Comparación de proyecto</th><th>status</th><th>origen/razón</th><th>acciones</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row) => (
              <tr key={row.id}>
                <td>
                  <input
                    type="checkbox"
                    checked={Boolean(selectedIds[row.id])}
                    onChange={(e) => setSelectedIds((prev) => ({ ...prev, [row.id]: e.target.checked }))}
                  />
                </td>
                <td>{row.date || ''}</td>
                <td>{row.sourceSbo || row.sourceDb || ''}</td>
                <td>{row.supplier || ''}</td>
                <td>{row.paymentNum || ''}</td>
                <td>{row.invoiceNum || ''}</td>
                <td>{row.amount ?? ''}</td>
                <td>
                  <div style={{ display: 'grid', gap: 4 }}>
                    {formatProjectBadge('Doc', row.documentProjectName, '#dbeafe')}
                    {formatProjectBadge('Pago', row.paymentProjectName, '#fee2e2')}
                    {formatProjectBadge(row.manualResolvedProjectName ? 'Manual' : 'Actual', row.manualResolvedProjectName || row.currentAssignedProjectName || row.currentAssignedProjectCode, '#dcfce7')}
                  </div>
                </td>
                <td>{row.status || ''}</td>
                <td>
                  <div style={{ display: 'grid', gap: 4 }}>
                    <span style={{ background: '#f3f4f6', borderRadius: 8, padding: '2px 8px', fontSize: 12 }}>{row.projectResolutionSource || 'n/a'}</span>
                    <span style={{ background: '#f3f4f6', borderRadius: 8, padding: '2px 8px', fontSize: 12 }}>{Array.isArray(row.suspicionReasons) ? row.suspicionReasons.join(', ') : ''}</span>
                  </div>
                </td>
                <td>
                  <div style={{ display: 'grid', gap: 4 }}>
                    <input
                      value={reasonById[getSuspiciousTransactionId(row)] || ''}
                      onChange={(e) => setReasonById((prev) => ({ ...prev, [getSuspiciousTransactionId(row)]: e.target.value }))}
                      placeholder="nota"
                    />
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      <button type="button" onClick={() => resolveRow(row, 'document')}>Asignar doc</button>
                      <button type="button" onClick={() => resolveRow(row, 'payment')}>Asignar pago</button>
                    </div>
                  </div>
                </td>
              </tr>
            ))}
            {visibleRows.length === 0 && (
              <tr><td colSpan={11} className="small">No hay registros para los filtros actuales.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function renderRoute(pathname) {
  if (pathname === '/imports/sap') return <ImportSapScreen />;
  if (pathname === '/admin/suppliers/unclassified') return <UnclassifiedSuppliersScreen />;
  if (pathname === '/admin/categories') return <SupplierCategoriesScreen />;
  if (pathname === '/admin/suspicious-project-resolutions') return <SuspiciousProjectResolutionScreen />;
  return null;
}
