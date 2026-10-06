import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';
import { ExtrasPanel } from './ExtrasPanel.jsx';
import { CaptureBlock } from './CaptureBlock.jsx';
import { EstimationBatchView, StatusBadge } from './EstimationBatchView.jsx';
import { formatCurrency, formatDate, formatPct, openAuthorizedBatchSheet } from './estimationShared.js';
import { previousCumulativeForBudget, todayIsoDate } from './estimationCapture.js';

// Estimaciones POR PROVEEDOR: una estimación puede llevar avance de cualquiera de los
// presupuestos del proveedor (p. ej. uno por departamento) que aún no esté al 100 %.
export function EstimacionesSection({ projects, selectedProjectId, isReviewer = false, initialBudgetId = null, onInitialBudgetConsumed, onOpenBudgets, onWorkflowChange, approvalProjectIds = null }) {
  const [view, setView] = useState('list');
  // Un admin puede tener asignadas solo algunas obras para autorizar (null = todas).
  const canApproveProject = (projectId) =>
    isReviewer && (approvalProjectIds === null || approvalProjectIds.includes(String(projectId || '')));
  const canApproveSelectedProject = canApproveProject(selectedProjectId);
  const [section, setSection] = useState('budgets');
  const [queueRows, setQueueRows] = useState([]);
  const [queueLoading, setQueueLoading] = useState(false);
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [supplierFilter, setSupplierFilter] = useState('');
  const [includeInactive, setIncludeInactive] = useState(false);
  const [saving, setSaving] = useState(false);

  // Detalle de un proveedor
  const [selectedSupplierKey, setSelectedSupplierKey] = useState(null);
  const [supplierBudgets, setSupplierBudgets] = useState([]);
  const [batches, setBatches] = useState([]);
  const [detailLoading, setDetailLoading] = useState(false);
  const [extrasBudgetId, setExtrasBudgetId] = useState(null);

  // Formulario de estimación
  const [showForm, setShowForm] = useState(false);
  const [editingBatch, setEditingBatch] = useState(null);
  const [formMeta, setFormMeta] = useState(null);
  const [selectedBudgetIds, setSelectedBudgetIds] = useState([]);
  const [blockResults, setBlockResults] = useState({});

  // Detalle / autorización
  const [viewingBatch, setViewingBatch] = useState(null);
  const [authorizedAmount, setAuthorizedAmount] = useState('');
  const [authorizationNote, setAuthorizationNote] = useState('');

  const budgetsById = useMemo(() => Object.fromEntries(supplierBudgets.map((budget) => [budget.id, budget])), [supplierBudgets]);
  const projectName = useMemo(() => {
    const proj = (projects || []).find((p) => String(p._id) === String(selectedProjectId));
    return proj?.displayName || proj?.name || '';
  }, [projects, selectedProjectId]);

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
    setSelectedSupplierKey(null);
    setSupplierBudgets([]);
    setBatches([]);
    setShowForm(false);
    setEditingBatch(null);
    setViewingBatch(null);
  }, [selectedProjectId]);

  // Llegada desde Presupuestos ("Estimaciones →"): abre el proveedor de ese presupuesto.
  useEffect(() => {
    if (!initialBudgetId || !selectedProjectId) return;
    setSection('budgets');
    api.getEstimationBudget(initialBudgetId)
      .then((budget) => openSupplier(budget.supplierKey))
      .catch((e) => setError(e.message || 'No se pudo abrir el presupuesto'));
    if (onInitialBudgetConsumed) onInitialBudgetConsumed();
  }, [initialBudgetId, selectedProjectId]);

  // ---- Lista de proveedores ----
  const suppliers = useMemo(() => {
    const byKey = new Map();
    rows.forEach((row) => {
      const key = row.supplierKey || row.supplierNameSnapshot || row.id;
      if (!byKey.has(key)) byKey.set(key, { key, name: row.supplierNameSnapshot || row.supplierKey, budgets: [] });
      byKey.get(key).budgets.push(row);
    });
    return Array.from(byKey.values()).map((supplier) => {
      const contracted = supplier.budgets.reduce((sum, b) => sum + (Number(b.totalContractedAmount) || 0), 0);
      const progress = supplier.budgets.reduce((sum, b) => sum + (Number(b.approvedProgressAmount) || 0), 0);
      return {
        ...supplier,
        contracted,
        paid: supplier.budgets.reduce((sum, b) => sum + (Number(b.paidAmount) || 0), 0),
        progressPct: contracted > 0 ? (progress / contracted) * 100 : 0,
        completeCount: supplier.budgets.filter((b) => b.isComplete).length,
        pendingCount: supplier.budgets.filter((b) => b.approvalStatus === 'PENDIENTE').length,
      };
    });
  }, [rows]);

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
  const listProgressPct = listTotals.totalContractedAmount > 0 ? (listTotals.approvedProgressAmount / listTotals.totalContractedAmount) * 100 : 0;
  const listPaidPct = listTotals.totalContractedAmount > 0 ? (listTotals.paidAmount / listTotals.totalContractedAmount) * 100 : 0;
  const activeBudgetsCount = rows.filter((row) => row.isActive !== false).length;

  // ---- Detalle del proveedor ----
  async function loadSupplier(supplierKey) {
    setDetailLoading(true);
    setError('');
    try {
      const list = await api.estimationBudgets({ projectId: selectedProjectId, supplier: supplierKey, includeInactive: 'true' });
      const own = (Array.isArray(list) ? list : []).filter((budget) => budget.supplierKey === supplierKey);
      const [details, supplierBatches] = await Promise.all([
        Promise.all(own.map((budget) => api.getEstimationBudget(budget.id))),
        api.supplierEstimations(supplierKey, selectedProjectId),
      ]);
      setSupplierBudgets(details);
      setBatches(Array.isArray(supplierBatches) ? supplierBatches : []);
      return { details, batches: Array.isArray(supplierBatches) ? supplierBatches : [] };
    } catch (e) {
      setError(e.message || 'No se pudo cargar el proveedor');
      return null;
    } finally {
      setDetailLoading(false);
    }
  }

  async function openSupplier(supplierKey, { viewBatchId = null } = {}) {
    setSelectedSupplierKey(supplierKey);
    setView('supplier');
    setShowForm(false);
    setEditingBatch(null);
    setViewingBatch(null);
    setExtrasBudgetId(null);
    const loaded = await loadSupplier(supplierKey);
    if (loaded && viewBatchId) {
      const batch = loaded.batches.find((b) => b.id === viewBatchId);
      if (batch) openBatchView(batch);
    }
  }

  function backToList() {
    setView('list');
    setSelectedSupplierKey(null);
    setSupplierBudgets([]);
    setBatches([]);
    setShowForm(false);
    setEditingBatch(null);
    setViewingBatch(null);
    setExtrasBudgetId(null);
  }

  async function refreshAll() {
    if (selectedSupplierKey) await loadSupplier(selectedSupplierKey);
    await loadEstimationBudgets();
    await loadQueue();
    if (onWorkflowChange) onWorkflowChange();
  }

  const openBatches = batches.filter((b) => b.workflowStatus === 'BORRADOR' || b.workflowStatus === 'ENVIADA');
  const hasOpenBatch = openBatches.length > 0;
  const latestFolio = batches.reduce((max, b) => Math.max(max, Number(b.folio) || 0), 0);
  const supplierName = supplierBudgets[0]?.supplierNameSnapshot || suppliers.find((s) => s.key === selectedSupplierKey)?.name || selectedSupplierKey;

  // Presupuestos con los que se puede estimar: activos, autorizados y no al 100 %.
  function budgetAvailability(budget) {
    if (budget.isActive === false) return { ok: false, reason: 'Inactivo' };
    if (budget.approvalStatus === 'PENDIENTE') return { ok: false, reason: 'Pendiente de autorización' };
    const inThisBatch = editingBatch && (editingBatch.parts || []).some((part) => part.estimationBudgetId === budget.id);
    if (budget.isComplete && !inThisBatch) return { ok: false, reason: 'Al 100 % (completo)' };
    return { ok: true, reason: '' };
  }
  const eligibleBudgets = supplierBudgets.filter((budget) => budgetAvailability(budget).ok);

  // ---- Formulario ----
  function startCreate() {
    setViewingBatch(null);
    setEditingBatch(null);
    setBlockResults({});
    setFormMeta({ periodStart: todayIsoDate(), periodEnd: todayIsoDate(), notes: '', requestedAmount: '' });
    setSelectedBudgetIds(eligibleBudgets.length === 1 ? [eligibleBudgets[0].id] : []);
    setShowForm(true);
  }

  function startEdit(batch) {
    setViewingBatch(null);
    setEditingBatch(batch);
    setBlockResults({});
    setFormMeta({
      periodStart: batch.periodStart || '',
      periodEnd: batch.periodEnd || '',
      notes: batch.notes || '',
      requestedAmount: batch.requestedAmount != null ? String(batch.requestedAmount) : '',
    });
    setSelectedBudgetIds((batch.parts || []).map((part) => part.estimationBudgetId));
    setShowForm(true);
  }

  function resetForm() {
    setShowForm(false);
    setEditingBatch(null);
    setFormMeta(null);
    setSelectedBudgetIds([]);
    setBlockResults({});
  }

  function toggleBudget(budgetId) {
    setSelectedBudgetIds((prev) => (prev.includes(budgetId) ? prev.filter((id) => id !== budgetId) : [...prev, budgetId]));
  }

  const activeResults = selectedBudgetIds.map((id) => blockResults[id]).filter(Boolean);
  const formTotals = activeResults.reduce(
    (acc, result) => ({
      periodSubtotal: acc.periodSubtotal + result.preview.periodSubtotal,
      retentionAmount: acc.retentionAmount + result.preview.retentionAmount,
      advanceAmortizationAmount: acc.advanceAmortizationAmount + result.preview.advanceAmortizationAmount,
      priorPaidApplied: acc.priorPaidApplied + result.preview.priorPaidApplied,
      totalToPay: acc.totalToPay + result.preview.totalToPay,
    }),
    { periodSubtotal: 0, retentionAmount: 0, advanceAmortizationAmount: 0, priorPaidApplied: 0, totalToPay: 0 },
  );

  function handleBlockChange(result) {
    setBlockResults((prev) => ({ ...prev, [result.budgetId]: result }));
  }

  async function submitForm(event, { sendForReview = false } = {}) {
    event?.preventDefault?.();
    if (!formMeta) return;
    const parts = activeResults.filter((result) => result.hasProgress).map((result) => result.payload);
    if (!selectedBudgetIds.length) {
      setError('Elige al menos un presupuesto para estimar.');
      return;
    }
    if (!parts.length) {
      setError('Captura el avance de al menos un presupuesto.');
      return;
    }
    setSaving(true);
    setError('');
    try {
      const payload = {
        projectId: selectedProjectId,
        supplierKey: selectedSupplierKey,
        periodStart: formMeta.periodStart,
        periodEnd: formMeta.periodEnd,
        notes: formMeta.notes,
        requestedAmount: formMeta.requestedAmount === '' ? null : Number(formMeta.requestedAmount),
        parts,
        submit: sendForReview,
      };
      if (editingBatch) await api.updateSupplierEstimation(editingBatch.id, payload);
      else await api.createSupplierEstimation(payload);
      resetForm();
      await refreshAll();
    } catch (e) {
      setError(e.message || 'No se pudo guardar la estimación');
    } finally {
      setSaving(false);
    }
  }

  // ---- Acciones sobre una estimación ----
  async function runAction(action, ...args) {
    setSaving(true);
    setError('');
    try {
      await action(...args);
      setViewingBatch(null);
      await refreshAll();
    } catch (e) {
      setError(e.message || 'No se pudo completar la acción');
    } finally {
      setSaving(false);
    }
  }

  function sendDraftForReview(batch) {
    if (!window.confirm(`¿Enviar la estimación #${batch.folio} a revisión? Ya no podrás editarla.`)) return;
    runAction(api.submitSupplierEstimation, batch.id);
  }

  function deleteBatch(batch) {
    if (!window.confirm(`¿Eliminar la estimación #${batch.folio}? Esta acción no se puede deshacer.`)) return;
    runAction(api.deleteSupplierEstimation, batch.id);
  }

  function changeFolio(batch) {
    const raw = window.prompt(
      `Número de la estimación #${batch.folio}. Las siguientes continuarán a partir del número más alto de este proveedor.`,
      String(batch.folio),
    );
    if (raw === null || raw.trim() === '' || Number(raw) === Number(batch.folio)) return;
    runAction(api.setSupplierEstimationFolio, batch.id, Number(raw));
  }

  function openBatchView(batch) {
    setShowForm(false);
    setEditingBatch(null);
    setViewingBatch(batch);
    setAuthorizedAmount(String(batch.authorizedAmount ?? batch.requestedAmount ?? batch.totalToPay ?? ''));
    setAuthorizationNote(batch.authorizationNote || '');
  }

  function approveViewing() {
    const batch = viewingBatch;
    if (!batch) return;
    const calculated = Number(batch.totalToPay) || 0;
    const requested = batch.requestedAmount != null ? Number(batch.requestedAmount) : null;
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
      setError(amount < baseline ? 'Indica el motivo por el que se autoriza menos de lo solicitado.' : 'Indica el motivo por el que se autoriza más de lo que marca el avance.');
      return;
    }
    runAction(api.approveSupplierEstimation, batch.id, { authorizedAmount: amount, authorizationNote: authorizationNote.trim() });
  }

  function returnViewing() {
    const batch = viewingBatch;
    if (!batch) return;
    const reason = window.prompt('Motivo de la devolución (se le mostrará a quien capturó):');
    if (!reason || !reason.trim()) return;
    runAction(api.returnSupplierEstimation, batch.id, { reason: reason.trim() });
  }

  // Hoja de autorización (imprimir / guardar como PDF).
  async function printBatch(batch) {
    setError('');
    try {
      const byId = { ...budgetsById };
      for (const part of batch.parts || []) {
        if (!byId[part.estimationBudgetId]) byId[part.estimationBudgetId] = await api.getEstimationBudget(part.estimationBudgetId);
      }
      if (!openAuthorizedBatchSheet(batch, byId, projectName)) {
        setError('El navegador bloqueó la ventana. Permite ventanas emergentes para generar el PDF.');
      }
    } catch (err) {
      setError(err?.message || 'No se pudo generar el PDF.');
    }
  }

  // ---- Bandejas (por autorizar / por pagar / abiertas) ----
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

  // Contador del tab "Por autorizar" aunque se esté en otra sección.
  const [pendingReviewCount, setPendingReviewCount] = useState(0);
  useEffect(() => {
    if (!isReviewer || !selectedProjectId || !canApproveSelectedProject) {
      setPendingReviewCount(0);
      return;
    }
    api.estimationsQueue({ projectId: selectedProjectId, status: 'ENVIADA' })
      .then((data) => setPendingReviewCount(Array.isArray(data?.items) ? data.items.length : 0))
      .catch(() => setPendingReviewCount(0));
  }, [isReviewer, selectedProjectId, canApproveSelectedProject, batches, section]);

  async function openFromQueue(row) {
    setSection('budgets');
    await openSupplier(row.supplierKey, { viewBatchId: row.id });
  }

  const extrasBudget = supplierBudgets.find((budget) => budget.id === extrasBudgetId) || null;
  const supplierContracted = supplierBudgets.reduce((sum, b) => sum + (Number(b.totalContractedAmount) || 0), 0);
  const supplierProgressAmount = supplierBudgets.reduce((sum, b) => sum + (Number(b.approvedProgressAmount) || 0), 0);
  const supplierPaid = supplierBudgets.reduce((sum, b) => sum + (Number(b.paidAmount) || 0), 0);

  return (
    <div style={{ display: 'grid', gap: 14 }}>
      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}

      <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
        <button type="button" className={section === 'budgets' ? '' : 'secondary'} onClick={() => setSection('budgets')}>
          Proveedores
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
                    <th>Presupuestos</th>
                    <th>Folio</th>
                    <th>Periodo</th>
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
                      <td>{formatCurrency(row.totalToPay)}</td>
                      <td>{row.requestedAmount != null ? formatCurrency(row.requestedAmount) : '—'}</td>
                      {section === 'payable' && <td><strong>{formatCurrency(row.authorizedAmount)}</strong></td>}
                      <td><StatusBadge estimation={row} /></td>
                      <td>
                        <div className="row" style={{ gap: 6 }}>
                          <button type="button" onClick={() => openFromQueue(row)}>
                            {section === 'review' && canApproveProject(row.projectId) ? 'Revisar' : section === 'review' ? 'Ver' : 'Abrir'}
                          </button>
                          {section === 'payable' && (
                            <button type="button" className="secondary" onClick={() => printBatch(row)}>PDF autorizado</button>
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
            <div className="kpi-card"><div>
              <div className="kpi-label">Total contratado</div>
              <div className="kpi-value">{formatCurrency(listTotals.totalContractedAmount)}</div>
              <div className="kpi-sub">en presupuestos por conceptos</div>
            </div></div>
            {isReviewer && (
              <div className="kpi-card"><div>
                <div className="kpi-label">Total pagado</div>
                <div className="kpi-value">{formatCurrency(listTotals.paidAmount)}</div>
                <div className="kpi-sub">egresos ligados a estos presupuestos</div>
              </div></div>
            )}
            {isReviewer && (
              <div className="kpi-card"><div>
                <div className="kpi-label">% pagado</div>
                <div className="kpi-value">{formatPct(listPaidPct)}</div>
                <div className="kpi-sub">pagado / contratado, sin importar el avance estimado</div>
              </div></div>
            )}
            {isReviewer && (
              <div className="kpi-card"><div>
                <div className="kpi-label">Saldo por pagar</div>
                <div className="kpi-value">{formatCurrency(listTotals.totalContractedAmount - listTotals.paidAmount)}</div>
                <div className="kpi-sub">contratado − pagado</div>
              </div></div>
            )}
            <div className="kpi-card"><div>
              <div className="kpi-label">% de avance estimado</div>
              <div className="kpi-value">{formatPct(listProgressPct)}</div>
              <div className="kpi-sub">{formatCurrency(listTotals.approvedProgressAmount)} en estimaciones aprobadas</div>
            </div></div>
            <div className="kpi-card"><div>
              <div className="kpi-label">Retenido a la fecha</div>
              <div className="kpi-value">{formatCurrency(listTotals.totalRetainedToDate)}</div>
              <div className="kpi-sub">fondo de garantía acumulado</div>
            </div></div>
            <div className="kpi-card"><div>
              <div className="kpi-label">Anticipo pendiente</div>
              <div className="kpi-value">{formatCurrency(listTotals.remainingAdvanceBalance)}</div>
              <div className="kpi-sub">saldo por amortizar</div>
            </div></div>
            <div className="kpi-card"><div>
              <div className="kpi-label">Proveedores / presupuestos</div>
              <div className="kpi-value">{suppliers.length} / {rows.length}</div>
              <div className="kpi-sub">{includeInactive ? `${activeBudgetsCount} presupuestos activos` : 'presupuestos activos'}</div>
            </div></div>
          </div>

          <div className="card" style={{ overflow: 'hidden' }}>
            <div className="card-header">
              <div className="search-input-wrap" style={{ maxWidth: 360 }}>
                <input className="search-input" value={supplierFilter} onChange={(e) => setSupplierFilter(e.target.value)} placeholder="Filtrar por proveedor" />
              </div>
              <button type="button" className="secondary" onClick={loadEstimationBudgets}>Buscar</button>
              <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
                <input type="checkbox" checked={includeInactive} onChange={(e) => setIncludeInactive(e.target.checked)} />
                Mostrar inactivos
              </label>
              <div style={{ flex: 1 }} />
              {onOpenBudgets && (
                <button type="button" className="secondary" onClick={onOpenBudgets} style={{ fontSize: 13 }}>
                  Capturar presupuestos →
                </button>
              )}
            </div>

            {loading ? (
              <div className="small" style={{ padding: 16 }}>Cargando proveedores...</div>
            ) : (
              <div style={{ overflowX: 'auto' }}>
                <table>
                  <thead>
                    <tr>
                      <th>Proveedor</th>
                      <th>Presupuestos</th>
                      <th>Total contratado</th>
                      {isReviewer && <th>Pagado</th>}
                      <th>% avance estimado</th>
                      <th>Estado</th>
                      <th>Acciones</th>
                    </tr>
                  </thead>
                  <tbody>
                    {suppliers.map((supplier) => (
                      <tr key={supplier.key}>
                        <td><strong>{supplier.name}</strong></td>
                        <td>
                          {supplier.budgets.length}
                          {supplier.completeCount > 0 && <span className="small" style={{ color: '#166534' }}> · {supplier.completeCount} al 100 %</span>}
                        </td>
                        <td>{formatCurrency(supplier.contracted)}</td>
                        {isReviewer && <td>{formatCurrency(supplier.paid)}</td>}
                        <td>{formatPct(supplier.progressPct)}</td>
                        <td>
                          {supplier.pendingCount > 0
                            ? <span className="small" style={{ color: '#92400e', fontWeight: 600 }}>{supplier.pendingCount} presupuesto(s) por autorizar</span>
                            : supplier.completeCount === supplier.budgets.length ? 'Completo' : 'Activo'}
                        </td>
                        <td>
                          <button type="button" onClick={() => openSupplier(supplier.key)}>Estimar / ver</button>
                        </td>
                      </tr>
                    ))}
                    {!suppliers.length && (
                      <tr>
                        <td colSpan={isReviewer ? 7 : 6} className="small" style={{ textAlign: 'center' }}>
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
            <button type="button" className="secondary" onClick={backToList}>← Volver a proveedores</button>
          </div>

          {detailLoading && !supplierBudgets.length ? (
            <div className="small">Cargando proveedor...</div>
          ) : (
            <>
              <div className="kpi-grid">
                <div className="kpi-card"><div>
                  <div className="kpi-label">{supplierName}</div>
                  <div className="kpi-value">{formatCurrency(supplierContracted)}</div>
                  <div className="kpi-sub">total contratado · {supplierBudgets.length} presupuesto(s)</div>
                </div></div>
                {isReviewer && (
                  <div className="kpi-card"><div>
                    <div className="kpi-label">Pagado</div>
                    <div className="kpi-value">{formatCurrency(supplierPaid)}</div>
                    <div className="kpi-sub">saldo: {formatCurrency(supplierContracted - supplierPaid)}</div>
                  </div></div>
                )}
                <div className="kpi-card"><div>
                  <div className="kpi-label">Avance estimado</div>
                  <div className="kpi-value">{formatPct(supplierContracted > 0 ? (supplierProgressAmount / supplierContracted) * 100 : 0)}</div>
                  <div className="kpi-sub">{formatCurrency(supplierProgressAmount)} en estimaciones aprobadas</div>
                </div></div>
                <div className="kpi-card"><div>
                  <div className="kpi-label">Retenido a la fecha</div>
                  <div className="kpi-value">{formatCurrency(supplierBudgets.reduce((sum, b) => sum + (Number(b.totalRetainedToDate) || 0), 0))}</div>
                  <div className="kpi-sub">fondo de garantía acumulado</div>
                </div></div>
                <div className="kpi-card"><div>
                  <div className="kpi-label">Saldo de anticipo</div>
                  <div className="kpi-value">{formatCurrency(supplierBudgets.reduce((sum, b) => sum + (Number(b.remainingAdvanceBalance) || 0), 0))}</div>
                  <div className="kpi-sub">por amortizar</div>
                </div></div>
              </div>

              <div className="card" style={{ overflow: 'hidden' }}>
                <div className="card-header">
                  <strong>Presupuestos de {supplierName}</strong>
                  <div style={{ flex: 1 }} />
                  {isReviewer && onOpenBudgets && (
                    <button type="button" className="secondary" onClick={onOpenBudgets} title="Editar presupuestos, asignar pagos o registrar el saldo inicial">
                      Administrar en Presupuestos
                    </button>
                  )}
                </div>
                <div style={{ overflowX: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Presupuesto</th>
                        <th>Contratado</th>
                        {isReviewer && <th>Pagado</th>}
                        <th>Avance estimado</th>
                        <th>Estado</th>
                        {isReviewer && <th>Acciones</th>}
                      </tr>
                    </thead>
                    <tbody>
                      {supplierBudgets.map((budget) => {
                        const availability = budgetAvailability(budget);
                        return (
                          <tr key={budget.id}>
                            <td>{budget.name || '—'}</td>
                            <td>{formatCurrency(budget.totalContractedAmount)}</td>
                            {isReviewer && <td>{formatCurrency(budget.paidAmount)}</td>}
                            <td>{formatPct(budget.approvedProgressPct)}</td>
                            <td>
                              {budget.isComplete
                                ? <span style={{ color: '#166534', fontWeight: 600 }}>Al 100 %</span>
                                : availability.ok ? 'Por estimar' : <span style={{ color: '#92400e' }}>{availability.reason}</span>}
                            </td>
                            {isReviewer && (
                              <td>
                                <button type="button" className="secondary" onClick={() => setExtrasBudgetId(extrasBudgetId === budget.id ? null : budget.id)} title="Agregar conceptos extra o un presupuesto adicional">
                                  + Extras
                                </button>
                              </td>
                            )}
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>

              {extrasBudget && isReviewer && (
                <ExtrasPanel
                  budget={extrasBudget}
                  onClose={() => setExtrasBudgetId(null)}
                  onSaved={async () => {
                    setExtrasBudgetId(null);
                    await refreshAll();
                  }}
                />
              )}

              <div className="card" style={{ overflow: 'hidden' }}>
                <div className="card-header">
                  <strong>Estimaciones de {supplierName}</strong>
                  <div style={{ flex: 1 }} />
                  {!showForm && (
                    <button
                      type="button"
                      onClick={startCreate}
                      disabled={hasOpenBatch || !eligibleBudgets.length}
                      title={hasOpenBatch ? 'Hay una estimación abierta de este proveedor; ciérrala (aprobada) antes de crear otra' : !eligibleBudgets.length ? 'Ningún presupuesto disponible para estimar (completos o pendientes de autorización)' : undefined}
                    >
                      + Nueva estimación
                    </button>
                  )}
                </div>
                {supplierBudgets.some((b) => b.approvalStatus === 'PENDIENTE') && (
                  <div className="small" style={{ background: '#fef3c7', color: '#92400e', borderRadius: 6, padding: 10 }}>
                    Hay presupuestos pendientes de autorización: un admin debe autorizarlos en Presupuestos antes de estimar sobre ellos.
                  </div>
                )}

                <div style={{ overflowX: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Folio</th>
                        <th>Periodo</th>
                        <th>Presupuestos</th>
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
                      {batches.map((batch) => {
                        const workflow = batch.workflowStatus || 'REGISTRADA';
                        const canEdit = workflow === 'BORRADOR';
                        const canDelete = workflow === 'BORRADOR' && Number(batch.folio) === latestFolio;
                        return (
                          <tr key={batch.id}>
                            <td>#{batch.folio}</td>
                            <td>{formatDate(batch.periodStart)} – {formatDate(batch.periodEnd)}</td>
                            <td>{batch.budgetName || '—'}</td>
                            <td>{formatCurrency(batch.periodSubtotal)}</td>
                            <td>{formatCurrency(batch.retentionAmount)}</td>
                            <td>{formatCurrency(batch.advanceAmortizationAmount)}</td>
                            <td>{formatCurrency(batch.totalToPay)}</td>
                            <td>{batch.requestedAmount != null ? formatCurrency(batch.requestedAmount) : '—'}</td>
                            <td>{workflow === 'APROBADA' ? <strong>{formatCurrency(batch.authorizedAmount)}</strong> : '—'}</td>
                            <td><StatusBadge estimation={batch} /></td>
                            <td>
                              <div className="row" style={{ gap: 6, flexWrap: 'wrap' }}>
                                <button type="button" className="secondary" onClick={() => openBatchView(batch)}>
                                  {workflow === 'ENVIADA' && canApproveProject(batch.projectId) ? 'Revisar' : 'Ver'}
                                </button>
                                {isReviewer && (
                                  <button type="button" className="secondary" onClick={() => changeFolio(batch)}>Cambiar nº</button>
                                )}
                                {workflow === 'APROBADA' && (
                                  <button type="button" className="secondary" onClick={() => printBatch(batch)}>PDF autorizado</button>
                                )}
                                {canEdit && (
                                  <button type="button" className="secondary" onClick={() => startEdit(batch)}>Editar</button>
                                )}
                                {workflow === 'BORRADOR' && (
                                  <button type="button" onClick={() => sendDraftForReview(batch)} disabled={saving}>Enviar a revisión</button>
                                )}
                                {canDelete && (
                                  <button type="button" className="secondary" onClick={() => deleteBatch(batch)} style={{ color: '#b91c1c' }}>Eliminar</button>
                                )}
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                      {!batches.length && (
                        <tr>
                          <td colSpan={11} className="small" style={{ textAlign: 'center' }}>Aún no hay estimaciones de este proveedor.</td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              {viewingBatch && !showForm && (
                <EstimationBatchView
                  batch={viewingBatch}
                  budgetsById={budgetsById}
                  canReview={canApproveProject(viewingBatch.projectId)}
                  isReviewer={isReviewer}
                  saving={saving}
                  authorizedAmount={authorizedAmount}
                  setAuthorizedAmount={setAuthorizedAmount}
                  authorizationNote={authorizationNote}
                  setAuthorizationNote={setAuthorizationNote}
                  onApprove={approveViewing}
                  onReturn={returnViewing}
                  onPrint={() => printBatch(viewingBatch)}
                  onClose={() => setViewingBatch(null)}
                />
              )}

              {showForm && formMeta && (
                <form className="card" style={{ display: 'grid', gap: 12, padding: 16 }} onSubmit={submitForm}>
                  <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                    <strong>{editingBatch ? `Editar estimación #${editingBatch.folio}` : 'Nueva estimación'} · {supplierName}</strong>
                    <button type="button" className="secondary" onClick={resetForm}>✕ Cancelar</button>
                  </div>

                  <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
                    <div>
                      <label>Periodo desde</label>
                      <input type="date" value={formMeta.periodStart} onChange={(e) => setFormMeta((prev) => ({ ...prev, periodStart: e.target.value }))} required />
                    </div>
                    <div>
                      <label>Periodo hasta</label>
                      <input type="date" value={formMeta.periodEnd} onChange={(e) => setFormMeta((prev) => ({ ...prev, periodEnd: e.target.value }))} required />
                    </div>
                    <div>
                      <label>Monto solicitado por el contratista</label>
                      <input
                        type="number"
                        min="0"
                        step="0.01"
                        value={formMeta.requestedAmount}
                        onChange={(e) => setFormMeta((prev) => ({ ...prev, requestedAmount: e.target.value }))}
                        placeholder="Igual al avance"
                        style={{ width: 190 }}
                      />
                    </div>
                    <div style={{ flex: 1, minWidth: 200 }}>
                      <label>Notas</label>
                      <input value={formMeta.notes} onChange={(e) => setFormMeta((prev) => ({ ...prev, notes: e.target.value }))} />
                    </div>
                  </div>

                  <div style={{ display: 'grid', gap: 6 }}>
                    <strong style={{ fontSize: 13 }}>¿De qué presupuestos vas a estimar?</strong>
                    <div className="small" style={{ color: 'var(--gray-600)' }}>
                      Marca los presupuestos de {supplierName} que avanzaron en este periodo. Los que ya están al 100 % o pendientes de autorización no se pueden elegir.
                    </div>
                    <div style={{ display: 'grid', gap: 4 }}>
                      {supplierBudgets.map((budget) => {
                        const availability = budgetAvailability(budget);
                        return (
                          <label key={budget.id} className="small" style={{ display: 'inline-flex', gap: 8, alignItems: 'center', opacity: availability.ok ? 1 : 0.55 }}>
                            <input
                              type="checkbox"
                              checked={selectedBudgetIds.includes(budget.id)}
                              disabled={!availability.ok}
                              onChange={() => toggleBudget(budget.id)}
                            />
                            <span>
                              {budget.name} · {formatCurrency(budget.totalContractedAmount)} · avance {formatPct(budget.approvedProgressPct)}
                              {!availability.ok && <em> — {availability.reason}</em>}
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  </div>

                  {selectedBudgetIds.map((budgetId) => {
                    const budget = budgetsById[budgetId];
                    if (!budget) return null;
                    const savedPart = editingBatch ? (editingBatch.parts || []).find((part) => part.estimationBudgetId === budgetId) || null : null;
                    return (
                      <CaptureBlock
                        key={`${budgetId}-${editingBatch?.id || 'new'}`}
                        budget={budget}
                        previousCumulative={previousCumulativeForBudget(budgetId, batches, editingBatch?.id || null)}
                        savedPart={savedPart}
                        onChange={handleBlockChange}
                        onRemove={() => toggleBudget(budgetId)}
                      />
                    );
                  })}

                  {activeResults.length > 0 && (
                    <div className="kpi-grid">
                      <div className="kpi-card"><div>
                        <div className="kpi-label">Subtotal del periodo</div>
                        <div className="kpi-value">{formatCurrency(formTotals.periodSubtotal)}</div>
                      </div></div>
                      <div className="kpi-card"><div>
                        <div className="kpi-label">Retención</div>
                        <div className="kpi-value">−{formatCurrency(formTotals.retentionAmount)}</div>
                      </div></div>
                      <div className="kpi-card"><div>
                        <div className="kpi-label">Amortización anticipo</div>
                        <div className="kpi-value">−{formatCurrency(formTotals.advanceAmortizationAmount)}</div>
                      </div></div>
                      <div className="kpi-card"><div>
                        <div className="kpi-label">Pagos previos reconocidos</div>
                        <div className="kpi-value">−{formatCurrency(formTotals.priorPaidApplied)}</div>
                        <div className="kpi-sub">ya pagado al contratista</div>
                      </div></div>
                      <div className="kpi-card"><div>
                        <div className="kpi-label">A liberar (monto a autorizar)</div>
                        <div className="kpi-value">{formatCurrency(formTotals.totalToPay)}</div>
                        <div className="kpi-sub">total de {activeResults.filter((r) => r.hasProgress).length} presupuesto(s)</div>
                      </div></div>
                      <div className="kpi-card"><div>
                        <div className="kpi-label">Solicitado por el contratista</div>
                        <div className="kpi-value">{formMeta.requestedAmount === '' ? '—' : formatCurrency(Number(formMeta.requestedAmount) || 0)}</div>
                        <div className="kpi-sub">
                          {formMeta.requestedAmount === ''
                            ? 'sin capturar: se toma el avance'
                            : (() => {
                                const diff = (Number(formMeta.requestedAmount) || 0) - formTotals.totalToPay;
                                if (Math.abs(diff) < 0.01) return 'igual al avance reportado';
                                return `${formatCurrency(Math.abs(diff))} ${diff > 0 ? 'más' : 'menos'} que el avance`;
                              })()}
                        </div>
                      </div></div>
                    </div>
                  )}

                  <div className="row" style={{ gap: 8 }}>
                    <button type="submit" className="secondary" disabled={saving}>{saving ? 'Guardando...' : 'Guardar borrador'}</button>
                    <button type="button" disabled={saving} onClick={(e) => submitForm(e, { sendForReview: true })}>Guardar y enviar a revisión</button>
                    <button type="button" className="secondary" onClick={resetForm}>Cancelar</button>
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
