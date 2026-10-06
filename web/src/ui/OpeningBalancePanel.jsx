import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';
import { formatCurrency, formatDate, formatPct } from './estimationShared.js';

// Saldo inicial de un presupuesto: anticipo ya entregado y pagos a cuenta. Sirve para obras
// a medias (se registran los pagos que ya se hicieron) y solo puede cambiarse antes de la
// primera estimación del presupuesto. Se usa en Presupuestos y dentro de Estimaciones.
export function OpeningBalancePanel({ budget, onSaved, onClose }) {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [transactions, setTransactions] = useState([]);
  const [requiresAssignment, setRequiresAssignment] = useState(false);
  const [assignments, setAssignments] = useState({});
  const [manualAdvance, setManualAdvance] = useState('');
  const [manualPrior, setManualPrior] = useState('');
  const [note, setNote] = useState('');

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    api.estimationBudgetTransactions(budget.id)
      .then((payload) => {
        if (!active) return;
        setTransactions(Array.isArray(payload?.items) ? payload.items : []);
        setRequiresAssignment(Boolean(payload?.supplierHasMultipleActiveBudgets));
        const next = {};
        (budget.openingAdvanceTransactionIds || []).forEach((id) => { next[id] = 'advance'; });
        (budget.openingPriorPaymentTransactionIds || []).forEach((id) => { next[id] = 'prior'; });
        setAssignments(next);
        setManualAdvance(budget.openingManualAdvanceAmount ? String(budget.openingManualAdvanceAmount) : '');
        setManualPrior(budget.openingManualPriorPaidAmount ? String(budget.openingManualPriorPaidAmount) : '');
        setNote(budget.openingNote || '');
      })
      .catch((e) => active && setError(e.message || 'No se pudieron cargar los pagos del proveedor'))
      .finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [budget.id]);

  const totals = useMemo(() => {
    let advance = Number(manualAdvance) || 0;
    let prior = Number(manualPrior) || 0;
    transactions.forEach((tx) => {
      if (assignments[tx.id] === 'advance') advance += Number(tx.amountWithTax) || 0;
      if (assignments[tx.id] === 'prior') prior += Number(tx.amountWithTax) || 0;
    });
    const total = Number(budget.totalContractedAmount) || 0;
    return { advance, prior, paid: advance + prior, paidPct: total > 0 ? ((advance + prior) / total) * 100 : 0 };
  }, [transactions, assignments, manualAdvance, manualPrior, budget]);

  async function save() {
    setSaving(true);
    setError('');
    try {
      const ids = (kind) => Object.entries(assignments).filter(([, value]) => value === kind).map(([id]) => id);
      await api.saveEstimationOpeningBalance(budget.id, {
        advanceTransactionIds: ids('advance'),
        priorPaymentTransactionIds: ids('prior'),
        manualAdvanceAmount: Number(manualAdvance) || 0,
        manualPriorPaidAmount: Number(manualPrior) || 0,
        note,
      });
      if (onSaved) await onSaved();
    } catch (e) {
      setError(e.message || 'No se pudo guardar el saldo inicial');
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="grid budgets-assignment-panel" style={{ gap: 10, borderRadius: 10, padding: 12 }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <strong>Anticipo y pagos previos · {budget.supplierNameSnapshot || budget.supplierKey} · {budget.name || 'Presupuesto'}</strong>
        <button type="button" className="secondary" onClick={onClose} disabled={saving}>Cerrar</button>
      </div>
      <div className="small">
        Para presupuestos que ya traen pagos (obra a medias). Marca cuáles de los pagos ya hechos fueron <strong>anticipo</strong> y cuáles <strong>pagos a cuenta</strong>
        (o captura montos manuales). El anticipo se amortiza solo en cada estimación y los pagos a cuenta se descuentan de lo que se libera. Si no registras nada, se toman
        todos los pagos asignados al presupuesto. Solo puede cambiarse antes de la primera estimación de este presupuesto.
      </div>
      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}
      {loading ? (
        <div className="small">Cargando pagos del proveedor...</div>
      ) : (
        <>
          {requiresAssignment && (
            <div className="small" style={{ color: 'var(--gray-600)' }}>
              Este proveedor tiene varios presupuestos activos: los pagos que marques aquí quedan <strong>asignados a este presupuesto</strong>. Solo se bloquean los que ya
              están asignados a otro presupuesto.
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
                {transactions.map((tx) => {
                  const blocked = Boolean(tx.isAssignedToOtherBudget);
                  return (
                    <tr key={tx.id} style={blocked ? { opacity: 0.5 } : undefined}>
                      <td>{formatDate(tx.date)}</td>
                      <td>{tx.description || '—'}</td>
                      <td>{formatCurrency(tx.amountWithTax)}</td>
                      <td>
                        <select
                          value={assignments[tx.id] || ''}
                          disabled={blocked}
                          onChange={(e) => setAssignments((prev) => {
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
                {!transactions.length && (
                  <tr><td colSpan={4} className="small" style={{ textAlign: 'center' }}>Este proveedor no tiene pagos registrados.</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
            <div>
              <label>Anticipo manual (opcional)</label>
              <input type="number" min="0" step="0.01" value={manualAdvance} onChange={(e) => setManualAdvance(e.target.value)} style={{ width: 170 }} />
            </div>
            <div>
              <label>Otros pagos a cuenta manuales (opcional)</label>
              <input type="number" min="0" step="0.01" value={manualPrior} onChange={(e) => setManualPrior(e.target.value)} style={{ width: 170 }} />
            </div>
            <div style={{ flex: 1, minWidth: 200 }}>
              <label>Nota</label>
              <input value={note} onChange={(e) => setNote(e.target.value)} />
            </div>
          </div>
          <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13 }}>
            <div><strong>Anticipo:</strong> {formatCurrency(totals.advance)}</div>
            <div><strong>Pagos a cuenta:</strong> {formatCurrency(totals.prior)}</div>
            <div><strong>Pagado a la fecha:</strong> {formatCurrency(totals.paid)} ({formatPct(totals.paidPct)} del presupuesto)</div>
          </div>
          <div className="row" style={{ gap: 8 }}>
            <button type="button" onClick={save} disabled={saving}>{saving ? 'Guardando...' : 'Guardar'}</button>
            <button type="button" className="secondary" onClick={onClose} disabled={saving}>Cancelar</button>
          </div>
        </>
      )}
    </div>
  );
}
