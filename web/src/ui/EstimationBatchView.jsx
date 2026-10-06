import React from 'react';
import { formatCurrency, formatDate, formatPct, groupLabel, extraConceptsOfGroup } from './estimationShared.js';

const WORKFLOW_LABELS = { BORRADOR: 'Borrador', ENVIADA: 'Por autorizar', APROBADA: 'Aprobada', REGISTRADA: 'Registrada' };

export function describeEstimationStatus(estimation) {
  const workflow = estimation?.workflowStatus || 'REGISTRADA';
  if (workflow === 'APROBADA') return estimation?.paymentStatus === 'PAGADA' ? 'Pagada' : 'Aprobada · Por pagar';
  return WORKFLOW_LABELS[workflow] || workflow;
}

function statusBadgeStyle(estimation) {
  const workflow = estimation?.workflowStatus || 'REGISTRADA';
  if (workflow === 'APROBADA') {
    return estimation?.paymentStatus === 'PAGADA' ? { background: '#dcfce7', color: '#166534' } : { background: '#dbeafe', color: '#1e40af' };
  }
  if (workflow === 'ENVIADA') return { background: '#fef3c7', color: '#92400e' };
  if (workflow === 'BORRADOR') return { background: '#e5e7eb', color: '#374151' };
  return { background: '#f3f4f6', color: '#4b5563' };
}

export function StatusBadge({ estimation }) {
  return <span className="badge" style={statusBadgeStyle(estimation)}>{describeEstimationStatus(estimation)}</span>;
}

// Hoja de UN presupuesto dentro de la estimación del proveedor (conceptos + hoja por grupo).
function PartSheet({ part, budget }) {
  const sheet = part.groupBreakdown || [];
  const sum = (key) => sheet.reduce((total, row) => total + (Number(row[key]) || 0), 0);
  const hasGroups = sheet.some((row) => row.group);
  const totalBudget = sum('budgetAmount');
  const advanceGiven = budget?.advanceAmortizationEnabled ? Number(budget?.advanceAmount) || 0 : 0;
  const showPaidLines = Number(part.priorPaidApplied) > 0;
  const paidToDate = (Number(part.priorPaidApplied) || 0) + advanceGiven;
  return (
    <div style={{ display: 'grid', gap: 8 }}>
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
            {(part.lineItems || []).filter((li) => Number(li.periodAmount) !== 0 || Number(li.periodQuantity) !== 0).map((li) => (
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

      {hasGroups && (
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
                ), ...(row.isExtra ? extraConceptsOfGroup(row.group, part, budget) : []).map((c) => (
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
            {Number(part.retentionAmount) > 0 && <div>retención − <strong>{formatCurrency(part.retentionAmount)}</strong></div>}
            {showPaidLines && advanceGiven > 0 && <div>anticipo + <strong>{formatCurrency(advanceGiven)}</strong></div>}
            {showPaidLines && <div>pagado a la fecha − <strong>{formatCurrency(paidToDate)}</strong></div>}
            <div style={{ fontSize: 13 }}>saldo de este presupuesto (a liberar) <strong>{formatCurrency(part.totalToPay)}</strong></div>
          </div>
        </div>
      )}
    </div>
  );
}

// Detalle de una estimación del proveedor: partes por presupuesto, montos y autorización.
export function EstimationBatchView({
  batch,
  budgetsById,
  canReview,
  isReviewer,
  saving,
  authorizedAmount,
  setAuthorizedAmount,
  authorizationNote,
  setAuthorizationNote,
  onApprove,
  onReturn,
  onPrint,
  onClose,
}) {
  const parts = batch.parts || [];
  const calculated = Number(batch.totalToPay) || 0;
  const requested = batch.requestedAmount != null ? Number(batch.requestedAmount) : null;
  const baseline = requested ?? calculated;
  const amount = authorizedAmount === '' ? baseline : Number(authorizedAmount) || 0;
  const needsNote = Math.abs(amount - baseline) >= 0.01 || amount - calculated >= 0.01;

  return (
    <div className="card" style={{ display: 'grid', gap: 10, padding: 16 }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
        <div className="row" style={{ gap: 8, alignItems: 'center' }}>
          <strong>Estimación #{batch.folio} · {batch.supplierName}</strong>
          <StatusBadge estimation={batch} />
          <span className="small">{formatDate(batch.periodStart)} – {formatDate(batch.periodEnd)}</span>
        </div>
        <div className="row" style={{ gap: 6 }}>
          {batch.workflowStatus === 'APROBADA' && <button type="button" onClick={onPrint}>PDF autorizado</button>}
          <button type="button" className="secondary" onClick={onClose}>✕ Cerrar</button>
        </div>
      </div>

      {batch.returnReason && batch.workflowStatus === 'BORRADOR' && (
        <div className="small" style={{ color: '#92400e' }}>Devuelta por {batch.returnedBy || 'un admin'}: {batch.returnReason}</div>
      )}
      {batch.notes && <div className="small">Notas: {batch.notes}</div>}

      {parts.map((part) => {
        const budget = budgetsById[part.estimationBudgetId] || {};
        return (
          <div key={part.id} style={{ display: 'grid', gap: 8, borderTop: '1px solid var(--border, #e5e7eb)', paddingTop: 10 }}>
            <div className="row" style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: 6 }}>
              <strong>Presupuesto: {part.budgetName || budget.name}</strong>
              <span className="small">
                Subtotal {formatCurrency(part.periodSubtotal)} · retención −{formatCurrency(part.retentionAmount)}
                {Number(part.advanceAmortizationAmount) > 0 && <> · anticipo −{formatCurrency(part.advanceAmortizationAmount)}</>}
                {Number(part.priorPaidApplied) > 0 && <> · pagos previos −{formatCurrency(part.priorPaidApplied)}</>}
                {' '}= <strong>{formatCurrency(part.totalToPay)}</strong>
              </span>
            </div>
            <PartSheet part={part} budget={budget} />
          </div>
        );
      })}

      <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13, borderTop: '1px solid var(--border, #e5e7eb)', paddingTop: 10 }}>
        <div><strong>Subtotal:</strong> {formatCurrency(batch.periodSubtotal)}</div>
        <div><strong>Retención:</strong> {formatCurrency(batch.retentionAmount)}</div>
        <div><strong>Amortización anticipo:</strong> {formatCurrency(batch.advanceAmortizationAmount)}</div>
        {Number(batch.priorPaidApplied) > 0 && <div><strong>Pagos previos reconocidos:</strong> −{formatCurrency(batch.priorPaidApplied)}</div>}
        <div><strong>Total calculado (a liberar):</strong> {formatCurrency(batch.totalToPay)}</div>
      </div>

      {batch.workflowStatus === 'APROBADA' && (
        <div className="small" style={{ display: 'grid', gap: 2 }}>
          <div>
            <strong>Autorizado: {formatCurrency(batch.authorizedAmount)}</strong>
            {requested !== null && (
              <> · Solicitado por el contratista: {formatCurrency(requested)}
                {Math.abs(Number(batch.authorizedVsRequested) || 0) >= 0.01 && <> ({formatCurrency(batch.authorizedVsRequested)} vs. solicitado)</>}
              </>
            )}
            {Math.abs(Number(batch.authorizedDifference) || 0) >= 0.01 && (
              <> · {Number(batch.authorizedDifference) > 0 ? '+' : ''}{formatCurrency(batch.authorizedDifference)} vs. avance calculado</>
            )}
          </div>
          {batch.authorizationNote && <div>Motivo: {batch.authorizationNote}</div>}
          <div>Aprobada por {batch.approvedBy} · {formatDate(batch.approvedAt)}</div>
        </div>
      )}

      {isReviewer && batch.workflowStatus === 'ENVIADA' && !canReview && (
        <div className="small" style={{ background: '#fef3c7', color: '#92400e', borderRadius: 6, padding: 10 }}>
          Esta estimación espera autorización, pero esta obra no está asignada a tu usuario para autorizar. La aprueba otro admin.
        </div>
      )}

      {isReviewer && batch.workflowStatus === 'ENVIADA' && canReview && (
        <div style={{ display: 'grid', gap: 8, borderTop: '1px solid var(--border, #e5e7eb)', paddingTop: 10 }}>
          <strong>Autorización</strong>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div>
                <div className="kpi-label">Avance reportado (a liberar)</div>
                <div className="kpi-value">{formatCurrency(calculated)}</div>
                <div className="kpi-sub">{parts.length > 1 ? `suma de ${parts.length} presupuestos` : 'calculado con el avance capturado'}</div>
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
          <div className="row" style={{ gap: 8 }}>
            <button type="button" onClick={onApprove} disabled={saving}>{saving ? 'Procesando...' : 'Aprobar y pasar a pago'}</button>
            <button type="button" className="secondary" onClick={onReturn} disabled={saving}>Devolver a captura</button>
          </div>
        </div>
      )}
    </div>
  );
}
