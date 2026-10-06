import React, { useEffect, useMemo, useState } from 'react';
import { formatCurrency, formatPct, groupLabel } from './estimationShared.js';
import {
  buildCaptureForm,
  buildCapturePayload,
  computeEstimationPreview,
  computePeriodQuantity,
  hasNamedGroups,
  pctOf,
} from './estimationCapture.js';

// Captura del avance de UN presupuesto dentro de la estimación del proveedor.
// Avisa al padre (onChange) con lo que se enviaría al servidor y la vista previa de montos.
export function CaptureBlock({ budget, previousCumulative, savedPart = null, isReviewer = false, poolApplied = 0, onOpenOpening, onChange, onRemove }) {
  const [form, setForm] = useState(() => buildCaptureForm(budget, previousCumulative, savedPart));
  // Anticipo que se entrega con esta estimación (sin tener que ir al presupuesto).
  const [advance, setAdvance] = useState(savedPart?.advanceGivenAmount ? String(savedPart.advanceGivenAmount) : '');
  const [advancePct, setAdvancePct] = useState('');
  const advanceAmount = Math.max(Number(advance) || 0, 0);
  const groupsEnabled = hasNamedGroups(budget);

  const preview = useMemo(() => {
    const remainingAdvance = savedPart
      ? (Number(budget.remainingAdvanceBalance) || 0) + (Number(savedPart.advanceAmortizationAmount) || 0)
      : undefined;
    const remainingOpening = savedPart
      ? (Number(budget.remainingOpeningPaidBalance) || 0) + ((Number(savedPart.priorPaidApplied) || 0) - (Number(savedPart.priorPoolApplied) || 0))
      : undefined;
    const groupPcts = Object.fromEntries((form.groups || []).map((group) => [group.name, group.pctExact]));
    return computeEstimationPreview(budget, form.lineItems, remainingAdvance, form.captureMode, form.globalProgressPct, remainingOpening, groupPcts);
  }, [form, budget, savedPart]);

  useEffect(() => {
    const hasProgress = preview.periodSubtotal > 0.0001;
    let payload = buildCapturePayload(budget.id, form);
    if (advanceAmount > 0) {
      payload = hasProgress
        ? { ...payload, advanceAmount }
        : { estimationBudgetId: budget.id, advanceAmount, noProgress: true };
    }
    onChange({
      budgetId: budget.id,
      payload,
      preview: { ...preview, advanceGiven: advanceAmount, totalToPay: preview.totalToPay + advanceAmount },
      hasProgress,
      hasValue: hasProgress || advanceAmount > 0,
    });
  }, [form, preview, advanceAmount]);

  function setAdvanceFromPct(value) {
    setAdvancePct(value);
    const pct = Number(value);
    if (value === '' || !Number.isFinite(pct)) return;
    setAdvance((((Number(budget.totalContractedAmount) || 0) * pct) / 100).toFixed(2));
  }

  function updateLine(conceptoId, field, value) {
    setForm((prev) => ({
      ...prev,
      lineItems: prev.lineItems.map((li) => (li.conceptoId === conceptoId ? { ...li, [field]: value } : li)),
    }));
  }

  // El avance por grupo se captura solo en %; el monto es una consecuencia que se muestra.
  function updateGroup(name, value) {
    setForm((prev) => ({
      ...prev,
      groups: (prev.groups || []).map((group) => {
        if (group.name !== name) return group;
        const base = Number(group.budgetAmount) || 0;
        const pct = Number(value);
        const exact = Number.isFinite(pct) ? pct : 0;
        return { ...group, pct: value, pctExact: exact, amount: value === '' ? '' : ((base * exact) / 100).toFixed(2), source: 'pct', touched: true };
      }),
    }));
  }

  const groupPcts = Object.fromEntries((form.groups || []).map((group) => [group.name, group.pctExact]));

  return (
    <div className="card" style={{ padding: 12, display: 'grid', gap: 10 }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
        <strong>{budget.name || budget.supplierNameSnapshot}</strong>
        <span className="small" style={{ color: 'var(--gray-600)' }}>
          Contratado {formatCurrency(budget.totalContractedAmount)}{Number(budget.discountPct) > 0 ? ` (con ${formatPct(budget.discountPct)} de descuento)` : ''} · avance a la fecha {formatPct(budget.cumulativeProgressPct ?? budget.approvedProgressPct ?? 0)}
        </span>
        {onRemove && <button type="button" className="secondary" onClick={onRemove}>Quitar de esta estimación</button>}
      </div>

      {Number(budget.recognizedPaidAmount) > 0 && !savedPart && (
        <div className="small" style={{ background: 'var(--gray-100)', borderRadius: 6, padding: 8, display: 'grid', gap: 4 }}>
          <div>
            Pagado a la fecha al contratista en este presupuesto: <strong>{formatCurrency(budget.recognizedPaidAmount)}</strong>
            {' '}= {formatPct(budget.recognizedPaidPct)} del presupuesto. Lo ya pagado se descuenta de lo que se libera.
          </div>
          <div>
            <button
              type="button"
              className="secondary"
              onClick={() => setForm((prev) => ({ ...prev, captureMode: 'global', globalProgressPct: String(Math.min(100, Number(budget.recognizedPaidPct) || 0)) }))}
            >
              Usar el % pagado como avance global
            </button>
          </div>
        </div>
      )}

      <div style={{ background: 'var(--gray-100)', borderRadius: 6, padding: 8, display: 'grid', gap: 6 }}>
        <div className="row" style={{ gap: 10, flexWrap: 'wrap', alignItems: 'flex-end' }}>
          <div>
            <label>Anticipo a entregar con esta estimación ($)</label>
            <input type="number" min="0" step="0.01" value={advance} onChange={(e) => { setAdvance(e.target.value); setAdvancePct(''); }} placeholder="0.00" style={{ width: 180 }} />
          </div>
          <div>
            <label>o % del presupuesto</label>
            <input type="number" min="0" max="100" step="0.01" value={advancePct} onChange={(e) => setAdvanceFromPct(e.target.value)} style={{ width: 110 }} />
          </div>
          {isReviewer && Number(budget.estimationsCount) === 0 && onOpenOpening && (
            <button type="button" className="secondary" onClick={() => onOpenOpening(budget.id)}>
              Registrar anticipo / pagos ya entregados…
            </button>
          )}
        </div>
        <div className="small" style={{ color: 'var(--gray-600)' }}>
          {budget.supplierUsesPriorPool
            ? (Number(budget.advanceDeliveredAmount) > 0
              ? <>Anticipo entregado: <strong>{formatCurrency(budget.advanceDeliveredAmount)}</strong> · por amortizar {formatCurrency(budget.remainingAdvanceBalance)}. </>
              : <>Sin anticipo entregado{Number(budget.advanceAmount) > 0 ? ` (previsto ${formatCurrency(budget.advanceAmount)})` : ''}: no se amortiza nada hasta entregarlo. </>)
            : (Number(budget.advanceAmount) > 0 && budget.advanceAmortizationEnabled
              ? <>Anticipo registrado: <strong>{formatCurrency(budget.advanceAmount)}</strong> · por amortizar {formatCurrency(budget.remainingAdvanceBalance)}. </>
              : <>Este presupuesto aún no tiene anticipo. </>)}
          Si esta estimación entrega anticipo, se autoriza y se paga como parte de la estimación y desde la aprobación se amortiza en las siguientes (sin retención).
          Puedes entregar solo anticipo, sin avance.
        </div>
      </div>

      <div className="row" style={{ gap: 14, flexWrap: 'wrap', alignItems: 'center' }}>
        <strong style={{ fontSize: 13 }}>¿Cómo capturas el avance?</strong>
        {[
          ...(groupsEnabled ? [['group', 'Avance por grupo']] : []),
          ['global', 'Avance global (%)'],
          ['concept', 'Avance por concepto (%)'],
          ['quantity', 'Por unidad (m², pzas…)'],
        ].map(([value, label]) => (
          <label key={value} className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
            <input
              type="radio"
              name={`capture-mode-${budget.id}`}
              checked={form.captureMode === value}
              onChange={() => setForm((prev) => ({ ...prev, captureMode: value }))}
            />
            {label}
          </label>
        ))}
      </div>

      {form.captureMode === 'global' && (
        <div>
          <label>Avance acumulado del presupuesto (%)</label>
          <input
            type="number"
            min="0"
            max="100"
            step="0.01"
            value={form.globalProgressPct}
            onChange={(e) => setForm((prev) => ({ ...prev, globalProgressPct: e.target.value }))}
            style={{ width: 140 }}
          />
          <div className="small">
            Se aplica a todos los conceptos. Un concepto que ya va más adelante no baja. Es el avance total a la fecha, no solo el de esta semana.
          </div>
        </div>
      )}
      {form.captureMode === 'quantity' && (
        <div className="small">
          Escribe las unidades que avanzaron <strong>en este periodo</strong> (por ejemplo, los m² de mármol colocados). Se multiplican por el precio unitario y se
          muestra el avance acumulado en % de cada concepto.
        </div>
      )}
      {form.captureMode === 'concept' && (
        <div className="small">
          Escribe el avance acumulado (%) de cada concepto que avanzó. Los que no cambies no avanzan en esta estimación.
        </div>
      )}

      {form.captureMode === 'group' && (() => {
        const rows = (form.groups || []).map((group) => {
          const target = (group.budgetAmount * group.pctExact) / 100;
          const period = Math.max(target - group.previousAmount, 0);
          return { group, period, rawAmortization: (period * group.advancePct) / 100 };
        });
        const rawTotal = rows.reduce((sum, row) => sum + row.rawAmortization, 0);
        const cappedTotal = preview?.advanceAmortizationAmount ?? rawTotal;
        const scale = rawTotal > 0 ? Math.min(cappedTotal / rawTotal, 1) : 0;
        const totals = rows.reduce(
          (acc, row) => ({
            budget: acc.budget + row.group.budgetAmount,
            previous: acc.previous + row.group.previousAmount,
            period: acc.period + row.period,
            amortization: acc.amortization + row.rawAmortization * scale,
          }),
          { budget: 0, previous: 0, period: 0, amortization: 0 },
        );
        return (
          <>
            <div className="small">
              Escribe el avance <strong>acumulado</strong> de cada grupo en %. Se aplica a todos los conceptos del grupo; los grupos que no muevas no avanzan en esta
              estimación. Si avanzaron unidades (m², piezas…), usa «Por unidad».
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table>
                <thead>
                  <tr>
                    <th>Grupo</th>
                    <th>Presupuesto</th>
                    <th>Anticipo</th>
                    <th>Avance previo</th>
                    <th>Avance acumulado %</th>
                    <th>Avance acumulado $</th>
                    <th>Este periodo</th>
                    <th>Amortización</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(({ group, period, rawAmortization }) => {
                    const below = group.touched && group.pctExact < group.previousPct - 0.005;
                    const over = group.pctExact > 100.0001;
                    return (
                      <tr key={group.name || '__general__'}>
                        <td>
                          {groupLabel(group.name)}
                          {group.isExtra && <span className="small" style={{ marginLeft: 6, color: '#92400e' }}>extra</span>}
                        </td>
                        <td>{formatCurrency(group.budgetAmount)}</td>
                        <td>{group.advancePct > 0 ? formatPct(group.advancePct) : '—'}</td>
                        <td>{formatCurrency(group.previousAmount)} ({formatPct(group.previousPct)})</td>
                        <td>
                          <input
                            type="number"
                            min="0"
                            max="100"
                            step="0.01"
                            value={group.pct}
                            onChange={(e) => updateGroup(group.name, e.target.value)}
                            style={{ width: 90, borderColor: below || over ? '#b91c1c' : undefined }}
                          />
                        </td>
                        <td>{formatCurrency(group.amount === '' ? 0 : group.amount)}</td>
                        <td>{formatCurrency(period)}</td>
                        <td>{formatCurrency(rawAmortization * scale)}</td>
                      </tr>
                    );
                  })}
                  <tr style={{ fontWeight: 600 }}>
                    <td>Total</td>
                    <td>{formatCurrency(totals.budget)}</td>
                    <td></td>
                    <td>{formatCurrency(totals.previous)}</td>
                    <td></td>
                    <td></td>
                    <td>{formatCurrency(totals.period)}</td>
                    <td>{formatCurrency(totals.amortization)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </>
        );
      })()}

      {form.captureMode !== 'group' && (
        <div style={{ overflowX: 'auto' }}>
          <table>
            <thead>
              <tr>
                <th>Concepto</th>
                <th>Unidad</th>
                <th>Cant. contratada</th>
                <th>Avance previo</th>
                <th>{form.captureMode === 'quantity' ? 'Cantidad de este periodo' : 'Avance este periodo'}</th>
                <th>Avance acumulado</th>
                <th>Importe periodo</th>
              </tr>
            </thead>
            <tbody>
              {form.lineItems.map((li, liIndex) => {
                const periodQuantity = computePeriodQuantity(form.captureMode, li, form.globalProgressPct, groupPcts);
                const showGroupHeader = groupsEnabled && (liIndex === 0 || (form.lineItems[liIndex - 1].group || '') !== (li.group || ''));
                const cumulativeQuantity = li.previousCumulativeQuantity + periodQuantity;
                const cumulativePct = pctOf(cumulativeQuantity, li.contractedQuantity);
                const periodAmount = periodQuantity * (Number(li.unitPrice) || 0);
                const overContracted = cumulativeQuantity > li.contractedQuantity + 0.0001;
                const belowPrevious = form.captureMode === 'concept' && (Number(li.progressPct) || 0) < (Number(li.previousProgressPct) || 0) - 0.005;
                return (
                  <React.Fragment key={li.conceptoId}>
                    {showGroupHeader && (
                      <tr>
                        <td colSpan={7} style={{ fontWeight: 600, background: 'var(--gray-100)' }}>{groupLabel(li.group)}</td>
                      </tr>
                    )}
                    <tr>
                      <td>{li.description}</td>
                      <td>{li.unit || '—'}</td>
                      <td>{li.contractedQuantity}</td>
                      <td>{li.previousCumulativeQuantity} ({formatPct(li.previousProgressPct)})</td>
                      <td>
                        {form.captureMode === 'quantity' && (
                          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                            <input
                              type="number"
                              min="0"
                              step="0.01"
                              value={li.periodQuantity}
                              onChange={(e) => updateLine(li.conceptoId, 'periodQuantity', e.target.value)}
                              style={{ width: 100 }}
                            />
                            {li.unit}
                          </span>
                        )}
                        {form.captureMode === 'concept' && (
                          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                            <input
                              type="number"
                              min="0"
                              max="100"
                              step="0.01"
                              value={li.progressPct}
                              onChange={(e) => updateLine(li.conceptoId, 'progressPct', e.target.value)}
                              style={{ width: 90, borderColor: belowPrevious ? '#b91c1c' : undefined }}
                            />
                            %
                          </span>
                        )}
                        {form.captureMode === 'global' && <span>{Math.round(periodQuantity * 10000) / 10000}</span>}
                        {form.captureMode !== 'quantity' && (
                          <div className="small">{Math.round(periodQuantity * 10000) / 10000} {li.unit}</div>
                        )}
                      </td>
                      <td style={{ color: overContracted || belowPrevious ? 'var(--danger-text, #b91c1c)' : undefined }}>
                        {Math.round(cumulativeQuantity * 10000) / 10000} ({formatPct(cumulativePct)})
                      </td>
                      <td>{formatCurrency(periodAmount)}</td>
                    </tr>
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div className="small" style={{ textAlign: 'right' }}>
        Subtotal {formatCurrency(preview.periodSubtotal)} · retención −{formatCurrency(preview.retentionAmount)}
        {preview.advanceAmortizationAmount > 0 && <> · anticipo −{formatCurrency(preview.advanceAmortizationAmount)}</>}
        {preview.priorPaidApplied > 0 && <> · pagos previos −{formatCurrency(preview.priorPaidApplied)}</>}
        {poolApplied > 0 && <> · pagos al proveedor −{formatCurrency(poolApplied)}</>}
        {advanceAmount > 0 && <> · anticipo a entregar +{formatCurrency(advanceAmount)}</>}
        {' '}= <strong>{formatCurrency(preview.totalToPay + advanceAmount - poolApplied)}</strong>
      </div>
    </div>
  );
}
