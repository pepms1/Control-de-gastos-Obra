import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';
import { formatCurrency, formatDate } from './estimationShared.js';

// Pagos del proveedor: por defecto TODOS cuentan como pagados a sus presupuestos; aquí se
// desasignan los que no corresponden (p. ej. un pago de otra obra o de otro concepto).
export function SupplierPaymentsPanel({ supplierKey, supplierName, projectId, onSaved, onClose }) {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [items, setItems] = useState([]);
  const [excluded, setExcluded] = useState(new Set());

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    api.supplierPayments(supplierKey, projectId)
      .then((data) => {
        if (!active) return;
        const rows = Array.isArray(data?.items) ? data.items : [];
        setItems(rows);
        setExcluded(new Set(rows.filter((row) => row.isExcluded).map((row) => row.id)));
      })
      .catch((e) => active && setError(e.message || 'No se pudieron cargar los pagos del proveedor'))
      .finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [supplierKey, projectId]);

  const totals = useMemo(() => {
    const included = items.filter((row) => !excluded.has(row.id)).reduce((sum, row) => sum + (Number(row.amountWithTax) || 0), 0);
    const out = items.filter((row) => excluded.has(row.id)).reduce((sum, row) => sum + (Number(row.amountWithTax) || 0), 0);
    return { included, out };
  }, [items, excluded]);

  function toggle(id) {
    setExcluded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  async function save() {
    setSaving(true);
    setError('');
    try {
      await api.saveSupplierPayments({ projectId, supplierKey, excludedTransactionIds: Array.from(excluded) });
      if (onSaved) await onSaved();
    } catch (e) {
      setError(e.message || 'No se pudo guardar');
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="grid budgets-assignment-panel" style={{ gap: 10, borderRadius: 10, padding: 12 }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <strong>Pagos de {supplierName}</strong>
        <button type="button" className="secondary" onClick={onClose} disabled={saving}>Cerrar</button>
      </div>
      <div className="small">
        Todos los pagos al proveedor cuentan como <strong>pagados a la fecha</strong> de sus presupuestos. Quita la marca de los que no correspondan
        (se desasignan y dejan de contar) y vuelve a marcarlos si cambias de opinión.
      </div>
      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}
      {loading ? (
        <div className="small">Cargando pagos...</div>
      ) : (
        <>
          <div style={{ overflowX: 'auto', maxHeight: 300, overflowY: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>Cuenta como pagado</th>
                  <th>Fecha</th>
                  <th>Descripción</th>
                  <th>Monto</th>
                </tr>
              </thead>
              <tbody>
                {items.map((row) => (
                  <tr key={row.id} style={excluded.has(row.id) ? { opacity: 0.55 } : undefined}>
                    <td>
                      <input type="checkbox" checked={!excluded.has(row.id)} onChange={() => toggle(row.id)} aria-label={`Incluir pago ${row.description || row.id}`} />
                    </td>
                    <td>{formatDate(row.date)}</td>
                    <td>{row.description || '—'}</td>
                    <td>{formatCurrency(row.amountWithTax)}</td>
                  </tr>
                ))}
                {!items.length && (
                  <tr><td colSpan={4} className="small" style={{ textAlign: 'center' }}>Este proveedor no tiene pagos registrados.</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <div className="row" style={{ gap: 16, flexWrap: 'wrap', fontSize: 13 }}>
            <div><strong>Pagado a la fecha:</strong> {formatCurrency(totals.included)}</div>
            <div><strong>Desasignado:</strong> {formatCurrency(totals.out)}</div>
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
