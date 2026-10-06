import React, { useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { PasteTextImport } from './PasteTextImport.jsx';
import { formatCurrency, generateId } from './estimationShared.js';

// Conceptos que no estaban en el presupuesto original. Se agregan a un grupo
// propio ("Extras" o "Adicional N") con su avance y sin anticipo; no se toca
// nada de lo ya estimado.

function emptyExtraRow() {
  return { id: generateId(), description: '', unit: 'pza', quantity: '1', unitPrice: '' };
}

function isBlank(row) {
  return !String(row.description || '').trim() && !String(row.unitPrice || '').trim();
}

export function ExtrasPanel({ budget, onClose, onSaved }) {
  const [kind, setKind] = useState('extra');
  const [groupName, setGroupName] = useState('');
  const [note, setNote] = useState('');
  const [rows, setRows] = useState([emptyExtraRow()]);
  const [saving, setSaving] = useState(false);
  const [importing, setImporting] = useState(false);
  const [showPasteText, setShowPasteText] = useState(false);
  const [warnings, setWarnings] = useState([]);
  const [error, setError] = useState('');
  const fileInputRef = useRef(null);

  const existingGroups = useMemo(
    () => (budget?.groups || []).filter((g) => g.isExtra).map((g) => g.name),
    [budget],
  );
  const total = rows.reduce((sum, row) => sum + (Number(row.quantity) || 0) * (Number(row.unitPrice) || 0), 0);
  const suggestedGroup = kind === 'extra' ? 'Extras' : `Adicional ${(budget?.groups || []).filter((g) => /^adicional/i.test(g.name)).length + 1}`;

  function updateRow(id, patch) {
    setRows((prev) => prev.map((row) => (row.id === id ? { ...row, ...patch } : row)));
  }

  async function handleImport(event) {
    const file = event.target.files?.[0];
    if (event.target) event.target.value = '';
    if (!file) return;
    setImporting(true);
    setError('');
    setWarnings([]);
    try {
      applyImportResult(await api.importEstimationConceptos(file));
    } catch (e) {
      setError(e.message || 'No se pudo importar el archivo');
    } finally {
      setImporting(false);
    }
  }

  // Agrega a la tabla los conceptos extraídos de un archivo o de texto pegado.
  function applyImportResult(result) {
    const imported = (Array.isArray(result?.items) ? result.items : []).map((item) => ({
      id: generateId(),
      description: item.description || '',
      unit: item.unit || '',
      quantity: String(item.quantity ?? ''),
      unitPrice: String(item.unitPrice ?? ''),
    }));
    if (!imported.length) {
      setError('No se obtuvieron conceptos importables.');
      return;
    }
    setRows((prev) => (prev.length === 1 && isBlank(prev[0]) ? imported : [...prev, ...imported]));
    setWarnings(Array.isArray(result?.warnings) ? result.warnings : []);
  }

  async function save(event) {
    event.preventDefault();
    const conceptos = rows
      .filter((row) => String(row.description || '').trim())
      .map((row) => ({
        description: row.description,
        unit: row.unit,
        quantity: Number(row.quantity),
        unitPrice: Number(row.unitPrice),
      }));
    if (!conceptos.length) {
      setError('Agrega al menos un concepto con descripción y precio.');
      return;
    }
    setSaving(true);
    setError('');
    try {
      const saved = await api.addEstimationExtras(budget.id, {
        kind,
        groupName: groupName.trim(),
        note: note.trim(),
        conceptos,
      });
      onSaved?.(saved);
    } catch (e) {
      setError(e.message || 'No se pudieron agregar los conceptos');
    } finally {
      setSaving(false);
    }
  }

  return (
    <form className="card" style={{ display: 'grid', gap: 10, padding: 16 }} onSubmit={save}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <strong>Agregar extras · {budget?.supplierNameSnapshot} · {budget?.name || 'Presupuesto'}</strong>
          <div className="small">
            Se suman al presupuesto actual sin tocar lo ya estimado. Quedan en su propio grupo, con avance propio y sin anticipo.
          </div>
        </div>
        <button type="button" className="secondary" onClick={onClose} disabled={saving}>✕ Cerrar</button>
      </div>

      <div className="row" style={{ gap: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
          <input type="radio" name="extras-kind" checked={kind === 'extra'} onChange={() => setKind('extra')} />
          Concepto extra no contemplado
        </label>
        <label className="small" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
          <input type="radio" name="extras-kind" checked={kind === 'adicional'} onChange={() => setKind('adicional')} />
          Presupuesto adicional
        </label>
        <div>
          <label>Grupo</label>
          <input
            list="extras-group-options"
            value={groupName}
            onChange={(e) => setGroupName(e.target.value)}
            placeholder={suggestedGroup}
            style={{ width: 200 }}
          />
          <datalist id="extras-group-options">
            {existingGroups.map((name) => <option key={name} value={name} />)}
          </datalist>
        </div>
        <div style={{ flex: 1, minWidth: 200 }}>
          <label>Nota (opcional)</label>
          <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Ej. autorizado por el cliente el 05-oct" />
        </div>
      </div>

      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
        <label>Conceptos</label>
        <div className="row" style={{ gap: 6 }}>
          <input ref={fileInputRef} type="file" accept=".xlsx,.csv,.pdf,.docx" onChange={handleImport} style={{ display: 'none' }} />
          <button type="button" className="secondary" onClick={() => fileInputRef.current?.click()} disabled={importing}>
            {importing ? 'Importando...' : '⭱ Importar Excel/CSV/PDF/Word'}
          </button>
          <button type="button" className="secondary" onClick={() => setShowPasteText((prev) => !prev)}>📋 Pegar texto</button>
          <button type="button" className="secondary" onClick={() => setRows((prev) => [...prev, emptyExtraRow()])}>+ Agregar concepto</button>
        </div>
      </div>
      {showPasteText && (
        <PasteTextImport onResult={(result) => { setError(''); applyImportResult(result); }} onClose={() => setShowPasteText(false)} />
      )}
      {warnings.length > 0 && (
        <div className="small" style={{ background: 'var(--gray-100)', borderRadius: 6, padding: 8 }}>
          {warnings.map((warning, idx) => <div key={idx}>⚠ {warning}</div>)}
        </div>
      )}

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
            {rows.map((row) => (
              <tr key={row.id}>
                <td><input value={row.description} onChange={(e) => updateRow(row.id, { description: e.target.value })} /></td>
                <td><input value={row.unit} onChange={(e) => updateRow(row.id, { unit: e.target.value })} style={{ width: 80 }} /></td>
                <td>
                  <input type="number" min="0" step="0.01" value={row.quantity} onChange={(e) => updateRow(row.id, { quantity: e.target.value })} style={{ width: 90 }} />
                </td>
                <td>
                  <input type="number" min="0" step="0.01" value={row.unitPrice} onChange={(e) => updateRow(row.id, { unitPrice: e.target.value })} style={{ width: 110 }} />
                </td>
                <td>{formatCurrency((Number(row.quantity) || 0) * (Number(row.unitPrice) || 0))}</td>
                <td>
                  <button
                    type="button"
                    className="secondary"
                    onClick={() => setRows((prev) => (prev.length > 1 ? prev.filter((r) => r.id !== row.id) : prev))}
                    disabled={rows.length <= 1}
                  >
                    ✕
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="row" style={{ gap: 16, fontSize: 13, flexWrap: 'wrap' }}>
        <div><strong>Total a agregar:</strong> {formatCurrency(total)}</div>
        {budget && (
          <div>
            <strong>Presupuesto después:</strong> {formatCurrency((Number(budget.totalContractedAmount) || 0) + total)}
          </div>
        )}
      </div>
      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}
      <div className="row" style={{ gap: 8 }}>
        <button type="submit" disabled={saving}>{saving ? 'Guardando...' : 'Agregar al presupuesto'}</button>
        <button type="button" className="secondary" onClick={onClose} disabled={saving}>Cancelar</button>
      </div>
    </form>
  );
}
