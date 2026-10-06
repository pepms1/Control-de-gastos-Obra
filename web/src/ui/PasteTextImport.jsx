import React, { useState } from 'react';
import { api } from '../api.js';

// Panel para pegar el presupuesto tal como lo manda el proveedor (p. ej. un mensaje de WhatsApp).
// El servidor extrae los conceptos; `onResult` recibe { items, warnings } igual que al subir un archivo.
export function PasteTextImport({ onResult, onClose }) {
  const [text, setText] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  async function extract() {
    setLoading(true);
    setError('');
    try {
      const result = await api.importEstimationConceptosText(text);
      onResult(result);
      onClose();
    } catch (e) {
      setError(e.message || 'No se pudo leer el texto');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ display: 'grid', gap: 8, background: 'var(--gray-100)', borderRadius: 6, padding: 10 }}>
      <label>Pega aquí el presupuesto del proveedor</label>
      <textarea
        rows={9}
        value={text}
        onChange={(e) => setText(e.target.value)}
        placeholder={'Ejemplo:\nCOLOCACIÓN\nColocación de mármol 45 m2 x $350\nBoquillas y resanes 45 m2 x $40\nSALIDAS PARA MUEBLES\nSalida sanitaria 6 pza $450 c/u\nSellado: $2,500'}
        style={{ width: '100%', fontFamily: 'inherit' }}
      />
      <div className="small" style={{ color: 'var(--gray-600)' }}>
        Un concepto por renglón, con cantidad, unidad y precio. Los títulos en MAYÚSCULAS o terminados en «:» se toman como grupo.
        Un renglón solo con monto («Sellado: $2,500») entra como 1 lote. Después de extraer podrás revisar y corregir todo.
      </div>
      {error && <div className="small" style={{ color: '#b91c1c' }}>{error}</div>}
      <div className="row" style={{ gap: 8 }}>
        <button type="button" onClick={extract} disabled={loading || !text.trim()}>
          {loading ? 'Extrayendo...' : 'Extraer conceptos'}
        </button>
        <button type="button" className="secondary" onClick={onClose}>Cancelar</button>
      </div>
    </div>
  );
}
