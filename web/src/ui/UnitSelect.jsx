import React, { useState } from 'react';
import { COMMON_UNITS } from './estimationShared.js';

// Unidad de un concepto: desplegable con las más comunes y «Otra…» para escribir una distinta.
export function UnitSelect({ value, onChange, width = 120, required = false }) {
  const current = String(value || '');
  const known = COMMON_UNITS.some((unit) => unit.value === current);
  const [custom, setCustom] = useState(current !== '' && !known);

  if (custom) {
    return (
      <span style={{ display: 'inline-flex', gap: 4, alignItems: 'center' }}>
        <input
          autoFocus={!current}
          value={current}
          onChange={(e) => onChange(e.target.value)}
          placeholder="Unidad"
          style={{ width: Math.max(width - 34, 70) }}
          required={required}
        />
        <button type="button" className="secondary" title="Elegir de la lista" aria-label="Elegir unidad de la lista" onClick={() => { setCustom(false); onChange(''); }} style={{ padding: '4px 8px' }}>
          ▾
        </button>
      </span>
    );
  }
  return (
    <select
      value={current}
      onChange={(e) => {
        if (e.target.value === '__other__') {
          setCustom(true);
          onChange('');
        } else {
          onChange(e.target.value);
        }
      }}
      required={required}
      style={{ width }}
      aria-label="Unidad"
    >
      <option value="">Unidad…</option>
      {COMMON_UNITS.map((unit) => (
        <option key={unit.value} value={unit.value}>{unit.label}</option>
      ))}
      <option value="__other__">Otra…</option>
    </select>
  );
}
