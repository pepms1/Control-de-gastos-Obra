import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { matchesSearch, normalizeText, resolveCategory2, resolveSupplierIdentity, getTypeLabel } from './searchV2.helpers.js';

const moneyFormatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function formatMoney(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '0.00';
  return moneyFormatter.format(Math.abs(amount));
}

function formatCurrency(value) {
  return `$${formatMoney(value || 0)}`;
}

function getAmountWithoutTax(row) {
  const subtotal = Number(row?.subtotal ?? row?.montoSinIva);
  if (Number.isFinite(subtotal)) return subtotal;
  const total = Number(row?.totalFactura ?? row?.amount);
  const iva = Number(row?.iva ?? row?.montoIva);
  if (Number.isFinite(total) && Number.isFinite(iva)) return total - iva;
  return Number.isFinite(total) ? total : 0;
}

function getAmountWithTax(row) {
  const total = Number(row?.totalFactura ?? row?.amount);
  if (Number.isFinite(total)) return total;
  return getAmountWithoutTax(row) + (Number(row?.iva ?? row?.montoIva) || 0);
}

function buildPdfContent({ query, supplierLabel, categoryLabel, typeLabel, rows, totalWithoutTax, totalWithTax }) {
  const filters = [
    supplierLabel !== 'Todos' ? `Proveedor: ${supplierLabel}` : null,
    categoryLabel !== 'Todas' ? `Categoría 2: ${categoryLabel}` : null,
    typeLabel !== 'Todos' ? `Tipo: ${typeLabel}` : null,
  ].filter(Boolean).join(' · ') || 'Sin filtros activos';

  const rowsHtml = rows.map((row) => {
    const supplier = resolveSupplierIdentity(row);
    const category2 = resolveCategory2(row, {});
    return `<tr>
      <td>${row?.date || '—'}</td>
      <td>${supplier?.name || '—'}</td>
      <td>${row?.description || '—'}</td>
      <td>${category2?.name || '—'}</td>
      <td style="text-align:right">$${formatMoney(getAmountWithTax(row))}</td>
      <td>${getTypeLabel(row?.type)}</td>
      <td>${row?.sourceSbo || '—'}</td>
    </tr>`;
  }).join('');

  return `<!doctype html>
  <html><head><meta charset="utf-8" /><title>Buscar V2</title>
  <style>
    body{font-family:Arial,sans-serif;padding:24px;color:#0f172a} h1{margin:0 0 6px}
    .meta{color:#475569;font-size:12px;margin-bottom:16px} .totals{display:flex;gap:18px;margin:12px 0 18px}
    table{width:100%;border-collapse:collapse;font-size:12px} th,td{border:1px solid #e2e8f0;padding:6px;vertical-align:top}
    th{background:#f8fafc;text-align:left}
  </style></head><body>
    <h1>BUSCAR V2 · Reporte</h1>
    <div class="meta">Término: ${query || '—'}<br/>Filtros: ${filters}</div>
    <div class="totals">
      <div><strong>Total sin IVA:</strong> $${formatMoney(totalWithoutTax)}</div>
      <div><strong>Total con IVA:</strong> $${formatMoney(totalWithTax)}</div>
    </div>
    <table>
      <thead><tr><th>Fecha</th><th>Proveedor</th><th>Descripción</th><th>Categoría 2</th><th>Total</th><th>Tipo</th><th>SBO</th></tr></thead>
      <tbody>${rowsHtml || '<tr><td colspan="7">Sin resultados</td></tr>'}</tbody>
    </table>
  </body></html>`;
}

export function SearchTransactionsV2({ cats, vendors, selectedProjectId }) {
  const searchInputRef = useRef(null);
  const [query, setQuery] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [supplierFilter, setSupplierFilter] = useState('ALL');
  const [category2Filter, setCategory2Filter] = useState('ALL');
  const [typeFilter, setTypeFilter] = useState('ALL');
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [exporting, setExporting] = useState(false);
  const [category2Catalog, setCategory2Catalog] = useState([]);

  useEffect(() => {
    searchInputRef.current?.focus();
  }, []);

  const categoryMap = useMemo(() => {
    const map = new Map();
    cats.forEach((item) => {
      const key = String(item?.code || item?.id || '').trim();
      const name = String(item?.displayLabel || item?.name || '').trim();
      if (key && name) map.set(key, name);
    });
    category2Catalog.forEach((item) => {
      const key = String(item?.id || item?.code || '').trim();
      const name = String(item?.name || item?.displayLabel || '').trim();
      if (key && name) map.set(key, name);
    });
    return Object.fromEntries(map.entries());
  }, [cats, category2Catalog]);

  useEffect(() => {
    api.supplierCategories().then((data) => setCategory2Catalog(Array.isArray(data) ? data : [])).catch(() => setCategory2Catalog([]));
  }, []);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);
    setError('');
    api.transactions({
      q: query.trim(),
      type: typeFilter === 'ALL' ? '' : typeFilter,
      page: '1',
      limit: '500',
      projectId: String(selectedProjectId || ''),
    }).then((data) => {
      if (!isMounted) return;
      setRows(Array.isArray(data?.items) ? data.items : []);
    }).catch((err) => {
      if (!isMounted) return;
      setRows([]);
      setError(err?.message || 'No se pudo cargar la búsqueda V2');
    }).finally(() => {
      if (isMounted) setLoading(false);
    });

    return () => {
      isMounted = false;
    };
  }, [query, typeFilter, selectedProjectId]);

  const supplierOptions = useMemo(() => {
    const source = new Map();

    const normalizeVendorIdentity = (vendor) => {
      const vendorId = String(vendor?._id || vendor?.id || vendor?.vendorId || vendor?.supplierId || '').trim();
      const cardCode = String(vendor?.supplierCardCode || vendor?.cardCode || vendor?.externalIds?.sapCardCode || '').trim();
      const businessPartner = String(vendor?.businessPartner || vendor?.externalIds?.sapBusinessPartner || '').trim();
      const normalizedName = String(vendor?.name || vendor?.displayName || '').trim();
      const isSapSynthetic = vendorId.toLowerCase().startsWith('sap-sbo:');

      if (isSapSynthetic && businessPartner && cardCode) {
        return { key: `bpcc:${normalizeText(businessPartner)}|${normalizeText(cardCode)}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }
      if (isSapSynthetic && businessPartner) {
        return { key: `bp:${normalizeText(businessPartner)}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }
      if (isSapSynthetic && cardCode) {
        return { key: `card:${normalizeText(cardCode)}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }

      if (vendorId) {
        return { key: `id:${vendorId}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }
      if (businessPartner && cardCode) {
        return { key: `bpcc:${normalizeText(businessPartner)}|${normalizeText(cardCode)}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }
      if (businessPartner) {
        return { key: `bp:${normalizeText(businessPartner)}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }
      if (cardCode) {
        return { key: `card:${normalizeText(cardCode)}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }
      if (normalizedName) {
        return { key: `name:${normalizeText(normalizedName)}`, name: normalizedName, businessPartner, cardCode, vendorId };
      }
      return { key: '', name: '', businessPartner: '', cardCode: '', vendorId: '' };
    };

    const chooseLabel = ({ catalogName, businessPartner, supplierName, cardCode, fallbackKey }) => (
      String(catalogName || businessPartner || supplierName || cardCode || fallbackKey || 'Sin proveedor').trim()
    );

    vendors.forEach((vendor) => {
      const identity = normalizeVendorIdentity(vendor);
      if (!identity.key) return;
      source.set(identity.key, {
        value: identity.key,
        label: chooseLabel({
          catalogName: identity.name,
          businessPartner: identity.businessPartner,
          supplierName: '',
          cardCode: identity.cardCode,
          fallbackKey: identity.vendorId,
        }),
      });
    });

    rows.forEach((row) => {
      const supplier = resolveSupplierIdentity(row);
      if (!supplier?.key) return;
      const existing = source.get(supplier.key);
      source.set(supplier.key, {
        value: supplier.key,
        label: chooseLabel({
          catalogName: existing?.label,
          businessPartner: supplier.businessPartner,
          supplierName: supplier.name,
          cardCode: supplier.cardCode,
          fallbackKey: supplier.key,
        }),
      });
    });

    return Array.from(source.values()).sort((a, b) => {
      const byLabel = a.label.localeCompare(b.label, 'es', { sensitivity: 'base' });
      if (byLabel !== 0) return byLabel;
      return a.value.localeCompare(b.value, 'es', { sensitivity: 'base' });
    });
  }, [rows, vendors]);

  const category2Options = useMemo(() => {
    const source = new Map();
    rows.forEach((row) => {
      const cat = resolveCategory2(row, categoryMap);
      if (!cat?.id) return;
      source.set(cat.id, { value: cat.id, label: cat.name || cat.id });
    });
    return Array.from(source.values()).sort((a, b) => a.label.localeCompare(b.label, 'es'));
  }, [rows, categoryMap]);

  const visibleRows = useMemo(() => rows
    .filter((row) => matchesSearch(row, query, categoryMap))
    .filter((row) => {
      if (supplierFilter === 'ALL') return true;
      return resolveSupplierIdentity(row).key === supplierFilter;
    })
    .filter((row) => {
      if (category2Filter === 'ALL') return true;
      return resolveCategory2(row, categoryMap).id === category2Filter;
    })
    .filter((row) => {
      if (typeFilter === 'ALL') return true;
      return row?.type === typeFilter;
    }), [rows, query, categoryMap, supplierFilter, category2Filter, typeFilter]);

  const totalWithoutTax = useMemo(() => visibleRows.reduce((acc, row) => acc + getAmountWithoutTax(row), 0), [visibleRows]);
  const totalWithTax = useMemo(() => visibleRows.reduce((acc, row) => acc + getAmountWithTax(row), 0), [visibleRows]);

  function exportPdf() {
    setExporting(true);
    try {
      const supplierLabel = supplierFilter === 'ALL'
        ? 'Todos'
        : (supplierOptions.find((option) => option.value === supplierFilter)?.label || supplierFilter);
      const categoryLabel = category2Filter === 'ALL'
        ? 'Todas'
        : (category2Options.find((option) => option.value === category2Filter)?.label || category2Filter);
      const typeLabel = typeFilter === 'ALL' ? 'Todos' : getTypeLabel(typeFilter);

      const html = buildPdfContent({
        query: query.trim(),
        supplierLabel,
        categoryLabel,
        typeLabel,
        rows: visibleRows,
        totalWithoutTax,
        totalWithTax,
      });

      const win = window.open('', '_blank', 'noopener,noreferrer');
      if (!win) return;
      win.document.write(html);
      win.document.close();
      win.focus();
      win.print();
    } finally {
      setExporting(false);
    }
  }

  return (
    <div className="card">
      <h2 style={{ marginTop: 0 }}>BUSCAR V2</h2>
      <div className="search-toolbar" style={{ flexWrap: 'wrap', gap: 8 }}>
        <input
          ref={searchInputRef}
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Buscar por proveedor, SAP, categoría 2, descripción o referencias"
          style={{ minWidth: 320, flex: 1 }}
        />
        <button type="button" className="secondary" onClick={() => setShowFilters((value) => !value)}>
          {showFilters ? 'Ocultar filtros' : 'Mostrar filtros'}
        </button>
        <button type="button" onClick={exportPdf} disabled={!visibleRows.length || loading || exporting}>
          {exporting ? 'Exportando...' : 'Exportar PDF'}
        </button>
      </div>

      {showFilters && (
        <div className="search-toolbar" style={{ flexWrap: 'wrap', gap: 8, marginTop: 8 }}>
          <select value={supplierFilter} onChange={(event) => setSupplierFilter(event.target.value)}>
            <option value="ALL">Proveedor (todos)</option>
            {supplierOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
          <select value={category2Filter} onChange={(event) => setCategory2Filter(event.target.value)}>
            <option value="ALL">Categoría 2 (todas)</option>
            {category2Options.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
          <select value={typeFilter} onChange={(event) => setTypeFilter(event.target.value)}>
            <option value="ALL">Ingreso / Egreso (todos)</option>
            <option value="INCOME">Ingreso</option>
            <option value="EXPENSE">Egreso</option>
          </select>
        </div>
      )}

      <div className="small" style={{ marginTop: 8 }}>
        {loading ? 'Buscando...' : `${visibleRows.length} resultados visibles`}
      </div>
      {!!error && <div className="small" style={{ marginTop: 8, color: '#b91c1c' }}>{error}</div>}

      <div style={{ overflowX: 'auto', marginTop: 10 }}>
        <table>
          <thead>
            <tr>
              <th>Fecha</th>
              <th>Proveedor</th>
              <th>Descripción</th>
              <th>Categoría 2</th>
              <th>Total</th>
              <th>Tipo</th>
              <th>SBO</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row) => {
              const supplier = resolveSupplierIdentity(row);
              const category2 = resolveCategory2(row, categoryMap);
              return (
                <tr key={row.id}>
                  <td>{row.date || '—'}</td>
                  <td>{supplier?.name || '—'}</td>
                  <td>{row.description || '—'}</td>
                  <td>{category2?.name || '—'}</td>
                  <td>{formatCurrency(getAmountWithTax(row))}</td>
                  <td>{getTypeLabel(row?.type)}</td>
                  <td>{row.sourceSbo || '—'}</td>
                </tr>
              );
            })}
            {!visibleRows.length && !loading && <tr><td colSpan={7} className="small">Sin resultados</td></tr>}
          </tbody>
          <tfoot>
            <tr>
              <td colSpan={3} />
              <td style={{ textAlign: 'right', fontWeight: 700 }}>Totales visibles</td>
              <td style={{ fontWeight: 700 }}>{formatCurrency(totalWithTax)}</td>
              <td colSpan={2} className="small">Sin IVA: {formatCurrency(totalWithoutTax)}</td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}
