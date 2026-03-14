import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { matchesSearch, resolveCategory2, resolveSupplierIdentity, resolveVendorIdentity, getTypeLabel } from './searchV2.helpers.js';

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
  const subtotal = Number(row?.subtotal ?? row?.montoSinIva ?? row?.tax?.subtotal);
  return Number.isFinite(subtotal) ? subtotal : null;
}

function getAmountWithTax(row) {
  const total = Number(row?.totalFactura ?? row?.tax?.totalFactura);
  return Number.isFinite(total) ? total : null;
}

function formatCurrencyWithFallback(value) {
  return Number.isFinite(value) ? formatCurrency(value) : '—';
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
      <td style="text-align:right">${formatCurrencyWithFallback(getAmountWithoutTax(row))}</td>
      <td style="text-align:right">${formatCurrencyWithFallback(getAmountWithTax(row))}</td>
      <td>${getTypeLabel(row?.type)}</td>
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
      <thead><tr><th>Fecha</th><th>Proveedor</th><th>Descripción</th><th>Categoría 2</th><th>Sin IVA</th><th>Con IVA</th><th>Tipo</th></tr></thead>
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
  const [typeFilter, setTypeFilter] = useState('EXPENSE');
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [exporting, setExporting] = useState(false);
  const [category2Catalog, setCategory2Catalog] = useState([]);
  const [supplierDebugEnabled, setSupplierDebugEnabled] = useState(false);

  useEffect(() => {
    searchInputRef.current?.focus();
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const enabledByQuery = window.location?.search?.includes('debugSuppliers=1');
    const enabledByStorage = window.localStorage?.getItem('searchV2.debugSuppliers') === '1';
    setSupplierDebugEnabled(Boolean(enabledByQuery || enabledByStorage));
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
    const rawEntries = [];

    const chooseLabel = ({ catalogName, businessPartner, supplierName, cardCode, fallbackKey }) => (
      String(catalogName || businessPartner || supplierName || cardCode || fallbackKey || 'Sin proveedor').trim()
    );

    const getKeyRank = (key = '') => {
      if (key.startsWith('bpcc:')) return 5;
      if (key.startsWith('card:')) return 4;
      if (key.startsWith('bp:')) return 3;
      if (key.startsWith('id:')) return 2;
      if (key.startsWith('name:')) return 1;
      return 0;
    };

    const supplierPreferredKey = new Map();

    const registerPreferredKey = ({ supplierId, key }) => {
      if (!supplierId || !key) return;
      const current = supplierPreferredKey.get(supplierId);
      if (!current || getKeyRank(key) > getKeyRank(current)) {
        supplierPreferredKey.set(supplierId, key);
      }
    };

    vendors.forEach((vendor) => {
      const identity = resolveVendorIdentity(vendor);
      registerPreferredKey({ supplierId: identity?.supplierId, key: identity?.key });
    });

    rows.forEach((row) => {
      const supplier = resolveSupplierIdentity(row);
      registerPreferredKey({ supplierId: supplier?.supplierId, key: supplier?.key });
    });

    const getCanonicalKey = (identity) => {
      const supplierId = String(identity?.supplierId || '').trim();
      if (supplierId && supplierPreferredKey.has(supplierId)) return supplierPreferredKey.get(supplierId);
      return identity?.key;
    };

    vendors.forEach((vendor) => {
      const identity = resolveVendorIdentity(vendor);
      if (!identity.key) return;
      const canonicalKey = getCanonicalKey(identity);
      rawEntries.push({
        source: 'catalog',
        rawKey: identity.key,
        canonicalKey,
        supplierId: identity.supplierId,
        supplierName: identity.name,
        businessPartner: identity.businessPartner,
        cardCode: identity.cardCode,
      });
      source.set(canonicalKey, {
        value: canonicalKey,
        label: chooseLabel({
          catalogName: identity.name,
          businessPartner: identity.businessPartner,
          supplierName: '',
          cardCode: identity.cardCode,
          fallbackKey: identity.supplierId || canonicalKey,
        }),
        debug: {
          source: 'catalog',
          supplierId: identity.supplierId,
          supplierName: identity.name,
          businessPartner: identity.businessPartner,
          cardCode: identity.cardCode,
          rawKey: identity.key,
        },
      });
    });

    rows.forEach((row) => {
      const supplier = resolveSupplierIdentity(row);
      if (!supplier?.key) return;
      const canonicalKey = getCanonicalKey(supplier);
      rawEntries.push({
        source: 'rows',
        rawKey: supplier.key,
        canonicalKey,
        supplierId: supplier.supplierId,
        supplierName: supplier.name,
        businessPartner: supplier.businessPartner,
        cardCode: supplier.cardCode,
      });
      const existing = source.get(canonicalKey);
      source.set(canonicalKey, {
        value: canonicalKey,
        label: chooseLabel({
          catalogName: existing?.label,
          businessPartner: supplier.businessPartner,
          supplierName: supplier.name,
          cardCode: supplier.cardCode,
          fallbackKey: canonicalKey,
        }),
        debug: {
          source: existing ? 'merged' : 'rows',
          supplierId: supplier.supplierId,
          supplierName: supplier.name,
          businessPartner: supplier.businessPartner,
          cardCode: supplier.cardCode,
          rawKey: supplier.key,
        },
      });
    });

    const sortedOptions = Array.from(source.values()).sort((a, b) => {
      const byLabel = a.label.localeCompare(b.label, 'es', { sensitivity: 'base' });
      if (byLabel !== 0) return byLabel;
      return a.value.localeCompare(b.value, 'es', { sensitivity: 'base' });
    });

    if (typeof window !== 'undefined') {
      window.__searchV2SupplierOptions = sortedOptions.map((option) => ({
        label: option.label,
        value: option.value,
        source: option?.debug?.source || '',
        supplierId: option?.debug?.supplierId || '',
        supplierName: option?.debug?.supplierName || '',
        businessPartner: option?.debug?.businessPartner || '',
        cardCode: option?.debug?.cardCode || '',
        rawKey: option?.debug?.rawKey || '',
      }));
      window.__searchV2SupplierRawEntries = rawEntries;
      window.__searchV2SupplierPreferredKeyBySupplierId = Object.fromEntries(supplierPreferredKey.entries());

      if (supplierDebugEnabled) {
        // eslint-disable-next-line no-console
        console.table(window.__searchV2SupplierOptions);
      }
    }

    return sortedOptions;
  }, [rows, vendors, supplierDebugEnabled]);

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

  const totalWithoutTax = useMemo(
    () => visibleRows.reduce((acc, row) => acc + (getAmountWithoutTax(row) || 0), 0),
    [visibleRows],
  );
  const totalWithTax = useMemo(
    () => visibleRows.reduce((acc, row) => acc + (getAmountWithTax(row) || 0), 0),
    [visibleRows],
  );

  function exportPdf() {
    setExporting(true);
    setError('');
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

      const win = window.open('about:blank', '_blank');
      if (!win) {
        const message = 'No se pudo abrir la ventana de impresión. Habilita pop-ups e inténtalo nuevamente.';
        setError(message);
        console.error(message);
        return;
      }

      win.document.open('text/html', 'replace');
      win.document.write(html);
      win.document.close();

      const triggerPrint = () => {
        win.focus();
        win.print();
      };

      if (win.document.readyState === 'complete') {
        setTimeout(triggerPrint, 50);
      } else {
        win.addEventListener('load', () => setTimeout(triggerPrint, 50), { once: true });
      }
    } catch (err) {
      const message = 'Ocurrió un error al preparar el PDF para impresión.';
      setError(message);
      console.error(message, err);
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
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: 4 }}>
          <button type="button" onClick={exportPdf} disabled={!visibleRows.length || loading || exporting}>
            {exporting ? 'Exportando...' : 'Exportar PDF'}
          </button>
          <div className="small">Total sin IVA filtrado: {formatCurrency(totalWithoutTax)}</div>
        </div>
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
              <th>Sin IVA</th>
              <th>Con IVA</th>
              <th>Tipo</th>
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
                  <td style={{ whiteSpace: 'nowrap' }}>{formatCurrencyWithFallback(getAmountWithoutTax(row))}</td>
                  <td style={{ whiteSpace: 'nowrap' }}>{formatCurrencyWithFallback(getAmountWithTax(row))}</td>
                  <td>{getTypeLabel(row?.type)}</td>
                </tr>
              );
            })}
            {!visibleRows.length && !loading && <tr><td colSpan={7} className="small">Sin resultados</td></tr>}
          </tbody>
          <tfoot>
            <tr>
              <td colSpan={3} />
              <td style={{ textAlign: 'right', fontWeight: 700 }}>Totales visibles</td>
              <td style={{ fontWeight: 700 }}>{formatCurrency(totalWithoutTax)}</td>
              <td style={{ fontWeight: 700 }}>{formatCurrency(totalWithTax)}</td>
              <td />
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}
