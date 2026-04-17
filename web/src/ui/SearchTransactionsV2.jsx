import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import {
  matchesSearch,
  resolveCategory2,
  resolveProjectDisplayName,
  resolveSupplierIdentity,
  resolveVendorIdentity,
  getTypeLabel,
} from './searchV2.helpers.js';

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

function getTaxAmount(row) {
  const iva = Number(row?.iva ?? row?.montoIva ?? row?.tax?.iva);
  return Number.isFinite(iva) ? iva : null;
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

  const providerName = String(supplierLabel || 'Sin proveedor').trim() || 'Sin proveedor';
  const reportTitle = `(${providerName.toUpperCase()}) - Reporte de egresos`;

  const rowsHtml = rows.map((row) => {
    const supplier = resolveSupplierIdentity(row);
    const category2 = resolveCategory2(row, {});
    const projectDisplayName = resolveProjectDisplayName(row);
    const sourceSbo = String(row?.sourceSbo || '').trim() || '—';
    return `<tr>
      <td>${row?.date || '—'}</td>
      <td class="wrap project-cell">${projectDisplayName}</td>
      <td>${sourceSbo}</td>
      <td>${supplier?.name || '—'}</td>
      <td class="wrap description-cell">${row?.description || '—'}</td>
      <td>${category2?.name || '—'}</td>
      <td class="amount">${formatCurrencyWithFallback(getAmountWithoutTax(row))}</td>
      <td class="amount">${formatCurrencyWithFallback(getTaxAmount(row))}</td>
      <td class="amount">${formatCurrencyWithFallback(getAmountWithTax(row))}</td>
    </tr>`;
  }).join('');

  return `<!doctype html>
  <html lang="es">
    <head>
      <meta charset="utf-8" />
      <title>Buscar V2</title>
      <style>
        @page { size: A4 landscape; margin: 12mm; }
        :root {
          --bg: #f1f5f9;
          --paper: #ffffff;
          --primary: #1d4ed8;
          --primary-dark: #1e3a8a;
          --text: #0f172a;
          --muted: #475569;
          --line: #dbe3ef;
          --chip: #dbeafe;
          --chip-text: #1e3a8a;
        }
        * { box-sizing: border-box; }
        body {
          margin: 0;
          padding: 14px;
          color: var(--text);
          background: var(--bg);
          font-family: 'Segoe UI', Roboto, Arial, sans-serif;
        }
        .sheet {
          border: 1px solid var(--line);
          border-radius: 16px;
          overflow: hidden;
          background: var(--paper);
          box-shadow: 0 14px 36px rgba(15, 23, 42, 0.08);
        }
        .header {
          background: linear-gradient(135deg, var(--primary-dark), var(--primary));
          color: #fff;
          padding: 22px 24px;
        }
        .header h1 {
          margin: 0;
          font-size: 23px;
          letter-spacing: .01em;
        }
        .header p {
          margin: 8px 0 0;
          font-size: 12px;
          opacity: .95;
        }
        .details {
          padding: 16px 24px 8px;
          border-bottom: 1px solid var(--line);
          background: #f8fafc;
        }
        .details-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 8px;
          margin-top: 8px;
          font-size: 12px;
          color: var(--muted);
        }
        .chips {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          margin-top: 10px;
        }
        .chip {
          border-radius: 999px;
          padding: 5px 11px;
          background: var(--chip);
          color: var(--chip-text);
          font-size: 11px;
          font-weight: 600;
        }
        .summary {
          padding: 14px 24px;
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 10px;
          border-bottom: 1px solid var(--line);
        }
        .summary-card {
          border: 1px solid var(--line);
          border-radius: 12px;
          padding: 10px 12px;
          background: #fff;
        }
        .summary-card .label {
          text-transform: uppercase;
          letter-spacing: .05em;
          color: #64748b;
          font-size: 10px;
          margin-bottom: 6px;
        }
        .summary-card .value {
          font-size: 18px;
          font-weight: 700;
          color: var(--primary-dark);
        }
        .table-wrap {
          padding: 16px 24px 24px;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          font-size: 11px;
        }
        th, td {
          border-bottom: 1px solid var(--line);
          padding: 8px 7px;
          vertical-align: top;
          text-align: left;
        }
        .wrap {
          white-space: normal;
          word-break: break-word;
        }
        .project-cell {
          min-width: 140px;
          max-width: 220px;
        }
        .description-cell {
          min-width: 180px;
          max-width: 300px;
        }
        thead th {
          background: #eff6ff;
          color: var(--primary-dark);
          font-weight: 700;
        }
        tbody tr:nth-child(even) {
          background: #f8fafc;
        }
        .amount {
          text-align: right;
          font-variant-numeric: tabular-nums;
          font-weight: 700;
          color: #1e293b;
          white-space: nowrap;
        }
      </style>
    </head>
    <body>
      <section class="sheet">
        <header class="header">
          <h1>BUSCAR V2 · Reporte</h1>
          <p>Generado: ${new Date().toLocaleString('es-MX')}</p>
        </header>

        <section class="details">
          <strong style="font-size:13px">Contexto de búsqueda</strong>
          <div class="details-grid">
            <div><strong>Término:</strong> ${query || '—'}</div>
            <div><strong>Filtros:</strong> ${filters}</div>
            <div><strong>Movimientos exportados:</strong> ${rows.length}</div>
          </div>
          <div class="chips">
            <span class="chip">Proveedor: ${supplierLabel}</span>
            <span class="chip">Categoría 2: ${categoryLabel}</span>
            <span class="chip">Tipo: ${typeLabel}</span>
          </div>
        </section>

        <section class="summary">
          <div class="summary-card"><div class="label">Total sin IVA</div><div class="value">$${formatMoney(totalWithoutTax)}</div></div>
          <div class="summary-card"><div class="label">Total con IVA</div><div class="value">$${formatMoney(totalWithTax)}</div></div>
          <div class="summary-card"><div class="label">Registros exportados</div><div class="value">${rows.length}</div></div>
        </section>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Fecha</th>
                <th>Proyecto</th>
                <th>SBO</th>
                <th>Proveedor</th>
                <th>Descripción</th>
                <th>Categoría</th>
                <th style="text-align:right">Subtotal</th>
                <th style="text-align:right">IVA</th>
                <th style="text-align:right">Total</th>
              </tr>
            </thead>
            <tbody>${rowsHtml || '<tr><td colspan="9">Sin resultados</td></tr>'}</tbody>
          </table>
        </div>
      </section>
    </body>
  </html>`;
}

export function SearchTransactionsV2({
  cats,
  vendors,
  selectedProjectId,
  title = 'BUSCAR V2',
  forceGlobalProjectScope = false,
  lockTypeTo = '',
}) {
  const searchInputRef = useRef(null);
  const [query, setQuery] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [supplierFilter, setSupplierFilter] = useState('ALL');
  const [category2Filter, setCategory2Filter] = useState('ALL');
  const initialType = String(lockTypeTo || '').trim().toUpperCase() || 'EXPENSE';
  const [typeFilter, setTypeFilter] = useState(initialType);
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [exporting, setExporting] = useState(false);
  const [category2Catalog, setCategory2Catalog] = useState([]);
  const [catalogVendors, setCatalogVendors] = useState([]);
  const [supplierDebugEnabled, setSupplierDebugEnabled] = useState(false);

  useEffect(() => {
    searchInputRef.current?.focus();
  }, []);

  useEffect(() => {
    const nextType = String(lockTypeTo || '').trim().toUpperCase();
    if (!nextType) return;
    setTypeFilter(nextType);
  }, [lockTypeTo]);

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

  const buildBaseParams = React.useCallback(() => {
    const params = {
      q: query.trim(),
      type: typeFilter === 'ALL' ? '' : typeFilter,
      page: '1',
      limit: '500',
    };
    if (forceGlobalProjectScope) {
      params.allProjects = '1';
    } else if (selectedProjectId) {
      params.projectId = String(selectedProjectId);
    }
    return params;
  }, [query, typeFilter, forceGlobalProjectScope, selectedProjectId]);

  useEffect(() => {
    if (forceGlobalProjectScope) {
      setCategory2Catalog([]);
      return;
    }
    api.supplierCategories().then((data) => setCategory2Catalog(Array.isArray(data) ? data : [])).catch(() => setCategory2Catalog([]));
  }, [forceGlobalProjectScope]);

  useEffect(() => {
    let isMounted = true;
    const params = forceGlobalProjectScope
      ? { allProjects: '1', type: 'EXPENSE' }
      : (selectedProjectId ? { projectId: String(selectedProjectId), type: typeFilter === 'ALL' ? '' : typeFilter } : {});
    api.vendors(params)
      .then((data) => {
        if (!isMounted) return;
        setCatalogVendors(Array.isArray(data) ? data : []);
      })
      .catch(() => {
        if (!isMounted) return;
        setCatalogVendors([]);
      });
    return () => {
      isMounted = false;
    };
  }, [forceGlobalProjectScope, selectedProjectId, typeFilter]);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);
    setError('');
    const transactionParams = buildBaseParams();
    api.transactions(transactionParams).then((data) => {
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
  }, [buildBaseParams]);

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

    const optionVendors = forceGlobalProjectScope ? catalogVendors : vendors;
    optionVendors.forEach((vendor) => {
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

    optionVendors.forEach((vendor) => {
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
  }, [rows, vendors, catalogVendors, supplierDebugEnabled, forceGlobalProjectScope]);

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

  async function exportPdf() {
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

      const params = buildBaseParams();
      const refreshed = await api.transactions(params);
      const exportRows = Array.isArray(refreshed?.items) ? refreshed.items : rows;
      const filteredExportRows = exportRows
        .filter((row) => matchesSearch(row, query, categoryMap))
        .filter((row) => (supplierFilter === 'ALL' ? true : resolveSupplierIdentity(row).key === supplierFilter))
        .filter((row) => (category2Filter === 'ALL' ? true : resolveCategory2(row, categoryMap).id === category2Filter))
        .filter((row) => (typeFilter === 'ALL' ? true : row?.type === typeFilter));
      const exportTotalWithoutTax = filteredExportRows.reduce((acc, row) => acc + (getAmountWithoutTax(row) || 0), 0);
      const exportTotalWithTax = filteredExportRows.reduce((acc, row) => acc + (getAmountWithTax(row) || 0), 0);

      const html = buildPdfContent({
        query: query.trim(),
        supplierLabel,
        categoryLabel,
        typeLabel,
        rows: filteredExportRows,
        totalWithoutTax: exportTotalWithoutTax,
        totalWithTax: exportTotalWithTax,
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

  const typeSelectorVisible = !String(lockTypeTo || '').trim();

  return (
    <div className="card">
      <h2 style={{ marginTop: 0 }}>{title}</h2>
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
      <div
        className="small"
        style={{
          marginTop: 6,
          color: 'var(--primary)',
          fontSize: '16px',
          fontWeight: 700,
        }}
      >
        Total sin IVA filtrado: {formatCurrency(totalWithoutTax)}
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
          {typeSelectorVisible && (
            <select value={typeFilter} onChange={(event) => setTypeFilter(event.target.value)}>
              <option value="ALL">Ingreso / Egreso (todos)</option>
              <option value="INCOME">Ingreso</option>
              <option value="EXPENSE">Egreso</option>
            </select>
          )}
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
              <th>Proyecto</th>
              <th>SBO</th>
              <th>Proveedor</th>
              <th>Descripción</th>
              <th>Categoría</th>
              <th>Subtotal</th>
              <th>IVA</th>
              <th>Total</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row) => {
              const supplier = resolveSupplierIdentity(row);
              const category2 = resolveCategory2(row, categoryMap);
              const projectDisplayName = resolveProjectDisplayName(row);
              return (
                <tr key={row.id}>
                  <td>{row.date || '—'}</td>
                  <td>{projectDisplayName || 'Sin proyecto'}</td>
                  <td>{row.sourceSbo || '—'}</td>
                  <td>{supplier?.name || '—'}</td>
                  <td>{row.description || '—'}</td>
                  <td>{category2?.name || '—'}</td>
                  <td style={{ whiteSpace: 'nowrap' }}>{formatCurrencyWithFallback(getAmountWithoutTax(row))}</td>
                  <td style={{ whiteSpace: 'nowrap' }}>{formatCurrencyWithFallback(getTaxAmount(row))}</td>
                  <td style={{ whiteSpace: 'nowrap' }}>{formatCurrencyWithFallback(getAmountWithTax(row))}</td>
                </tr>
              );
            })}
            {!visibleRows.length && !loading && <tr><td colSpan={9} className="small">Sin resultados</td></tr>}
          </tbody>
          <tfoot>
            <tr>
              <td colSpan={5} />
              <td style={{ textAlign: 'right', fontWeight: 700 }}>Totales visibles</td>
              <td style={{ fontWeight: 700 }}>{formatCurrency(totalWithoutTax)}</td>
              <td style={{ fontWeight: 700 }}>{formatCurrency(totalWithTax - totalWithoutTax)}</td>
              <td style={{ fontWeight: 700 }}>{formatCurrency(totalWithTax)}</td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}
