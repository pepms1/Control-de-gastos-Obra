import React, { useEffect, useMemo, useState } from 'react';
import { api, clearSession, getSession, saveSession } from '../api.js';

const moneyFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatMoney(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '0.00';
  return moneyFormatter.format(amount);
}

function parseMoneyInput(value) {
  if (typeof value !== 'string') return Number(value);
  return Number(value.replace(/,/g, '').trim());
}

/* ================= NAV ================= */
function Nav({ tab, setTab, role, username, onLogout }) {
  const items = [
    ['dashboard', 'Dashboard', true],
    ['add-expense', 'Nuevo egreso', role === 'ADMIN'],
    ['add-income', 'Nuevo ingreso', role === 'ADMIN'],
    ['transactions', 'Movimientos', true],
    ['search', 'Buscar', true],
    ['catalog', 'Catálogo', true],
  ];

  const linkStyle = (active) => ({
    background: 'transparent',
    border: 'none',
    padding: 0,
    cursor: 'pointer',
    textAlign: 'left',
    font: 'inherit',
    color: 'inherit',
    opacity: active ? 1 : 0.85,
    fontWeight: active ? 800 : 600,
  });

  return (
    <div className="nav">
      <div className="nav-header">
        <img src="/logo-grupo-mdi.svg" alt="Logo Grupo MDI" className="nav-logo" />
        <div className="nav-title-wrap">
          <div className="nav-title">Grupo MDI</div>
          <div className="nav-subtitle">Control de Gastos Calderon de la Barca</div>
        </div>
      </div>

      <div className="nav-items">
        {items
          .filter(([, , show]) => show)
          .map(([k, label]) => (
            <button
              key={k}
              type="button"
              className={tab === k ? 'active' : ''}
              onClick={() => setTab(k)}
              style={linkStyle(tab === k)}
            >
              {label}
            </button>
          ))}
      </div>

      <div className="nav-user-actions">
        <div className="small nav-user">
          {username} ({role})
        </div>

        <button className="secondary" type="button" onClick={onLogout}>
          Salir
        </button>
      </div>
    </div>
  );
}

/* ================= LOGIN ================= */
function Login({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [err, setErr] = useState('');
  const [saving, setSaving] = useState(false);

  async function submit(e) {
    e.preventDefault();
    setErr('');
    setSaving(true);
    try {
      const data = await api.login(username.trim(), password);
      saveSession(data);
      onLogin(getSession());
    } catch (e) {
      setErr(e.message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="container login-container">
      <div className="card">
        <div className="login-brand">
          <img src="/logo-grupo-mdi.svg" alt="Logo Grupo MDI" className="login-brand-image" />
          <h1>Grupo MDI</h1>
          <p>control de obra</p>
        </div>
        <h2 style={{ marginTop: 0 }}>Iniciar sesión</h2>
        <form onSubmit={submit} className="grid">
          <div>
            <label>Usuario</label>
            <input value={username} onChange={(e) => setUsername(e.target.value)} />
          </div>
          <div>
            <label>Contraseña</label>
            <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </div>
          {err && <div style={{ color: '#334155' }}>{err}</div>}
          <button disabled={saving}>{saving ? 'Ingresando...' : 'Entrar'}</button>
        </form>
      </div>
    </div>
  );
}

/* ================= APP ================= */
export default function App() {
  const [tab, setTab] = useState('dashboard');
  const [cats, setCats] = useState([]);
  const [vendors, setVendors] = useState([]);
  const [toast, setToast] = useState('');
  const [session, setSession] = useState(getSession());

  const isAdmin = session.role === 'ADMIN';

  async function refreshCatalog() {
    const [c, v] = await Promise.all([api.categories(), api.vendors()]);
    setCats(Array.isArray(c) ? c : []);
    setVendors(Array.isArray(v) ? v : []);
  }

  useEffect(() => {
    if (!session.token) return;

    api.me()
      .then((me) =>
        setSession((prev) => ({
          ...prev,
          ...me, // conserva token
        }))
      )
      .catch(() => {
        clearSession();
        setSession(getSession());
      });

    refreshCatalog().catch(() => {});
  }, [session.token]);

  function logout() {
    clearSession();
    setSession(getSession());
    setTab('dashboard');
  }

  if (!session.token) return <Login onLogin={setSession} />;

  return (
    <>
      <Nav tab={tab} setTab={setTab} role={session.role} username={session.username} onLogout={logout} />

      <div className="container grid" style={{ gap: 14 }}>
        {toast && <div className="card">{toast}</div>}

        {tab === 'dashboard' && <Dashboard isAdmin={isAdmin} />}

        {tab === 'add-expense' && isAdmin && (
          <TxnForm
            kind="EXPENSE"
            cats={cats}
            vendors={vendors}
            onDone={(m) => {
              setToast(m);
              setTab('transactions');
            }}
          />
        )}

        {tab === 'add-income' && isAdmin && (
          <TxnForm
            kind="INCOME"
            cats={cats}
            vendors={vendors}
            onDone={(m) => {
              setToast(m);
              setTab('transactions');
            }}
          />
        )}

        {tab === 'transactions' && (
          <Transactions isAdmin={isAdmin} cats={cats} vendors={vendors} onCatalogChanged={refreshCatalog} />
        )}

        {tab === 'search' && <SearchTransactions cats={cats} vendors={vendors} />}

        {tab === 'catalog' && (
          <Catalog
            isAdmin={isAdmin}
            cats={cats}
            vendors={vendors}
            onChanged={async () => {
              await refreshCatalog();
              setToast('Catálogo actualizado');
            }}
          />
        )}
      </div>
    </>
  );
}

/* ================= DASHBOARD ================= */
function Dashboard({ isAdmin }) {
  const [stats, setStats] = useState(null);
  const [supplierSummary, setSupplierSummary] = useState([]);
  const [supplierSummaryError, setSupplierSummaryError] = useState('');
  const [viewMode, setViewMode] = useState('category');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let ok = true;
    setLoading(true);

    Promise.allSettled([api.spendByCategory(), api.expensesSummaryBySupplier()]).then(([categoryResult, supplierResult]) => {
      if (!ok) return;

      if (categoryResult.status === 'fulfilled') {
        setStats(categoryResult.value);
      } else {
        setStats({ error: categoryResult.reason?.message || 'No se pudo cargar el dashboard.' });
      }

      if (supplierResult.status === 'fulfilled') {
        setSupplierSummary(Array.isArray(supplierResult.value) ? supplierResult.value : []);
        setSupplierSummaryError('');
      } else {
        setSupplierSummary([]);
        setSupplierSummaryError(supplierResult.reason?.message || 'No se pudo cargar la vista por proveedor.');
      }

      setLoading(false);
    });
    Promise.all([api.spendByCategory(), api.expensesSummaryBySupplier()])
      .then(([categoryStats, supplierStats]) => {
        if (ok) {
          setStats(categoryStats);
          setSupplierSummary(Array.isArray(supplierStats) ? supplierStats : []);
          setLoading(false);
        }
      })
      .catch((e) => {
        if (ok) {
          setStats({ error: e.message });
          setSupplierSummary([]);
          setLoading(false);
        }
      });

    return () => {
      ok = false;
    };
  }, []);

  const supplierTotal = supplierSummary.reduce((acc, row) => acc + (Number(row.totalAmount) || 0), 0);

  return (
    <div className="card">
      <h2 style={{ margin: '0 0 8px' }}>Dashboard de egresos</h2>
      <div className="row" style={{ gap: 8, marginBottom: 8 }}>
        <button className={viewMode === 'category' ? '' : 'secondary'} onClick={() => setViewMode('category')}>
          Por categoría
        </button>
        <button className={viewMode === 'supplier' ? '' : 'secondary'} onClick={() => setViewMode('supplier')}>
          Por proveedor
        </button>
      </div>
      <div className="small">
        {viewMode === 'category'
          ? 'Porcentaje = gasto de la categoría / total de egresos'
          : 'Totales agrupados por proveedor (SAP).'}
      </div>

      {loading ? (
        <div style={{ padding: '12px 0' }}>Cargando...</div>
      ) : stats?.error ? (
        <div style={{ padding: '12px 0' }}>Error: {stats.error}</div>
      ) : viewMode === 'category' && stats?.rows?.length ? (
        <div style={{ marginTop: 12 }} className="grid">
          <div className="row" style={{ justifyContent: 'space-between' }}>
            <div className="badge">Total egresos: ${formatMoney(stats.total_expenses || 0)}</div>
            {isAdmin && (
              <button className="secondary" onClick={() => api.seed().then(() => location.reload()).catch(() => {})}>
                Seed categorías
              </button>
            )}
          </div>

          <PieChart rows={stats.rows} />

          {stats.rows.map((r) => (
            <div key={r.category_id} style={{ display: 'grid', gap: 6 }}>
              <div className="row" style={{ justifyContent: 'space-between' }}>
                <div style={{ fontWeight: 700 }}>{r.category_name}</div>
                <div>
                  ${formatMoney(r.amount)} <span className="small">({r.percent}%)</span>
                </div>
              </div>
              <div className="bar">
                <div style={{ width: Math.min(100, r.percent) + '%' }} />
              </div>
            </div>
          ))}
        </div>
      ) : viewMode === 'supplier' ? (
        supplierSummaryError ? (
          <div style={{ padding: '12px 0' }}>Error: {supplierSummaryError}</div>
        ) : supplierSummary.length ? (
          <div style={{ marginTop: 12 }} className="grid">
            <div className="badge">Total egresos SAP: ${formatMoney(supplierTotal)}</div>
            <div style={{ overflowX: 'auto' }}>
              <table>
                <thead>
                  <tr>
                    <th>Proveedor</th>
                    <th>Movimientos</th>
                    <th>Subtotal</th>
                  </tr>
                </thead>
                <tbody>
                  {supplierSummary.map((row) => (
                    <tr key={row.supplierId || row.supplierName}>
                      <td>{row.supplierName || '(Sin proveedor)'}</td>
                      <td>{row.count}</td>
                      <td>${formatMoney(row.totalAmount)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <div style={{ padding: '12px 0' }}>No hay egresos SAP agrupados por proveedor para mostrar.</div>
        )
      ) : viewMode === 'supplier' && supplierSummary.length ? (
        <div style={{ marginTop: 12 }} className="grid">
          <div className="badge">Total egresos SAP: ${formatMoney(supplierTotal)}</div>
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>Proveedor</th>
                  <th>Movimientos</th>
                  <th>Subtotal</th>
                </tr>
              </thead>
              <tbody>
                {supplierSummary.map((row) => (
                  <tr key={row.supplierId || row.supplierName}>
                    <td>{row.supplierName || '(Sin proveedor)'}</td>
                    <td>{row.count}</td>
                    <td>${formatMoney(row.totalAmount)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : (
        <div style={{ padding: '12px 0' }}>No hay egresos aún. Registra uno para ver el dashboard.</div>
      )}
    </div>
  );
}

function PieChart({ rows }) {
  const slices = rows
    .map((row, index) => {
      const percent = Number(row.percent) || 0;
      return {
        ...row,
        percent,
        color: PIE_COLORS[index % PIE_COLORS.length],
      };
    })
    .filter((row) => row.percent > 0);

  if (!slices.length) return null;

  let current = 0;
  const gradient = slices
    .map((slice) => {
      const start = current;
      const end = current + slice.percent;
      current = end;
      return `${slice.color} ${start}% ${Math.min(100, end)}%`;
    })
    .join(', ');

  return (
    <div className="pie-card">
      <div className="pie-chart" style={{ background: `conic-gradient(${gradient})` }} aria-label="Gráfica de pastel por categoría" />
      <div className="pie-legend">
        {slices.map((slice) => (
          <div key={slice.category_id} className="pie-legend-item">
            <span className="pie-dot" style={{ background: slice.color }} />
            <span>{slice.category_name}</span>
            <strong>{slice.percent.toFixed(2)}%</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

const PIE_COLORS = ['#12305f', '#1f4d96', '#3b629b', '#5f7ea8', '#7f97b8', '#9fafc7', '#c4cfdf'];
const ADD_NEW_VENDOR_VALUE = '__add_new_vendor__';

/* ================= TXN FORM ================= */
function TxnForm({ kind, cats, vendors, onDone }) {
  const [amount, setAmount] = useState('');
  const [date, setDate] = useState(new Date().toISOString().slice(0, 10));
  const [categoryId, setCategoryId] = useState('');
  const [vendorId, setVendorId] = useState('');
  const [newVendorName, setNewVendorName] = useState('');
  const [description, setDescription] = useState('');
  const [reference, setReference] = useState('');
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState('');

  useEffect(() => {
    if (cats.length && !categoryId) setCategoryId(cats[0].id);
    if (vendors.length && !vendorId) setVendorId(vendors[0].id);
    if (!vendors.length) setVendorId(ADD_NEW_VENDOR_VALUE);
  }, [cats, vendors]);

  async function submit(e) {
    e.preventDefault();
    setErr('');
    const a = parseMoneyInput(amount);
    if (!a || a <= 0) return setErr('Monto inválido');

    if (kind === 'EXPENSE' && (!categoryId || !vendorId)) return setErr('Selecciona categoría y proveedor');

    const creatingNewVendor = kind === 'EXPENSE' && vendorId === ADD_NEW_VENDOR_VALUE;
    if (creatingNewVendor && newVendorName.trim().length < 2) {
      return setErr('Escribe un nombre de proveedor válido');
    }

    setSaving(true);
    try {
      let finalVendorId = vendorId;
      if (creatingNewVendor) {
        const createdVendor = await api.createVendor({ name: newVendorName.trim(), category_ids: [] });
        finalVendorId = createdVendor?.id;
      }

      await api.createTransaction({
        type: kind,
        date,
        amount: a,
        category_id: kind === 'EXPENSE' ? categoryId : null,
        vendor_id: kind === 'EXPENSE' ? finalVendorId : null,
        description,
        reference,
      });
      onDone(kind === 'EXPENSE' ? 'Egreso guardado' : 'Ingreso guardado');
    } catch (e) {
      setErr(e.message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="card">
      <h2 style={{ margin: '0 0 8px' }}>{kind === 'EXPENSE' ? 'Nuevo egreso' : 'Nuevo ingreso'}</h2>

      <form onSubmit={submit} className="grid grid2">
        <div>
          <label>Monto</label>
          <input value={amount} onChange={(e) => setAmount(e.target.value)} placeholder="0.00" inputMode="decimal" />
        </div>
        <div>
          <label>Fecha</label>
          <input value={date} onChange={(e) => setDate(e.target.value)} placeholder="YYYY-MM-DD" />
        </div>

        {kind === 'EXPENSE' && (
          <>
            <div>
              <label>Categoría</label>
              <select value={categoryId} onChange={(e) => setCategoryId(e.target.value)}>
                {cats.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Proveedor</label>
              <select value={vendorId} onChange={(e) => setVendorId(e.target.value)}>
                {vendors.map((v) => (
                  <option key={v.id} value={v.id}>
                    {v.name}
                  </option>
                ))}
                <option value={ADD_NEW_VENDOR_VALUE}>Agregar nuevo...</option>
              </select>
              {vendorId === ADD_NEW_VENDOR_VALUE && (
                <input
                  style={{ marginTop: 8 }}
                  value={newVendorName}
                  onChange={(e) => setNewVendorName(e.target.value)}
                  placeholder="Nombre del proveedor"
                />
              )}
            </div>
          </>
        )}

        <div>
          <label>Descripción</label>
          <input value={description} onChange={(e) => setDescription(e.target.value)} placeholder="Opcional" />
        </div>
        <div>
          <label>Referencia</label>
          <input value={reference} onChange={(e) => setReference(e.target.value)} placeholder="Factura/nota (opcional)" />
        </div>

        {err && <div style={{ gridColumn: '1/-1', color: '#334155' }}>{err}</div>}

        <div style={{ gridColumn: '1/-1' }}>
          <button disabled={saving}>{saving ? 'Guardando...' : 'Guardar'}</button>
        </div>
      </form>

      <div className="small" style={{ marginTop: 10 }}>
        Nota: si no ves categorías/proveedores, ve a “Catálogo” o presiona “Seed categorías” en Dashboard.
      </div>
    </div>
  );
}

/* ================= MODAL ================= */
function EditModal({ title, children, onClose, onSave }) {
  return (
    <div className="modal-backdrop">
      <div className="modal">
        <h3>{title}</h3>
        {children}
        <div className="row" style={{ justifyContent: 'flex-end', marginTop: 10 }}>
          <button className="secondary" type="button" onClick={onClose}>
            Cancelar
          </button>
          <button type="button" onClick={onSave}>
            Guardar
          </button>
        </div>
      </div>
    </div>
  );
}

/* ================= TRANSACTIONS ================= */
function Transactions({ isAdmin, cats, vendors, onCatalogChanged }) {
  const catMap = useMemo(() => Object.fromEntries(cats.map((c) => [c.id, c.name])), [cats]);
  const vendorMap = useMemo(() => Object.fromEntries(vendors.map((v) => [v.id, v.name])), [vendors]);

  const [rows, setRows] = useState([]);
  const [editing, setEditing] = useState(null);
  const [filter, setFilter] = useState('ALL');
  const [categoryFilter, setCategoryFilter] = useState('ALL');
  const [supplierFilter, setSupplierFilter] = useState('ALL');
  const [searchFilter, setSearchFilter] = useState('');
  const [sortBy, setSortBy] = useState('date_desc');
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');
  const [newCategoryName, setNewCategoryName] = useState('');
  const [editErr, setEditErr] = useState('');
  const [savingCategory, setSavingCategory] = useState(false);
  const [selectedRows, setSelectedRows] = useState([]);
  const [bulkCategoryId, setBulkCategoryId] = useState('');
  const [bulkSaving, setBulkSaving] = useState(false);

  async function load() {
    setLoading(true);
    setErr('');
    try {
      const t = await api.transactions();
      setRows(Array.isArray(t) ? t : []);
    } catch (e) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  useEffect(() => {
    setSelectedRows([]);
  }, [filter, categoryFilter, supplierFilter, searchFilter, sortBy, rows]);

  const normalizedSearch = searchFilter.trim().toLowerCase();

  const supplierOptions = useMemo(() => {
    const byId = new Map();
    rows.forEach((r) => {
      if (r.supplierId) byId.set(r.supplierId, r.proveedorNombre || r.supplierName || r.proveedor?.name || 'Proveedor SAP');
      if (r.vendor_id) byId.set(r.vendor_id, vendorMap[r.vendor_id] || 'Proveedor');
    });
    return Array.from(byId.entries()).sort((a, b) => a[1].localeCompare(b[1], 'es'));
  }, [rows, vendorMap]);

  const shown = [...rows]
    .filter((r) => (filter === 'ALL' ? true : r.type === filter))
    .filter((r) => (categoryFilter === 'ALL' ? true : r.category_id === categoryFilter))
    .filter((r) => {
      if (supplierFilter === 'ALL') return true;
      return (r.supplierId || r.vendor_id || '') === supplierFilter;
    })
    .filter((r) => {
      if (!normalizedSearch) return true;
      const supplierName = (r.proveedorNombre || r.supplierName || vendorMap[r.vendor_id] || r.proveedor?.name || '').toLowerCase();
      const concept = (r.description || r.concept || '').toLowerCase();
      const categoryName = (r.category_id ? catMap[r.category_id] || '' : '').toLowerCase();
      const typeLabel = (r.type === 'EXPENSE' ? 'egreso' : 'ingreso').toLowerCase();
      return [supplierName, concept, categoryName, typeLabel, r.date || ''].some((value) => value.includes(normalizedSearch));
    })
    .sort((a, b) => {
      if (sortBy === 'supplier_asc') {
        const aSupplier = (a.proveedorNombre || a.supplierName || vendorMap[a.vendor_id] || a.proveedor?.name || '').toLowerCase();
        const bSupplier = (b.proveedorNombre || b.supplierName || vendorMap[b.vendor_id] || b.proveedor?.name || '').toLowerCase();
        if (aSupplier !== bSupplier) return aSupplier.localeCompare(bSupplier, 'es');
      }

      if (a.date === b.date) {
        const aCreatedAt = a.created_at || '';
        const bCreatedAt = b.created_at || '';
        return bCreatedAt.localeCompare(aCreatedAt);
      }
      return b.date.localeCompare(a.date);
    });

  async function saveEdit() {
    setEditErr('');
    const payload = {
      date: editing.date,
      amount: parseMoneyInput(editing.amount),
      description: editing.description,
      category_id: editing.category_id || null,
    };
    await api.updateTransaction(editing.id, payload);
    setEditing(null);
    setNewCategoryName('');
    load();
  }

  async function createCategoryFromEdit() {
    const cleanName = newCategoryName.trim();
    if (cleanName.length < 2) {
      setEditErr('Escribe un nombre de categoría válido.');
      return;
    }

    setSavingCategory(true);
    setEditErr('');
    try {
      const created = await api.createCategory(cleanName);
      await onCatalogChanged?.();
      setEditing((prev) => (prev ? { ...prev, category_id: created.id } : prev));
      setNewCategoryName('');
    } catch (e) {
      setEditErr(e.message || 'No se pudo crear la categoría.');
    } finally {
      setSavingCategory(false);
    }
  }

  async function remove(id) {
    if (confirm('¿Eliminar movimiento?')) {
      await api.deleteTransaction(id);
      load();
    }
  }

  function toggleSelected(id) {
    setSelectedRows((prev) => (prev.includes(id) ? prev.filter((rowId) => rowId !== id) : [...prev, id]));
  }

  function toggleSelectAllShown() {
    const shownIds = shown.map((r) => r.id);
    const allSelected = shownIds.length > 0 && shownIds.every((id) => selectedRows.includes(id));
    setSelectedRows((prev) => {
      if (allSelected) return prev.filter((id) => !shownIds.includes(id));
      const merged = new Set([...prev, ...shownIds]);
      return Array.from(merged);
    });
  }

  async function applyBulkCategory() {
    if (!selectedRows.length) return;
    setBulkSaving(true);
    setErr('');
    try {
      const selected = rows.filter((r) => selectedRows.includes(r.id));
      await Promise.all(
        selected.map((r) =>
          api.updateTransaction(r.id, {
            date: r.date,
            amount: parseMoneyInput(r.amount),
            description: r.description,
            category_id: bulkCategoryId || null,
          })
        )
      );
      setSelectedRows([]);
      await load();
    } catch (e) {
      setErr(e.message || 'No se pudo actualizar la categoría en lote.');
    } finally {
      setBulkSaving(false);
    }
  }

  const shownIds = shown.map((r) => r.id);
  const allShownSelected = shownIds.length > 0 && shownIds.every((id) => selectedRows.includes(id));

  return (
    <div className="card">
      <div className="row" style={{ justifyContent: 'space-between' }}>
        <h2 style={{ margin: 0 }}>Movimientos</h2>
        <div className="row">
          <input
            type="search"
            placeholder="Buscar en movimientos"
            value={searchFilter}
            onChange={(e) => setSearchFilter(e.target.value)}
            style={{ minWidth: 220 }}
          />
          <select value={filter} onChange={(e) => setFilter(e.target.value)}>
            <option value="ALL">Todos</option>
            <option value="INCOME">Ingresos</option>
            <option value="EXPENSE">Egresos</option>
          </select>
          <select value={categoryFilter} onChange={(e) => setCategoryFilter(e.target.value)}>
            <option value="ALL">Todas las categorías</option>
            {cats.map((c) => (
              <option key={c.id} value={c.id}>{c.name}</option>
            ))}
          </select>
          <select value={supplierFilter} onChange={(e) => setSupplierFilter(e.target.value)}>
            <option value="ALL">Todos los proveedores</option>
            {supplierOptions.map(([id, name]) => (
              <option key={id} value={id}>{name}</option>
            ))}
          </select>
          <select value={sortBy} onChange={(e) => setSortBy(e.target.value)}>
            <option value="date_desc">Fecha (más reciente)</option>
            <option value="supplier_asc">Proveedor (A-Z)</option>
          </select>
          <button className="secondary" onClick={load}>Refrescar</button>
        </div>
      </div>

      {isAdmin && (
        <div className="row" style={{ marginTop: 10, justifyContent: 'space-between' }}>
          <div className="small">Seleccionados: {selectedRows.length}</div>
          <div className="row">
            <select value={bulkCategoryId} onChange={(e) => setBulkCategoryId(e.target.value)}>
              <option value="">Sin categoría</option>
              {cats.map((c) => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
            </select>
            <button
              className="secondary"
              type="button"
              onClick={applyBulkCategory}
              disabled={!selectedRows.length || bulkSaving}
            >
              {bulkSaving ? 'Aplicando...' : 'Cambiar categoría (selección múltiple)'}
            </button>
          </div>
        </div>
      )}

      {loading ? (
        <div style={{ padding: '12px 0' }}>Cargando...</div>
      ) : err ? (
        <div style={{ padding: '12px 0' }}>Error: {err}</div>
      ) : shown.length ? (
        <div style={{ overflowX: 'auto', marginTop: 10 }}>
          <table>
            <thead>
              <tr>
                <th>Fecha</th>
                <th>Tipo</th>
                <th>Origen</th>
                <th>Descripción</th>
                <th>Categoría</th>
                <th>Proveedor</th>
                <th>Monto</th>
                {isAdmin && <th>Acciones</th>}
                {isAdmin && <th>Seleccionar</th>}
              </tr>
            </thead>
            <tbody>
              {isAdmin && (
                <tr>
                  <td colSpan={9} style={{ textAlign: 'right' }}>
                    <label className="row" style={{ justifyContent: 'flex-end' }}>
                      <input type="checkbox" checked={allShownSelected} onChange={toggleSelectAllShown} />
                      Seleccionar todos (filtro actual)
                    </label>
                  </td>
                </tr>
              )}
              {shown.map((r) => (
                <tr key={r.id}>
                  <td>{r.date}</td>
                  <td>{r.type === 'INCOME' ? 'Ingreso' : 'Egreso'}</td>
                  <td>{r.source === 'sap' ? <span className="badge">SAP</span> : ''}</td>
                  <td>{r.description || r.concept || ''}</td>
                  <td>{r.category_id ? catMap[r.category_id] || '' : ''}</td>
                  <td>{r.proveedorNombre || r.supplierName || vendorMap[r.vendor_id] || r.proveedor?.name || '—'}</td>
                  <td style={{ fontWeight: 800 }}>{r.type === 'EXPENSE' ? '-' : '+'}${formatMoney(r.amount)}</td>
                  {isAdmin && (
                    <td>
                      <button
                        className="secondary"
                        onClick={() => {
                          setEditErr('');
                          setNewCategoryName('');
                          setEditing({ ...r });
                        }}
                      >
                        {r.source === 'sap' ? 'Categorizar' : 'Editar'}
                      </button>
                      {r.source !== 'sap' && (
                        <>
                          {' '}
                          <button className="secondary" onClick={() => remove(r.id)}>Eliminar</button>
                        </>
                      )}
                    </td>
                  )}
                  {isAdmin && (
                    <td>
                      <input
                        type="checkbox"
                        checked={selectedRows.includes(r.id)}
                        onChange={() => toggleSelected(r.id)}
                        aria-label={`Seleccionar movimiento ${r.id}`}
                      />
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div style={{ padding: '12px 0' }}>No hay movimientos.</div>
      )}

      {editing && (
        <EditModal title="Editar movimiento" onClose={() => setEditing(null)} onSave={saveEdit}>
          <div className="grid">
            <label>Fecha</label>
            <input value={editing.date || ''} onChange={(e) => setEditing({ ...editing, date: e.target.value })} />
            <label>Monto</label>
            <input value={editing.amount || ''} onChange={(e) => setEditing({ ...editing, amount: e.target.value })} />
            <label>Descripción</label>
            <input value={editing.description || ''} onChange={(e) => setEditing({ ...editing, description: e.target.value })} />
            <label>Categoría</label>
            <select value={editing.category_id || ''} onChange={(e) => setEditing({ ...editing, category_id: e.target.value || null })}>
              <option value="">Sin categoría</option>
              {cats.map((c) => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
            </select>
            <label>Crear categoría nueva</label>
            <div className="row">
              <input
                placeholder="Ej. Herramientas"
                value={newCategoryName}
                onChange={(e) => setNewCategoryName(e.target.value)}
              />
              <button type="button" className="secondary" onClick={createCategoryFromEdit} disabled={savingCategory}>
                {savingCategory ? 'Creando...' : 'Crear'}
              </button>
            </div>
            {editErr && <div style={{ color: '#b91c1c' }}>{editErr}</div>}
          </div>
        </EditModal>
      )}
    </div>
  );
}

function SearchTransactions({ cats, vendors }) {
  const [rows, setRows] = useState([]);
  const [query, setQuery] = useState('');
  const catMap = useMemo(() => Object.fromEntries(cats.map((c) => [c.id, c.name])), [cats]);
  const vendorMap = useMemo(() => Object.fromEntries(vendors.map((v) => [v.id, v.name])), [vendors]);

  useEffect(() => {
    api.transactions().then((data) => setRows(Array.isArray(data) ? data : [])).catch(() => setRows([]));
  }, []);

  const needle = query.trim().toLowerCase();
  const shown = rows.filter((r) => {
    if (!needle) return true;
    const supplier = (r.proveedorNombre || r.supplierName || vendorMap[r.vendor_id] || r.proveedor?.name || '').toLowerCase();
    const concept = (r.description || r.concept || '').toLowerCase();
    return supplier.includes(needle) || concept.includes(needle);
  });

  return (
    <div className="card">
      <h2 style={{ marginTop: 0 }}>Buscar movimientos</h2>
      <input
        placeholder="Buscar por proveedor o concepto"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        style={{ maxWidth: 420 }}
      />
      <div className="small" style={{ marginTop: 8 }}>{shown.length} resultados</div>
      <div style={{ overflowX: 'auto', marginTop: 10 }}>
        <table>
          <thead>
            <tr>
              <th>Fecha</th><th>Proveedor</th><th>Concepto</th><th>Categoría</th><th>Monto</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((r) => (
              <tr key={r.id}>
                <td>{r.date}</td>
                <td>{r.proveedorNombre || r.supplierName || vendorMap[r.vendor_id] || r.proveedor?.name || '—'}</td>
                <td>{r.description || r.concept || ''}</td>
                <td>{r.category_id ? catMap[r.category_id] || '' : ''}</td>
                <td>{r.type === 'EXPENSE' ? '-' : '+'}${formatMoney(r.amount)}</td>
              </tr>
            ))}
            {!shown.length && (
              <tr><td colSpan={5} className="small">Sin resultados</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ================= CATALOG ================= */
function Catalog({ isAdmin, cats, vendors, onChanged }) {
  const [catName, setCatName] = useState('');
  const [vendorName, setVendorName] = useState('');
  const [catEdit, setCatEdit] = useState(null);
  const [vendorEdit, setVendorEdit] = useState(null);
  const [err, setErr] = useState('');

  async function addCat(e) {
    e.preventDefault();
    setErr('');
    if (catName.trim().length < 2) return setErr('Nombre de categoría inválido');
    await api.createCategory(catName.trim());
    setCatName('');
    onChanged();
  }

  async function addVendor(e) {
    e.preventDefault();
    setErr('');
    if (vendorName.trim().length < 2) return setErr('Nombre de proveedor inválido');
    await api.createVendor({ name: vendorName.trim(), category_ids: [] });
    setVendorName('');
    onChanged();
  }

  return (
    <div className="grid grid2">
      <div className="card">
        <h2 style={{ margin: '0 0 8px' }}>Categorías</h2>

        {isAdmin && (
          <form onSubmit={addCat} className="row">
            <div style={{ flex: 1 }}>
              <label>Nueva categoría</label>
              <input value={catName} onChange={(e) => setCatName(e.target.value)} placeholder="Ej. Acabados" />
            </div>
            <div style={{ marginTop: 18 }}>
              <button>Agregar</button>
            </div>
          </form>
        )}

        <div style={{ marginTop: 12 }} className="grid">
          {cats.map((c) => (
            <div key={c.id} className="row" style={{ justifyContent: 'space-between' }}>
              <span className="badge">{c.name}</span>
              {isAdmin && (
                <span>
                  <button className="secondary" onClick={() => setCatEdit({ ...c })}>
                    Editar
                  </button>{' '}
                  <button
                    className="secondary"
                    onClick={async () => {
                      await api.deleteCategory(c.id);
                      onChanged();
                    }}
                  >
                    Eliminar
                  </button>
                </span>
              )}
            </div>
          ))}
          {!cats.length && <div className="small">No hay categorías. Puedes presionar “Seed categorías” en Dashboard.</div>}
        </div>
      </div>

      <div className="card">
        <h2 style={{ margin: '0 0 8px' }}>Proveedores</h2>

        {isAdmin && (
          <form onSubmit={addVendor} className="row">
            <div style={{ flex: 1 }}>
              <label>Nuevo proveedor</label>
              <input value={vendorName} onChange={(e) => setVendorName(e.target.value)} placeholder="Ej. Ferretería X" />
            </div>
            <div style={{ marginTop: 18 }}>
              <button>Agregar</button>
            </div>
          </form>
        )}

        <div style={{ marginTop: 12 }} className="grid">
          {vendors.map((v) => (
            <div key={v.id} className="row" style={{ justifyContent: 'space-between' }}>
              <span className="badge">{v.name}</span>
              {isAdmin && (
                <span>
                  <button className="secondary" onClick={() => setVendorEdit({ ...v })}>
                    Editar
                  </button>{' '}
                  <button
                    className="secondary"
                    onClick={async () => {
                      await api.deleteVendor(v.id);
                      onChanged();
                    }}
                  >
                    Eliminar
                  </button>
                </span>
              )}
            </div>
          ))}
          {!vendors.length && <div className="small">No hay proveedores aún.</div>}
        </div>

        {err && <div style={{ marginTop: 10, color: '#b91c1c' }}>{err}</div>}
      </div>

      {catEdit && (
        <EditModal
          title="Editar categoría"
          onClose={() => setCatEdit(null)}
          onSave={async () => {
            await api.updateCategory(catEdit.id, { name: catEdit.name });
            setCatEdit(null);
            onChanged();
          }}
        >
          <label>Nombre</label>
          <input value={catEdit.name || ''} onChange={(e) => setCatEdit({ ...catEdit, name: e.target.value })} />
        </EditModal>
      )}

      {vendorEdit && (
        <EditModal
          title="Editar proveedor"
          onClose={() => setVendorEdit(null)}
          onSave={async () => {
            await api.updateVendor(vendorEdit.id, { name: vendorEdit.name });
            setVendorEdit(null);
            onChanged();
          }}
        >
          <label>Nombre</label>
          <input value={vendorEdit.name || ''} onChange={(e) => setVendorEdit({ ...vendorEdit, name: e.target.value })} />
        </EditModal>
      )}
    </div>
  );
}
