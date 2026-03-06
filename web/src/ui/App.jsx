import React, { useEffect, useMemo, useState } from 'react';
import { api, clearSession, getSession, saveSession, SELECTED_PROJECT_KEY } from '../api.js';
import { ImportSapScreen } from './ImportAndAdminScreens.jsx';
import { dedupeCategories, dedupeVendors } from './dropdownOptions.js';

const THEME_STORAGE_KEY = 'mdi-theme-preference';

const moneyFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatMoney(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '0.00';
  return moneyFormatter.format(Math.abs(amount));
}

function parseMoneyInput(value) {
  if (typeof value !== 'string') return Number(value);
  return Number(value.replace(/,/g, '').trim());
}

function getCategoryHintName(transaction) {
  return (
    transaction?.categoryHintName
    || transaction?.category_hint_name
    || transaction?.CategoryHintName
    || ''
  ).trim();
}

function getCategoryHintCode(transaction) {
  return (
    transaction?.categoryHintCode
    || transaction?.category_hint_code
    || transaction?.CategoryHintCode
    || ''
  ).trim();
}

function getCategory2Label(transaction) {
  return (
    transaction?.categoryManualName
    || transaction?.categoryManualCode
    || ''
  ).trim();
}

function getSapCategoryLabel(transaction) {
  return (
    transaction?.categoryHintName
    || transaction?.category_hint_name
    || transaction?.categoryHintCode
    || transaction?.category_hint_code
    || ''
  ).trim();
}

function getTransactionCategoryLabel(transaction, catMap) {
  const effectiveName = (transaction?.categoryEffectiveName || '').trim();
  if (effectiveName) return effectiveName;

  const effectiveCode = (transaction?.categoryEffectiveCode || '').trim();
  if (effectiveCode) {
    const mappedCategory = (catMap[effectiveCode] || '').trim();
    if (mappedCategory) return mappedCategory;
    return effectiveCode;
  }

  const legacyCategory = (transaction?.category_name || transaction?.category || '').trim();
  if (legacyCategory) return legacyCategory;

  const hintName = getCategoryHintName(transaction);
  if (hintName) return hintName;

  return 'Sin categoría';
}

function normalizeSlug(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-]/g, '');
}

function normalizeS3Prefix(value, slug = '') {
  let normalized = String(value || '').trim();
  const cleanSlug = normalizeSlug(slug);
  if (!normalized) return normalized;
  if (cleanSlug && (normalized === cleanSlug || normalized === `${cleanSlug}/`)) {
    normalized = `exports/${cleanSlug}`;
  }
  normalized = normalized.replace(/\/+$/, '');
  return normalized;
}

function isMongoObjectId(value) {
  return /^[a-fA-F0-9]{24}$/.test(String(value || '').trim());
}

/* ================= NAV ================= */
function Nav({
  tab,
  setTab,
  role,
  username,
  displayName,
  onLogout,
  isDarkMode,
  onToggleTheme,
  projects,
  selectedProjectId,
  onProjectChange,
}) {
  const canSeeSettings = role !== 'VIEWER';
  const items = [
    ['dashboard', 'Dashboard', true],
    ['transactions', 'Movimientos', true],
    ['search', 'Buscar', true],
    ['settings', 'Ajustes', canSeeSettings],
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
          <div className="nav-subtitle">Control de Gastos de Obra</div>
        </div>
      </div>

      <div className="nav-items">
        <div className="small" style={{ marginBottom: 6 }}>Proyecto</div>
        <select value={selectedProjectId} onChange={(e) => onProjectChange(e.target.value)} disabled={!projects.length}>
          {!projects.length && <option value="">Sin proyectos</option>}
          {projects.map((project) => (
            <option key={project._id} value={project._id}>
              {project.name}
            </option>
          ))}
        </select>

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
        <button className="secondary theme-toggle" type="button" onClick={onToggleTheme}>
          {isDarkMode ? '☀️ Modo día' : '🌙 Modo noche'}
        </button>

        <div className="small nav-user">
          {displayName || username} ({role})
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
  const [projects, setProjects] = useState([]);
  const [selectedProjectId, setSelectedProjectId] = useState(localStorage.getItem(SELECTED_PROJECT_KEY) || '');
  const [dataVersion, setDataVersion] = useState(0);
  const [themePreference, setThemePreference] = useState(() => {
    const storedPreference = localStorage.getItem(THEME_STORAGE_KEY);
    if (storedPreference === 'dark') return 'dark';
    return 'light';
  });

  const isAdmin = session.role === 'ADMIN';
  const isDarkMode = themePreference === 'dark';

  useEffect(() => {
    document.body.classList.toggle('theme-dark', isDarkMode);
  }, [isDarkMode]);

  useEffect(() => {
    localStorage.setItem(THEME_STORAGE_KEY, themePreference);
  }, [themePreference]);

  function toggleTheme() {
    setThemePreference((prev) => (prev === 'dark' ? 'light' : 'dark'));
  }

  async function loadProjects() {
    const data = await api.projects();
    const list = Array.isArray(data) ? data : [];
    setProjects(list);

    if (!list.length) {
      setSelectedProjectId('');
      localStorage.removeItem(SELECTED_PROJECT_KEY);
      return;
    }

    const currentProjectId = localStorage.getItem(SELECTED_PROJECT_KEY) || '';
    const exists = list.some((project) => project._id === currentProjectId);
    const fallbackProjectId = list[0]?._id || '';
    const nextProjectId = exists ? currentProjectId : fallbackProjectId;

    setSelectedProjectId(nextProjectId);
    if (nextProjectId) localStorage.setItem(SELECTED_PROJECT_KEY, nextProjectId);
  }

  async function refreshCatalog() {
    const [c, v] = await Promise.all([api.categories(), api.vendors()]);
    setCats(dedupeCategories(Array.isArray(c) ? c : []));
    setVendors(dedupeVendors(Array.isArray(v) ? v : []));
  }

  async function invalidateData() {
    await refreshCatalog();
    setDataVersion((prev) => prev + 1);
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

    loadProjects().catch(() => {
      setProjects([]);
    });
  }, [session.token]);

  // When switching projects, refresh vendors (and other catalogs) immediately.
  useEffect(() => {
    if (!session.token) return;
    if (!selectedProjectId) return;
    setCats([]);
    setVendors([]);
    refreshCatalog().catch(() => {});
  }, [session.token, selectedProjectId]);

  function handleProjectChange(nextProjectId) {
    setCats([]);
    setVendors([]);
    setSelectedProjectId(nextProjectId);
    if (nextProjectId) {
      localStorage.setItem(SELECTED_PROJECT_KEY, nextProjectId);
      return;
    }
    localStorage.removeItem(SELECTED_PROJECT_KEY);
  }

  function logout() {
    clearSession();
    setSession(getSession());
    setTab('dashboard');
  }

  if (!session.token) return <Login onLogin={setSession} />;

  return (
    <>
      <Nav
        tab={tab}
        setTab={setTab}
        role={session.role}
        username={session.username}
        displayName={session.displayName}
        onLogout={logout}
        isDarkMode={isDarkMode}
        onToggleTheme={toggleTheme}
        projects={projects}
        selectedProjectId={selectedProjectId}
        onProjectChange={handleProjectChange}
      />

      <div className="container grid" style={{ gap: 14 }}>
        {toast && <div className="card">{toast}</div>}

        {tab === 'dashboard' && (
          <Dashboard isAdmin={isAdmin} selectedProjectId={selectedProjectId} refreshKey={dataVersion} />
        )}

        {tab === 'transactions' && (
          <Transactions
            isAdmin={isAdmin}
            cats={cats}
            vendors={vendors}
            onCatalogChanged={refreshCatalog}
            onTransactionsChanged={invalidateData}
            selectedProjectId={selectedProjectId}
          />
        )}

        {tab === 'search' && <SearchTransactions cats={cats} vendors={vendors} selectedProjectId={selectedProjectId} />}

        {tab === 'settings' && (
          <Settings
            isAdmin={isAdmin}
            cats={cats}
            vendors={vendors}
            selectedProjectId={selectedProjectId}
            onProjectCreated={loadProjects}
            onCatalogChanged={async () => {
              await refreshCatalog();
              setToast('Catálogo actualizado');
            }}
          />
        )}
      </div>
    </>
  );
}

function Settings({ isAdmin, cats, vendors, selectedProjectId, onCatalogChanged, onProjectCreated }) {
  const [section, setSection] = useState('catalog');

  return (
    <div className="grid" style={{ gap: 14 }}>
      <div className="card" style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        <button type="button" className={section === 'catalog' ? '' : 'secondary'} onClick={() => setSection('catalog')}>
          Catálogo
        </button>
        <button
          type="button"
          className={section === 'import-sap' ? '' : 'secondary'}
          onClick={() => setSection('import-sap')}
          disabled={!isAdmin}
          title={!isAdmin ? 'Solo disponible para administradores' : undefined}
        >
          Subir CSV
        </button>
        <button
          type="button"
          className={section === 'sap-latest' ? '' : 'secondary'}
          onClick={() => setSection('sap-latest')}
          disabled={!isAdmin}
          title={!isAdmin ? 'Solo disponible para administradores' : undefined}
        >
          SAP Import
        </button>
        <button
          type="button"
          className={section === 'supplier-category2' ? '' : 'secondary'}
          onClick={() => setSection('supplier-category2')}
          disabled={!isAdmin}
          title={!isAdmin ? 'Solo disponible para administradores' : undefined}
        >
          Proveedor → Categoría 2
        </button>
        {isAdmin && (
          <button
            type="button"
            className={section === 'projects' ? '' : 'secondary'}
            onClick={() => setSection('projects')}
          >
            Agregar proyecto
          </button>
        )}
        {isAdmin && (
          <button
            type="button"
            className={section === 's3-prefix' ? '' : 'secondary'}
            onClick={() => setSection('s3-prefix')}
          >
            Crear carpeta S3
          </button>
        )}
        <button
          type="button"
          className={section === 'raw-data' ? '' : 'secondary'}
          onClick={() => setSection('raw-data')}
          disabled={!isAdmin}
          title={!isAdmin ? 'Solo disponible para administradores' : undefined}
        >
          Raw data
        </button>
      </div>

      {section === 'catalog' && <Catalog isAdmin={isAdmin} cats={cats} vendors={vendors} onChanged={onCatalogChanged} />}

      {section === 'import-sap' &&
        (isAdmin ? (
          <ImportSapScreen />
        ) : (
          <div className="card">Solo los administradores pueden importar pagos SAP.</div>
        ))}

      {section === 'sap-latest' &&
        (isAdmin ? (
          <SapLatestImportSection selectedProjectId={selectedProjectId} />
        ) : (
          <div className="card">Solo los administradores pueden ejecutar el import SAP latest.</div>
        ))}

      {section === 'supplier-category2' &&
        (isAdmin ? (
          <SupplierCategory2Assignment cats={cats} selectedProjectId={selectedProjectId} />
        ) : (
          <div className="card">Solo los administradores pueden asignar categoría por proveedor.</div>
        ))}

      {section === 'projects' && isAdmin && <AdminProjectCreateSection onProjectCreated={onProjectCreated} />}

      {section === 's3-prefix' && isAdmin && <AdminS3PrefixCreateSection />}

      {section === 'raw-data' &&
        (isAdmin ? <RawDataAdmin /> : <div className="card">Solo los administradores pueden ver raw data.</div>)}
    </div>
  );
}

function SapLatestImportSection({ selectedProjectId }) {
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState(null);
  const [sources, setSources] = useState({ IVA: false, EFECTIVO: false });

  function toggleSource(sourceKey) {
    setSources((prev) => ({ ...prev, [sourceKey]: !prev[sourceKey] }));
  }

  async function onImportNow() {
    setError('');
    setResult(null);
    if (!selectedProjectId) {
      setError('Selecciona un proyecto activo antes de ejecutar el import.');
      return;
    }

    const selectedSources = Object.entries(sources)
      .filter(([, value]) => Boolean(value))
      .map(([key]) => key);

    setImporting(true);
    try {
      const response = await api.adminImportSapLatest({
        projectId: selectedProjectId,
        sources: selectedSources,
      });
      setResult(response);
    } catch (e) {
      setError(e.message || 'No se pudo ejecutar el import SAP latest.');
    } finally {
      setImporting(false);
    }
  }

  const rows = [
    ['IVA', result?.iva],
    ['EFECTIVO', result?.efectivo],
  ];

  return (
    <div className="card grid" style={{ gap: 12 }}>
      <div>
        <h3 style={{ margin: 0 }}>SAP Import</h3>
        <div className="small">Ejecuta manualmente el import de latest CSV (IVA/EFECTIVO).</div>
      </div>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <input type="checkbox" checked={sources.IVA} onChange={() => toggleSource('IVA')} disabled={importing} /> IVA
        </label>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <input
            type="checkbox"
            checked={sources.EFECTIVO}
            onChange={() => toggleSource('EFECTIVO')}
            disabled={importing}
          />{' '}
          EFECTIVO
        </label>
      </div>

      <div>
        <button type="button" onClick={onImportNow} disabled={importing || !selectedProjectId}>
          {importing ? 'Importando...' : 'Importar latest ahora'}
        </button>
      </div>

      {error && <div className="small" style={{ color: '#b00020' }}>{error}</div>}

      {result && (
        <div style={{ overflowX: 'auto' }}>
          <table>
            <thead>
              <tr>
                <th>Fuente</th>
                <th>already_imported</th>
                <th>importRunId</th>
                <th>etag</th>
                <th>lastModified</th>
                <th>contentLength</th>
                <th>rowsOk</th>
                <th>rowsError</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(([label, bucket]) => (
                <tr key={label}>
                  <td>{label}</td>
                  <td>{String(bucket?.already_imported ?? '')}</td>
                  <td>{bucket?.importRunId || ''}</td>
                  <td>{bucket?.etag || ''}</td>
                  <td>{bucket?.lastModified || ''}</td>
                  <td>{bucket?.contentLength ?? ''}</td>
                  <td>{bucket?.rowsOk ?? ''}</td>
                  <td>{bucket?.rowsError ?? ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function SupplierCategory2Assignment({ cats, selectedProjectId }) {
  const [suppliers, setSuppliers] = useState([]);
  const [supplierId, setSupplierId] = useState('');
  const [categoryCode, setCategoryCode] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError('');
    setSuccess('');
    api.suppliers()
      .then((rows) => {
        if (!active) return;
        const list = Array.isArray(rows) ? rows : [];
        setSuppliers(list);
        setSupplierId((prev) => prev || String(list[0]?._id || list[0]?.id || ''));
      })
      .catch((e) => {
        if (!active) return;
        setError(e.message || 'No se pudieron cargar proveedores.');
      })
      .finally(() => {
        if (active) setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [selectedProjectId]);

  async function onApply(event) {
    event.preventDefault();
    setError('');
    setSuccess('');
    if (!selectedProjectId) return setError('Selecciona un proyecto para continuar.');
    if (!supplierId || !categoryCode) return setError('Selecciona proveedor y categoría 2.');

    const selectedCategory = cats.find((cat) => String(cat.code || cat.id) === String(categoryCode));
    setSaving(true);
    try {
      const result = await api.assignCategory2BySupplier(
        selectedProjectId,
        supplierId,
        categoryCode,
        selectedCategory?.name || selectedCategory?.nombre || categoryCode,
      );
      setSuccess(`Categoría 2 aplicada. Movimientos actualizados: ${result?.modified ?? 0}.`);
    } catch (e) {
      setError(e.message || 'No se pudo aplicar la categoría.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="card">
      <h3 style={{ marginTop: 0 }}>Asignar categoría 2 manual por proveedor</h3>
      <div className="small" style={{ marginBottom: 10 }}>
        Esta acción aplica la categoría 2 seleccionada a todos los egresos del proveedor en el proyecto activo.
      </div>
      <form className="grid" onSubmit={onApply}>
        <div>
          <label>Proveedor</label>
          <select value={supplierId} onChange={(e) => setSupplierId(e.target.value)} disabled={loading || !suppliers.length}>
            {!suppliers.length && <option value="">Sin proveedores</option>}
            {suppliers.map((supplier) => (
              <option key={supplier._id || supplier.id} value={supplier._id || supplier.id}>
                {supplier.name || supplier.nombre || supplier.cardCode || supplier._id}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label>Categoría 2</label>
          <select value={categoryCode} onChange={(e) => setCategoryCode(e.target.value)} disabled={!cats.length}>
            <option value="">Selecciona una categoría</option>
            {cats.map((category) => (
              <option key={category.id || category.code} value={category.code || category.id}>
                {category.name}
              </option>
            ))}
          </select>
        </div>
        {error && <div>{error}</div>}
        {success && <div>{success}</div>}
        <button type="submit" disabled={saving || loading || !suppliers.length || !cats.length}>
          {saving ? 'Aplicando...' : 'Aplicar a todos los egresos del proveedor'}
        </button>
      </form>
    </div>
  );
}

function AdminS3PrefixCreateSection() {
  const [slug, setSlug] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [createdPath, setCreatedPath] = useState('');

  async function onSubmit(event) {
    event.preventDefault();
    setSaving(true);
    setError('');
    setCreatedPath('');

    try {
      const response = await api.createS3PrefixAdmin({ slug });
      const bucket = response?.bucket || 'calderon-sap-exports';
      const prefix = response?.prefix || '';
      setCreatedPath(`s3://${bucket}/${prefix}`);
      setSlug('');
    } catch (e) {
      setError(e.message || 'No se pudo crear la carpeta en S3');
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="card">
      <h3 style={{ marginTop: 0 }}>Crear carpeta S3</h3>
      <form className="grid" onSubmit={onSubmit}>
        <div>
          <label>Slug</label>
          <input
            value={slug}
            onChange={(e) => setSlug(e.target.value)}
            placeholder="ej. colima"
            required
          />
        </div>
        {error && <div>{error}</div>}
        {createdPath && <div>Creado: <code>{createdPath}</code></div>}
        <button type="submit" disabled={saving}>{saving ? 'Creando...' : 'Crear carpeta en S3'}</button>
      </form>
    </div>
  );
}

function AdminProjectCreateSection({ onProjectCreated }) {
  const [name, setName] = useState('');
  const [slug, setSlug] = useState('');
  const [s3Prefix, setS3Prefix] = useState('');
  const [slugTouched, setSlugTouched] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [created, setCreated] = useState(null);

  function onNameChange(nextName) {
    setName(nextName);
    if (!slugTouched) {
      const generatedSlug = normalizeSlug(nextName);
      setSlug(generatedSlug);
      if (!s3Prefix.trim()) setS3Prefix(generatedSlug ? `exports/${generatedSlug}` : '');
    }
  }

  function onSlugChange(nextSlug) {
    const cleanSlug = normalizeSlug(nextSlug);
    setSlugTouched(true);
    setSlug(cleanSlug);
  }

  function onS3PrefixBlur() {
    setS3Prefix((current) => normalizeS3Prefix(current, slug));
  }

  async function onSubmit(event) {
    event.preventDefault();
    setSaving(true);
    setError('');
    setCreated(null);

    const payload = {
      name: name.trim(),
      slug: normalizeSlug(slug),
      s3Prefix: normalizeS3Prefix(s3Prefix, slug),
    };

    try {
      const response = await api.createProjectAdmin(payload);
      setCreated(response);
      setName('');
      setSlug('');
      setS3Prefix('');
      setSlugTouched(false);
      await onProjectCreated?.();
    } catch (e) {
      if (e?.status === 409) {
        setError('Ya existe un proyecto con ese nombre o slug');
      } else {
        setError(e.message || 'No se pudo crear el proyecto');
      }
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="card">
      <h3 style={{ marginTop: 0 }}>Agregar proyecto</h3>
      <form className="grid" onSubmit={onSubmit}>
        <div>
          <label>Nombre</label>
          <input value={name} onChange={(e) => onNameChange(e.target.value)} required />
        </div>
        <div>
          <label>Slug</label>
          <input value={slug} onChange={(e) => onSlugChange(e.target.value)} required />
        </div>
        <div>
          <label>S3 Prefix</label>
          <input
            placeholder="exports/&lt;slug&gt;"
            value={s3Prefix}
            onChange={(e) => setS3Prefix(e.target.value)}
            onBlur={onS3PrefixBlur}
            required
          />
        </div>
        {error && <div>{error}</div>}
        {created?.projectId && (
          <div>
            Proyecto creado. ID: <code>{created.projectId}</code>
          </div>
        )}
        <button type="submit" disabled={saving}>{saving ? 'Creando...' : 'Crear proyecto'}</button>
      </form>
    </div>
  );
}

function RawDataAdmin() {
  const [collections, setCollections] = useState([]);
  const [collection, setCollection] = useState('');
  const [fields, setFields] = useState([]);
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [savingRow, setSavingRow] = useState('');

  useEffect(() => {
    let active = true;
    api
      .adminRawCollections()
      .then((data) => {
        if (!active) return;
        const items = Array.isArray(data?.collections) ? data.collections : [];
        setCollections(items);
        if (items.length) setCollection(items[0]);
      })
      .catch((e) => {
        if (!active) return;
        setError(e.message || 'No se pudieron cargar las colecciones.');
      });

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!collection) return;
    let active = true;
    setLoading(true);
    setError('');

    api
      .adminRawRows(collection, { limit: 200 })
      .then((data) => {
        if (!active) return;
        setFields(Array.isArray(data?.fields) ? data.fields : []);
        setRows(Array.isArray(data?.rows) ? data.rows : []);
      })
      .catch((e) => {
        if (!active) return;
        setFields([]);
        setRows([]);
        setError(e.message || 'No se pudo cargar la colección.');
      })
      .finally(() => {
        if (active) setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [collection]);

  function editCell(rowId, field) {
    const row = rows.find((item) => item.id === rowId);
    if (!row) return;

    const current = row[field];
    const nextRaw = window.prompt(`Editar ${field} (valor JSON):`, JSON.stringify(current ?? null));
    if (nextRaw === null) return;

    let parsed;
    try {
      parsed = JSON.parse(nextRaw);
    } catch {
      window.alert('Valor inválido. Debe ser JSON válido, por ejemplo: "texto", 123, true, null o {"a":1}.');
      return;
    }

    setSavingRow(rowId);
    api
      .adminRawUpdateRow(collection, rowId, { [field]: parsed })
      .then((updatedRow) => {
        setRows((prev) => prev.map((item) => (item.id === rowId ? updatedRow : item)));
      })
      .catch((e) => {
        window.alert(e.message || 'No se pudo actualizar el campo.');
      })
      .finally(() => setSavingRow(''));
  }

  return (
    <div className="card" style={{ overflowX: 'auto' }}>
      <h3 style={{ marginTop: 0 }}>Raw data (solo admin)</h3>
      <div className="row" style={{ alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <label htmlFor="raw-data-collection">Colección:</label>
        <select id="raw-data-collection" value={collection} onChange={(e) => setCollection(e.target.value)}>
          {collections.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
      </div>

      {error && <div className="small" style={{ color: '#b00020', marginBottom: 8 }}>{error}</div>}
      {loading ? (
        <div>Cargando...</div>
      ) : !rows.length ? (
        <div className="small">No hay filas para mostrar.</div>
      ) : (
        <table>
          <thead>
            <tr>
              {fields.map((field) => (
                <th key={field}>{field}</th>
              ))}
              <th>Acciones</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                {fields.map((field) => (
                  <td key={`${row.id}-${field}`} style={{ maxWidth: 260 }}>
                    <code style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                      {JSON.stringify(row[field] ?? null)}
                    </code>
                  </td>
                ))}
                <td>
                  {savingRow === row.id ? (
                    <span className="small">Guardando…</span>
                  ) : (
                    <select defaultValue="" onChange={(e) => e.target.value && editCell(row.id, e.target.value)}>
                      <option value="" disabled>
                        Editar campo…
                      </option>
                      {fields
                        .filter((field) => field !== 'id')
                        .map((field) => (
                          <option key={`${row.id}-action-${field}`} value={field}>
                            {field}
                          </option>
                        ))}
                    </select>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

/* ================= DASHBOARD ================= */
function Dashboard({ isAdmin, selectedProjectId, refreshKey }) {
  const [stats, setStats] = useState(null);
  const [supplierSummary, setSupplierSummary] = useState([]);
  const [supplierSummaryError, setSupplierSummaryError] = useState('');
  const [viewMode, setViewMode] = useState('experimental');
  const [showCategoryIva, setShowCategoryIva] = useState(false);
  const [showSupplierIva, setShowSupplierIva] = useState(false);
  const [supplierSortMode, setSupplierSortMode] = useState('alpha');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let ok = true;
    setLoading(true);

    Promise.allSettled([
      api.spendByCategory({ include_iva: showCategoryIva ? 'true' : 'false' }),
      api.expensesSummaryBySupplier({ include_iva: showSupplierIva ? 'true' : 'false' }),
    ]).then(([categoryResult, supplierResult]) => {
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

    return () => {
      ok = false;
    };
  }, [showCategoryIva, showSupplierIva, selectedProjectId, refreshKey]);

  const supplierTotal = supplierSummary.reduce((acc, row) => acc + (Number(row.totalAmount) || 0), 0);
  const sortedSupplierSummary = useMemo(() => {
    const rows = [...supplierSummary];
    if (supplierSortMode === 'amount') {
      return rows.sort((a, b) => {
        const amountDiff = (Number(b.totalAmount) || 0) - (Number(a.totalAmount) || 0);
        if (amountDiff !== 0) return amountDiff;
        return (a.supplierName || '').localeCompare(b.supplierName || '', 'es');
      });
    }

    return rows.sort((a, b) => (a.supplierName || '').localeCompare(b.supplierName || '', 'es'));
  }, [supplierSummary, supplierSortMode]);
  const categoryRows = Array.isArray(stats?.rows) ? stats.rows : [];
  const topCategories = useMemo(
    () => [...categoryRows].sort((a, b) => (Number(b.amount) || 0) - (Number(a.amount) || 0)).slice(0, 6),
    [categoryRows]
  );
  const chartPoints = useMemo(() => {
    if (!categoryRows.length) return '';
    const sorted = [...categoryRows].sort((a, b) => (Number(b.amount) || 0) - (Number(a.amount) || 0)).slice(0, 10);
    const maxAmount = Math.max(...sorted.map((row) => Number(row.amount) || 0), 1);
    return sorted
      .map((row, index) => {
        const x = sorted.length === 1 ? 0 : (index / (sorted.length - 1)) * 100;
        const y = 100 - ((Number(row.amount) || 0) / maxAmount) * 80;
        return `${x},${Math.max(4, y)}`;
      })
      .join(' ');
  }, [categoryRows]);
  const biggestCategory = topCategories[0];
  const allocatedPercent = Math.min(
    100,
    Math.max(0, topCategories.reduce((acc, row) => acc + (Number(row.percent) || 0), 0))
  );

  const subtitle =
    viewMode === 'supplier'
      ? 'Totales agrupados por proveedor (SAP).'
      : viewMode === 'experimental'
        ? 'Vista visual experimental de categorías.'
        : 'Porcentaje = gasto de la categoría / total de egresos';

  const renderCategorySummaryHeader = () => (
    <div className="row" style={{ justifyContent: 'space-between' }}>
      <div className="badge">Total egresos {showCategoryIva ? 'con IVA' : 'sin IVA'}: ${formatMoney(stats.total_expenses || 0)}</div>
      {isAdmin && (
        <button
          className="secondary"
          onClick={async () => {
            const confirmed = window.confirm('Esto solo agrega faltantes, no borra');
            if (!confirmed) return;
            try {
              await api.seed();
              location.reload();
            } catch (_) {
              // Intencionalmente silencioso para mantener el comportamiento previo.
            }
          }}
        >
          Seed categorías
        </button>
      )}
    </div>
  );

  let dashboardContent = <div style={{ padding: '12px 0' }}>No hay egresos aún. Registra uno para ver el dashboard.</div>;

  if (loading) {
    dashboardContent = <div style={{ padding: '12px 0' }}>Cargando...</div>;
  } else if (viewMode === 'supplier') {
    if (supplierSummaryError) {
      dashboardContent = <div style={{ padding: '12px 0' }}>Error: {supplierSummaryError}</div>;
    } else if (supplierSummary.length) {
      dashboardContent = (
        <div style={{ marginTop: 12 }} className="grid">
          <div className="badge">Total egresos SAP {showSupplierIva ? 'con IVA' : 'sin IVA'}: ${formatMoney(supplierTotal)}</div>
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
                {sortedSupplierSummary.map((row) => (
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
      );
    } else {
      dashboardContent = <div style={{ padding: '12px 0' }}>No hay egresos SAP agrupados por proveedor para mostrar.</div>;
    }
  } else if (stats?.error) {
    dashboardContent = <div style={{ padding: '12px 0' }}>Error: {stats.error}</div>;
  } else if (categoryRows.length) {
    if (viewMode === 'experimental') {
      dashboardContent = (
        <div style={{ marginTop: 12 }} className="dashboard-experimental">
          <div className="dashboard-kpi-grid">
            <div className="dashboard-kpi-card">
              <div className="dashboard-kpi-icon">💰</div>
              <strong>${formatMoney(stats.total_expenses || 0)}</strong>
              <span>Total egresos {showCategoryIva ? 'con IVA' : 'sin IVA'}</span>
            </div>
            <div className="dashboard-kpi-card">
              <div className="dashboard-kpi-icon">📊</div>
              <strong>{categoryRows.length}</strong>
              <span>Categorías con movimiento</span>
            </div>
            <div className="dashboard-kpi-card">
              <div className="dashboard-kpi-icon">🏷️</div>
              <strong>{biggestCategory?.category_name || 'Sin datos'}</strong>
              <span>Mayor categoría (${formatMoney(biggestCategory?.amount || 0)})</span>
            </div>
          </div>

          <div className="dashboard-experimental-grid">
            <section className="dashboard-panel">
              <h3>Comportamiento por categoría</h3>
              <svg
                viewBox="0 0 100 100"
                preserveAspectRatio="none"
                className="dashboard-line-chart"
                role="img"
                aria-label="Tendencia de categorías por monto"
              >
                <polyline fill="none" stroke="#1f4d96" strokeWidth="2.5" points={chartPoints} />
              </svg>
              <div className="small">Visual experimental para comparar magnitudes entre categorías.</div>
            </section>

            <section className="dashboard-panel dashboard-gauge-panel">
              <h3>Distribución top categorías</h3>
              <div
                className="dashboard-gauge"
                style={{
                  background: `conic-gradient(#1f4d96 0deg ${(allocatedPercent / 100) * 360}deg, #e2e8f0 ${(allocatedPercent / 100) * 360}deg 360deg)`,
                }}
              >
                <span>{allocatedPercent.toFixed(1)}%</span>
              </div>
              <div className="small">Participación acumulada de las 6 categorías principales.</div>
            </section>

            <section className="dashboard-panel">
              <h3>Avance por categoría</h3>
              <div className="grid">
                {topCategories.map((r) => {
                  const percent = Number(r.percent) || 0;
                  const fillWidth = Math.max(0, Math.min(100, percent));
                  return (
                    <div key={r.category_id} style={{ display: 'grid', gap: 4 }}>
                      <div className="row" style={{ justifyContent: 'space-between' }}>
                        <div style={{ fontWeight: 700 }}>{r.category_name}</div>
                        <div className="small">{percent.toFixed(2)}%</div>
                      </div>
                      <div className="bar" aria-label={`Barra de avance de ${r.category_name}`}>
                        <div style={{ width: fillWidth + '%' }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>

            <section className="dashboard-panel">
              <h3>Montos principales</h3>
              <div className="dashboard-column-chart">
                {topCategories.slice(0, 5).map((r) => {
                  const amount = Number(r.amount) || 0;
                  const maxAmount = Number(biggestCategory?.amount) || 1;
                  return (
                    <div key={`column-${r.category_id}`} className="dashboard-column-item">
                      <div className="dashboard-column-value">${formatMoney(amount)}</div>
                      <div className="dashboard-column-track">
                        <div className="dashboard-column-fill" style={{ height: `${Math.max(12, (amount / maxAmount) * 100)}%` }} />
                      </div>
                      <div className="dashboard-column-label">{r.category_name}</div>
                    </div>
                  );
                })}
              </div>
            </section>
          </div>

          {renderCategorySummaryHeader()}
        </div>
      );
    } else {
      dashboardContent = (
        <div style={{ marginTop: 12 }} className="grid">
          {renderCategorySummaryHeader()}
          {categoryRows.map((r) => {
            const percent = Number(r.percent) || 0;
            const fillWidth = Math.max(0, Math.min(100, percent));

            return (
              <div key={r.category_id} style={{ display: 'grid', gap: 6 }}>
                <div className="row" style={{ justifyContent: 'space-between' }}>
                  <div style={{ fontWeight: 700 }}>{r.category_name}</div>
                  <div>
                    ${formatMoney(r.amount)} <span className="small">({percent.toFixed(2)}%)</span>
                  </div>
                </div>
                <div className="bar" aria-label={`Barra de avance de ${r.category_name}`}>
                  <div style={{ width: fillWidth + '%' }}>
                    <span>{percent.toFixed(2)}%</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      );
    }
  }

  return (
    <div className="card">
      <h2 style={{ margin: '0 0 8px' }}>Dashboard de egresos</h2>
      <div className="row" style={{ gap: 8, marginBottom: 8 }}>
        <button className={viewMode === 'experimental' ? '' : 'secondary'} onClick={() => setViewMode('experimental')}>
          Vista experimental
        </button>
        <button className={viewMode === 'category' ? '' : 'secondary'} onClick={() => setViewMode('category')}>
          Por categoría
        </button>
        <button className={viewMode === 'supplier' ? '' : 'secondary'} onClick={() => setViewMode('supplier')}>
          Por proveedor
        </button>
      </div>
      <div className="small">{subtitle}</div>

      {viewMode === 'supplier' ? (
        <div className="row" style={{ marginTop: 8 }}>
          <label className="small" style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
            <input type="checkbox" checked={showSupplierIva} onChange={(e) => setShowSupplierIva(e.target.checked)} />
            Mostrar IVA
          </label>
          <label className="small" style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
            <input
              type="checkbox"
              checked={supplierSortMode === 'amount'}
              onChange={(e) => setSupplierSortMode(e.target.checked ? 'amount' : 'alpha')}
            />
            Ordenar por monto (mayor a menor)
          </label>
        </div>
      ) : (
        <label className="small" style={{ display: 'inline-flex', alignItems: 'center', gap: 8, marginTop: 8 }}>
          <input type="checkbox" checked={showCategoryIva} onChange={(e) => setShowCategoryIva(e.target.checked)} />
          Mostrar IVA
        </label>
      )}

      {dashboardContent}
    </div>
  );
}


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
  const [sourceDb, setSourceDb] = useState('EFECTIVO');
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
        sourceDb,
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
                    {c.displayLabel || c.name}
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
        <div>
          <label>Base</label>
          <select value={sourceDb} onChange={(e) => setSourceDb(e.target.value)}>
            <option value="IVA">IVA</option>
            <option value="EFECTIVO">EFECTIVO</option>
          </select>
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
function Transactions({ isAdmin, cats, vendors, onCatalogChanged, onTransactionsChanged, selectedProjectId }) {
  const UNCATEGORIZED_FILTER = '__UNCATEGORIZED__';
  const getTransactionStableKey = (tx) =>
    tx?._id ?? tx?.id ?? `${tx?.sourceDb || tx?.source || ''}|${tx?.sap?.pagoNum || ''}|${tx?.sap?.facturaNum || ''}|${tx?.sap?.montoAplicado || ''}`;
  const dedupeTransactions = (items) => Array.from(new Map((Array.isArray(items) ? items : []).map((tx) => [getTransactionStableKey(tx), tx])).values());
  const catMap = useMemo(() => Object.fromEntries(cats.map((c) => [c.id, c.name])), [cats]);
  const vendorMap = useMemo(() => Object.fromEntries(vendors.map((v) => [v.id, v.name])), [vendors]);

  const [rows, setRows] = useState([]);
  const [serverTotals, setServerTotals] = useState(null);
  const [editing, setEditing] = useState(null);
  const [filter, setFilter] = useState('ALL');
  const [categoryFilter, setCategoryFilter] = useState('ALL');
  const [sapCategoryFilter, setSapCategoryFilter] = useState('ALL');
  const [supplierFilter, setSupplierFilter] = useState('ALL');
  const [sourceDbFilter, setSourceDbFilter] = useState('ALL');
  const [searchFilter, setSearchFilter] = useState('');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [sortBy, setSortBy] = useState('date_desc');
  const [page, setPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [limit] = useState(50);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');
  const [newCategoryName, setNewCategoryName] = useState('');
  const [editErr, setEditErr] = useState('');
  const [savingCategory, setSavingCategory] = useState(false);
  const [selectedRows, setSelectedRows] = useState([]);
  const [bulkCategoryId, setBulkCategoryId] = useState('');
  const [bulkSaving, setBulkSaving] = useState(false);

  const isUncategorizedFilter = categoryFilter === UNCATEGORIZED_FILTER;
  const isSapIvaTransaction = (transaction) =>
    transaction?.source === 'sap' && String(transaction?.sourceDb || '').trim().toUpperCase() === 'IVA';

  async function load(targetPage = page) {
    setLoading(true);
    setErr('');
    try {
      const response = await api.transactions({
        page: String(targetPage),
        limit: String(limit),
        type: filter === 'ALL' ? '' : filter,
        category_id: '',
        supplierId: supplierFilter === 'ALL' ? '' : supplierFilter,
        sourceDb: sourceDbFilter === 'ALL' ? '' : sourceDbFilter,
        q: searchFilter.trim(),
        from: dateFrom,
        to: dateTo,
      });

      setRows(dedupeTransactions(response?.items));
      setServerTotals(response?.totals || null);
      setTotalCount(Number(response?.totalCount) || 0);
      setPage(Number(response?.page) || targetPage);
    } catch (e) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setPage(1);
    setRows([]);
    load(1);
  }, [filter, supplierFilter, sourceDbFilter, searchFilter, dateFrom, dateTo, selectedProjectId]);

  useEffect(() => {
    setSelectedRows([]);
  }, [rows, sortBy]);

  const supplierOptions = useMemo(() => {
    const byVendorId = new Map();

    vendors.forEach((vendor) => {
      const value = String(vendor?._id || vendor?.id || '').trim();
      if (!value || byVendorId.has(value)) return;
      const name = String(vendor?.name || '').trim().replace(/\s+/g, ' ');
      if (!name) return;
      const cardCode = String(vendor?.supplierCardCode || vendor?.externalIds?.sapCardCode || '').trim();
      const label = `${name} (${cardCode || 'Sin CardCode'})`;
      byVendorId.set(value, label);
    });

    return Array.from(byVendorId.entries()).sort((a, b) => a[1].localeCompare(b[1], 'es', { sensitivity: 'base' }));
  }, [vendors]);

  useEffect(() => {
    setSupplierFilter('ALL');
  }, [selectedProjectId]);

  const sapCategoryOptions = useMemo(() => {
    const map = new Map();
    cats
      .filter((c) => c?.source === 'sap')
      .forEach((c) => {
        const value = String(c?.code || c?.name || '').trim();
        if (!value || map.has(value)) return;
        map.set(value, c?.displayLabel || c?.name || value);
      });
    rows.forEach((row) => {
      const value = String(row?.categoryHintCode || row?.categoryHintName || '').trim();
      if (!value || map.has(value)) return;
      map.set(value, row?.categoryHintName || row?.categoryHintCode || value);
    });
    return Array.from(map.entries()).sort((a, b) => a[1].localeCompare(b[1], 'es', { sensitivity: 'base' }));
  }, [cats, rows]);

  const shown = rows
    .filter((row) => {
      if (categoryFilter === 'ALL') return true;
      if (categoryFilter === UNCATEGORIZED_FILTER) return getTransactionCategoryLabel(row, catMap) === 'Sin categoría';
      return (row.categoryEffectiveCode || row.categoryEffectiveName || row.category_id || row.categoryId) === categoryFilter;
    })
    .filter((row) => {
      const query = searchFilter.trim().toLowerCase();
      if (!query) return true;
      const searchableFields = [
        row.description,
        row.concept,
        row.proveedorNombre,
        row.supplierName,
        row.proveedor?.name,
        getTransactionCategoryLabel(row, catMap),
        getCategoryHintCode(row),
      ];
      return searchableFields.some((field) => String(field || '').toLowerCase().includes(query));
    })
    .sort((a, b) => {
    if (sortBy === 'created_desc') {
      const aCreatedAt = a.created_at || '';
      const bCreatedAt = b.created_at || '';
      if (aCreatedAt !== bCreatedAt) return bCreatedAt.localeCompare(aCreatedAt);

      if (a.date === b.date) return 0;
      return b.date.localeCompare(a.date);
    }

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
    const isSapIva = isSapIvaTransaction(editing);
    const payload = isSapIva
      ? { categoryManualCode: editing.categoryManualCode ?? '', categoryManualName: editing.categoryManualName ?? '' }
      : {
        date: editing.date,
        amount: parseMoneyInput(editing.amount),
        description: editing.description,
        categoryManualCode: editing.categoryManualCode || '',
        categoryManualName: editing.categoryManualName || '',
      };
    if (isSapIva) {
      await api.updateProjectTransaction(selectedProjectId, editing.id, payload);
    } else {
      await api.updateTransaction(editing.id, payload);
    }
    await onTransactionsChanged?.();
    setEditing(null);
    setNewCategoryName('');
    load(page);
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
      await onTransactionsChanged?.();
      setEditing((prev) => (prev ? { ...prev, categoryManualCode: created.code || created.id, categoryManualName: created.name } : prev));
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
      load(page);
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

  const allShownSelected = shown.length > 0 && shown.every((r) => selectedRows.includes(r.id));

  async function applyBulkCategory() {
    if (!selectedRows.length) return;
    setBulkSaving(true);
    try {
      await api.bulkUpdateProjectTransactionCategory(selectedProjectId, {
        ids: selectedRows,
        categoryManualCode: bulkCategoryId,
      categoryManualName: cats.find((c) => (c.code || c.id) === bulkCategoryId)?.name || bulkCategoryId || "",
      });
      await Promise.all([
        onTransactionsChanged?.(),
        load(page),
      ]);
      setSelectedRows([]);
    } finally {
      setBulkSaving(false);
    }
  }

  const backendTotals = {
    expensesGross: Number(serverTotals?.expensesGross ?? 0),
    expensesTax: Number(serverTotals?.expensesTax ?? 0),
    expensesWithoutTax: Number(serverTotals?.expensesWithoutTax ?? 0),
    incomeGross: Number(serverTotals?.incomeGross ?? 0),
    net: Number(serverTotals?.net ?? 0),
  };

  const rangeStart = totalCount === 0 ? 0 : (page - 1) * limit + 1;
  const rangeEnd = Math.min(page * limit, totalCount);

  return (
    <div className="card">
      <div className="row" style={{ justifyContent: 'space-between' }}>
        <h2 style={{ margin: 0 }}>Movimientos</h2>
        <div className="row">
          <input
            type="search"
            placeholder="Buscar en movimientos"
            value={searchFilter}
            onChange={(e) => {
              setPage(1);
              setSearchFilter(e.target.value);
            }}
            style={{ minWidth: 220 }}
          />
          <div className="date-filter-inline">
            <input
              type="date"
              aria-label="Fecha desde"
              value={dateFrom}
              onChange={(e) => {
                setPage(1);
                setDateFrom(e.target.value);
              }}
            />
            <input
              type="date"
              aria-label="Fecha hasta"
              value={dateTo}
              onChange={(e) => {
                setPage(1);
                setDateTo(e.target.value);
              }}
            />
          </div>
          <select value={filter} onChange={(e) => { setPage(1); setFilter(e.target.value); }}>
            <option value="ALL">Todos</option>
            <option value="INCOME">Ingresos</option>
            <option value="EXPENSE">Egresos</option>
          </select>
          <select value={categoryFilter} onChange={(e) => { setPage(1); setCategoryFilter(e.target.value); }}>
            <option value="ALL">Todas las categorías 2</option>
            <option value={UNCATEGORIZED_FILTER}>Sin categoría 2</option>
            {cats.map((c) => (
              <option key={c.id} value={c.code || c.id}>{c.displayLabel || c.name}</option>
            ))}
          </select>
          <select value={supplierFilter} onChange={(e) => { setPage(1); setSupplierFilter(e.target.value); }}>
            <option value="ALL">Todos los proveedores</option>
            {supplierOptions.map(([id, name]) => (
              <option key={id} value={id}>{name}</option>
            ))}
          </select>
          <select value={sourceDbFilter} onChange={(e) => { setPage(1); setSourceDbFilter(e.target.value); }}>
            <option value="ALL">Todas las bases</option>
            <option value="IVA">Base IVA</option>
            <option value="EFECTIVO">Base EFECTIVO</option>
          </select>
          <button
            type="button"
            className={sourceDbFilter === 'IVA' ? '' : 'secondary'}
            onClick={() => {
              setPage(1);
              setSourceDbFilter(sourceDbFilter === 'IVA' ? 'ALL' : 'IVA');
            }}
          >
            Solo IVA
          </button>
          <select value={sortBy} onChange={(e) => setSortBy(e.target.value)}>
            <option value="date_desc">Fecha (más reciente)</option>
            <option value="created_desc">Fecha de añadido (más reciente)</option>
            <option value="supplier_asc">Proveedor (A-Z)</option>
          </select>
          <button className="secondary" onClick={() => load(page)}>Refrescar</button>
        </div>
      </div>

      <div className="small" style={{ marginTop: 8 }}>Mostrando {rangeStart}–{rangeEnd} de {totalCount}</div>

      {isAdmin && (
        <div className="row" style={{ marginTop: 10, justifyContent: 'space-between' }}>
          <div className="small">Seleccionados: {selectedRows.length}</div>
          <div className="row">
            <select value={bulkCategoryId} onChange={(e) => setBulkCategoryId(e.target.value)}>
              <option value="">Sin categoría</option>
              {cats.map((c) => (
                <option key={c.id} value={c.code || c.id}>{c.displayLabel || c.name}</option>
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

      <div className="row" style={{ gap: 10, marginTop: 8, flexWrap: 'wrap' }}>
        <div className="badge">Total egresos sin IVA: ${formatMoney(backendTotals.expensesWithoutTax)}</div>
        <div className="badge">Total IVA: ${formatMoney(backendTotals.expensesTax)}</div>
        <div className="badge">Total egresos con IVA: ${formatMoney(backendTotals.expensesGross)}</div>
      </div>

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
                <th>Base</th>
                <th>Descripción</th>
                <th>Categoría 2</th>
                <th>Categoría SAP</th>
                <th>Proveedor</th>
                <th>Monto</th>
                <th>IVA</th>
                <th>Total Factura</th>
                {isAdmin && <th>Acciones</th>}
                {isAdmin && <th>Seleccionar</th>}
              </tr>
            </thead>
            <tbody>
              {isAdmin && (
                <tr>
                  <td colSpan={isAdmin ? 13 : 11} style={{ textAlign: 'right' }}>
                    <label className="row" style={{ justifyContent: 'flex-end' }}>
                      <input type="checkbox" checked={allShownSelected} onChange={toggleSelectAllShown} />
                      Seleccionar todos (página actual)
                    </label>
                  </td>
                </tr>
              )}
              {shown.map((r) => {
                const isSapIva = isSapIvaTransaction(r);
                return (
                <tr key={getTransactionStableKey(r)}>
                  <td>{r.date}</td>
                  <td>{r.type === 'INCOME' ? 'Ingreso' : 'Egreso'}</td>
                  <td>{r.source === 'sap' ? <span className="badge">SAP</span> : ''}</td>
                  <td>{r.sourceDb ? <span className="badge">{String(r.sourceDb).toUpperCase()}</span> : 'LEGACY/UNKNOWN'}</td>
                  <td>{r.description || r.concept || ''}</td>
                  <td>
                    {getTransactionCategoryLabel(r, catMap)}
                    {r.categoryManualName && <span className="badge" style={{ marginLeft: 6 }}>Manual</span>}
                  </td>
                  <td>{getSapCategoryLabel(r) || '—'}</td>
                  <td>{r.proveedorNombre || r.supplierName || vendorMap[r.vendor_id] || r.proveedor?.name || '—'}</td>
                  <td style={{ fontWeight: 800 }}>${formatMoney(r.subtotal ?? r.amount)}</td>
                  <td style={{ fontWeight: 700 }}>${formatMoney(r.tax?.iva ?? 0)}</td>
                  <td style={{ fontWeight: 700 }}>${formatMoney(r.tax?.totalFactura ?? 0)}</td>
                  {isAdmin && (
                    <td>
                      {(r.source !== 'sap' || isSapIva) && (
                        <button
                          className="secondary"
                          onClick={() => {
                            setEditErr('');
                            setNewCategoryName('');
                            setEditing({ ...r });
                          }}
                        >
                          {isSapIva ? 'Editar' : 'Editar'}
                        </button>
                      )}
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
                );
              })}
            </tbody>
            <tfoot>
              <tr>
                <td colSpan={7} style={{ fontWeight: 700, textAlign: 'right' }}>Sumatoria (dataset filtrado):</td>
                <td style={{ fontWeight: 800 }}>
                  ${formatMoney(backendTotals.incomeGross)} / ${formatMoney(backendTotals.expensesGross)}
                </td>
                <td style={{ fontWeight: 700 }}>—</td>
                <td style={{ fontWeight: 700 }}>—</td>
                {isAdmin && <td style={{ fontWeight: 700 }}>Neto: ${formatMoney(backendTotals.net)}</td>}
                {isAdmin && <td />}
              </tr>
            </tfoot>
          </table>
        </div>
      ) : (
        <div style={{ padding: '12px 0' }}>No hay movimientos.</div>
      )}

      <div className="row" style={{ justifyContent: 'flex-end', marginTop: 12 }}>
        <button className="secondary" onClick={() => load(page - 1)} disabled={page <= 1 || loading}>Anterior</button>
        <button className="secondary" onClick={() => load(page + 1)} disabled={rangeEnd >= totalCount || loading}>Siguiente</button>
      </div>

      {editing && (
        <EditModal
          title={isSapIvaTransaction(editing) ? 'Editar categoría IVA' : 'Editar movimiento'}
          onClose={() => setEditing(null)}
          onSave={saveEdit}
        >
          <div className="grid">
            {!isSapIvaTransaction(editing) && (
              <>
                <label>Fecha</label>
                <input value={editing.date || ''} onChange={(e) => setEditing({ ...editing, date: e.target.value })} />
                <label>Monto</label>
                <input value={editing.amount || ''} onChange={(e) => setEditing({ ...editing, amount: e.target.value })} />
                <label>Descripción</label>
                <input value={editing.description || ''} onChange={(e) => setEditing({ ...editing, description: e.target.value })} />
              </>
            )}
            <label>Categoría</label>
            <select value={editing.categoryManualCode || editing.categoryEffectiveCode || ''} onChange={(e) => {
              const selected = cats.find((c) => (c.code || c.id) === e.target.value);
              setEditing({ ...editing, categoryManualCode: e.target.value || null, categoryManualName: selected?.name || null });
            }}>
              <option value="">Sin categoría</option>
              {cats.map((c) => (
                <option key={c.id} value={c.code || c.id}>{c.displayLabel || c.name}</option>
              ))}
            </select>
            <button
              type="button"
              className="secondary"
              onClick={() => setEditing({ ...editing, categoryManualCode: null, categoryManualName: null })}
            >
              Revertir a SAP
            </button>
            <label>Crear categoría nueva</label>
            <div className="row">
              <input
                placeholder="Ej. Herramientas"
                value={newCategoryName}
                onChange={(e) => setNewCategoryName(e.target.value)}
              />
              <button type="button" className="secondary" onClick={createCategoryFromEdit} disabled={savingCategory}>
                {savingCategory ? 'Creando...' : 'Crear nueva categoría'}
              </button>
            </div>
            {editErr && <div style={{ color: '#b91c1c' }}>{editErr}</div>}
          </div>
        </EditModal>
      )}
    </div>
  );
}

function SearchTransactions({ cats, vendors, selectedProjectId }) {
  const [rows, setRows] = useState([]);
  const [query, setQuery] = useState('');
  const [page, setPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [exportingPdf, setExportingPdf] = useState(false);
  const [error, setError] = useState('');
  const limit = 50;
  const catMap = useMemo(() => Object.fromEntries(cats.map((c) => [c.id, c.name])), [cats]);
  const vendorMap = useMemo(() => Object.fromEntries(vendors.map((v) => [v.id, v.name])), [vendors]);

  useEffect(() => {
    setPage(1);
  }, [query]);

  useEffect(() => {
    setLoading(true);
    setError('');
    api.transactions({
      type: 'EXPENSE',
      page: String(page),
      limit: String(limit),
      q: query.trim(),
    })
      .then((data) => {
        setRows(Array.isArray(data?.items) ? data.items : []);
        setTotalCount(Number(data?.totalCount) || 0);
      })
      .catch((err) => {
        setRows([]);
        setTotalCount(0);
        setError(err?.message || 'No se pudo buscar movimientos');
      })
      .finally(() => setLoading(false));
  }, [page, query, selectedProjectId]);

  const getAmountWithoutIva = (row) => {
    const totalAmount = Number(row.amount) || 0;
    const ivaAmount = Number(row.tax?.iva ?? row.iva);
    const subtotalAmount = Number(row.tax?.subtotal);

    if (Number.isFinite(subtotalAmount)) return subtotalAmount;
    if (Number.isFinite(ivaAmount)) return totalAmount - ivaAmount;
    return totalAmount;
  };

  const filteredTotal = useMemo(
    () => rows.reduce((acc, r) => acc + getAmountWithoutIva(r), 0),
    [rows],
  );
  const rangeStart = totalCount === 0 ? 0 : (page - 1) * limit + 1;
  const rangeEnd = Math.min(page * limit, totalCount);

  function exportSearchResultsToPdf() {
    if (!rows.length) return;
    setExportingPdf(true);

    try {
      let filteredTotalWithoutIva = 0;

      const printableRows = rows
        .map((r) => {
          const provider = r.proveedorNombre || r.supplierName || vendorMap[r.vendor_id] || r.proveedor?.name || '—';
          const concept = r.description || r.concept || '—';
          const category = getTransactionCategoryLabel(r, catMap) || '—';
          const ivaAmount = Number(r.tax?.iva ?? r.iva);
          const hasIva = Number.isFinite(ivaAmount) && Math.abs(ivaAmount) > 0;
          const totalAmount = Number(r.amount) || 0;
          const safeSubtotal = getAmountWithoutIva(r);

          filteredTotalWithoutIva += safeSubtotal;

          const ivaBreakdown = hasIva
            ? `Subtotal: $${formatMoney(safeSubtotal)} + IVA: $${formatMoney(ivaAmount)}`
            : '—';

          return `
            <tr>
              <td>${r.date || '—'}</td>
              <td>${provider}</td>
              <td>${concept}</td>
              <td>${category}</td>
              <td>${ivaBreakdown}</td>
              <td class="amount">$${formatMoney(safeSubtotal)}</td>
            </tr>`;
        })
        .join('');

      const amountSummaryCards = `
                <div class="summary-card"><div class="label">MONTO SIN IVA</div><div class="value">$${formatMoney(filteredTotalWithoutIva)}</div></div>
          `;

      const footerTotalLabel = 'Total filtrado sin IVA';
      const footerTotalAmount = filteredTotalWithoutIva;

      const popup = window.open('', '_blank', 'width=1200,height=800');
      if (!popup) return;

      popup.document.write(`
        <!doctype html>
        <html lang="es">
          <head>
            <meta charset="UTF-8" />
            <title>CALDERON DE LA BARCA - REPORTE DE EGRESOS</title>
            <style>
              :root { --primary:#1f4d96; --primary-dark:#12305f; --soft:#e3ebf8; --gray:#334155; --line:#e2e8f0; }
              * { box-sizing: border-box; }
              body { margin: 0; font-family: 'Segoe UI', Arial, sans-serif; color: var(--gray); background: #f8fafc; }
              .sheet { margin: 24px; border: 1px solid var(--line); border-radius: 16px; overflow: hidden; background: #fff; }
              .header { background: linear-gradient(135deg, var(--primary-dark), var(--primary)); color: #fff; padding: 26px 28px; }
              .header h1 { margin: 0; font-size: 26px; letter-spacing: .02em; }
              .header p { margin: 8px 0 0; font-size: 13px; opacity: .95; }
              .summary { display: flex; gap: 12px; flex-wrap: wrap; padding: 16px 24px; background: var(--soft); border-bottom: 1px solid var(--line); }
              .summary-card { background: #fff; border: 1px solid #cbd5e1; border-radius: 12px; padding: 10px 14px; min-width: 200px; }
              .summary-card .label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: #64748b; }
              .summary-card .value { margin-top: 4px; font-size: 18px; font-weight: 700; color: var(--primary-dark); }
              .table-wrap { padding: 14px 24px 24px; }
              table { width: 100%; border-collapse: collapse; }
              th, td { padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; font-size: 13px; }
              thead th { background: #f1f5f9; color: var(--primary-dark); font-weight: 700; }
              .amount { text-align: right; font-weight: 700; color: var(--primary-dark); }
              .footer-total { text-align: right; padding: 14px 24px 22px; font-size: 18px; font-weight: 800; color: var(--primary-dark); }
              @media print {
                body { background: #fff; }
                .sheet { margin: 0; border: 0; border-radius: 0; }
              }
            </style>
          </head>
          <body>
            <section class="sheet">
              <header class="header">
                <h1>CALDERON DE LA BARCA - REPORTE DE EGRESOS</h1>
                <p>Generado: ${new Date().toLocaleString('es-MX')} · Consulta: ${query.trim() || 'Sin filtro de texto'}</p>
              </header>
              <div class="summary">
                <div class="summary-card"><div class="label">Resultados en página</div><div class="value">${rows.length}</div></div>
                <div class="summary-card"><div class="label">Resultados totales</div><div class="value">${totalCount}</div></div>
                ${amountSummaryCards}
              </div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr><th>Fecha</th><th>Proveedor</th><th>Concepto</th><th>Categoría</th><th>Desglose IVA</th><th style="text-align:right">Monto</th></tr>
                  </thead>
                  <tbody>${printableRows}</tbody>
                </table>
              </div>
              <div class="footer-total">${footerTotalLabel}: $${formatMoney(footerTotalAmount)}</div>
            </section>
          </body>
        </html>
      `);
      popup.document.close();
      popup.focus();
      popup.print();
    } finally {
      setExportingPdf(false);
    }
  }


  return (
    <div className="card">
      <h2 style={{ marginTop: 0 }}>Buscar movimientos</h2>
      <div className="search-toolbar">
        <input
          placeholder="Buscar por proveedor, concepto o categoría"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          style={{ maxWidth: 420 }}
        />
        <button type="button" onClick={exportSearchResultsToPdf} disabled={loading || exportingPdf || !rows.length}>
          {exportingPdf ? 'Exportando...' : 'Exportar PDF'}
        </button>
      </div>
      <div className="small" style={{ marginTop: 8 }}>
        {loading ? 'Buscando...' : `${totalCount} resultados en egresos${totalCount ? ` (mostrando ${rangeStart}-${rangeEnd})` : ''}`}
      </div>
      {!!error && <div className="small" style={{ marginTop: 8, color: '#b91c1c' }}>{error}</div>}
      <div style={{ overflowX: 'auto', marginTop: 10 }}>
        <table>
          <thead>
            <tr>
              <th>Fecha</th><th>Proveedor</th><th>Concepto</th><th>Categoría</th><th>Monto sin IVA</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id}>
                <td>{r.date}</td>
                <td>{r.proveedorNombre || r.supplierName || vendorMap[r.vendor_id] || r.proveedor?.name || '—'}</td>
                <td>{r.description || r.concept || ''}</td>
                <td>{getTransactionCategoryLabel(r, catMap)}</td>
                <td>${formatMoney(getAmountWithoutIva(r))}</td>
              </tr>
            ))}
            {!rows.length && !loading && (
              <tr><td colSpan={5} className="small">Sin resultados</td></tr>
            )}
          </tbody>
          <tfoot>
            <tr>
              <td colSpan={4} style={{ textAlign: 'right', fontWeight: 700 }}>Total filtrado sin IVA</td>
              <td style={{ fontWeight: 700 }}>${formatMoney(filteredTotal)}</td>
            </tr>
          </tfoot>
        </table>
      </div>
      <div className="row" style={{ justifyContent: 'flex-end', marginTop: 12 }}>
        <button className="secondary" onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page <= 1 || loading}>Anterior</button>
        <button className="secondary" onClick={() => setPage((p) => p + 1)} disabled={rangeEnd >= totalCount || loading}>Siguiente</button>
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
                  <button
                    className="secondary"
                    onClick={() => setCatEdit({ ...c })}
                    disabled={!isMongoObjectId(c.id)}
                    title={!isMongoObjectId(c.id) ? 'Solo se pueden editar categorías del catálogo manual.' : ''}
                  >
                    Editar
                  </button>{' '}
                  <button
                    className="secondary"
                    disabled={!isMongoObjectId(c.id)}
                    title={!isMongoObjectId(c.id) ? 'Solo se pueden eliminar categorías del catálogo manual.' : ''}
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
