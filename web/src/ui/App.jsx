import React, { useEffect, useMemo, useState } from 'react';
import { api, clearSession, getSession, saveSession, SELECTED_PROJECT_KEY } from '../api.js';
import { isSapSboTransaction } from '../transactions/helpers.js';
import { ImportSapScreen } from './ImportAndAdminScreens.jsx';
import { dedupeCategories, dedupeVendors } from './dropdownOptions.js';
import { isAdmin as isAdminRole, isSuperAdmin, isViewer, normalizeRole } from './roles.js';

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

function getTransactionCategoryLabel(transaction, catMap) {
  const normalizedCategory = String(
    transaction?.categoryEffectiveName
    || transaction?.categoryManualName
    || transaction?.categoryName
    || '',
  ).trim();
  if (normalizedCategory) return normalizedCategory;

  const effectiveCode = (transaction?.categoryEffectiveCode || '').trim();
  if (effectiveCode) {
    const mappedCategory = (catMap[effectiveCode] || '').trim();
    if (mappedCategory) return mappedCategory;
    return effectiveCode;
  }

  const legacyCategory = (transaction?.category_name || transaction?.category || '').trim();
  if (legacyCategory) return legacyCategory;

  const hintName = String(transaction?.categoryName || transaction?.categoryHintName || '').trim();
  if (hintName) return hintName;

  return 'Sin categoría';
}

function getTransactionTotalValue(transaction) {
  if (Number.isFinite(transaction?.totalFactura)) return transaction.totalFactura;
  if (Number.isFinite(transaction?.amount)) return transaction.amount;
  if (Number.isFinite(transaction?.subtotal)) return transaction.subtotal;
  return 0;
}

function matchesDateRange(value, from, to) {
  const date = String(value || '').slice(0, 10);
  if (!date) return false;
  if (from && date < from) return false;
  if (to && date > to) return false;
  return true;
}

function isMongoObjectId(value) {
  return /^[a-fA-F0-9]{24}$/.test(String(value || '').trim());
}

function getProjectDisplayName(project) {
  return project?.displayName || project?.name || 'Sin nombre';
}

function getAdminPersonalizedProjects(projects, session) {
  const normalizedRole = normalizeRole(session?.role);
  if (!isAdminRole(normalizedRole)) return Array.isArray(projects) ? projects : [];

  const hiddenIds = new Set(
    Array.isArray(session?.uiPrefs?.hiddenProjectIds)
      ? session.uiPrefs.hiddenProjectIds.map((id) => String(id || '').trim()).filter(Boolean)
      : [],
  );
  return (Array.isArray(projects) ? projects : []).filter((project) => !hiddenIds.has(String(project?._id || '')));
}

function formatCurrency(value) {
  return `$${formatMoney(value || 0)}`;
}

function getTransactionSourceLabel(transaction) {
  return `${transaction?.sapBadgeLabel || transaction?.source || '—'} ${transaction?.sourceSbo || ''}`.trim();
}

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim()
    .replace(/\s+/g, ' ');
}

function tokenizeSearchQuery(query) {
  return normalizeSearchText(query)
    .split(' ')
    .filter(Boolean);
}

function buildTransactionSearchHaystack(transaction, catMap) {
  const sapMeta = transaction?.sapMeta || {};
  const fields = [
    transaction?.description,
    transaction?.supplierName,
    transaction?.categoryName,
    getTransactionCategoryLabel(transaction, catMap),
    transaction?.projectDisplayName,
    transaction?.sourceSbo,
    sapMeta?.businessPartner,
    sapMeta?.invoiceNum,
    sapMeta?.paymentNum,
    sapMeta?.externalDocNum,
  ];
  return normalizeSearchText(fields.filter(Boolean).join(' '));
}

function matchesTransactionSearch(transaction, query, catMap) {
  const tokens = tokenizeSearchQuery(query);
  if (!tokens.length) return true;
  const haystack = buildTransactionSearchHaystack(transaction, catMap);
  return tokens.every((token) => haystack.includes(token));
}

function SourceBadges({ transaction }) {
  const sourceLabel = transaction?.sapBadgeLabel || 'SAP/SBO';
  const sourceSbo = String(transaction?.sourceSbo || '').trim();

  if (isSapSboTransaction(transaction)) {
    return (
      <span className="badge-row">
        <span className="badge badge-source">{sourceLabel}</span>
        {sourceSbo && <span className="badge badge-sbo">{sourceSbo}</span>}
      </span>
    );
  }

  return <span className="small">{transaction?.source || '—'}</span>;
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
  const normalizedRole = normalizeRole(role);
  const canSeeSettings = !isViewer(normalizedRole);
  const canSeeTransactionsAdmin = isSuperAdmin(normalizedRole);
  const items = [
    ['dashboard', 'Dashboard', true],
    ['search', 'Buscar movimientos', true],
    ['transactions', 'Movimientos (Admin)', canSeeTransactionsAdmin],
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
        <img
          src="/LOGO%20GRUPO%20MDI.jpg"
          alt="Logo Grupo MDI"
          className="nav-logo"
          onError={(event) => {
            event.currentTarget.onerror = null;
            event.currentTarget.src = '/logo-grupo-mdi.svg';
          }}
        />
        <div className="nav-title-wrap">
          <div className="nav-title">CONTROL DE GASTOS 2.0</div>
          <div className="nav-subtitle">Grupo MDI</div>
        </div>
      </div>

      <div className="nav-items">
        <div className="small" style={{ marginBottom: 6 }}>Proyecto</div>
        <select value={selectedProjectId} onChange={(e) => onProjectChange(e.target.value)} disabled={!projects.length}>
          {!projects.length && <option value="">Sin proyectos</option>}
          {projects.map((project) => (
            <option key={project._id} value={project._id}>
              {getProjectDisplayName(project)}
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
          <img
            src="/LOGO%20GRUPO%20MDI.jpg"
            alt="Logo Grupo MDI"
            className="login-brand-image"
            onError={(event) => {
              event.currentTarget.onerror = null;
              event.currentTarget.src = '/logo-grupo-mdi.svg';
            }}
          />
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
  const [dashboardType, setDashboardType] = useState('expenses');
  const [cats, setCats] = useState([]);
  const [vendors, setVendors] = useState([]);
  const [toast, setToast] = useState('');
  const [session, setSession] = useState(getSession());
  const [projects, setProjects] = useState([]);
  const personalizedProjects = useMemo(() => getAdminPersonalizedProjects(projects, session), [projects, session]);
  const [selectedProjectId, setSelectedProjectId] = useState(localStorage.getItem(SELECTED_PROJECT_KEY) || '');
  const [dataVersion, setDataVersion] = useState(0);
  const [themePreference, setThemePreference] = useState(() => {
    const storedPreference = localStorage.getItem(THEME_STORAGE_KEY);
    if (storedPreference === 'dark') return 'dark';
    return 'light';
  });
  const userRole = normalizeRole(session.role);
  const isSuperAdminUser = isSuperAdmin(userRole);
  const isViewerUser = isViewer(userRole);
  const isAdminUser = isAdminRole(userRole);
  const canUseAdminPreferences = isAdminUser || isSuperAdminUser;
  const isAdmin = isSuperAdminUser;
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

    const selectableList = getAdminPersonalizedProjects(list, session);

    if (!selectableList.length) {
      setSelectedProjectId('');
      localStorage.removeItem(SELECTED_PROJECT_KEY);
      return;
    }

    const currentProjectId = localStorage.getItem(SELECTED_PROJECT_KEY) || '';
    const exists = selectableList.some((project) => project._id === currentProjectId);
    const fallbackProjectId = selectableList[0]?._id || '';
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
      .then((me) => {
        const nextSession = {
          ...getSession(),
          ...me,
          role: normalizeRole(me?.role || session.role),
          displayName: me?.name || me?.displayName || session.displayName,
        };
        saveSession(nextSession);
        setSession(nextSession);
      })
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

  useEffect(() => {
    if (!session.token) return;
    const current = String(selectedProjectId || '');
    const exists = personalizedProjects.some((project) => String(project?._id || '') === current);
    if (exists) return;
    const fallbackProjectId = personalizedProjects[0]?._id || '';
    setSelectedProjectId(fallbackProjectId);
    if (fallbackProjectId) localStorage.setItem(SELECTED_PROJECT_KEY, fallbackProjectId);
    else localStorage.removeItem(SELECTED_PROJECT_KEY);
  }, [session.token, personalizedProjects, selectedProjectId]);

  useEffect(() => {
    if (!isSuperAdminUser && tab === 'transactions') {
      setTab('search');
    }
    if (isViewerUser && tab === 'settings') {
      setTab('dashboard');
    }
  }, [isSuperAdminUser, isViewerUser, tab]);

  if (!session.token) return <Login onLogin={setSession} />;

  return (
    <>
      <Nav
        tab={tab}
        setTab={setTab}
        role={userRole}
        username={session.username}
        displayName={session.displayName}
        onLogout={logout}
        isDarkMode={isDarkMode}
        onToggleTheme={toggleTheme}
        projects={personalizedProjects}
        selectedProjectId={selectedProjectId}
        onProjectChange={handleProjectChange}
      />

      <div className="container grid" style={{ gap: 14 }}>
        {toast && <div className="card">{toast}</div>}

        {tab === 'dashboard' && (
          <DashboardSection
            dashboardType={dashboardType}
            onDashboardTypeChange={setDashboardType}
            isAdmin={isAdmin}
            selectedProjectId={selectedProjectId}
            refreshKey={dataVersion}
          />
        )}

        {tab === 'transactions' && isSuperAdminUser && (
          <Transactions
            isAdmin={isAdmin}
            cats={cats}
            vendors={vendors}
            onCatalogChanged={refreshCatalog}
            onTransactionsChanged={invalidateData}
            selectedProjectId={selectedProjectId}
          />
        )}

        {tab === 'search' && (
          <SearchTransactions
            cats={cats}
            vendors={vendors}
            projects={personalizedProjects}
            selectedProjectId={selectedProjectId}
          />
        )}

        {tab === 'settings' && !isViewerUser && (
          <Settings
            isAdmin={isAdmin}
            isSuperAdmin={isSuperAdminUser}
            cats={cats}
            vendors={vendors}
            projects={personalizedProjects}
            allProjects={projects}
            session={session}
            canUseAdminPreferences={canUseAdminPreferences}
            selectedProjectId={selectedProjectId}
            onProjectCreated={loadProjects}
            onSessionUpdated={setSession}
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

function Settings({ isAdmin, isSuperAdmin, cats, vendors, projects, allProjects, session, canUseAdminPreferences, selectedProjectId, onCatalogChanged, onProjectCreated, onSessionUpdated }) {
  const [section, setSection] = useState('catalog');

  return (
    <div className="grid" style={{ gap: 14 }}>
      <div className="card" style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {canUseAdminPreferences && (
          <button type="button" className={section === 'my-project-visibility' ? '' : 'secondary'} onClick={() => setSection('my-project-visibility')}>
            Mi visualización de proyectos
          </button>
        )}
        {isSuperAdmin && (
          <button
            type="button"
            className={section === 'users-access' ? '' : 'secondary'}
            onClick={() => setSection('users-access')}
          >
            Usuarios y accesos
          </button>
        )}
        {isSuperAdmin && (
          <button
            type="button"
            className={section === 'projects-visibility' ? '' : 'secondary'}
            onClick={() => setSection('projects-visibility')}
          >
            Visibilidad de proyectos
          </button>
        )}
        <button type="button" className={section === 'catalog' ? '' : 'secondary'} onClick={() => setSection('catalog')}>
          Catálogo
        </button>
        <button
          type="button"
          className={section === 'import-sap' ? '' : 'secondary'}
          onClick={() => setSection('import-sap')}
          disabled={!isSuperAdmin}
          title={!isSuperAdmin ? 'Solo disponible para superadministradores' : undefined}
        >
          Subir CSV
        </button>
        <button
          type="button"
          className={section === 'sap-latest' ? '' : 'secondary'}
          onClick={() => setSection('sap-latest')}
          disabled={!isSuperAdmin}
          title={!isSuperAdmin ? 'Solo disponible para superadministradores' : undefined}
        >
          SAP Import
        </button>
        <button
          type="button"
          className={section === 'supplier-category2' ? '' : 'secondary'}
          onClick={() => setSection('supplier-category2')}
          disabled={!isSuperAdmin}
          title={!isSuperAdmin ? 'Solo disponible para superadministradores' : undefined}
        >
          Proveedor → Categoría 2
        </button>
        {isSuperAdmin && (
          <button
            type="button"
            className={section === 'projects-unmatched' ? '' : 'secondary'}
            onClick={() => setSection('projects-unmatched')}
          >
            Proyectos unmatched
          </button>
        )}
        <button
          type="button"
          className={section === 'raw-data' ? '' : 'secondary'}
          onClick={() => setSection('raw-data')}
          disabled={!isSuperAdmin}
          title={!isSuperAdmin ? 'Solo disponible para superadministradores' : undefined}
        >
          Raw data
        </button>
      </div>

      {section === 'my-project-visibility' && canUseAdminPreferences && (
        <MyProjectVisibilitySection
          allProjects={allProjects}
          session={session}
          onSessionUpdated={onSessionUpdated}
        />
      )}

      {section === 'catalog' && <Catalog isAdmin={isAdmin} cats={cats} vendors={vendors} onChanged={onCatalogChanged} />}

      {section === 'import-sap' &&
        (isSuperAdmin ? (
          <ImportSapScreen />
        ) : (
          <div className="card">Solo los superadministradores pueden importar pagos SAP.</div>
        ))}

      {section === 'sap-latest' &&
        (isSuperAdmin ? (
          <SapLatestImportSection projects={projects} selectedProjectId={selectedProjectId} />
        ) : (
          <div className="card">Solo los superadministradores pueden ejecutar el import SAP latest.</div>
        ))}

      {section === 'supplier-category2' &&
        (isSuperAdmin ? (
          <SupplierCategory2Assignment cats={cats} selectedProjectId={selectedProjectId} />
        ) : (
          <div className="card">Solo los superadministradores pueden asignar categoría por proveedor.</div>
        ))}

      {section === 'projects-unmatched' && isSuperAdmin && <AdminProjectsFromUnmatchedSection onProjectCreated={onProjectCreated} />}

      {section === 'projects-visibility' && isSuperAdmin && <AdminProjectVisibilitySection onProjectUpdated={onProjectCreated} />}

      {section === 'users-access' && isSuperAdmin && <AdminUsersAccessSection />}

      {section === 'raw-data' &&
        (isAdmin ? <RawDataAdmin /> : <div className="card">Solo los superadministradores pueden ver raw data.</div>)}
    </div>
  );
}


function MyProjectVisibilitySection({ allProjects, session, onSessionUpdated }) {
  const initialHiddenProjectIds = useMemo(
    () => (Array.isArray(session?.uiPrefs?.hiddenProjectIds)
      ? session.uiPrefs.hiddenProjectIds.map(String)
      : []),
    [session?.uiPrefs?.hiddenProjectIds],
  );
  const [hiddenProjectIds, setHiddenProjectIds] = useState(() => (
    initialHiddenProjectIds
  ));
  const [search, setSearch] = useState('');
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState('');

  const visibleProjects = useMemo(
    () => (Array.isArray(allProjects) ? allProjects.filter((project) => project?.visibleInFrontend !== false) : []),
    [allProjects],
  );

  useEffect(() => {
    setHiddenProjectIds(initialHiddenProjectIds);
  }, [initialHiddenProjectIds]);

  const visibleProjectIds = useMemo(
    () => visibleProjects.map((project) => String(project?._id || '')).filter(Boolean),
    [visibleProjects],
  );

  const visibleProjectIdSet = useMemo(() => new Set(visibleProjectIds), [visibleProjectIds]);

  const projectCounts = useMemo(() => {
    const hiddenInVisibleCount = visibleProjectIds.filter((projectId) => hiddenProjectIds.includes(projectId)).length;
    return {
      visible: visibleProjectIds.length - hiddenInVisibleCount,
      hidden: hiddenInVisibleCount,
      total: visibleProjectIds.length,
    };
  }, [hiddenProjectIds, visibleProjectIds]);

  const filteredVisibleProjects = useMemo(() => {
    const normalizedQuery = normalizeSearchText(search);
    if (!normalizedQuery) return visibleProjects;
    return visibleProjects.filter((project) => {
      const haystack = normalizeSearchText([
        project?.displayName,
        project?.slug,
        project?.sap?.sourceSbo,
      ].join(' '));
      return haystack.includes(normalizedQuery);
    });
  }, [search, visibleProjects]);

  function isVisibleForMe(projectId) {
    return !hiddenProjectIds.includes(String(projectId || ''));
  }

  function toggleProject(projectId) {
    const key = String(projectId || '');
    setHiddenProjectIds((prev) => (
      prev.includes(key) ? prev.filter((id) => id !== key) : [...prev, key]
    ));
    setMessage('');
  }

  function showAllProjects() {
    setHiddenProjectIds((prev) => prev.filter((id) => !visibleProjectIdSet.has(id)));
    setMessage('');
  }

  function hideAllProjects() {
    setHiddenProjectIds((prev) => {
      const next = new Set(prev);
      visibleProjectIds.forEach((projectId) => next.add(projectId));
      return Array.from(next);
    });
    setMessage('');
  }

  function resetMyView() {
    setHiddenProjectIds(initialHiddenProjectIds);
    setMessage('Preferencias restablecidas sin guardar.');
  }

  async function savePreferences() {
    setSaving(true);
    setMessage('');
    try {
      const response = await api.updateMyPreferences({ uiPrefs: { hiddenProjectIds } });
      const nextUiPrefs = response?.uiPrefs || { hiddenProjectIds: [] };
      const nextSession = { ...getSession(), ...session, uiPrefs: nextUiPrefs };
      saveSession(nextSession);
      onSessionUpdated(nextSession);
      setMessage('Preferencias guardadas.');
    } catch (error) {
      setMessage(error.message || 'No se pudieron guardar las preferencias.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="card grid" style={{ gap: 12 }}>
      <div>
        <h3 style={{ margin: 0 }}>Mi visualización de proyectos</h3>
        <div className="small">El estado es personal: visible para mí u oculto para mí. No cambia la publicación global ni el acceso de otros usuarios.</div>
      </div>

      {!visibleProjects.length && <div className="small">No hay proyectos publicados para configurar.</div>}

      {!!visibleProjects.length && (
        <div className="grid" style={{ gap: 8 }}>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
            <span className="small">
              {projectCounts.visible} visibles · {projectCounts.hidden} ocultos
            </span>
            <button type="button" className="secondary" onClick={showAllProjects}>Mostrar todos</button>
            <button type="button" className="secondary" onClick={hideAllProjects}>Ocultar todos</button>
            <button type="button" className="secondary" onClick={resetMyView}>Restablecer mi vista</button>
          </div>

          <input
            type="search"
            placeholder="Buscar por displayName, slug o sourceSbo"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />

          {!filteredVisibleProjects.length && (
            <div className="small">No hay coincidencias para “{search.trim()}”.</div>
          )}

          {filteredVisibleProjects.map((project) => {
            const projectId = String(project?._id || '');
            const visibleForMe = isVisibleForMe(projectId);
            const sourceSbo = String(project?.sap?.sourceSbo || '').trim();
            return (
              <label key={projectId} style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <input
                  type="checkbox"
                  checked={visibleForMe}
                  onChange={() => toggleProject(projectId)}
                />
                <span>
                  {getProjectDisplayName(project)}
                  <span className="small"> {project?.slug ? `(${project.slug})` : ''} {sourceSbo ? `· ${sourceSbo}` : ''}</span>
                  <span className="small" style={{ marginLeft: 6 }}>
                    {visibleForMe ? '· Visible para mí' : '· Oculto para mí'}
                  </span>
                </span>
              </label>
            );
          })}
        </div>
      )}

      <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
        <button type="button" onClick={savePreferences} disabled={saving}>
          {saving ? 'Guardando...' : 'Guardar'}
        </button>
        {message && <span className="small">{message}</span>}
      </div>
    </div>
  );
}

function AdminUsersAccessSection() {
  const [users, setUsers] = useState([]);
  const [projects, setProjects] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [roleFilter, setRoleFilter] = useState('ALL');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [editingUserId, setEditingUserId] = useState('');
  const [draftProjectIds, setDraftProjectIds] = useState([]);
  const [draftRoleByUserId, setDraftRoleByUserId] = useState({});
  const [savingRoleUserId, setSavingRoleUserId] = useState('');
  const [saving, setSaving] = useState(false);
  const [creating, setCreating] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [createForm, setCreateForm] = useState({
    displayName: '',
    username: '',
    password: '',
    email: '',
    role: 'VIEWER',
    allowedProjectIds: [],
  });
  const [editingNameUserId, setEditingNameUserId] = useState('');
  const [draftDisplayName, setDraftDisplayName] = useState('');

  const projectMap = useMemo(() => {
    const map = new Map();
    projects.forEach((project) => map.set(String(project?._id || ''), project));
    return map;
  }, [projects]);

  const summary = useMemo(() => {
    const counters = { total: users.length, active: 0, inactive: 0, SUPERADMIN: 0, ADMIN: 0, VIEWER: 0 };
    users.forEach((user) => {
      const role = normalizeRole(user?.role);
      counters[role] = (counters[role] || 0) + 1;
      if (user?.isActive === false) counters.inactive += 1;
      else counters.active += 1;
    });
    return counters;
  }, [users]);

  const filteredUsers = useMemo(() => {
    const query = searchTerm.trim().toLowerCase();
    return users.filter((user) => {
      const role = normalizeRole(user?.role);
      if (roleFilter !== 'ALL' && role !== roleFilter) return false;
      if (!query) return true;
      const candidates = [user?.displayName, user?.name, user?.username, user?.email].filter(Boolean).map((value) => String(value).toLowerCase());
      return candidates.some((value) => value.includes(query));
    });
  }, [users, searchTerm, roleFilter]);

  async function loadData() {
    setLoading(true);
    setError('');
    try {
      const [usersData, projectsData] = await Promise.all([api.adminUsers(), api.adminProjects()]);
      setUsers(Array.isArray(usersData) ? usersData : []);
      setProjects(Array.isArray(projectsData) ? projectsData : []);
    } catch (e) {
      setError(e.message || 'No se pudo cargar usuarios/proyectos.');
      setUsers([]);
      setProjects([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadData(); }, []);

  function startEdit(user) {
    const nextId = String(user?.id || user?._id || '');
    setEditingUserId(nextId);
    setDraftProjectIds(Array.isArray(user?.allowedProjectIds) ? user.allowedProjectIds.map(String) : []);
  }

  function startEditName(user) {
    const userId = String(user?.id || user?._id || '');
    setEditingNameUserId(userId);
    setDraftDisplayName(String(user?.displayName || user?.name || user?.username || ''));
  }

  async function saveDisplayName(user) {
    const userId = String(user?.id || user?._id || '');
    if (!userId) return;
    const nextDisplayName = draftDisplayName.trim();
    if (!nextDisplayName) {
      setError('El nombre visible no puede estar vacío.');
      return;
    }
    setSaving(true);
    setError('');
    try {
      const updated = await api.updateAdminUser(userId, { displayName: nextDisplayName });
      setUsers((prev) => prev.map((row) => (String(row?.id || row?._id || '') === userId ? { ...row, ...updated } : row)));
      setEditingNameUserId('');
      setDraftDisplayName('');
    } catch (e) {
      setError(e.message || 'No se pudo actualizar el nombre visible.');
    } finally {
      setSaving(false);
    }
  }

  function toggleCreateProject(projectId) {
    const key = String(projectId || '');
    setCreateForm((prev) => ({
      ...prev,
      allowedProjectIds: prev.allowedProjectIds.includes(key) ? prev.allowedProjectIds.filter((id) => id !== key) : [...prev.allowedProjectIds, key],
    }));
  }

  async function handleCreateUser() {
    const payload = {
      displayName: createForm.displayName,
      username: createForm.username,
      password: createForm.password,
      email: createForm.email,
      role: createForm.role,
      allowedProjectIds: createForm.role === 'VIEWER' ? createForm.allowedProjectIds : [],
    };
    setCreating(true);
    setError('');
    try {
      const created = await api.createAdminUser(payload);
      setUsers((prev) => [created, ...prev]);
      setShowCreateForm(false);
      setCreateForm({ displayName: '', username: '', password: '', email: '', role: 'VIEWER', allowedProjectIds: [] });
    } catch (e) {
      setError(e.message || 'No se pudo crear el usuario.');
    } finally {
      setCreating(false);
    }
  }

  function handleRoleDraft(userId, role) {
    const key = String(userId || '');
    if (!key) return;
    setDraftRoleByUserId((prev) => ({ ...prev, [key]: normalizeRole(role) }));
  }

  async function saveRole(user) {
    const userId = String(user?.id || user?._id || '');
    if (!userId) return;
    const nextRole = normalizeRole(draftRoleByUserId[userId] || user?.role);
    const currentRole = normalizeRole(user?.role);
    if (nextRole === currentRole) return;

    setSavingRoleUserId(userId);
    setError('');
    try {
      const updated = await api.updateAdminUser(userId, { role: nextRole });
      setUsers((prev) => prev.map((row) => (String(row?.id || row?._id || '') === userId ? { ...row, ...updated } : row)));
    } catch (e) {
      setError(e.message || 'No se pudo cambiar el rol del usuario.');
    } finally {
      setSavingRoleUserId('');
    }
  }

  function cancelEdit() {
    setEditingUserId('');
    setDraftProjectIds([]);
  }

  function toggleProject(projectId) {
    const key = String(projectId || '');
    setDraftProjectIds((prev) => (prev.includes(key) ? prev.filter((id) => id !== key) : [...prev, key]));
  }

  async function saveViewerAccess(user) {
    const userId = String(user?.id || user?._id || '');
    if (!userId) return;
    setSaving(true);
    setError('');
    try {
      const updated = await api.updateAdminUser(userId, { allowedProjectIds: draftProjectIds });
      setUsers((prev) => prev.map((row) => (String(row?.id || row?._id || '') === userId ? { ...row, ...updated } : row)));
      cancelEdit();
    } catch (e) {
      setError(e.message || 'No se pudo guardar accesos del usuario.');
    } finally {
      setSaving(false);
    }
  }

  function renderAllowedProjects(user) {
    const ids = Array.isArray(user?.allowedProjectIds) ? user.allowedProjectIds : [];
    if (!ids.length) return 'Sin proyectos asignados';
    return ids.map((id) => {
      const project = projectMap.get(String(id));
      return project ? getProjectDisplayName(project) : String(id);
    }).join(', ');
  }

  function getViewerProjectsLabel(user) {
    const count = Array.isArray(user?.allowedProjectIds) ? user.allowedProjectIds.length : 0;
    return `${count} ${count === 1 ? 'proyecto asignado' : 'proyectos asignados'}`;
  }

  function getRoleHelp(role) {
    if (role === 'SUPERADMIN') return 'Acceso total';
    if (role === 'ADMIN') return 'Operación general';
    return 'Solo verá proyectos asignados';
  }

  return (
    <div className="card grid" style={{ gap: 12 }}>
      <div>
        <h3 style={{ margin: 0 }}>Usuarios y accesos</h3>
        <div className="small">SUPERADMIN = acceso total · ADMIN = operación general · VIEWER = solo proyectos asignados.</div>
        <div className="small">Username = login · Nombre visible = cómo se muestra en el sistema.</div>
      </div>

      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        <span className="badge">{summary.SUPERADMIN} superadmins</span>
        <span className="badge">{summary.ADMIN} admins</span>
        <span className="badge">{summary.VIEWER} viewers</span>
        <button type="button" onClick={() => setShowCreateForm((prev) => !prev)}>{showCreateForm ? 'Cerrar' : 'Agregar usuario'}</button>
      </div>

      {showCreateForm && (
        <div className="card" style={{ margin: 0 }}>
          <div className="small" style={{ marginBottom: 8 }}>Crear usuario nuevo</div>
          <div style={{ display: 'grid', gap: 8 }}>
            <label className="small">Nombre visible
              <input value={createForm.displayName} onChange={(e) => setCreateForm((prev) => ({ ...prev, displayName: e.target.value }))} placeholder="Ej: Juan Pérez" />
            </label>
            <label className="small">Username (login)
              <input value={createForm.username} onChange={(e) => setCreateForm((prev) => ({ ...prev, username: e.target.value }))} placeholder="usuario_login" />
            </label>
            <label className="small">Password inicial
              <input type="password" value={createForm.password} onChange={(e) => setCreateForm((prev) => ({ ...prev, password: e.target.value }))} />
            </label>
            <label className="small">Email (opcional)
              <input value={createForm.email} onChange={(e) => setCreateForm((prev) => ({ ...prev, email: e.target.value }))} placeholder="usuario@empresa.com" />
            </label>
            <label className="small">Rol
              <select value={createForm.role} onChange={(e) => setCreateForm((prev) => ({ ...prev, role: normalizeRole(e.target.value), allowedProjectIds: normalizeRole(e.target.value) === 'VIEWER' ? prev.allowedProjectIds : [] }))}>
                <option value="SUPERADMIN">SUPERADMIN</option>
                <option value="ADMIN">ADMIN</option>
                <option value="VIEWER">VIEWER</option>
              </select>
            </label>
            {createForm.role === 'VIEWER' && (
              <div>
                <div className="small" style={{ marginBottom: 6 }}>Proyectos permitidos (el VIEWER solo verá estos proyectos)</div>
                <div style={{ display: 'grid', gap: 6, maxHeight: 180, overflowY: 'auto' }}>
                  {projects.map((project) => {
                    const projectId = String(project?._id || '');
                    return (
                      <label key={projectId} style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                        <input type="checkbox" checked={createForm.allowedProjectIds.includes(projectId)} onChange={() => toggleCreateProject(projectId)} />
                        <span>{getProjectDisplayName(project)}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
            )}
            <div style={{ display: 'flex', gap: 8 }}>
              <button type="button" onClick={handleCreateUser} disabled={creating}>{creating ? 'Creando...' : 'Crear usuario'}</button>
              <button type="button" className="secondary" onClick={() => setShowCreateForm(false)} disabled={creating}>Cancelar</button>
            </div>
          </div>
        </div>
      )}

      <div style={{ display: 'grid', gap: 8 }}>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <input value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} placeholder="Buscar por nombre, usuario o email" />
          <label className="small">Rol
            <select value={roleFilter} onChange={(e) => setRoleFilter(e.target.value)}>
              <option value="ALL">Todos</option>
              <option value="SUPERADMIN">SUPERADMIN</option>
              <option value="ADMIN">ADMIN</option>
              <option value="VIEWER">VIEWER</option>
            </select>
          </label>
        </div>
      </div>

      {loading && <div className="small">Cargando usuarios...</div>}
      {error && <div className="small" style={{ color: '#b00020' }}>{error}</div>}

      {!loading && (
        <div style={{ overflowX: 'auto' }}>
          <table>
            <thead>
              <tr>
                <th>Nombre y usuario</th>
                <th>Email</th>
                <th>Rol</th>
                <th>Estado</th>
                <th>Proyectos permitidos (VIEWER)</th>
                <th>Acción</th>
              </tr>
            </thead>
            <tbody>
              {!filteredUsers.length && <tr><td colSpan={6} className="small">No hay usuarios que coincidan con la búsqueda/filtro.</td></tr>}
              {filteredUsers.map((user) => {
                const userId = String(user?.id || user?._id || '');
                const role = normalizeRole(user?.role);
                const viewer = role === 'VIEWER';
                const isEditing = editingUserId === userId;
                const isEditingName = editingNameUserId === userId;
                const draftRole = normalizeRole(draftRoleByUserId[userId] || user?.role);
                const roleDirty = draftRole !== normalizeRole(user?.role);
                const savingRole = savingRoleUserId === userId;
                return (
                  <React.Fragment key={userId || user?.username}>
                    <tr>
                      <td>
                        {isEditingName ? (
                          <div style={{ display: 'grid', gap: 6 }}>
                            <input value={draftDisplayName} onChange={(e) => setDraftDisplayName(e.target.value)} />
                            <div style={{ display: 'flex', gap: 6 }}>
                              <button type="button" onClick={() => saveDisplayName(user)} disabled={saving}>{saving ? 'Guardando...' : 'Guardar nombre'}</button>
                              <button type="button" className="secondary" onClick={() => setEditingNameUserId('')} disabled={saving}>Cancelar</button>
                            </div>
                          </div>
                        ) : (
                          <>
                            <div>{user?.displayName || user?.name || user?.username || 'Sin nombre'}</div>
                            <div className="small">@{user?.username || '—'}</div>
                            <button type="button" className="secondary" onClick={() => startEditName(user)} disabled={saving} style={{ marginTop: 6 }}>Editar nombre visible</button>
                          </>
                        )}
                      </td>
                      <td>{user?.email || '—'}</td>
                      <td>
                        <div style={{ display: 'grid', gap: 6 }}>
                          <select value={draftRole} onChange={(e) => handleRoleDraft(userId, e.target.value)} disabled={savingRole || saving}>
                            <option value="SUPERADMIN">SUPERADMIN</option>
                            <option value="ADMIN">ADMIN</option>
                            <option value="VIEWER">VIEWER</option>
                          </select>
                          <div><button type="button" className="secondary" onClick={() => saveRole(user)} disabled={!roleDirty || savingRole || saving}>{savingRole ? 'Guardando rol...' : 'Guardar rol'}</button></div>
                          <div className="small">{getRoleHelp(role)}</div>
                        </div>
                      </td>
                      <td><span className="badge">{user?.isActive === false ? 'Inactivo' : 'Activo'}</span></td>
                      <td className="small">{viewer ? <div style={{ display: 'grid', gap: 4 }}><strong style={{ fontSize: 12 }}>{getViewerProjectsLabel(user)}</strong><span>{renderAllowedProjects(user)}</span></div> : 'No aplica'}</td>
                      <td>{viewer ? <button type="button" className="secondary" onClick={() => startEdit(user)} disabled={saving}>Editar accesos</button> : <span className="small">No editable</span>}</td>
                    </tr>
                    {viewer && isEditing && (
                      <tr>
                        <td colSpan={6}>
                          <div className="card" style={{ margin: 0 }}>
                            <div className="small" style={{ marginBottom: 8 }}>Selecciona proyectos permitidos para este VIEWER (solo verá estos proyectos):</div>
                            <div style={{ display: 'grid', gap: 6, maxHeight: 240, overflowY: 'auto' }}>
                              {projects.map((project) => {
                                const projectId = String(project?._id || '');
                                const checked = draftProjectIds.includes(projectId);
                                const sourceSbo = project?.sap?.sourceSbo || '';
                                return (
                                  <label key={projectId} style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                                    <input type="checkbox" checked={checked} onChange={() => toggleProject(projectId)} disabled={saving} />
                                    <span>{getProjectDisplayName(project)}<span className="small"> {project?.slug ? `(${project.slug})` : ''} {sourceSbo ? `· ${sourceSbo}` : ''}</span></span>
                                  </label>
                                );
                              })}
                            </div>
                            <div style={{ display: 'flex', gap: 8, marginTop: 10 }}>
                              <button type="button" onClick={() => saveViewerAccess(user)} disabled={saving}>{saving ? 'Guardando...' : 'Guardar accesos'}</button>
                              <button type="button" className="secondary" onClick={cancelEdit} disabled={saving}>Cancelar</button>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function AdminProjectVisibilitySection({ onProjectUpdated }) {
  const [projects, setProjects] = useState([]);
  const [query, setQuery] = useState('');
  const [visibilityFilter, setVisibilityFilter] = useState('all');
  const [loading, setLoading] = useState(false);
  const [savingId, setSavingId] = useState('');
  const [error, setError] = useState('');

  async function loadProjects() {
    setLoading(true);
    setError('');
    try {
      const data = await api.adminProjects();
      setProjects(Array.isArray(data) ? data : []);
    } catch (e) {
      setError(e.message || 'No se pudieron cargar los proyectos');
      setProjects([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadProjects();
  }, []);

  async function onToggleVisibility(projectId, nextVisible) {
    setSavingId(projectId);
    setError('');
    const previous = projects;
    setProjects((prev) => prev.map((row) => (row._id === projectId ? { ...row, visibleInFrontend: nextVisible } : row)));
    try {
      await api.updateAdminProjectVisibility(projectId, nextVisible);
      await onProjectUpdated?.();
    } catch (e) {
      setProjects(previous);
      setError(e.message || 'No se pudo actualizar la visibilidad');
    } finally {
      setSavingId('');
    }
  }

  const normalizedQuery = query.trim().toLowerCase();
  const filtered = projects.filter((row) => {
    const isVisible = row?.visibleInFrontend !== false;
    if (visibilityFilter === 'visible' && !isVisible) return false;
    if (visibilityFilter === 'hidden' && isVisible) return false;

    if (!normalizedQuery) return true;
    const sourceSbo = row?.sap?.sourceSbo || '';
    const rawProjectName = row?.sap?.rawProjectName || '';
    const haystack = `${row?.displayName || ''} ${row?.slug || ''} ${rawProjectName} ${sourceSbo}`.toLowerCase();
    return haystack.includes(normalizedQuery);
  });

  return (
    <div className="card">
      <h3 style={{ marginTop: 0 }}>Visibilidad de proyectos</h3>
      <div className="grid" style={{ gap: 10, marginBottom: 10 }}>
        <div>
          <label>Buscar</label>
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="displayName, slug o rawProjectName"
          />
        </div>
        <div>
          <label>Filtro</label>
          <select value={visibilityFilter} onChange={(e) => setVisibilityFilter(e.target.value)}>
            <option value="all">Todos</option>
            <option value="visible">Visibles</option>
            <option value="hidden">Ocultos</option>
          </select>
        </div>
      </div>

      {error && <div style={{ marginBottom: 10 }}>{error}</div>}

      <table>
        <thead>
          <tr>
            <th>displayName</th>
            <th>slug</th>
            <th>sourceSbo</th>
            <th>rawProjectName</th>
            <th>visibleInFrontend</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((row) => {
            const isVisible = row?.visibleInFrontend !== false;
            const rowId = String(row?._id || '');
            return (
              <tr key={rowId}>
                <td>{row?.displayName || row?.name || ''}</td>
                <td>{row?.slug || ''}</td>
                <td>{row?.sap?.sourceSbo || ''}</td>
                <td>{row?.sap?.rawProjectName || ''}</td>
                <td>
                  <label style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                    <input
                      type="checkbox"
                      checked={isVisible}
                      disabled={savingId === rowId}
                      onChange={(e) => onToggleVisibility(rowId, e.target.checked)}
                    />
                    <span className={isVisible ? 'badge badge-visible' : 'badge badge-hidden'}>{isVisible ? 'Visible' : 'Oculto'}</span>
                  </label>
                </td>
              </tr>
            );
          })}

          {!loading && filtered.length === 0 && (
            <tr>
              <td colSpan={5} className="small">Sin proyectos para mostrar.</td>
            </tr>
          )}
          {loading && (
            <tr>
              <td colSpan={5} className="small">Cargando proyectos...</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function AdminProjectsFromUnmatchedSection({ onProjectCreated }) {
  const [running, setRunning] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState(null);

  async function onCreateFromUnmatched() {
    setRunning(true);
    setError('');
    setResult(null);
    try {
      const response = await api.createProjectsFromUnmatchedAdmin();
      setResult(response);
      await onProjectCreated?.();
    } catch (e) {
      setError(e.message || 'No se pudieron crear proyectos desde unmatched');
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="card">
      <h3 style={{ marginTop: 0 }}>Proyectos desde unmatched</h3>
      <div className="small" style={{ marginBottom: 10 }}>
        Crea proyectos automáticamente desde <code>unmatched_projects</code>. Los nuevos quedan ocultos del frontend.
      </div>
      <button type="button" onClick={onCreateFromUnmatched} disabled={running}>
        {running ? 'Creando...' : 'Crear proyectos desde unmatched'}
      </button>
      {error && <div style={{ marginTop: 10 }}>{error}</div>}
      {result && (
        <div style={{ marginTop: 10 }}>
          <div>createdCount: <strong>{result?.createdCount ?? 0}</strong></div>
          <div>skippedExistingCount: <strong>{result?.skippedExistingCount ?? 0}</strong></div>
        </div>
      )}
    </div>
  );
}

function SapLatestImportSection({ projects, selectedProjectId }) {
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState(null);
  const [sbo, setSbo] = useState('SBO_Rafael');
  const [mode, setMode] = useState('latest');
  const [forceReimport, setForceReimport] = useState(false);
  const selectedProject =
    projects.find((project) => String(project?._id || '') === String(selectedProjectId || '')) || null;
  const destinationProjectName = selectedProject ? getProjectDisplayName(selectedProject) : 'Sin proyecto seleccionado';
  const sboOptions = [
    'SBO_Rafael',
    'SBO_GMDI',
    'SBOCitySur',
    'SBO_CPSantaFE',
    'SBO_Mazatlan',
    'SBOIndiana',
    'SBO_Colima334',
  ];

  async function onImportNow() {
    setError('');
    setResult(null);
    if (!selectedProjectId) {
      setError('Selecciona un proyecto activo antes de ejecutar el import.');
      return;
    }

    const accepted = window.confirm(
      `Vas a ejecutar SAP Import para el proyecto: \"${destinationProjectName}\".\nSBO: ${sbo}.\nModo: ${mode}.\nForzar reimportación: ${forceReimport ? 'Sí' : 'No'}.\n\n¿Deseas continuar?`
    );
    if (!accepted) return;

    setImporting(true);
    try {
      const response = await api.importSapMovementsBySbo({ sbo, mode, force: forceReimport });
      setResult(response);
    } catch (e) {
      const errorStatus = e?.status ? `HTTP ${e.status}` : 'HTTP desconocido';
      const errorBody = e?.body ? JSON.stringify(e.body) : (e?.message || 'Sin body');
      setError(`No se pudo ejecutar el import SAP latest. ${errorStatus}. Body: ${errorBody}`);
    } finally {
      setImporting(false);
    }
  }

  function withFallback(value, fallback = 'N/A') {
    return value === null || value === undefined || value === '' ? fallback : String(value);
  }

  return (
    <div className="card grid" style={{ gap: 12 }}>
      <div>
        <h3 style={{ margin: 0 }}>SAP Import</h3>
        <div className="small">Ejecuta manualmente el import SAP por SBO + modo.</div>
        <div className="small" style={{ marginTop: 4 }}>
          Proyecto destino: <strong>{destinationProjectName}</strong>
        </div>
      </div>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        <label style={{ display: 'grid', gap: 6 }}>
          <span>SBO</span>
          <select value={sbo} onChange={(e) => setSbo(e.target.value)} disabled={importing}>
            {sboOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <label style={{ display: 'grid', gap: 6 }}>
          <span>Modo</span>
          <select value={mode} onChange={(e) => setMode(e.target.value)} disabled={importing}>
            <option value="latest">latest</option>
            <option value="backfill">backfill</option>
          </select>
        </label>
        <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 28 }}>
          <input
            type="checkbox"
            checked={forceReimport}
            onChange={(e) => setForceReimport(e.target.checked)}
            disabled={importing}
          />
          <span>Forzar reimportación</span>
        </label>
      </div>

      <div>
        <button type="button" onClick={onImportNow} disabled={importing || !selectedProjectId}>
          {importing ? 'Importando...' : 'Importar latest ahora'}
        </button>
      </div>

      {error && <div className="small" style={{ color: '#b00020' }}>{error}</div>}

      {result && (
        <div className="card" style={{ margin: 0 }}>
          <h4 style={{ margin: 0, marginBottom: 8 }}>Resultado</h4>
          <div className="small">status: {withFallback(result?.status)}</div>
          <div className="small">rowsTotal: {withFallback(result?.rowsTotal, '0')}</div>
          <div className="small">rowsOk: {withFallback(result?.rowsOk, '0')}</div>
          <div className="small">imported: {withFallback(result?.imported, '0')}</div>
          <div className="small">updated: {withFallback(result?.updated, '0')}</div>
          <div className="small">unmatched: {withFallback(result?.unmatched, '0')}</div>
          <div className="small">importRunId: {withFallback(result?.importRunId)}</div>
          <div className="small">already_imported: {withFallback(result?.already_imported)}</div>
          {result?.already_imported === true && (
            <div className="small" style={{ marginTop: 8 }}>
              Este archivo ya había sido importado. importRunId: {withFallback(result?.importRunId)}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function SupplierCategory2Assignment({ cats, selectedProjectId }) {
  const [suppliers, setSuppliers] = useState([]);
  const [supplierId, setSupplierId] = useState('');
  const [categoryCode, setCategoryCode] = useState('');
  const [applyToExisting, setApplyToExisting] = useState(false);
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

    setSaving(true);
    try {
      const result = await api.setSupplierCategory2Rule(
        selectedProjectId,
        supplierId,
        categoryCode,
        applyToExisting,
      );
      setSuccess(`Regla guardada. Movimientos actualizados: ${result?.applyToExistingModified ?? 0}.`);
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
        <label style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <input
            type="checkbox"
            checked={applyToExisting}
            onChange={(e) => setApplyToExisting(e.target.checked)}
          />
          Aplicar a existentes (solo donde no hay manual)
        </label>
        {error && <div>{error}</div>}
        {success && <div>{success}</div>}
        <button type="submit" disabled={saving || loading || !suppliers.length || !cats.length}>
          {saving ? 'Guardando...' : 'Guardar regla de categoría 2'}
        </button>
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


function DashboardSection({ dashboardType, onDashboardTypeChange, isAdmin, selectedProjectId, refreshKey }) {
  const isIncome = dashboardType === 'income';

  return (
    <div className="grid" style={{ gap: 14 }}>
      <div className="card" style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        <button
          type="button"
          className={isIncome ? 'secondary' : ''}
          onClick={() => onDashboardTypeChange('expenses')}
        >
          Dashboard egresos
        </button>
        <button
          type="button"
          className={isIncome ? '' : 'secondary'}
          onClick={() => onDashboardTypeChange('income')}
        >
          Dashboard ingresos
        </button>
      </div>

      {isIncome ? (
        <DashboardIngresos selectedProjectId={selectedProjectId} refreshKey={refreshKey} />
      ) : (
        <Dashboard isAdmin={isAdmin} selectedProjectId={selectedProjectId} refreshKey={refreshKey} />
      )}
    </div>
  );
}

/* ================= DASHBOARD ================= */
function Dashboard({ isAdmin, selectedProjectId, refreshKey }) {
  const [stats, setStats] = useState(null);
  const [supplierSummary, setSupplierSummary] = useState([]);
  const [supplierSummaryError, setSupplierSummaryError] = useState('');
  const [viewMode, setViewMode] = useState('summary');
  const [showCategoryIva, setShowCategoryIva] = useState(false);
  const [showSupplierIva, setShowSupplierIva] = useState(false);
  const [supplierSortMode, setSupplierSortMode] = useState('alpha');
  const [projectBalance, setProjectBalance] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let ok = true;
    setLoading(true);

    Promise.allSettled([
      api.spendByCategory({ include_iva: showCategoryIva ? 'true' : 'false' }),
      api.expensesSummaryBySupplier({ include_iva: showSupplierIva ? 'true' : 'false' }),
      fetchTransactionsTotalByType('EXPENSE', showCategoryIva),
      fetchTransactionsTotalByType('INCOME', showCategoryIva),
    ]).then(([categoryResult, supplierResult, expenseTotalResult, incomeTotalResult]) => {
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
        setSupplierSummaryError(supplierResult.reason?.message || 'No se pudo cargar el resumen por proveedor.');
      }

      const expensesTotal = expenseTotalResult.status === 'fulfilled' ? Number(expenseTotalResult.value) || 0 : 0;
      const incomeTotal = incomeTotalResult.status === 'fulfilled' ? Number(incomeTotalResult.value) || 0 : 0;
      setProjectBalance(Number((incomeTotal - expensesTotal).toFixed(2)));

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

  const biggestCategory = topCategories[0] || null;
  const allocatedPercent = Math.min(
    100,
    Math.max(0, topCategories.reduce((acc, row) => acc + (Number(row.percent) || 0), 0))
  );

  const subtitle =
    viewMode === 'supplier'
      ? 'Resumen operativo de egresos SAP/SBO por proveedor.'
      : viewMode === 'summary'
        ? 'KPIs y visuales de categorías para seguimiento diario.'
        : 'Detalle por categoría con proporción sobre el total de egresos.';

  const dashboardTotals = [
    {
      label: showCategoryIva ? 'Total egresos con IVA' : 'Total egresos sin IVA',
      value: formatCurrency(stats?.total_expenses || 0),
      helper: 'Modelo V2 normalizado',
    },
    {
      label: 'Categorías con movimiento',
      value: String(categoryRows.length),
      helper: 'Con al menos un movimiento',
    },
    {
      label: 'Proveedores con gasto',
      value: String(supplierSummary.length),
      helper: showSupplierIva ? 'Resumen con IVA' : 'Resumen sin IVA',
    },
    {
      label: 'Balance total del proyecto',
      value: formatCurrency(projectBalance),
      helper: 'Ingresos menos egresos',
    },
  ];

  return (
    <div className="card dashboard-shell">
      <div className="dashboard-header">
        <div>
          <h2 style={{ margin: 0 }}>Dashboard de egresos</h2>
          <div className="small" style={{ marginTop: 4 }}>{subtitle}</div>
        </div>
      </div>

      <div className="dashboard-tabs row" style={{ gap: 8 }}>
        <button className={viewMode === 'summary' ? '' : 'secondary'} onClick={() => setViewMode('summary')}>
          Resumen
        </button>
        <button className={viewMode === 'category' ? '' : 'secondary'} onClick={() => setViewMode('category')}>
          Por categoría
        </button>
        <button className={viewMode === 'supplier' ? '' : 'secondary'} onClick={() => setViewMode('supplier')}>
          Por proveedor
        </button>
      </div>

      <div className="dashboard-controls row">
        {viewMode === 'supplier' ? (
          <>
            <label className="small dashboard-checkbox">
              <input type="checkbox" checked={showSupplierIva} onChange={(e) => setShowSupplierIva(e.target.checked)} />
              Mostrar IVA
            </label>
            <label className="small dashboard-checkbox">
              <input
                type="checkbox"
                checked={supplierSortMode === 'amount'}
                onChange={(e) => setSupplierSortMode(e.target.checked ? 'amount' : 'alpha')}
              />
              Ordenar por monto (mayor a menor)
            </label>
          </>
        ) : (
          <label className="small dashboard-checkbox">
            <input type="checkbox" checked={showCategoryIva} onChange={(e) => setShowCategoryIva(e.target.checked)} />
            Mostrar IVA
          </label>
        )}
      </div>

      {loading ? (
        <div className="dashboard-state">Cargando indicadores del dashboard...</div>
      ) : stats?.error ? (
        <div className="dashboard-state dashboard-state-error">Error al cargar categorías: {stats.error}</div>
      ) : (
        <>
          <div className="dashboard-kpi-grid">
            {dashboardTotals.map((item) => (
              <div key={item.label} className="dashboard-kpi-card">
                <span>{item.label}</span>
                <strong>{item.value}</strong>
                <div className="small">{item.helper}</div>
              </div>
            ))}
          </div>

          {viewMode === 'supplier' ? (
            supplierSummaryError ? (
              <div className="dashboard-state dashboard-state-error">Error al cargar proveedores: {supplierSummaryError}</div>
            ) : !sortedSupplierSummary.length ? (
              <div className="dashboard-state">No hay egresos SAP/SBO agrupados por proveedor para este proyecto.</div>
            ) : (
              <div className="dashboard-panel" style={{ marginTop: 4 }}>
                <div className="row" style={{ justifyContent: 'space-between' }}>
                  <h3 style={{ margin: 0 }}>Resumen por proveedor</h3>
                  <span className="badge">Total del resumen: {formatCurrency(supplierTotal)}</span>
                </div>
                <div style={{ overflowX: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Proveedor</th>
                        <th>Movimientos</th>
                        <th>Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedSupplierSummary.map((row) => (
                        <tr key={row.supplierId || row.supplierName}>
                          <td>{row.supplierName || '(Sin proveedor)'}</td>
                          <td>{row.count || 0}</td>
                          <td>{formatCurrency(row.totalAmount)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )
          ) : !categoryRows.length ? (
            <div className="dashboard-state">No hay egresos registrados para mostrar en categorías.</div>
          ) : viewMode === 'summary' ? (
            <div className="dashboard-summary">
              <div className="dashboard-summary-grid">
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
                  <div className="small">Comparativo visual de montos entre categorías principales.</div>
                </section>

                <section className="dashboard-panel dashboard-gauge-panel">
                  <h3>Peso del top de categorías</h3>
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
                  <h3>Categorías principales</h3>
                  <div className="grid">
                    {topCategories.map((row) => {
                      const percent = Number(row.percent) || 0;
                      const fillWidth = Math.max(0, Math.min(100, percent));
                      return (
                        <div key={row.category_id} style={{ display: 'grid', gap: 4 }}>
                          <div className="row" style={{ justifyContent: 'space-between' }}>
                            <strong>{row.category_name}</strong>
                            <span className="small">{percent.toFixed(2)}%</span>
                          </div>
                          <div className="bar" aria-label={`Barra de avance de ${row.category_name}`}>
                            <div style={{ width: `${fillWidth}%` }} />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </section>

                <section className="dashboard-panel">
                  <h3>Montos principales</h3>
                  <div className="dashboard-column-chart">
                    {topCategories.slice(0, 5).map((row) => {
                      const amount = Number(row.amount) || 0;
                      const maxAmount = Number(biggestCategory?.amount) || 1;
                      return (
                        <div key={`column-${row.category_id}`} className="dashboard-column-item">
                          <div className="dashboard-column-value">{formatCurrency(amount)}</div>
                          <div className="dashboard-column-track">
                            <div className="dashboard-column-fill" style={{ height: `${Math.max(12, (amount / maxAmount) * 100)}%` }} />
                          </div>
                          <div className="dashboard-column-label">{row.category_name}</div>
                        </div>
                      );
                    })}
                  </div>
                </section>
              </div>
            </div>
          ) : (
            <div className="dashboard-panel" style={{ marginTop: 4 }}>
              <h3 style={{ margin: 0 }}>Resumen por categoría</h3>
              <div className="grid" style={{ marginTop: 8 }}>
                {categoryRows.map((row) => {
                  const percent = Number(row.percent) || 0;
                  const fillWidth = Math.max(0, Math.min(100, percent));

                  return (
                    <div key={row.category_id} style={{ display: 'grid', gap: 6 }}>
                      <div className="row" style={{ justifyContent: 'space-between' }}>
                        <strong>{row.category_name}</strong>
                        <div>
                          {formatCurrency(row.amount)} <span className="small">({percent.toFixed(2)}%)</span>
                        </div>
                      </div>
                      <div className="bar" aria-label={`Barra de avance de ${row.category_name}`}>
                        <div style={{ width: `${fillWidth}%` }}>
                          <span>{percent.toFixed(2)}%</span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}


function summarizeTransactionsByCategory(transactions, includeIva = false) {
  const totalsByCategory = new Map();
  let total = 0;

  transactions.forEach((tx) => {
    const categoryId = String(tx?.categoryEffectiveCode || tx?.categoryCode || tx?.category_id || tx?.categoryId || 'SIN_CATEGORIA');
    const categoryName = String(tx?.categoryEffectiveName || tx?.categoryName || tx?.category_hint_name || 'Sin categoría');
    const amount = Number(includeIva ? tx?.amount : tx?.subtotal) || 0;
    total += amount;

    const current = totalsByCategory.get(categoryId) || { category_id: categoryId, category_name: categoryName, amount: 0 };
    current.amount += amount;
    if (!current.category_name && categoryName) current.category_name = categoryName;
    totalsByCategory.set(categoryId, current);
  });

  const safeTotal = total || 0;
  const rows = [...totalsByCategory.values()]
    .map((row) => ({
      ...row,
      amount: Number(row.amount.toFixed(2)),
      percent: safeTotal > 0 ? Number(((row.amount / safeTotal) * 100).toFixed(2)) : 0,
    }))
    .sort((a, b) => (Number(b.amount) || 0) - (Number(a.amount) || 0));

  return {
    rows,
    total_expenses: Number(safeTotal.toFixed(2)),
  };
}

function summarizeTransactionsBySupplier(transactions, includeIva = false) {
  const totalsBySupplier = new Map();

  transactions.forEach((tx) => {
    const supplierName = String(tx?.supplierName || tx?.sapMeta?.businessPartner || '(Sin proveedor)');
    const supplierId = String(tx?.supplierId || tx?.supplier_id || tx?.vendor_id || supplierName);
    const amount = Number(includeIva ? tx?.amount : tx?.subtotal) || 0;

    const current = totalsBySupplier.get(supplierId) || {
      supplierId,
      supplierName,
      totalAmount: 0,
      count: 0,
    };
    current.totalAmount += amount;
    current.count += 1;
    if (!current.supplierName && supplierName) current.supplierName = supplierName;
    totalsBySupplier.set(supplierId, current);
  });

  return [...totalsBySupplier.values()].map((row) => ({
    ...row,
    totalAmount: Number(row.totalAmount.toFixed(2)),
  }));
}

async function fetchTransactionsTotalByType(type, includeIva = false) {
  const PAGE_LIMIT = 500;
  let page = 1;
  let totalCount = 0;
  let total = 0;

  do {
    const response = await api.transactions({
      type,
      page: String(page),
      limit: String(PAGE_LIMIT),
    });
    const chunk = Array.isArray(response?.items) ? response.items : [];
    chunk.forEach((tx) => {
      total += Number(includeIva ? tx?.amount : tx?.subtotal) || 0;
    });
    totalCount = Number(response?.totalCount) || 0;
    page += 1;
    if (!chunk.length) break;
  } while ((page - 1) * PAGE_LIMIT < totalCount);

  return Number(total.toFixed(2));
}

function DashboardIngresos({ selectedProjectId, refreshKey }) {
  const [stats, setStats] = useState(null);
  const [supplierSummary, setSupplierSummary] = useState([]);
  const [supplierSummaryError, setSupplierSummaryError] = useState('');
  const [viewMode, setViewMode] = useState('summary');
  const [showCategoryIva, setShowCategoryIva] = useState(false);
  const [showSupplierIva, setShowSupplierIva] = useState(false);
  const [supplierSortMode, setSupplierSortMode] = useState('alpha');
  const [projectBalance, setProjectBalance] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let ok = true;
    const PAGE_LIMIT = 500;

    async function loadIncomeDashboard() {
      setLoading(true);
      setSupplierSummaryError('');
      try {
        let page = 1;
        let totalCount = 0;
        let items = [];

        do {
          const response = await api.transactions({
            type: 'INCOME',
            page: String(page),
            limit: String(PAGE_LIMIT),
          });
          const chunk = Array.isArray(response?.items) ? response.items : [];
          items = items.concat(chunk);
          totalCount = Number(response?.totalCount) || items.length;
          page += 1;
          if (!chunk.length) break;
        } while (items.length < totalCount);

        if (!ok) return;

        const incomeByCategory = summarizeTransactionsByCategory(items, showCategoryIva);
        setStats(incomeByCategory);
        setSupplierSummary(summarizeTransactionsBySupplier(items, showSupplierIva));

        const expenseTotal = await fetchTransactionsTotalByType('EXPENSE', showCategoryIva);
        if (!ok) return;
        const incomeTotal = Number(incomeByCategory.total_expenses) || 0;
        setProjectBalance(Number((incomeTotal - expenseTotal).toFixed(2)));
      } catch (error) {
        if (!ok) return;
        setStats({ error: error?.message || 'No se pudo cargar el dashboard de ingresos.' });
        setSupplierSummary([]);
        setSupplierSummaryError(error?.message || 'No se pudo cargar el resumen por proveedor de ingresos.');
      } finally {
        if (ok) setLoading(false);
      }
    }

    loadIncomeDashboard();
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

  const biggestCategory = topCategories[0] || null;
  const allocatedPercent = Math.min(
    100,
    Math.max(0, topCategories.reduce((acc, row) => acc + (Number(row.percent) || 0), 0))
  );

  const subtitle =
    viewMode === 'supplier'
      ? 'Resumen operativo de ingresos por proveedor.'
      : viewMode === 'summary'
        ? 'KPIs y visuales de categorías para seguimiento diario de ingresos.'
        : 'Detalle por categoría con proporción sobre el total de ingresos.';

  const dashboardTotals = [
    {
      label: showCategoryIva ? 'Total ingresos con IVA' : 'Total ingresos sin IVA',
      value: formatCurrency(stats?.total_expenses || 0),
      helper: 'Movimientos tipo ingreso',
    },
    {
      label: 'Categorías con movimiento',
      value: String(categoryRows.length),
      helper: 'Con al menos un movimiento',
    },
    {
      label: 'Proveedores con ingreso',
      value: String(supplierSummary.length),
      helper: showSupplierIva ? 'Resumen con IVA' : 'Resumen sin IVA',
    },
    {
      label: 'Balance total del proyecto',
      value: formatCurrency(projectBalance),
      helper: 'Ingresos menos egresos',
    },
  ];

  return (
    <div className="card dashboard-shell">
      <div className="dashboard-header">
        <div>
          <h2 style={{ margin: 0 }}>Dashboard ingresos</h2>
          <div className="small" style={{ marginTop: 4 }}>{subtitle}</div>
        </div>
      </div>

      <div className="dashboard-tabs row" style={{ gap: 8 }}>
        <button className={viewMode === 'summary' ? '' : 'secondary'} onClick={() => setViewMode('summary')}>
          Resumen
        </button>
        <button className={viewMode === 'category' ? '' : 'secondary'} onClick={() => setViewMode('category')}>
          Por categoría
        </button>
        <button className={viewMode === 'supplier' ? '' : 'secondary'} onClick={() => setViewMode('supplier')}>
          Por proveedor
        </button>
      </div>

      <div className="dashboard-controls row">
        {viewMode === 'supplier' ? (
          <>
            <label className="small dashboard-checkbox">
              <input type="checkbox" checked={showSupplierIva} onChange={(e) => setShowSupplierIva(e.target.checked)} />
              Mostrar IVA
            </label>
            <label className="small dashboard-checkbox">
              <input
                type="checkbox"
                checked={supplierSortMode === 'amount'}
                onChange={(e) => setSupplierSortMode(e.target.checked ? 'amount' : 'alpha')}
              />
              Ordenar por monto (mayor a menor)
            </label>
          </>
        ) : (
          <label className="small dashboard-checkbox">
            <input type="checkbox" checked={showCategoryIva} onChange={(e) => setShowCategoryIva(e.target.checked)} />
            Mostrar IVA
          </label>
        )}
      </div>

      {loading ? (
        <div className="dashboard-state">Cargando indicadores del dashboard de ingresos...</div>
      ) : stats?.error ? (
        <div className="dashboard-state dashboard-state-error">Error al cargar categorías: {stats.error}</div>
      ) : (
        <>
          <div className="dashboard-kpi-grid">
            {dashboardTotals.map((item) => (
              <div key={item.label} className="dashboard-kpi-card">
                <span>{item.label}</span>
                <strong>{item.value}</strong>
                <div className="small">{item.helper}</div>
              </div>
            ))}
          </div>

          {viewMode === 'supplier' ? (
            supplierSummaryError ? (
              <div className="dashboard-state dashboard-state-error">Error al cargar proveedores: {supplierSummaryError}</div>
            ) : !sortedSupplierSummary.length ? (
              <div className="dashboard-state">No hay ingresos agrupados por proveedor para este proyecto.</div>
            ) : (
              <div className="dashboard-panel" style={{ marginTop: 4 }}>
                <div className="row" style={{ justifyContent: 'space-between' }}>
                  <h3 style={{ margin: 0 }}>Resumen por proveedor</h3>
                  <span className="badge">Total del resumen: {formatCurrency(supplierTotal)}</span>
                </div>
                <div style={{ overflowX: 'auto' }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Proveedor</th>
                        <th>Movimientos</th>
                        <th>Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedSupplierSummary.map((row) => (
                        <tr key={row.supplierId || row.supplierName}>
                          <td>{row.supplierName || '(Sin proveedor)'}</td>
                          <td>{row.count || 0}</td>
                          <td>{formatCurrency(row.totalAmount)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )
          ) : !categoryRows.length ? (
            <div className="dashboard-state">No hay ingresos registrados para mostrar en categorías.</div>
          ) : viewMode === 'summary' ? (
            <div className="dashboard-summary">
              <div className="dashboard-summary-grid">
                <section className="dashboard-panel">
                  <h3>Comportamiento por categoría</h3>
                  <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="dashboard-line-chart" role="img" aria-label="Tendencia de categorías por monto">
                    <polyline fill="none" stroke="#1f4d96" strokeWidth="2.5" points={chartPoints} />
                  </svg>
                  <div className="small">Comparativo visual de montos entre categorías principales.</div>
                </section>

                <section className="dashboard-panel dashboard-gauge-panel">
                  <h3>Peso del top de categorías</h3>
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
                  <h3>Categorías principales</h3>
                  <div className="grid">
                    {topCategories.map((row) => {
                      const percent = Number(row.percent) || 0;
                      const fillWidth = Math.max(0, Math.min(100, percent));
                      return (
                        <div key={row.category_id} style={{ display: 'grid', gap: 4 }}>
                          <div className="row" style={{ justifyContent: 'space-between' }}>
                            <strong>{row.category_name}</strong>
                            <span className="small">{percent.toFixed(2)}%</span>
                          </div>
                          <div className="bar" aria-label={`Barra de avance de ${row.category_name}`}>
                            <div style={{ width: `${fillWidth}%` }} />
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </section>

                <section className="dashboard-panel">
                  <h3>Montos principales</h3>
                  <div className="dashboard-column-chart">
                    {topCategories.slice(0, 5).map((row) => {
                      const amount = Number(row.amount) || 0;
                      const maxAmount = Number(biggestCategory?.amount) || 1;
                      return (
                        <div key={`column-${row.category_id}`} className="dashboard-column-item">
                          <div className="dashboard-column-value">{formatCurrency(amount)}</div>
                          <div className="dashboard-column-track">
                            <div className="dashboard-column-fill" style={{ height: `${Math.max(12, (amount / maxAmount) * 100)}%` }} />
                          </div>
                          <div className="dashboard-column-label">{row.category_name}</div>
                        </div>
                      );
                    })}
                  </div>
                </section>
              </div>
            </div>
          ) : (
            <div className="dashboard-panel" style={{ marginTop: 4 }}>
              <h3 style={{ margin: 0 }}>Resumen por categoría</h3>
              <div className="grid" style={{ marginTop: 8 }}>
                {categoryRows.map((row) => {
                  const percent = Number(row.percent) || 0;
                  const fillWidth = Math.max(0, Math.min(100, percent));

                  return (
                    <div key={row.category_id} style={{ display: 'grid', gap: 6 }}>
                      <div className="row" style={{ justifyContent: 'space-between' }}>
                        <strong>{row.category_name}</strong>
                        <div>
                          {formatCurrency(row.amount)} <span className="small">({percent.toFixed(2)}%)</span>
                        </div>
                      </div>
                      <div className="bar" aria-label={`Barra de avance de ${row.category_name}`}>
                        <div style={{ width: `${fillWidth}%` }}>
                          <span>{percent.toFixed(2)}%</span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </>
      )}
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
        Nota: si no ves categorías/proveedores, ve a “Ajustes” → “Catálogo”.
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
  const getVendorIdentity = (vendor) => String(vendor?._id || vendor?.id || vendor?.vendorId || vendor?.supplierId || '').trim();
  const getVendorSupplierId = (vendor) => String(vendor?._id || vendor?.id || vendor?.vendorId || vendor?.supplierId || '').trim();
  const getTransactionStableKey = (tx) =>
    tx?._id
    ?? tx?.id
    ?? `${tx?.sourceDb || tx?.source || ''}|${tx?.sapMeta?.paymentNum || ''}|${tx?.sapMeta?.invoiceNum || ''}|${tx?.amount || ''}`;
  const dedupeTransactions = (items) => Array.from(new Map((Array.isArray(items) ? items : []).map((tx) => [getTransactionStableKey(tx), tx])).values());
  const catMap = useMemo(() => Object.fromEntries(cats.map((c) => [c.id, c.name])), [cats]);

  const [rows, setRows] = useState([]);
  const [serverTotals, setServerTotals] = useState(null);
  const [editing, setEditing] = useState(null);
  const [filter, setFilter] = useState('ALL');
  const [categoryFilter, setCategoryFilter] = useState('ALL');
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
      const selectedVendor = vendors.find((vendor) => {
        const vendorId = getVendorIdentity(vendor);
        return vendorId && vendorId === supplierFilter;
      }) || null;

      const supplierIdParam = supplierFilter === 'ALL'
        ? ''
        : String(getVendorSupplierId(selectedVendor) || supplierFilter || '').trim();

      const requestParams = {
        page: String(targetPage),
        limit: String(limit),
        type: filter === 'ALL' ? '' : filter,
        category_id: '',
        supplierId: supplierIdParam,
        projectId: String(selectedProjectId || ''),
        sourceDb: sourceDbFilter === 'ALL' ? '' : sourceDbFilter,
        q: searchFilter.trim(),
        from: dateFrom,
        to: dateTo,
      };
      const response = await api.transactions(requestParams);

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
  }, [filter, supplierFilter, sourceDbFilter, searchFilter, dateFrom, dateTo, selectedProjectId, vendors]);

  useEffect(() => {
    setSelectedRows([]);
  }, [rows, sortBy]);

  const supplierOptions = useMemo(() => {
    const byVendorId = new Map();

    vendors.forEach((vendor) => {
      const value = getVendorIdentity(vendor);
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
        row.supplierName,
        row.projectDisplayName,
        getTransactionCategoryLabel(row, catMap),
        row.categoryCode,
        row.sourceSbo,
      ];
      return searchableFields.some((field) => String(field || '').toLowerCase().includes(query));
    })
    .sort((a, b) => {
    if (sortBy === 'created_desc') {
      const aCreatedAt = a.created_at || '';
      const bCreatedAt = b.created_at || '';
      if (aCreatedAt !== bCreatedAt) return bCreatedAt.localeCompare(aCreatedAt);

      const aDate = String(a.date || '');
      const bDate = String(b.date || '');
      if (aDate === bDate) return 0;
      return bDate.localeCompare(aDate);
    }

    if (sortBy === 'supplier_asc') {
      const aSupplier = String(a.supplierName || '').toLowerCase();
      const bSupplier = String(b.supplierName || '').toLowerCase();
      if (aSupplier !== bSupplier) return aSupplier.localeCompare(bSupplier, 'es');
    }

    const aDate = String(a.date || '');
    const bDate = String(b.date || '');
    if (aDate === bDate) {
      const aCreatedAt = a.created_at || '';
      const bCreatedAt = b.created_at || '';
      return bCreatedAt.localeCompare(aCreatedAt);
    }
    return bDate.localeCompare(aDate);
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
            <option value="ALL">Todos los orígenes</option>
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
        <div className="badge">Total egresos sin IVA: {formatCurrency(backendTotals.expensesWithoutTax)}</div>
        <div className="badge">Total IVA: {formatCurrency(backendTotals.expensesTax)}</div>
        <div className="badge">Total egresos con IVA: {formatCurrency(backendTotals.expensesGross)}</div>
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
                <th>Proyecto</th>
                <th>Proveedor</th>
                <th>Descripción</th>
                <th>Categoría</th>
                <th>Subtotal</th>
                <th>IVA</th>
                <th>Total</th>
                <th>Tipo</th>
                <th>Origen / SBO</th>
                {isAdmin && <th>Acciones</th>}
                {isAdmin && <th>Seleccionar</th>}
              </tr>
            </thead>
            <tbody>
              {isAdmin && (
                <tr>
                  <td colSpan={isAdmin ? 12 : 10} style={{ textAlign: 'right' }}>
                    <label className="row" style={{ justifyContent: 'flex-end' }}>
                      <input type="checkbox" checked={allShownSelected} onChange={toggleSelectAllShown} />
                      Seleccionar todos (página actual)
                    </label>
                  </td>
                </tr>
              )}
              {shown.map((r) => {
                const isSapIva = isSapIvaTransaction(r);
                const totalValue = getTransactionTotalValue(r);
                return (
                <tr key={getTransactionStableKey(r)}>
                  <td>{r.date || '—'}</td>
                  <td>{r.projectDisplayName || 'Sin proyecto'}</td>
                  <td>{r.supplierName || '—'}</td>
                  <td>{r.description || ''}</td>
                  <td>
                    {getTransactionCategoryLabel(r, catMap)}
                    {r.categoryManualName && <span className="badge badge-manual" style={{ marginLeft: 6 }}>Manual</span>}
                  </td>
                  <td style={{ fontWeight: 800 }}>{formatCurrency(r.subtotal ?? 0)}</td>
                  <td style={{ fontWeight: 700 }}>{formatCurrency(r.iva ?? 0)}</td>
                  <td style={{ fontWeight: 700 }}>{formatCurrency(totalValue)}</td>
                  <td>{r.type === 'INCOME' ? 'Ingreso' : 'Egreso'}</td>
                  <td>
                    <SourceBadges transaction={r} />
                  </td>
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
                  {formatCurrency(backendTotals.incomeGross)} / {formatCurrency(backendTotals.expensesGross)}
                </td>
                <td style={{ fontWeight: 700 }}>—</td>
                <td style={{ fontWeight: 700 }}>—</td>
                {isAdmin && <td style={{ fontWeight: 700 }}>Neto: {formatCurrency(backendTotals.net)}</td>}
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
            <label>Proyecto</label>
            <input value={editing.projectDisplayName || ''} disabled />
            <label>Proveedor</label>
            <input value={editing.supplierName || ''} disabled />
            <label>Categoría actual</label>
            <input value={getTransactionCategoryLabel(editing, catMap)} disabled />
            <label>Subtotal / IVA / Total</label>
            <input value={`${formatCurrency(editing.subtotal ?? 0)} / ${formatCurrency(editing.iva ?? 0)} / ${formatCurrency(getTransactionTotalValue(editing))}`} disabled />
            <label>Origen / SBO</label>
            <input value={getTransactionSourceLabel(editing)} disabled />
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

function SearchTransactions({ cats, vendors, projects, selectedProjectId }) {
  const [rows, setRows] = useState([]);
  const [query, setQuery] = useState('');
  const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
  const [supplierFilter, setSupplierFilter] = useState('ALL');
  const [categoryFilter, setCategoryFilter] = useState('ALL');
  const [typeFilter, setTypeFilter] = useState('ALL');
  const [sourceSboFilter, setSourceSboFilter] = useState('ALL');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [page, setPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [exportingPdf, setExportingPdf] = useState(false);
  const [error, setError] = useState('');
  const limit = 50;
  const catMap = useMemo(() => Object.fromEntries(cats.map((c) => [c.id, c.name])), [cats]);
  const selectedProjectName = useMemo(
    () => {
      const project = projects.find((item) => String(item?._id || '') === String(selectedProjectId || ''));
      return project ? getProjectDisplayName(project) : 'SIN PROYECTO';
    },
    [projects, selectedProjectId],
  );
  const reportTitle = `${selectedProjectName} - REPORTE DE EGRESOS`;

  useEffect(() => {
    setPage(1);
  }, [query, supplierFilter, categoryFilter, typeFilter, sourceSboFilter, dateFrom, dateTo, selectedProjectId]);

  useEffect(() => {
    setLoading(true);
    setError('');
    api.transactions({
      type: typeFilter === 'ALL' ? '' : typeFilter,
      page: String(page),
      limit: String(limit),
      q: query.trim(),
      from: dateFrom,
      to: dateTo,
      projectId: String(selectedProjectId || ''),
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
  }, [page, query, typeFilter, dateFrom, dateTo, selectedProjectId]);

  const shown = useMemo(
    () => rows
      .filter((row) => {
        if (supplierFilter === 'ALL') return true;
        return String(row?.supplierName || '').trim() === supplierFilter;
      })
      .filter((row) => {
        if (categoryFilter === 'ALL') return true;
        return String(row?.categoryCode || row?.categoryEffectiveCode || '').trim() === categoryFilter;
      })
      .filter((row) => {
        if (sourceSboFilter === 'ALL') return true;
        return String(row?.sourceSbo || '').trim() === sourceSboFilter;
      })
      .filter((row) => {
        if (!dateFrom && !dateTo) return true;
        return matchesDateRange(row?.date, dateFrom, dateTo);
      })
      .filter((row) => {
        return matchesTransactionSearch(row, query, catMap);
      }),
    [rows, supplierFilter, categoryFilter, sourceSboFilter, dateFrom, dateTo, query, catMap],
  );

  const supplierOptions = useMemo(() => {
    const source = new Set();
    vendors.forEach((vendor) => {
      const name = String(vendor?.name || '').trim();
      if (name) source.add(name);
    });
    rows.forEach((row) => {
      const name = String(row?.supplierName || '').trim();
      if (name) source.add(name);
    });
    return Array.from(source).sort((a, b) => a.localeCompare(b, 'es'));
  }, [vendors, rows]);

  const categoryOptions = useMemo(() => {
    const source = new Map();
    cats.forEach((category) => {
      const key = String(category?.code || category?.id || '').trim();
      if (!key) return;
      source.set(key, category?.displayLabel || category?.name || key);
    });
    rows.forEach((row) => {
      const key = String(row?.categoryCode || row?.categoryEffectiveCode || '').trim();
      if (!key || source.has(key)) return;
      source.set(key, row?.categoryName || row?.categoryEffectiveName || key);
    });
    return Array.from(source.entries()).sort((a, b) => a[1].localeCompare(b[1], 'es'));
  }, [cats, rows]);

  const sourceSboOptions = useMemo(() => {
    const source = new Set();
    rows.forEach((row) => {
      const value = String(row?.sourceSbo || '').trim();
      if (value) source.add(value);
    });
    return Array.from(source).sort((a, b) => a.localeCompare(b, 'es'));
  }, [rows]);

  const filteredTotal = useMemo(
    () => shown.reduce((acc, row) => acc + getTransactionTotalValue(row), 0),
    [shown],
  );
  const rangeStart = totalCount === 0 ? 0 : (page - 1) * limit + 1;
  const rangeEnd = Math.min(page * limit, totalCount);

  function exportSearchResultsToPdf() {
    if (!shown.length) return;
    setExportingPdf(true);

    try {
      const printableRows = shown
        .map((row) => `
            <tr>
              <td>${row.date || '—'}</td>
              <td>${row.projectDisplayName || 'Sin proyecto'}</td>
              <td>${row.supplierName || '—'}</td>
              <td>${row.description || '—'}</td>
              <td>${getTransactionCategoryLabel(row, catMap) || '—'}</td>
              <td class="amount">$${formatMoney(row.subtotal ?? 0)}</td>
              <td class="amount">$${formatMoney(row.iva ?? 0)}</td>
              <td class="amount">$${formatMoney(getTransactionTotalValue(row))}</td>
              <td>${row.type === 'INCOME' ? 'Ingreso' : 'Egreso'}</td>
              <td>${getTransactionSourceLabel(row)}</td>
            </tr>`)
        .join('');

      const popup = window.open('', '_blank', 'width=1400,height=800');
      if (!popup) return;

      popup.document.write(`
        <!doctype html>
        <html lang="es">
          <head>
            <meta charset="UTF-8" />
            <title>${reportTitle}</title>
            <style>
              :root { --primary:#1f4d96; --primary-dark:#12305f; --soft:#e3ebf8; --gray:#334155; --line:#e2e8f0; }
              * { box-sizing: border-box; }
              body { margin: 0; font-family: 'Segoe UI', Arial, sans-serif; color: var(--gray); background: #f8fafc; }
              .sheet { margin: 24px; border: 1px solid var(--line); border-radius: 16px; overflow: hidden; background: #fff; }
              .header { background: linear-gradient(135deg, var(--primary-dark), var(--primary)); color: #fff; padding: 26px 28px; }
              .header h1 { margin: 0; font-size: 26px; letter-spacing: .02em; }
              .header p { margin: 8px 0 0; font-size: 13px; opacity: .95; }
              .summary { display: flex; gap: 12px; flex-wrap: wrap; padding: 16px 24px; background: var(--soft); border-bottom: 1px solid var(--line); }
              .summary-card { background: #fff; border: 1px solid #cbd5e1; border-radius: 12px; padding: 10px 14px; min-width: 220px; }
              .summary-card .label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: #64748b; }
              .summary-card .value { margin-top: 4px; font-size: 18px; font-weight: 700; color: var(--primary-dark); }
              .table-wrap { padding: 14px 24px 24px; }
              table { width: 100%; border-collapse: collapse; }
              th, td { padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; font-size: 12px; }
              thead th { background: #f1f5f9; color: var(--primary-dark); font-weight: 700; }
              .amount { text-align: right; font-weight: 700; color: var(--primary-dark); }
              .footer-total { text-align: right; padding: 14px 24px 22px; font-size: 18px; font-weight: 800; color: var(--primary-dark); }
            </style>
          </head>
          <body>
            <section class="sheet">
              <header class="header">
                <h1>${reportTitle}</h1>
                <p>Generado: ${new Date().toLocaleString('es-MX')} · Consulta: ${query.trim() || 'Sin filtro de texto'}</p>
              </header>
              <div class="summary">
                <div class="summary-card"><div class="label">Resultados en página</div><div class="value">${shown.length}</div></div>
                <div class="summary-card"><div class="label">Resultados totales</div><div class="value">${totalCount}</div></div>
                <div class="summary-card"><div class="label">TOTAL VISIBLE</div><div class="value">$${formatMoney(filteredTotal)}</div></div>
              </div>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr><th>Fecha</th><th>Proyecto</th><th>Proveedor</th><th>Descripción</th><th>Categoría</th><th style="text-align:right">Subtotal</th><th style="text-align:right">IVA</th><th style="text-align:right">Total</th><th>Tipo</th><th>Origen / SBO</th></tr>
                  </thead>
                  <tbody>${printableRows}</tbody>
                </table>
              </div>
              <div class="footer-total">Total visible: $${formatMoney(filteredTotal)}</div>
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
      <div className="search-toolbar" style={{ flexWrap: 'wrap', gap: 8 }}>
        <input
          placeholder="Buscar por descripción, proveedor, categoría, proyecto o SBO"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          style={{ minWidth: 320 }}
        />
        <button type="button" className="secondary" onClick={() => setShowAdvancedFilters((prev) => !prev)}>
          {showAdvancedFilters ? 'Ocultar filtros' : 'Mostrar filtros'}
        </button>
        <button type="button" onClick={exportSearchResultsToPdf} disabled={loading || exportingPdf || !shown.length}>
          {exportingPdf ? 'Exportando...' : 'Exportar PDF'}
        </button>
      </div>
      {showAdvancedFilters && (
        <div className="search-toolbar" style={{ flexWrap: 'wrap', gap: 8, marginTop: 8 }}>
          <select value={supplierFilter} onChange={(e) => setSupplierFilter(e.target.value)}>
            <option value="ALL">Todos los proveedores</option>
            {supplierOptions.map((supplierName) => (
              <option key={supplierName} value={supplierName}>{supplierName}</option>
            ))}
          </select>
          <select value={categoryFilter} onChange={(e) => setCategoryFilter(e.target.value)}>
            <option value="ALL">Todas las categorías</option>
            {categoryOptions.map(([categoryCode, categoryLabel]) => (
              <option key={categoryCode} value={categoryCode}>{categoryLabel}</option>
            ))}
          </select>
          <select value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)}>
            <option value="ALL">Todos los tipos</option>
            <option value="EXPENSE">Egreso</option>
            <option value="INCOME">Ingreso</option>
          </select>
          <select value={sourceSboFilter} onChange={(e) => setSourceSboFilter(e.target.value)}>
            <option value="ALL">Todos los SBO</option>
            {sourceSboOptions.map((sourceSbo) => (
              <option key={sourceSbo} value={sourceSbo}>{sourceSbo}</option>
            ))}
          </select>
          <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} aria-label="Fecha desde" />
          <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} aria-label="Fecha hasta" />
        </div>
      )}
      <div className="small" style={{ marginTop: 8 }}>
        {loading ? 'Buscando...' : `${totalCount} resultados${totalCount ? ` (mostrando ${rangeStart}-${rangeEnd})` : ''} · ${shown.length} visibles tras filtros`}
      </div>
      {!!error && <div className="small" style={{ marginTop: 8, color: '#b91c1c' }}>{error}</div>}
      <div style={{ overflowX: 'auto', marginTop: 10 }}>
        <table>
          <thead>
            <tr>
              <th>Fecha</th>
              <th>Proyecto</th>
              <th>Proveedor</th>
              <th>Descripción</th>
              <th>Categoría</th>
              <th>Subtotal</th>
              <th>IVA</th>
              <th>Total</th>
              <th>Tipo</th>
              <th>Origen / SBO</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((row) => (
              <tr key={row.id}>
                <td>{row.date || '—'}</td>
                <td>{row.projectDisplayName || 'Sin proyecto'}</td>
                <td>{row.supplierName || '—'}</td>
                <td>{row.description || '—'}</td>
                <td>{getTransactionCategoryLabel(row, catMap)}</td>
                <td>{formatCurrency(row.subtotal ?? 0)}</td>
                <td>{formatCurrency(row.iva ?? 0)}</td>
                <td>{formatCurrency(getTransactionTotalValue(row))}</td>
                <td>{row.type === 'INCOME' ? 'Ingreso' : 'Egreso'}</td>
                <td>
                  <SourceBadges transaction={row} />
                </td>
              </tr>
            ))}
            {!shown.length && !loading && (
              <tr><td colSpan={10} className="small">Sin resultados</td></tr>
            )}
          </tbody>
          <tfoot>
            <tr>
              <td colSpan={7} style={{ textAlign: 'right', fontWeight: 700 }}>Total visible</td>
              <td style={{ fontWeight: 700 }}>{formatCurrency(filteredTotal)}</td>
              <td colSpan={2} />
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
        <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
          <h2 style={{ margin: 0 }}>Categorías</h2>
          {isAdmin && (
            <button
              className="secondary"
              onClick={async () => {
                const confirmed = window.confirm('Esto solo agrega faltantes, no borra datos existentes.');
                if (!confirmed) return;
                await api.seed();
                onChanged();
              }}
            >
              Seed categorías
            </button>
          )}
        </div>

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
          {!cats.length && <div className="small">No hay categorías. Puedes presionar “Seed categorías” aquí.</div>}
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
