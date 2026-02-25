import React, { useEffect, useMemo, useState } from 'react';
import { api, clearSession, getSession, saveSession } from '../api.js';

/* ================= NAV ================= */

function Nav({ tab, setTab, role, username, onLogout }) {
  const items = [
    ['dashboard', 'Dashboard', true],
    ['add-expense', 'Nuevo egreso', role === 'ADMIN'],
    ['add-income', 'Nuevo ingreso', role === 'ADMIN'],
    ['transactions', 'Movimientos', true],
    ['catalog', 'Catálogo', true],
  ];

  return (
    <div className="nav">
      <div style={{ fontWeight: 800 }}>Control de Obra</div>

      {items
        .filter(([, , show]) => show)
        .map(([k, label]) => (
          <button
            key={k}
            type="button"
            className={tab === k ? 'active' : ''}
            onClick={() => setTab(k)}
            style={{
              background: 'transparent',
              border: 'none',
              padding: 0,
              cursor: 'pointer',
              textAlign: 'left'
            }}
          >
            {label}
          </button>
        ))}

      <div style={{ marginLeft: 'auto' }} className="small">
        {username} ({role})
      </div>

      <button className="secondary" type="button" onClick={onLogout}>
        Salir
      </button>
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
    <div className="container" style={{ maxWidth: 420, marginTop: 80 }}>
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Iniciar sesión</h2>

        <form onSubmit={submit} className="grid">
          <div>
            <label>Usuario</label>
            <input value={username} onChange={(e) => setUsername(e.target.value)} />
          </div>

          <div>
            <label>Contraseña</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>

          {err && <div style={{ color: '#b91c1c' }}>{err}</div>}

          <button disabled={saving}>
            {saving ? 'Ingresando...' : 'Entrar'}
          </button>
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
          ...me,
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

  if (!session.token) {
    return <Login onLogin={setSession} />;
  }

  return (
    <>
      <Nav
        tab={tab}
        setTab={setTab}
        role={session.role}
        username={session.username}
        onLogout={logout}
      />

      <div className="container" style={{ paddingBottom: 40 }}>
        {toast && <div className="card">{toast}</div>}

        {tab === 'dashboard' && <Dashboard isAdmin={isAdmin} />}
        {tab === 'add-expense' && isAdmin && <TxnForm kind="EXPENSE" cats={cats} vendors={vendors} onDone={(m) => { setToast(m); setTab('transactions'); }} />}
        {tab === 'add-income' && isAdmin && <TxnForm kind="INCOME" cats={cats} vendors={vendors} onDone={(m) => { setToast(m); setTab('transactions'); }} />}
        {tab === 'transactions' && <Transactions isAdmin={isAdmin} cats={cats} vendors={vendors} />}
        {tab === 'catalog' && <Catalog isAdmin={isAdmin} cats={cats} vendors={vendors} onChanged={async () => { await refreshCatalog(); setToast('Catálogo actualizado'); }} />}
      </div>
    </>
  );
}

/* ================= DASHBOARD ================= */

function Dashboard({ isAdmin }) {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let ok = true;
    setLoading(true);

    api.spendByCategory()
      .then((s) => {
        if (ok) {
          setStats(s);
          setLoading(false);
        }
      })
      .catch((e) => {
        if (ok) {
          setStats({ error: e.message });
          setLoading(false);
        }
      });

    return () => {
      ok = false;
    };
  }, []);

  return (
    <div className="card">
      <h2 style={{ margin: '0 0 8px' }}>Gasto por categoría</h2>

      {loading ? (
        <div style={{ padding: '12px 0' }}>Cargando...</div>
      ) : stats?.error ? (
        <div style={{ padding: '12px 0' }}>Error: {stats.error}</div>
      ) : (
        <div style={{ marginTop: 12 }}>
          <div className="badge">
            Total egresos: $
            {Number(stats?.total_expenses || 0).toFixed(2)}
          </div>

          {isAdmin && (
            <button
              className="secondary"
              type="button"
              onClick={() =>
                api.seed()
                  .then(() => location.reload())
                  .catch(() => {})
              }
            >
              Seed categorías
            </button>
          )}
        </div>
      )}
    </div>
  );
}
