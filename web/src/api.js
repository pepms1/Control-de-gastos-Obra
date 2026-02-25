const RAW_API_URL =
  (import.meta.env?.VITE_API_URL && import.meta.env.VITE_API_URL.trim()) ||
  'https://control-de-gastos-obra.onrender.com';

// elimina cualquier slash al final
const API_URL = RAW_API_URL.replace(/\/+$/, '');

const TOKEN_KEY = 'obra_token';
const ROLE_KEY = 'obra_role';
const USER_KEY = 'obra_user';

export function getSession() {
  return {
    token: localStorage.getItem(TOKEN_KEY) || '',
    role: localStorage.getItem(ROLE_KEY) || '',
    username: localStorage.getItem(USER_KEY) || '',
  };
}

export function saveSession({ access_token, role, username }) {
  localStorage.setItem(TOKEN_KEY, access_token);
  localStorage.setItem(ROLE_KEY, role);
  localStorage.setItem(USER_KEY, username || '');
}

export function clearSession() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(ROLE_KEY);
  localStorage.removeItem(USER_KEY);
}

async function req(path, opts = {}) {
  const { token } = getSession();

  const headers = {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(opts.headers || {}),
  };

  // asegura que el path siempre empiece con /
  const cleanPath = path.startsWith('/') ? path : `/${path}`;

  const res = await fetch(`${API_URL}${cleanPath}`, {
    ...opts,
    headers,
  });

  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const j = await res.json();
      msg = j.detail || JSON.stringify(j);
    } catch {}

    if (res.status === 401) {
      clearSession();
    }

    throw new Error(msg);
  }

  if (res.status === 204) return null;
  return res.json();
}

export const api = {
  login: (username, password) =>
    req('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),

  me: () => req('/auth/me'),

  seed: () =>
    req('/seed', {
      method: 'POST',
    }),

  categories: () => req('/categories'),
  createCategory: (name) =>
    req('/categories', {
      method: 'POST',
      body: JSON.stringify({ name }),
    }),
  updateCategory: (id, payload) =>
    req(`/categories/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),
  deleteCategory: (id) =>
    req(`/categories/${id}`, {
      method: 'DELETE',
    }),

  vendors: () => req('/vendors'),
  createVendor: (payload) =>
    req('/vendors', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  updateVendor: (id, payload) =>
    req(`/vendors/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),
  deleteVendor: (id) =>
    req(`/vendors/${id}`, {
      method: 'DELETE',
    }),

  transactions: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return req(`/transactions${qs ? `?${qs}` : ''}`);
  },

  createTransaction: (payload) =>
    req('/transactions', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  updateTransaction: (id, payload) =>
    req(`/transactions/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),

  deleteTransaction: (id) =>
    req(`/transactions/${id}`, {
      method: 'DELETE',
    }),

  spendByCategory: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return req(`/stats/spend-by-category${qs ? `?${qs}` : ''}`);
  },
};
