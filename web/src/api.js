const RAW_API_URL =
  (import.meta.env?.VITE_API_URL && import.meta.env.VITE_API_URL.trim()) ||
  'https://control-de-gastos-obra.onrender.com';

const RAW_BACKEND_URL =
  (import.meta.env?.VITE_BACKEND_URL && import.meta.env.VITE_BACKEND_URL.trim()) || RAW_API_URL;

// elimina cualquier slash al final
const API_URL = RAW_API_URL.replace(/\/+$/, '');
const BACKEND_URL = RAW_BACKEND_URL.replace(/\/+$/, '');

const TOKEN_KEY = 'obra_token';
const ROLE_KEY = 'obra_role';
const USER_KEY = 'obra_user';
const DISPLAY_NAME_KEY = 'obra_display_name';
export const SELECTED_PROJECT_KEY = 'selectedProjectId';

function getSelectedProjectId() {
  return localStorage.getItem(SELECTED_PROJECT_KEY) || '';
}

function shouldInjectProjectId(method, path) {
  if ((method || 'GET').toUpperCase() !== 'GET') return false;

  const pathname = path.split('?')[0] || '';
  if (pathname === '/api/projects' || pathname.startsWith('/auth/')) return false;

  return [
    '/transactions',
    '/api/transactions',
    '/api/movimientos',
    '/api/expenses/summary-by-supplier',
    '/api/suppliers',
    '/stats/spend-by-category',
    '/categories',
    '/vendors',
  ].some((target) => pathname === target || pathname.startsWith(`${target}/`));
}

function withProjectId(path, opts = {}) {
  if (!shouldInjectProjectId(opts.method, path)) return path;

  const [pathname, queryString = ''] = path.split('?');
  const qs = new URLSearchParams(queryString);
  if (qs.has('projectId')) return path;

  const selectedProjectId = getSelectedProjectId();
  if (!selectedProjectId) return path;

  qs.set('projectId', selectedProjectId);
  return `${pathname}?${qs.toString()}`;
}

export function getSession() {
  return {
    token: localStorage.getItem(TOKEN_KEY) || '',
    role: localStorage.getItem(ROLE_KEY) || '',
    username: localStorage.getItem(USER_KEY) || '',
    displayName: localStorage.getItem(DISPLAY_NAME_KEY) || '',
  };
}

export function saveSession({ access_token, token, role, username, displayName }) {
  localStorage.setItem(TOKEN_KEY, access_token || token || '');
  localStorage.setItem(ROLE_KEY, role || '');
  localStorage.setItem(USER_KEY, username || '');
  localStorage.setItem(DISPLAY_NAME_KEY, displayName || username || '');
}

export function clearSession() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(ROLE_KEY);
  localStorage.removeItem(USER_KEY);
  localStorage.removeItem(DISPLAY_NAME_KEY);
}

async function request(baseUrl, path, opts = {}) {
  const { token } = getSession();
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  const cleanPath = withProjectId(normalizedPath, opts);
  const cleanPathname = cleanPath.split('?')[0] || '';
  const selectedProjectId = getSelectedProjectId();
  const shouldAttachProjectHeader = Boolean(
    selectedProjectId
      && !cleanPathname.startsWith('/auth/')
      && cleanPathname !== '/api/projects'
      && cleanPathname !== '/auth'
  );
  const isFormData = opts.body instanceof FormData;

  const headers = {
    ...(isFormData ? {} : { 'Content-Type': 'application/json' }),
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(shouldAttachProjectHeader ? { 'X-Project-Id': selectedProjectId } : {}),
    ...(opts.headers || {}),
  };

  const res = await fetch(`${baseUrl}${cleanPath}`, {
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

async function req(path, opts = {}) {
  return request(API_URL, path, opts);
}

async function backendReq(path, opts = {}) {
  return request(BACKEND_URL, path, opts);
}

/**
 * @typedef {Object} TransactionDTO
 * @property {string=} category_hint_code
 * @property {string=} category_hint_name
 * @property {string=} CategoryHintCode
 * @property {string=} CategoryHintName
 */

function normalizeTransaction(transaction) {
  if (!transaction || typeof transaction !== 'object') return transaction;

  const categoryHintName = transaction.categoryHintName
    || transaction.category_hint_name
    || transaction.CategoryHintName
    || '';
  const categoryHintCode = transaction.categoryHintCode
    || transaction.category_hint_code
    || transaction.CategoryHintCode
    || '';

  return {
    ...transaction,
    categoryHintName,
    categoryHintCode,
    category_hint_name: transaction.category_hint_name || categoryHintName,
    category_hint_code: transaction.category_hint_code || categoryHintCode,
  };
}

export const api = {
  projects: () => req('/api/projects'),

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
    return req(`/transactions${qs ? `?${qs}` : ''}`).then((response) => {
      if (!response || typeof response !== 'object') return response;
      return {
        ...response,
        items: Array.isArray(response.items) ? response.items.map(normalizeTransaction) : [],
      };
    });
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

  updateProjectTransaction: (projectId, txId, payload) =>
    backendReq(`/api/projects/${projectId}/transactions/${txId}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),

  bulkUpdateTransactionCategory: (payload) =>
    req('/transactions/bulk-update-category', {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),

  bulkUpdateProjectTransactionCategory: (projectId, payload) =>
    backendReq(`/api/projects/${projectId}/transactions/bulk-update-category`, {
      method: 'POST',
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

  expensesSummaryBySupplier: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return backendReq(`/api/expenses/summary-by-supplier${qs ? `?${qs}` : ''}`);
  },

  importSapPayments: (file, project = 'CALDERON DE LA BARCA') => {
    const formData = new FormData();
    formData.append('file', file);
    const qs = new URLSearchParams({ project }).toString();
    return backendReq(`/api/import/sap-payments?${qs}`, {
      method: 'POST',
      body: formData,
    });
  },

  supplierCategories: () => backendReq('/api/supplier-categories'),

  createSupplierCategory: (name) =>
    backendReq('/api/supplier-categories', {
      method: 'POST',
      body: JSON.stringify({ name }),
    }),

  unclassifiedSuppliers: () => backendReq('/api/suppliers?uncategorized=1'),

  suppliers: () => backendReq('/api/suppliers'),

  updateSupplierCategory: (id, categoryId) =>
    backendReq(`/api/suppliers/${id}`, {
      method: 'PATCH',
      body: JSON.stringify({ category_id: categoryId }),
    }),

  adminRawCollections: () => backendReq('/api/admin/raw-data/collections'),

  adminRawRows: (collection, params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return backendReq(`/api/admin/raw-data/${encodeURIComponent(collection)}${qs ? `?${qs}` : ''}`);
  },

  adminRawUpdateRow: (collection, rowId, changes) =>
    backendReq(`/api/admin/raw-data/${encodeURIComponent(collection)}/${rowId}`, {
      method: 'PATCH',
      body: JSON.stringify({ changes }),
    }),
};
