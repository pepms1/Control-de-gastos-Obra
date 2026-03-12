import { normalizeTransaction } from './transactions/normalizeTransaction.js';

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
const USER_ID_KEY = 'obra_user_id';
const USER_EMAIL_KEY = 'obra_user_email';
const USER_ACTIVE_KEY = 'obra_user_is_active';
const USER_UI_PREFS_KEY = 'obra_user_ui_prefs';
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
    id: localStorage.getItem(USER_ID_KEY) || '',
    email: localStorage.getItem(USER_EMAIL_KEY) || '',
    isActive: localStorage.getItem(USER_ACTIVE_KEY) !== 'false',
    uiPrefs: (() => {
      try {
        const raw = localStorage.getItem(USER_UI_PREFS_KEY);
        return raw ? JSON.parse(raw) : { hiddenProjectIds: [], defaultProjectId: '' };
      } catch {
        return { hiddenProjectIds: [], defaultProjectId: '' };
      }
    })(),
  };
}

export function saveSession({ access_token, token, role, username, displayName, id, email, isActive, name, uiPrefs }) {
  localStorage.setItem(TOKEN_KEY, access_token || token || '');
  localStorage.setItem(ROLE_KEY, role || '');
  localStorage.setItem(USER_KEY, username || '');
  localStorage.setItem(DISPLAY_NAME_KEY, displayName || name || username || '');
  localStorage.setItem(USER_ID_KEY, id || '');
  localStorage.setItem(USER_EMAIL_KEY, email || '');
  localStorage.setItem(USER_ACTIVE_KEY, String(isActive !== false));
  localStorage.setItem(USER_UI_PREFS_KEY, JSON.stringify(uiPrefs && typeof uiPrefs === 'object' ? uiPrefs : { hiddenProjectIds: [], defaultProjectId: '' }));
}

export function clearSession() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(ROLE_KEY);
  localStorage.removeItem(USER_KEY);
  localStorage.removeItem(DISPLAY_NAME_KEY);
  localStorage.removeItem(USER_ID_KEY);
  localStorage.removeItem(USER_EMAIL_KEY);
  localStorage.removeItem(USER_ACTIVE_KEY);
  localStorage.removeItem(USER_UI_PREFS_KEY);
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
      && !cleanPathname.startsWith('/api/admin/projects')
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
    let errorBody = null;
    try {
      const j = await res.json();
      errorBody = j;
      msg = (j && (j.detail?.message || j.detail || j.message)) || JSON.stringify(j);
    } catch {}

    if (res.status === 401) {
      clearSession();
    }

    const error = new Error(msg);
    error.status = res.status;
    error.body = errorBody;
    error.detail = errorBody?.detail;
    throw error;
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

function normalizeSapMovementsBySboResponse(payload) {
  const candidates = [
    payload,
    payload?.data,
    payload?.result,
    payload?.data?.result,
    payload?.result?.data,
  ].filter(Boolean);

  const normalized = candidates.find(
    (item) => item && typeof item === 'object' && (
      'status' in item
      || 'rowsTotal' in item
      || 'rowsOk' in item
      || 'imported' in item
      || 'updated' in item
      || 'unmatched' in item
      || 'importRunId' in item
      || 'already_imported' in item
    )
  ) || payload;

  return {
    status: normalized?.status ?? null,
    rowsTotal: normalized?.rowsTotal ?? null,
    rowsOk: normalized?.rowsOk ?? null,
    imported: normalized?.imported ?? null,
    updated: normalized?.updated ?? null,
    unmatched: normalized?.unmatched ?? null,
    importRunId: normalized?.importRunId ?? null,
    already_imported: normalized?.already_imported ?? null,
  };
}

export const api = {
  projects: () => req('/api/projects'),

  login: (username, password) =>
    req('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),

  me: () => req('/api/me'),

  updateMyPreferences: (payload) =>
    req('/api/me/preferences', {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),

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
    const path = `/transactions${qs ? `?${qs}` : ''}`;

    return req(path).then((response) => {
      if (!response || typeof response !== 'object') return response;
      const items = Array.isArray(response.items)
        ? response.items
        : (Array.isArray(response.rows) ? response.rows : []);
      return {
        ...response,
        items: items.map(normalizeTransaction),
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
  projectSupplierCategories: (projectId) => backendReq(`/api/projects/${projectId}/supplier-categories`),

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

  importSapPayments: (file, project, projectId, force = false) => {
    const formData = new FormData();
    formData.append('file', file);
    const qs = new URLSearchParams();
    if (project) qs.set('project', project);
    if (projectId) qs.set('projectId', projectId);
    if (force) qs.set('force', '1');
    return backendReq(`/api/import/sap-payments${qs ? `?${qs}` : ''}`, {
      method: 'POST',
      body: formData,
    });
  },

  adminImportSapLatest: ({ projectId, sources = [] }) =>
    backendReq('/api/admin/import/sap-latest', {
      method: 'POST',
      body: JSON.stringify({ projectId, sources }),
    }),

  importSapMovementsBySbo: ({ sbo, mode, force = false }) => {
    const qs = new URLSearchParams({ sbo, mode });
    if (force) qs.set('force', '1');
    return backendReq(`/api/cron/import/sap-movements-by-sbo?${qs}`, {
      method: 'POST',
      headers: { 'X-Trigger-Source': 'frontend' },
    }).then((response) => normalizeSapMovementsBySboResponse(response));
  },

  supplierCategories: () => backendReq('/api/supplier-categories'),

  createSupplierCategory: (name) =>
    backendReq('/api/supplier-categories', {
      method: 'POST',
      body: JSON.stringify({ name }),
    }),

  unclassifiedSuppliers: () => backendReq('/api/suppliers?uncategorized=1'),

  suppliers: () => backendReq('/api/suppliers'),

  adminTrabajosEspecialesSuppliers: () => backendReq('/api/admin/trabajos-especiales/suppliers'),

  adminTrabajosEspecialesSupplierCategory2Rules: () => backendReq('/api/admin/trabajos-especiales/supplier-category2-rules'),
  adminGlobalCategories: () => backendReq('/api/admin/categories/global'),

  upsertAdminTrabajosEspecialesSupplierCategory2Rule: (payload) =>
    backendReq('/api/admin/trabajos-especiales/supplier-category2-rules', {
      method: 'PUT',
      body: JSON.stringify(payload),
    }),

  deactivateAdminTrabajosEspecialesSupplierCategory2Rule: (supplierKey) =>
    backendReq(`/api/admin/trabajos-especiales/supplier-category2-rules/${encodeURIComponent(supplierKey)}`, {
      method: 'DELETE',
    }),

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

  createProjectsFromUnmatchedAdmin: () =>
    backendReq('/api/admin/projects/create-from-unmatched', {
      method: 'POST',
    }),

  adminProjects: () => backendReq('/api/admin/projects'),

  adminUsers: () => backendReq('/api/admin/users'),

  createAdminUser: (payload) =>
    backendReq('/api/admin/users', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  updateAdminUser: (userId, payload) =>
    backendReq(`/api/admin/users/${userId}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),

  updateAdminProjectVisibility: (projectId, visibleInFrontend) =>
    backendReq(`/api/admin/projects/${projectId}/visibility`, {
      method: 'PATCH',
      body: JSON.stringify({ visibleInFrontend: Boolean(visibleInFrontend) }),
    }),
};
