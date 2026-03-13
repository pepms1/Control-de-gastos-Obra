export const ROLES = {
  SUPERADMIN: 'SUPERADMIN',
  ADMIN: 'ADMIN',
  VIEWER: 'VIEWER',
};

export function normalizeRole(role) {
  const value = String(role || '').trim().toUpperCase();
  if (value === 'LEGACY_ADMIN') return ROLES.SUPERADMIN;
  if (value === ROLES.SUPERADMIN || value === ROLES.ADMIN || value === ROLES.VIEWER) return value;
  if (value === 'ADMINISTRADOR') return ROLES.SUPERADMIN;
  return ROLES.SUPERADMIN;
}

export function isSuperAdmin(userOrRole) {
  const role = typeof userOrRole === 'string' ? userOrRole : userOrRole?.role;
  return normalizeRole(role) === ROLES.SUPERADMIN;
}

export function isAdmin(userOrRole) {
  const role = typeof userOrRole === 'string' ? userOrRole : userOrRole?.role;
  return normalizeRole(role) === ROLES.ADMIN;
}

export function isViewer(userOrRole) {
  const role = typeof userOrRole === 'string' ? userOrRole : userOrRole?.role;
  return normalizeRole(role) === ROLES.VIEWER;
}
