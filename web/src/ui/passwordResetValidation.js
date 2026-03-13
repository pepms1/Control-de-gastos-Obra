export function validatePasswordResetFields(newPassword, confirmPassword) {
  const nextPassword = String(newPassword || '');
  const nextConfirm = String(confirmPassword || '');

  if (!nextPassword) return 'La nueva contraseña es obligatoria.';
  if (nextPassword.length < 8) return 'La nueva contraseña debe tener al menos 8 caracteres.';
  if (nextPassword !== nextConfirm) return 'Las contraseñas no coinciden.';
  return '';
}
