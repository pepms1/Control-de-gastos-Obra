import test from 'node:test';
import assert from 'node:assert/strict';

import { validatePasswordResetFields } from '../src/ui/passwordResetValidation.js';

test('fails when password is empty', () => {
  assert.equal(validatePasswordResetFields('', ''), 'La nueva contraseña es obligatoria.');
});

test('fails when password is too short', () => {
  assert.equal(validatePasswordResetFields('abc123', 'abc123'), 'La nueva contraseña debe tener al menos 8 caracteres.');
});

test('fails when password confirmation does not match', () => {
  assert.equal(validatePasswordResetFields('12345678', '12345679'), 'Las contraseñas no coinciden.');
});

test('passes when both fields are valid', () => {
  assert.equal(validatePasswordResetFields('12345678', '12345678'), '');
});
