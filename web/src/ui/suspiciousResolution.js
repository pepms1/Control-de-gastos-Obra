export const MISSING_TRANSACTION_ID_ERROR = 'Missing transaction id for suspicious resolution';

export function getSuspiciousTransactionId(row) {
  const candidates = [row?.transactionId, row?.id, row?._id];
  for (const candidate of candidates) {
    const value = String(candidate || '').trim();
    if (value) return value;
  }
  return '';
}

export function getSuspiciousResolutionTarget(row) {
  const transactionId = getSuspiciousTransactionId(row);
  if (!transactionId) {
    return {
      transactionId: '',
      canResolve: false,
      error: MISSING_TRANSACTION_ID_ERROR,
    };
  }
  return {
    transactionId,
    canResolve: true,
    error: '',
  };
}
