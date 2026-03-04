export function normalizeOptionLabel(label) {
  return String(label || '')
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function buildOptionEntry(rawOption, getValue, getLabel) {
  const label = String(getLabel(rawOption) || '').trim().replace(/\s+/g, ' ');
  const value = String(getValue(rawOption) || '').trim();
  return { rawOption, label, value, normalizedLabel: normalizeOptionLabel(label) };
}

export function dedupeOptions(options, { getValue, getLabel }) {
  const byKey = new Map();

  options.forEach((option) => {
    const entry = buildOptionEntry(option, getValue, getLabel);
    const key = entry.value || `label::${entry.normalizedLabel}`;
    if (!key || key === 'label::') return;

    if (!byKey.has(key)) {
      byKey.set(key, entry);
      return;
    }

    const existing = byKey.get(key);
    if (!existing.label && entry.label) byKey.set(key, entry);
  });

  return Array.from(byKey.values())
    .sort((a, b) => a.label.localeCompare(b.label, 'es', { sensitivity: 'base' }))
    .map(({ rawOption }) => rawOption);
}

export function dedupeCategories(categories = []) {
  return dedupeOptions(categories, {
    getValue: (category) => category?.id || category?._id || category?.categoryId,
    getLabel: (category) => category?.name || category?.label,
  });
}

export function dedupeVendors(vendors = []) {
  return dedupeOptions(vendors, {
    getValue: (vendor) => vendor?.id || vendor?._id || vendor?.vendorId || vendor?.supplierId,
    getLabel: (vendor) => vendor?.name || vendor?.label,
  });
}

export function dedupeSupplierOptions(options = []) {
  return dedupeOptions(options, {
    getValue: (option) => option?.value,
    getLabel: (option) => option?.label,
  });
}
