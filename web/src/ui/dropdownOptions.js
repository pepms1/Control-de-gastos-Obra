export function normalizeOptionLabel(label) {
  return String(label || '')
    .trim()
    .replace(/\s+/g, ' ');
}

function normalizeKey(label) {
  return normalizeOptionLabel(label).toLowerCase();
}

function buildOptionEntry(rawOption, getValue, getLabel) {
  const label = String(getLabel(rawOption) || '').trim().replace(/\s+/g, ' ');
  const value = String(getValue(rawOption) || '').trim();
  return { rawOption, label, value, normalizedLabel: normalizeKey(label) };
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
  const byValue = new Map();

  const getCategoryDisplayLabel = (category) => {
    const name = normalizeOptionLabel(category?.name || category?.label || '');
    const code = normalizeOptionLabel(category?.code || '');
    if (!code) return name;
    return name ? `${name} (${code})` : code;
  };

  categories.forEach((category) => {
    const value = String(category?._id || category?.id || category?.categoryId || '').trim();
    if (!value) return;

    const name = normalizeOptionLabel(category?.name || category?.label || '');
    const displayLabel = getCategoryDisplayLabel(category);
    if (!byValue.has(value)) {
      byValue.set(value, {
        ...category,
        _id: category?._id || value,
        id: category?.id || value,
        name,
        displayLabel,
      });
      return;
    }

    const existing = byValue.get(value);
    if ((!existing?.name && name) || (!existing?.displayLabel && displayLabel)) {
      byValue.set(value, {
        ...existing,
        name,
        displayLabel,
      });
    }
  });

  return Array.from(byValue.values()).sort((a, b) => (a.displayLabel || a.name || '').localeCompare((b.displayLabel || b.name || ''), 'es', { sensitivity: 'base' }));
}

export function dedupeVendors(vendors = []) {
  const byValue = new Map();

  vendors.forEach((vendor) => {
    const value = String(vendor?._id || vendor?.id || vendor?.vendorId || vendor?.supplierId || '').trim();
    if (!value) return;

    const name = normalizeOptionLabel(vendor?.name || vendor?.label || '');
    if (!byValue.has(value)) {
      byValue.set(value, {
        ...vendor,
        _id: vendor?._id || value,
        id: vendor?.id || value,
        name,
      });
      return;
    }

    const existing = byValue.get(value);
    if (!existing?.name && name) {
      byValue.set(value, {
        ...existing,
        name,
      });
    }
  });

  return Array.from(byValue.values()).sort((a, b) => (a.name || '').localeCompare(b.name || '', 'es', { sensitivity: 'base' }));
}

export function dedupeSupplierOptions(options = []) {
  return dedupeOptions(options, {
    getValue: (option) => option?.value,
    getLabel: (option) => option?.label,
  });
}
