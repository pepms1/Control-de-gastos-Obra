# Handoff: Control de Gastos MDI — Rediseño UI

## Overview

Este paquete contiene el rediseño visual completo de la aplicación **Control de Gastos MDI** (Grupo MDI). Cubre las 4 pantallas principales: Dashboard, Buscar, Presupuestos y Ajustes.

## Sobre los archivos de diseño

Los archivos `.html` en este paquete son **referencias de diseño en HTML** — prototipos de alta fidelidad que muestran la apariencia y comportamiento deseados. **No son código de producción para copiar directamente.**

La tarea para Claude Code es **recrear estos diseños en el codebase existente** (React + Vite, con `styles.css` propio) usando los patrones, componentes y librerías ya establecidos. El HTML es la referencia visual; el target es el repo `pepms1/Control-de-gastos-Obra`.

## Fidelidad

**Alta fidelidad (hifi).** Los prototipos tienen colores finales, tipografía, espaciado e interacciones exactas. El desarrollador debe recrear la UI pixel-perfect usando el sistema de diseño del codebase.

---

## Sistema de diseño existente

El codebase ya tiene un `styles.css` robusto en `web/src/styles.css`. **Reutilizar todas las clases existentes.**

### Fuente
```css
font-family: "Jost", system-ui, sans-serif;
/* Google Fonts: https://fonts.googleapis.com/css2?family=Jost:wght@300;400;500;600;700;800 */
```

### Tokens de color
```css
--primary:        #1b1464;
--primary-dark:   #120d45;
--primary-mid:    #2d229e;
--primary-soft:   #eeedfb;
--gray-50:  #f8f9fa;
--gray-100: #ebeced;
--gray-150: #e0e2e3;
--gray-200: #d0d3d5;
--gray-500: #7c8082;
--gray-600: #5f6365;
--gray-700: #3d4042;
--success-bg: #dcfce7;  --success-text: #15803d;
--danger-bg:  #fee2e2;  --danger-text:  #b91c1c;
--info-bg:    #dbeafe;  --info-text:    #1e40af;
```

### Radios
```css
--radius-sm: 8px;  --radius-md: 12px;
--radius-lg: 16px; --radius-xl: 20px; --radius-full: 999px;
```

### Sombras
```css
--shadow-xs: 0 1px 3px rgba(27,20,100,.06);
--shadow-sm: 0 2px 8px rgba(27,20,100,.08);
--shadow-md: 0 4px 16px rgba(27,20,100,.10);
--shadow-btn: 0 4px 14px rgba(27,20,100,.22);
```

---

## Cambios por pantalla

---

### 1. Navegación (Nav) — `Nav` component en `App.jsx`

**Cambio principal:** Mover las tabs de navegación a la misma línea del header (ya no en segunda fila), centradas absolutamente entre el selector de proyecto y los controles de usuario.

**Layout actual:**
- Fila 1: Logo + select proyecto + (user actions en display:contents)
- Fila 2 (`.nav-links-desktop`): Pill tabs centrado con `flex-basis: 100%`

**Layout nuevo:**
```
[Logo | Grupo MDI]  [Divider]  [Select Proyecto ▾]  ←flex→  [Dashboard][Buscar][Presupuestos][Ajustes]  ←flex→  [🌙][Avatar][Nombre/Rol][|][Salir]
```
Todo en una sola línea de **56px de altura**.

**CSS a modificar en `.nav`:**
```css
.nav {
  display: flex;
  flex-wrap: nowrap;        /* era: wrap */
  align-items: center;
  padding: 0 24px;
  height: 56px;
  position: relative;       /* NUEVO — para el pill absoluto */
}
```

**Pill tabs — posición absoluta centrada:**
```css
/* Reemplazar .nav-links-desktop por: */
.nav-links-desktop {
  position: absolute;
  left: 50%;
  transform: translateX(-50%);
  display: flex;
  align-items: center;
}
```

**Logo box** — agregar un div contenedor coloreado en lugar de `<img>`:
```jsx
<div style={{
  width: 34, height: 34, borderRadius: 8,
  background: 'var(--primary-dark)',
  display: 'flex', alignItems: 'center', justifyContent: 'center'
}}>
  <BuildingIcon size={18} color="#fff" />
</div>
```

**User area** — eliminar los botones "Ajustes" y "Modo noche" del nav-user-actions. Dejar solo:
- Toggle dark mode (ghost button con icono luna/sol)
- Avatar circle con iniciales
- Nombre + rol (2 líneas)
- Divider vertical
- Botón "Salir" ghost

---

### 2. Dashboard — `DashboardSection` component

**Cambio principal:** Reemplazar el contenido actual del Dashboard con:

#### 2a. Switch Egresos / Ingresos
```jsx
// Pill switch arriba a la derecha del título
<div style={{ display:'inline-flex', gap:3, background:'var(--gray-100)', borderRadius:'var(--radius-full)', padding:4 }}>
  <button className={tipo==='EXPENSE' ? '' : 'secondary'}>Egresos</button>
  <button className={tipo==='INCOME' ? '' : 'secondary'}>Ingresos</button>
</div>
```

#### 2b. 3 KPI Cards (grid 3 columnas)
| Card | Valor | Sub |
|------|-------|-----|
| Total de egresos | `formatCurrency(totalSinIva)` | `{count} movimientos · sin IVA` |
| Total de egresos con IVA | `formatCurrency(totalConIva)` | `IVA: ${formatCurrency(totalIva)}` |
| Costo por m² | `formatCurrency(totalSinIva / m2)` | `${m2.toLocaleString()} m² del proyecto` |

**Estructura de cada KPI card:**
```jsx
<div className="dashboard-kpi-card">
  <div style={{ width:36, height:36, borderRadius:10, background:'var(--primary-soft)',
    display:'flex', alignItems:'center', justifyContent:'center' }}>
    <Icon size={18} color="var(--primary)" />
  </div>
  <div>
    <span>{label}</span>       {/* font-size:11px, color:var(--gray-500) */}
    <strong>{value}</strong>   {/* font-size:18px, font-weight:800, color:var(--primary-dark) */}
    <span>{sub}</span>         {/* font-size:10px, color:var(--gray-400) */}
  </div>
</div>
```

#### 2c. Layout en dos columnas (1.6fr / 1fr)
**Columna izquierda — Panel con 3 tabs:**
- `Por mes` → Gráfica de barras verticales con CSS flex (`.dashboard-column-chart`)
- `Por categoría` → Barras horizontales con % relativo al máximo
- `Por proveedor` → Top 6 proveedores por subtotal

**Gráfica de barras verticales:**
```jsx
<div className="dashboard-column-chart">
  {monthly.map(m => (
    <div className="dashboard-column-item">
      <div className="dashboard-column-value">{formatCurrency(m.value)}</div>
      <div className="dashboard-column-track">
        <div className="dashboard-column-fill"
          style={{ height: `${(m.value / maxValue) * 100}%` }} />
      </div>
      <div className="dashboard-column-label">{m.label}</div>
    </div>
  ))}
</div>
```

**Barras horizontales:**
```jsx
<div style={{ display:'grid', gap:8 }}>
  {items.map(([name, val]) => (
    <div>
      <div style={{ display:'flex', justifyContent:'space-between' }}>
        <span>{name}</span>
        <span style={{ fontWeight:700 }}>{formatCurrency(val)}</span>
      </div>
      <div style={{ height:6, background:'var(--gray-100)', borderRadius:99 }}>
        <div style={{ height:'100%', width:`${(val/max)*100}%`,
          background:'linear-gradient(90deg, var(--primary-mid), var(--primary-dark))',
          borderRadius:99 }} />
      </div>
    </div>
  ))}
</div>
```

**Columna derecha:**
1. **Gauge SVG** — semicírculo mostrando `% ejecutado vs presupuesto`
```jsx
<svg width={140} height={82} viewBox="0 0 140 82">
  {/* Track (gray) */}
  <path d="M 16 70 A 54 54 0 0 1 124 70"
    fill="none" stroke="var(--gray-150)" strokeWidth={12} strokeLinecap="round" />
  {/* Fill */}
  <path d="M 16 70 A 54 54 0 0 1 124 70"
    fill="none" stroke="var(--primary-dark)" strokeWidth={12} strokeLinecap="round"
    strokeDasharray={`${Math.PI * 54}`}
    strokeDashoffset={`${Math.PI * 54 * (1 - pct/100)}`} />
  <text x={70} y={64} textAnchor="middle" fontSize={22} fontWeight={800}>{pct}%</text>
</svg>
```

2. **Últimos 5 movimientos** — lista compacta con proveedor, fecha, badge proyecto, monto total

---

### 3. Buscar (SearchTransactionsV2) — `web/src/ui/SearchTransactionsV2.jsx`

**Cambios:**

#### 3a. Eliminar columna SBO de la tabla
Quitar `<th>SBO</th>` y su `<td>` correspondiente del render de filas y del `tfoot` (ajustar `colSpan` de 9 a 8, y el primer `<td colSpan>` del tfoot de 5 a 4 → ya que el total ahora es 8 columnas: Fecha, Proyecto, Proveedor, Descripción, Categoría, Subtotal, IVA, Total).

**Headers finales:** `Fecha | Proyecto | Proveedor | Descripción | Categoría | Subtotal | IVA | Total`

#### 3b. Quitar truncado de texto en celdas
```jsx
// ANTES:
<td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>

// DESPUÉS:
<td style={{ whiteSpace: 'nowrap' }}>
```

#### 3c. Agregar KPI bar arriba de la tabla
4 cards en grid `repeat(4, 1fr)` mostrando en tiempo real:
- Total sin IVA filtrado
- Total con IVA
- IVA acumulado
- Número de proyectos distintos en resultados

Usar clase `.dashboard-kpi-card` del CSS existente.

#### 3d. Switch Egresos / Ingresos / Todos
Reemplazar el `<select>` de tipo por un pill switch visual (3 opciones).

#### 3e. Mover "Total sin IVA filtrado" al status bar
```jsx
<div style={{ padding:'8px 20px', borderBottom:'1px solid var(--gray-100)',
  display:'flex', alignItems:'center', gap:12 }}>
  <span style={{ fontSize:15, fontWeight:700, color:'var(--primary-dark)' }}>
    Total sin IVA filtrado: {formatCurrency(totalWithoutTax)}
  </span>
  <span style={{ color:'var(--gray-300)' }}>·</span>
  <span style={{ fontSize:12, color:'var(--gray-500)' }}>{visibleRows.length} resultados visibles</span>
</div>
```

---

### 4. Presupuestos — `web/src/ui/BudgetsSection.jsx`

**Cambios:**

#### 4a. KPI bar (4 cards)
Antes de la tabla, agregar 4 KPIs calculados sobre `rows`:
- Presupuesto total (`sum(budgetAmount)`)
- Total pagado (`sum(paidAmount)`)
- Saldo disponible (`presupuesto - pagado`, rojo si negativo)
- Avance global (`%`)

#### 4b. Tabla agrupada — mejoras visuales
- Filas de grupo (`budgets-group-row`): agregar una barra de progreso inline en la celda "Avance"
```jsx
<td>
  <div style={{ display:'flex', alignItems:'center', gap:8 }}>
    <div style={{ flex:1, height:6, background:'var(--gray-150)', borderRadius:99, minWidth:60 }}>
      <div style={{ height:'100%', width:`${Math.min(pct,100)}%`,
        background: pct>100 ? 'var(--danger-text)' : 'var(--primary)',
        borderRadius:99 }} />
    </div>
    <span style={{ fontSize:11, fontWeight:700 }}>{pct.toFixed(0)}%</span>
  </div>
</td>
```

- Saldo en rojo si excedido: `style={{ color: remaining < 0 ? 'var(--danger-text)' : undefined }}`

#### 4c. Formulario inline
Convertir el form de grid full-width a una fila inline de 3 campos + botón, visible solo al hacer clic en "+ Nuevo presupuesto":
```
[Proveedor select]  [Concepto input]  [Monto input]  [Crear btn]
```

---

### 5. Ajustes — `web/src/ui/App.jsx` → `Settings` component

**Cambio principal:** El layout ya usa `.settings-layout` (grid 2 cols). No hay cambios estructurales mayores.

**Mejoras menores:**
- El `.settings-sidebar` usa `position: sticky; top: 70px` — verificar que funcione con el nuevo nav de 56px (cambiar a `top: 64px`)
- Los botones `.settings-menu-button` activos: reforzar el estilo active con border izquierdo de 3px:
```css
.settings-menu-button.active {
  border-left: 3px solid var(--primary);
  padding-left: 9px; /* compensar border */
}
```

---

## Interacciones y comportamiento

| Acción | Comportamiento |
|--------|---------------|
| Click tab nav | Cambia pantalla activa sin reload |
| Toggle dark mode | Añade/quita `body.theme-dark` (igual que antes) |
| Selector proyecto | Filtra datos en Dashboard, Buscar y Presupuestos |
| Buscar texto | Filtra filas en tiempo real (debounce 0ms está bien) |
| Mostrar/Ocultar filtros | Toggle de una fila extra de filtros bajo el toolbar |
| Click fila grupo (Presupuestos) | Expande/colapsa filas anidadas con `+/-` |
| Switch Egresos/Ingresos | Filtra `type === 'EXPENSE'` o `type === 'INCOME'` |

---

## Archivos relevantes en el repo

| Archivo | Qué toca este handoff |
|---------|-----------------------|
| `web/src/styles.css` | Tokens, nav, dashboard, budgets — no agregar CSS nuevo si existe clase |
| `web/src/ui/App.jsx` | Nav component, Settings component |
| `web/src/ui/SearchTransactionsV2.jsx` | Quitar SBO, agregar KPI bar, switch pill |
| `web/src/ui/BudgetsSection.jsx` | KPI bar, barra de progreso inline, form inline |

---

## Archivos en este paquete

| Archivo | Descripción |
|---------|-------------|
| `README.md` | Este documento |
| `Gastos Obra MDI v2.html` | Prototipo completo de alta fidelidad con las 4 pantallas |

---

## Notas para la implementación

1. **No reescribir `styles.css`** — todas las clases del diseño ya existen (`.dashboard-kpi-card`, `.budget-badge`, `.nav-tabs-pill`, etc.). Solo ajustar los valores de posicionamiento del nav.
2. **El pill del nav es el cambio más delicado** — asegurarse de que el `position: absolute` del pill no tape los dropdowns o modales que tengan `z-index` alto.
3. **La columna SBO** se elimina solo del render visual — el dato `sourceSbo` se puede mantener en el objeto para exportación PDF (ya se usa en `buildPdfContent`).
4. **m² configurable** — el costo por m² del Dashboard requiere un nuevo campo de configuración por proyecto (o un valor global en Settings). Sugerimos guardarlo en `project.metadata.m2` o como preferencia de usuario en `uiPrefs`.
