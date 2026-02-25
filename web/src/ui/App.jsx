import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';

function Nav({tab, setTab}){
  const items = [
    ['dashboard','Dashboard'],
    ['add-expense','Nuevo egreso'],
    ['add-income','Nuevo ingreso'],
    ['transactions','Movimientos'],
    ['catalog','Catálogo'],
  ];
  return (
    <div className="nav">
      <div style={{fontWeight:800}}>Control de Obra</div>
      {items.map(([k,label])=> (
        <a key={k} href="#" className={tab===k?'active':''} onClick={(e)=>{e.preventDefault(); setTab(k)}}>{label}</a>
      ))}
      <div style={{marginLeft:'auto'}} className="small">
        API: {import.meta.env.VITE_API_URL || '(local)'} 
      </div>
    </div>
  )
}

export default function App(){
  const [tab, setTab] = useState('dashboard');
  const [cats, setCats] = useState([]);
  const [vendors, setVendors] = useState([]);
  const [toast, setToast] = useState('');

  async function refreshCatalog(){
    const [c,v] = await Promise.all([api.categories(), api.vendors()]);
    setCats(c); setVendors(v);
  }

  useEffect(()=>{ refreshCatalog().catch(()=>{}); }, []);

  return (
    <>
      <Nav tab={tab} setTab={setTab} />
      <div className="container grid" style={{gap:14}}>
        {toast && <div className="card">{toast}</div>}

        {tab === 'dashboard' && <Dashboard cats={cats} />}
        {tab === 'add-expense' && <TxnForm kind="EXPENSE" cats={cats} vendors={vendors} onDone={(m)=>{setToast(m); setTab('transactions')}} />}
        {tab === 'add-income' && <TxnForm kind="INCOME" cats={cats} vendors={vendors} onDone={(m)=>{setToast(m); setTab('transactions')}} />}
        {tab === 'transactions' && <Transactions cats={cats} vendors={vendors} />}
        {tab === 'catalog' && <Catalog cats={cats} vendors={vendors} onChanged={async()=>{await refreshCatalog(); setToast('Catálogo actualizado')}} />}
      </div>
    </>
  )
}

function Dashboard({cats}){
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(()=>{
    let ok = true;
    setLoading(true);
    api.spendByCategory().then(s=>{ if(ok){ setStats(s); setLoading(false);} }).catch(e=>{ if(ok){ setStats({error:e.message}); setLoading(false);} });
    return ()=>{ ok=false; };
  }, []);

  return (
    <div className="card">
      <h2 style={{margin:'0 0 8px'}}>Gasto por categoría</h2>
      <div className="small">Porcentaje = gasto de la categoría / total de egresos</div>
      {loading ? <div style={{padding:'12px 0'}}>Cargando...</div> :
        stats?.error ? <div style={{padding:'12px 0'}}>Error: {stats.error}</div> :
        (stats?.rows?.length ? (
          <div style={{marginTop:12}} className="grid" >
            <div className="row" style={{justifyContent:'space-between'}}>
              <div className="badge">Total egresos: ${Number(stats.total_expenses||0).toFixed(2)}</div>
              <button className="secondary" onClick={()=> api.seed().then(()=>location.reload()).catch(()=>{})}>Seed categorías</button>
            </div>
            {stats.rows.map(r=>(
              <div key={r.category_id} style={{display:'grid', gap:6}}>
                <div className="row" style={{justifyContent:'space-between'}}>
                  <div style={{fontWeight:700}}>{r.category_name}</div>
                  <div>${Number(r.amount).toFixed(2)} <span className="small">({r.percent}%)</span></div>
                </div>
                <div className="bar"><div style={{width: Math.min(100, r.percent) + '%'}} /></div>
              </div>
            ))}
          </div>
        ) : (
          <div style={{padding:'12px 0'}}>No hay egresos aún. Registra uno para ver el dashboard.</div>
        ))
      }
    </div>
  );
}

function TxnForm({kind, cats, vendors, onDone}){
  const [amount, setAmount] = useState('');
  const [date, setDate] = useState(new Date().toISOString().slice(0,10));
  const [categoryId, setCategoryId] = useState('');
  const [vendorId, setVendorId] = useState('');
  const [description, setDescription] = useState('');
  const [reference, setReference] = useState('');
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState('');

  useEffect(()=>{
    if(cats.length && !categoryId) setCategoryId(cats[0].id);
    if(vendors.length && !vendorId) setVendorId(vendors[0].id);
  }, [cats, vendors]);

  async function submit(e){
    e.preventDefault();
    setErr('');
    const a = Number(amount);
    if(!a || a <= 0) return setErr('Monto inválido');
    if(kind==='EXPENSE' && (!categoryId || !vendorId)) return setErr('Selecciona categoría y proveedor');

    setSaving(true);
    try{
      await api.createTransaction({
        type: kind,
        date,
        amount: a,
        category_id: kind==='EXPENSE' ? categoryId : null,
        vendor_id: kind==='EXPENSE' ? vendorId : null,
        description,
        reference
      });
      onDone(kind==='EXPENSE' ? 'Egreso guardado' : 'Ingreso guardado');
    }catch(e){
      setErr(e.message);
    }finally{
      setSaving(false);
    }
  }

  return (
    <div className="card">
      <h2 style={{margin:'0 0 8px'}}>{kind==='EXPENSE'?'Nuevo egreso':'Nuevo ingreso'}</h2>
      <form onSubmit={submit} className="grid grid2">
        <div>
          <label>Monto</label>
          <input value={amount} onChange={e=>setAmount(e.target.value)} placeholder="0.00" inputMode="decimal" />
        </div>
        <div>
          <label>Fecha</label>
          <input value={date} onChange={e=>setDate(e.target.value)} placeholder="YYYY-MM-DD" />
        </div>

        {kind==='EXPENSE' && (
          <>
            <div>
              <label>Categoría</label>
              <select value={categoryId} onChange={e=>setCategoryId(e.target.value)}>
                {cats.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
            <div>
              <label>Proveedor</label>
              <select value={vendorId} onChange={e=>setVendorId(e.target.value)}>
                {vendors.map(v=> <option key={v.id} value={v.id}>{v.name}</option>)}
              </select>
            </div>
          </>
        )}

        <div>
          <label>Descripción</label>
          <input value={description} onChange={e=>setDescription(e.target.value)} placeholder="Opcional" />
        </div>
        <div>
          <label>Referencia</label>
          <input value={reference} onChange={e=>setReference(e.target.value)} placeholder="Factura/nota (opcional)" />
        </div>

        {err && <div style={{gridColumn:'1/-1', color:'#b91c1c'}}>{err}</div>}
        <div style={{gridColumn:'1/-1'}}>
          <button disabled={saving}>{saving?'Guardando...':'Guardar'}</button>
        </div>
      </form>
      <div className="small" style={{marginTop:10}}>
        Nota: si no ves categorías/proveedores, ve a “Catálogo” o presiona “Seed categorías” en Dashboard.
      </div>
    </div>
  );
}

function Transactions({cats, vendors}){
  const catMap = useMemo(()=>Object.fromEntries(cats.map(c=>[c.id,c.name])), [cats]);
  const venMap = useMemo(()=>Object.fromEntries(vendors.map(v=>[v.id,v.name])), [vendors]);
  const [rows, setRows] = useState([]);
  const [filter, setFilter] = useState('ALL');
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');

  async function load(){
    setLoading(true); setErr('');
    try{
      const t = await api.transactions();
      setRows(t);
    }catch(e){
      setErr(e.message);
    }finally{
      setLoading(false);
    }
  }

  useEffect(()=>{ load(); }, []);

  const shown = rows.filter(r=> filter==='ALL' ? true : r.type===filter);

  return (
    <div className="card">
      <div className="row" style={{justifyContent:'space-between'}}>
        <h2 style={{margin:0}}>Movimientos</h2>
        <div className="row">
          <select value={filter} onChange={e=>setFilter(e.target.value)}>
            <option value="ALL">Todos</option>
            <option value="INCOME">Ingresos</option>
            <option value="EXPENSE">Egresos</option>
          </select>
          <button className="secondary" onClick={load}>Refrescar</button>
        </div>
      </div>

      {loading ? <div style={{padding:'12px 0'}}>Cargando...</div> :
        err ? <div style={{padding:'12px 0'}}>Error: {err}</div> :
        (shown.length ? (
          <div style={{overflowX:'auto', marginTop:10}}>
            <table>
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Tipo</th>
                  <th>Descripción</th>
                  <th>Categoría</th>
                  <th>Proveedor</th>
                  <th>Monto</th>
                </tr>
              </thead>
              <tbody>
                {shown.map(r=>(
                  <tr key={r.id}>
                    <td>{r.date}</td>
                    <td><span className={'badge ' + (r.type==='INCOME'?'income':'expense')}>{r.type==='INCOME'?'Ingreso':'Egreso'}</span></td>
                    <td>{r.description || ''}</td>
                    <td>{r.category_id ? (catMap[r.category_id]||'') : ''}</td>
                    <td>{r.vendor_id ? (venMap[r.vendor_id]||'') : ''}</td>
                    <td style={{fontWeight:800}}>{r.type==='EXPENSE'?'-':'+'}${Number(r.amount).toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div style={{padding:'12px 0'}}>No hay movimientos.</div>
        ))
      }
    </div>
  )
}

function Catalog({cats, vendors, onChanged}){
  const [catName, setCatName] = useState('');
  const [vendorName, setVendorName] = useState('');
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState('');

  async function addCat(e){
    e.preventDefault();
    setErr('');
    if(catName.trim().length < 2) return setErr('Nombre de categoría inválido');
    setSaving(true);
    try{
      await api.createCategory(catName.trim());
      setCatName('');
      onChanged();
    }catch(e){ setErr(e.message); }
    finally{ setSaving(false); }
  }

  async function addVendor(e){
    e.preventDefault();
    setErr('');
    if(vendorName.trim().length < 2) return setErr('Nombre de proveedor inválido');
    setSaving(true);
    try{
      await api.createVendor({name: vendorName.trim(), category_ids: []});
      setVendorName('');
      onChanged();
    }catch(e){ setErr(e.message); }
    finally{ setSaving(false); }
  }

  return (
    <div className="grid grid2">
      <div className="card">
        <h2 style={{margin:'0 0 8px'}}>Categorías</h2>
        <form onSubmit={addCat} className="row">
          <div style={{flex:1}}>
            <label>Nueva categoría</label>
            <input value={catName} onChange={e=>setCatName(e.target.value)} placeholder="Ej. Acabados" />
          </div>
          <div style={{marginTop:18}}>
            <button disabled={saving}>Agregar</button>
          </div>
        </form>
        <div style={{marginTop:12}} className="grid">
          {cats.map(c=> <div key={c.id} className="badge">{c.name}</div>)}
          {!cats.length && <div className="small">No hay categorías. Puedes presionar “Seed categorías” en Dashboard.</div>}
        </div>
      </div>

      <div className="card">
        <h2 style={{margin:'0 0 8px'}}>Proveedores</h2>
        <form onSubmit={addVendor} className="row">
          <div style={{flex:1}}>
            <label>Nuevo proveedor</label>
            <input value={vendorName} onChange={e=>setVendorName(e.target.value)} placeholder="Ej. Ferretería X" />
          </div>
          <div style={{marginTop:18}}>
            <button disabled={saving}>Agregar</button>
          </div>
        </form>
        <div style={{marginTop:12}} className="grid">
          {vendors.map(v=> <div key={v.id} className="badge">{v.name}</div>)}
          {!vendors.length && <div className="small">No hay proveedores aún.</div>}
        </div>
        {err && <div style={{marginTop:10, color:'#b91c1c'}}>{err}</div>}
      </div>
    </div>
  )
}
