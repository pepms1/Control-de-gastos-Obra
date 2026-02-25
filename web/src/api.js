const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

async function req(path, opts = {}){
  const res = await fetch(API_URL + path, {
    headers: {'Content-Type':'application/json', ...(opts.headers||{})},
    ...opts
  });
  if(!res.ok){
    let msg = `HTTP ${res.status}`;
    try{ const j = await res.json(); msg = j.detail || JSON.stringify(j); }catch{}
    throw new Error(msg);
  }
  return res.json();
}

export const api = {
  seed: ()=> req('/seed', {method:'POST'}),
  categories: ()=> req('/categories'),
  createCategory: (name)=> req('/categories', {method:'POST', body: JSON.stringify({name})}),
  vendors: ()=> req('/vendors'),
  createVendor: (payload)=> req('/vendors', {method:'POST', body: JSON.stringify(payload)}),
  transactions: (params={})=>{
    const qs = new URLSearchParams(params).toString();
    return req('/transactions' + (qs?`?${qs}`:''));
  },
  createTransaction: (payload)=> req('/transactions', {method:'POST', body: JSON.stringify(payload)}),
  spendByCategory: (params={})=>{
    const qs = new URLSearchParams(params).toString();
    return req('/stats/spend-by-category' + (qs?`?${qs}`:''));
  }
};
