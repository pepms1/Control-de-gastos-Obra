import React from 'react';
import ReactDOM from 'react-dom/client';
import MigrationNotice from './ui/MigrationNotice.jsx';
import './styles.css';

// La aplicación se migró a Kesef (https://www.kesef.mx) y el backend está
// apagado, así que todas las rutas muestran el aviso de migración. Para
// reactivar la app, volver a renderizar <App /> y renderRoute(pathname).
ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <MigrationNotice />
  </React.StrictMode>
);
