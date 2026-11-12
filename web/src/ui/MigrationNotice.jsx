import React from 'react';

// La aplicación se migró a Kesef y el backend está apagado: en lugar del
// inicio de sesión (y de cualquier otra pantalla) se muestra este aviso.
export const KESEF_URL = 'https://www.kesef.mx';

export default function MigrationNotice() {
  return (
    <div className="login-page">
      <div className="login-card">
        <div className="login-brand">
          <div className="login-logo-wrap">
            <img
              src="/LOGO%20GRUPO%20MDI.jpg"
              alt="Logo Grupo MDI"
              onError={(event) => {
                event.currentTarget.onerror = null;
                event.currentTarget.src = '/logo-grupo-mdi.svg';
              }}
            />
          </div>
          <p className="login-brand-title">Grupo MDI</p>
          <p className="login-brand-subtitle">control de obra</p>
        </div>
        <p className="login-heading">Esta aplicación ahora es parte de Kesef</p>
        <a className="login-btn migration-link" href={KESEF_URL}>
          www.kesef.mx
        </a>
      </div>
    </div>
  );
}
