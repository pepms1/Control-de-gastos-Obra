import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './ui/App.jsx';
import { renderRoute } from './ui/ImportAndAdminScreens.jsx';
import './styles.css';

const pathname = window.location.pathname.replace(/\/+$/, '') || '/';
const routeView = renderRoute(pathname);

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>{routeView || <App />}</React.StrictMode>
);
