import { mountHome } from './views/home.js';

function route() {
  // For now, only Home is implemented; simple hash handling for future pages
  const path = (location.hash || '#/').replace(/^#/, '');
  if (path === '/' || path === '') {
    mountHome();
  } else {
    // fallback: always render home until other routes are added
    mountHome();
  }
}

window.addEventListener('hashchange', route);
route();