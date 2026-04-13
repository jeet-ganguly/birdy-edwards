// BIRDY-EDWARDS — dashboard.js
// Shared utilities loaded on every page

// Dismiss any lifted card when overlay clicked
window.dismissLift = window.dismissLift || function() {
  document.querySelectorAll('.lifted').forEach(c => c.classList.remove('lifted'));
  const overlay = document.getElementById('blur-overlay');
  if (overlay) overlay.classList.remove('active');
};