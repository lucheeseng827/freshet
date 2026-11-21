// Deployment settings for this console — the one file an install replaces.
//
// Served by freshet-state-server itself (`--console`), the defaults are right: the
// state API answers on the same origin. Served from anywhere else, set apiBase to the
// server's address; the server answers no cross-origin requests, so in practice that
// means a reverse proxy that puts both under one origin.
window.FRESHET_CONSOLE = {
  // Where the state API answers. "" is the origin that served this page.
  apiBase: "",
  // true runs the console on built-in sample data with no server at all. A URL with
  // ?demo does the same for one visit.
  demo: false,
};
