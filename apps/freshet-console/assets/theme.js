// Runs before first paint (a plain, blocking <script> in <head>), so the page never
// flashes the wrong firing. The choice is "system" unless the viewer picked one;
// app.js owns the toggle and keeps following the OS while the choice is "system".
(() => {
  let choice = "system";
  let focus = "off";
  try {
    choice = localStorage.getItem("freshet.theme") || "system";
    focus = localStorage.getItem("freshet.focus") || "off";
  } catch {
    // Storage can be blocked (private window, a locked-down profile). The defaults
    // are the right page, so there is nothing to recover.
  }
  const dark = choice === "dark" || (choice === "system" && matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.dataset.theme = dark ? "dark" : "light";
  document.documentElement.dataset.focus = focus === "on" ? "on" : "off";
})();
