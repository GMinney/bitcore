/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "vendor-chunks/buffer-compare";
exports.ids = ["vendor-chunks/buffer-compare"];
exports.modules = {

/***/ "(ssr)/../bitcore-lib/node_modules/buffer-compare/index.js":
/*!***********************************************************!*\
  !*** ../bitcore-lib/node_modules/buffer-compare/index.js ***!
  \***********************************************************/
/***/ ((module) => {

eval("module.exports = function(a, b) {\n  if (typeof a.compare === 'function') return a.compare(b)\n  if (a === b) return 0\n\n  var x = a.length\n  var y = b.length\n\n  var i = 0\n  var len = Math.min(x, y)\n  while (i < len) {\n    if (a[i] !== b[i]) break\n\n    ++i\n  }\n\n  if (i !== len) {\n    x = a[i]\n    y = b[i]\n  }\n\n  if (x < y) return -1\n  if (y < x) return 1\n  return 0\n}\n\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKHNzcikvLi4vYml0Y29yZS1saWIvbm9kZV9tb2R1bGVzL2J1ZmZlci1jb21wYXJlL2luZGV4LmpzIiwibWFwcGluZ3MiOiJBQUFBO0FBQ0E7QUFDQTs7QUFFQTtBQUNBOztBQUVBO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0EiLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly90aG91Z2h0LWluc2lnaHQvLi4vYml0Y29yZS1saWIvbm9kZV9tb2R1bGVzL2J1ZmZlci1jb21wYXJlL2luZGV4LmpzPzIyMjMiXSwic291cmNlc0NvbnRlbnQiOlsibW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbihhLCBiKSB7XG4gIGlmICh0eXBlb2YgYS5jb21wYXJlID09PSAnZnVuY3Rpb24nKSByZXR1cm4gYS5jb21wYXJlKGIpXG4gIGlmIChhID09PSBiKSByZXR1cm4gMFxuXG4gIHZhciB4ID0gYS5sZW5ndGhcbiAgdmFyIHkgPSBiLmxlbmd0aFxuXG4gIHZhciBpID0gMFxuICB2YXIgbGVuID0gTWF0aC5taW4oeCwgeSlcbiAgd2hpbGUgKGkgPCBsZW4pIHtcbiAgICBpZiAoYVtpXSAhPT0gYltpXSkgYnJlYWtcblxuICAgICsraVxuICB9XG5cbiAgaWYgKGkgIT09IGxlbikge1xuICAgIHggPSBhW2ldXG4gICAgeSA9IGJbaV1cbiAgfVxuXG4gIGlmICh4IDwgeSkgcmV0dXJuIC0xXG4gIGlmICh5IDwgeCkgcmV0dXJuIDFcbiAgcmV0dXJuIDBcbn1cblxuIl0sIm5hbWVzIjpbXSwic291cmNlUm9vdCI6IiJ9\n//# sourceURL=webpack-internal:///(ssr)/../bitcore-lib/node_modules/buffer-compare/index.js\n");

/***/ })

};
;