// @ts-ignore
const proxy = require("http-proxy-middleware");

module.exports = function(app) {
  app.use(proxy("/api", { target: "http://localhost:6800" }));
  app.use(proxy("/ws", { target: "ws://localhost:6800", ws: true }));
};
