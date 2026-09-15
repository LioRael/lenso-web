import test from "node:test";
import assert from "node:assert/strict";
import { createEventScope } from "@lenso/workers-runtime";
import { createWebSocketTransport } from "../websocket.mjs";
const tick = () => new Promise((resolve) => setImmediate(resolve));
class Socket extends EventTarget {
  sent = [];
  closes = [];
  accept(options) {
    this.options = options;
  }
  send(value) {
    this.sent.push(value);
  }
  close(code, reason) {
    this.closes.push({ code, reason });
  }
  emit(type, fields = {}) {
    const event = new Event(type);
    Object.assign(event, fields);
    this.dispatchEvent(event);
  }
}
function fixture(options = {}) {
  const server = new Socket(),
    scope = createEventScope();
  let finish, read;
  const closed = new Promise((resolve) => (finish = resolve)),
    received = [];
  let cancelled = 0,
    calls = 0;
  const session = {
    closed,
    value: {
      headers: [
        ["upgrade", "websocket"],
        ["sec-websocket-protocol", "lenso.echo"],
      ],
      read: () => new Promise((resolve) => (read = resolve)),
      send: async (frame) => received.push(frame),
    },
    invoke: (callback) => {
      calls++;
      return callback();
    },
    cancel: () => {
      cancelled++;
      finish();
    },
  };
  const transport = createWebSocketTransport({
    ...options,
    createPair: () => ({ 0: {}, 1: server }),
    createResponse: (socket, headers) => ({ socket, headers }),
  });
  const response = transport(null, session, scope);
  return {
    server,
    scope,
    session,
    response,
    received,
    finish,
    output: (frame) => read(frame),
    cancelled: () => cancelled,
    calls: () => calls,
  };
}
test("text, binary and empty frames preserve payload and selected protocol", async () => {
  const f = fixture();
  f.server.emit("message", { data: "你好" });
  await tick();
  f.server.emit("message", { data: new Uint8Array([0, 255]).buffer });
  await tick();
  f.server.emit("message", { data: "" });
  await tick();
  assert.deepEqual(f.received, [
    { kind: "text", text: "你好" },
    { kind: "binary", body: "AP8=" },
    { kind: "text", text: "" },
  ]);
  f.output({ kind: "binary", body: "AP8=" });
  await tick();
  assert.deepEqual(f.server.sent, [new Uint8Array([0, 255])]);
  assert.equal(f.response.headers.get("upgrade"), null);
  assert.equal(f.response.headers.get("sec-websocket-protocol"), "lenso.echo");
  assert.deepEqual(f.server.options, { allowHalfOpen: true });
  f.server.emit("close", { code: 1000, reason: "done" });
  await tick();
  assert.deepEqual(f.received.at(-1), {
    kind: "close",
    code: 1000,
    reason: "done",
  });
  f.finish();
  await tick();
});
test("blocked ingress is bounded and overflow cancels once", async () => {
  const f = fixture({ maxQueuedMessages: 2 });
  f.session.value.send = () => new Promise(() => {});
  for (let i = 0; i < 4; i++) f.server.emit("message", { data: "x" });
  await tick();
  assert.equal(f.cancelled(), 1);
  assert.equal(f.server.closes[0].code, 1009);
});
test("late native events cannot call a closed event", async () => {
  const f = fixture();
  f.finish();
  await tick();
  const calls = f.calls();
  f.server.emit("message", { data: "late" });
  f.server.emit("close", { code: 1000 });
  await tick();
  assert.equal(f.calls(), calls);
  assert.equal(f.received.length, 0);
});
test("oversized provider output fails the session without enqueueing data", async () => {
  const f = fixture({ maxMessageBytes: 2 });
  f.output({ kind: "text", text: "long" });
  await tick();
  assert.equal(f.cancelled(), 1);
  assert.equal(f.server.sent.length, 0);
  assert.equal(f.server.closes[0].code, 1011);
});
