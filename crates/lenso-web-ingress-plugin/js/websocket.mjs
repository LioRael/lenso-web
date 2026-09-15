// Workers transport only. The Rust Endpoint already authorized the upgrade.
const encoder = new TextEncoder();
function base64(bytes) {
  let text = "";
  for (let offset = 0; offset < bytes.length; offset += 16384)
    text += String.fromCharCode(...bytes.subarray(offset, offset + 16384));
  return btoa(text);
}
function binary(text, limit) {
  if (typeof text !== "string" || text.length > 4 * Math.ceil(limit / 3))
    throw new Error("invalid frame");
  const decoded = atob(text);
  if (decoded.length > limit || btoa(decoded) !== text)
    throw new Error("invalid frame");
  return Uint8Array.from(decoded, (character) => character.charCodeAt(0));
}
/** Workers send() has no drain/credit API. Bound queued input and total session
 * traffic; do not describe native enqueue as peer acknowledgement/backpressure.
 * Workers itself buffers incoming frames before dispatching a message event. */
export function createWebSocketTransport({
  maxMessageBytes = 65536,
  maxQueuedMessages = 16,
  maxQueuedBytes = 1048576,
  maxSessionBytes = 1048576,
  createPair = () => new WebSocketPair(),
  createResponse = (socket, headers) =>
    new Response(null, { status: 101, webSocket: socket, headers }),
} = {}) {
  for (const value of [
    maxMessageBytes,
    maxQueuedMessages,
    maxQueuedBytes,
    maxSessionBytes,
  ]) {
    if (!Number.isSafeInteger(value) || value < 1)
      throw new TypeError("invalid WebSocket limit");
  }
  return (_request, session, scope) => {
    const pair = createPair(),
      server = pair[1];
    let nativeFinished;
    const nativeClosed = new Promise((resolve) => (nativeFinished = resolve));
    let active = true,
      closing = false,
      sending = false,
      peerClosed = false,
      queuedBytes = 0,
      transferred = 0;
    const queue = [];
    const close = (code = 1011, reason = "session unavailable") => {
      if (closing) return;
      closing = true;
      try {
        server.close(code, reason);
      } catch {
        nativeFinished();
      }
    };
    const fail = (code = 1011) => {
      if (!active) return;
      active = false;
      close(code);
      session.cancel();
    };
    const native = scope.operation(() => ({
      promise: nativeClosed,
      abort: () =>
        close(
          scope.invalidated ? 1011 : 1000,
          scope.invalidated ? "session unavailable" : "",
        ),
    }));
    void native.promise.catch(() => {});
    const budget = (bytes) => {
      if (bytes > maxMessageBytes || bytes > maxSessionBytes - transferred)
        throw new Error("frame budget");
      transferred += bytes;
    };
    async function drain() {
      if (sending) return;
      sending = true;
      try {
        while (active && queue.length) {
          const { frame, bytes } = queue.shift();
          try {
            await session.invoke(() => session.value.send(frame));
          } finally {
            queuedBytes -= bytes;
          }
        }
      } catch {
        fail();
      } finally {
        sending = false;
      }
    }
    function enqueue(frame, bytes) {
      if (!active || scope.closed) return;
      if (
        queue.length + (sending ? 1 : 0) >= maxQueuedMessages ||
        bytes > maxQueuedBytes - queuedBytes
      ) {
        fail(1009);
        return;
      }
      queuedBytes += bytes;
      queue.push({ frame, bytes });
      void drain();
    }
    const message = (event) => {
      if (!active || peerClosed || scope.closed) return;
      try {
        let frame, bytes;
        if (typeof event.data === "string") {
          if (event.data.length > maxMessageBytes)
            throw new Error("frame budget");
          bytes = encoder.encode(event.data).byteLength;
          frame = { kind: "text", text: event.data };
        } else if (event.data instanceof ArrayBuffer) {
          bytes = event.data.byteLength;
          if (bytes > maxMessageBytes) throw new Error("frame budget");
          frame = { kind: "binary", body: base64(new Uint8Array(event.data)) };
        } else throw new Error("invalid binary type");
        budget(bytes);
        enqueue(frame, bytes);
      } catch {
        fail(1009);
      }
    };
    const closed = (event) => {
      nativeFinished();
      peerClosed = true;
      if (!active || scope.closed) return;
      if (event.code === 1006) {
        fail();
        return;
      }
      const code = event.code === 1005 ? 1000 : event.code;
      enqueue({ kind: "close", code, reason: event.reason || "" }, 0);
    };
    const failed = () => {
      nativeFinished();
      fail();
    };
    function cleanup() {
      active = false;
      queue.length = 0;
      server.removeEventListener("message", message);
      server.removeEventListener("close", closed);
      server.removeEventListener("error", failed);
    }
    server.binaryType = "arraybuffer";
    server.addEventListener("message", message);
    server.addEventListener("close", closed);
    server.addEventListener("error", failed);
    server.accept({ allowHalfOpen: true });
    session.closed.then(cleanup, () => {
      close();
      cleanup();
    });
    void (async () => {
      try {
        while (active && !scope.closed) {
          const frame = await session.invoke(() => session.value.read());
          if (!active || scope.closed) return;
          if (frame === null) {
            close(1000, "");
            return;
          }
          if (frame.kind === "text" && typeof frame.text === "string") {
            if (frame.text.length > maxMessageBytes)
              throw new Error("frame budget");
            budget(encoder.encode(frame.text).byteLength);
            server.send(frame.text);
          } else if (frame.kind === "binary") {
            const bytes = binary(frame.body, maxMessageBytes);
            budget(bytes.byteLength);
            server.send(bytes);
          } else if (frame.kind === "close") {
            close(Number(frame.code), frame.reason || "");
          } else throw new Error("invalid provider frame");
        }
      } catch {
        fail();
      }
    })();
    const headers = new Headers(session.value.headers);
    // The platform performs its own wire handshake; only the agreed subprotocol
    // and end-to-end fields cross into the Response constructor.
    headers.delete("connection");
    headers.delete("upgrade");
    headers.delete("sec-websocket-accept");
    try {
      return createResponse(pair[0], headers);
    } catch (error) {
      fail();
      cleanup();
      throw error;
    }
  };
}
