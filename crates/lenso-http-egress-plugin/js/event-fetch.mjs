/**
 * Create one event-owned Fetch transport for HttpEgressEventFactory::from_js.
 * Inject event-scoped fetch/timers. No request, controller, Promise or reader is
 * retained between calls. The caller supplies only Rust-validated requests.
 */
export function createEventHttpFetch({ fetch, setTimeout, clearTimeout }) {
  return function eventHttpFetch(request) {
    const controller = new AbortController();
    let reader;
    let timer;
    let timedOut = false;
    let finished = false;
    const failure = code => ({ code });
    let interrupt;
    const interrupted = new Promise((_, reject) => { interrupt = reject; });
    const abort = () => {
      if (finished) return;
      controller.abort();
      interrupt(failure(timedOut ? 'timeout' : 'transport_failure'));
      if (reader) void reader.cancel().catch(() => {});
    };
    const promise = (async () => {
      const { max_response_body_bytes: bodyLimit, max_response_head_bytes: headLimit,
        request_timeout_millis: timeout } = request.limits;
      // Validation is intentionally before allocation/I/O, even at this trusted
      // host boundary. Values come from validated immutable Rust configuration.
      if (![bodyLimit, headLimit, timeout].every(v => Number.isSafeInteger(v) && v > 0)) {
        throw failure('transport_failure');
      }
      timer = setTimeout(() => { timedOut = true; abort(); }, timeout);
      try {
        const response = await Promise.race([fetch(request.url, {
          method: request.method,
          headers: request.headers,
          body: request.body.byteLength ? request.body : undefined,
          redirect: 'manual',
          credentials: 'omit',
          cache: 'no-store',
          signal: controller.signal,
        }), interrupted]);
        if (response.redirected) throw failure('transport_failure');
        const headers = [];
        let headBytes = 0;
        const append = (name, value) => {
          headBytes += name.length + value.length + 4;
          if (headBytes > headLimit) throw failure('response_too_large');
          headers.push([name, value]);
        };
        for (const [name, value] of response.headers) {
          if (name.toLowerCase() !== 'set-cookie') append(name, value);
        }
        // Workers and modern Fetch expose separate Set-Cookie values.
        if (typeof response.headers.getSetCookie === 'function') {
          for (const value of response.headers.getSetCookie()) append('set-cookie', value);
        } else if (typeof response.headers.getAll === 'function') {
          for (const value of response.headers.getAll('set-cookie')) append('set-cookie', value);
        } else if (response.headers.has('set-cookie')) {
          throw failure('transport_failure');
        }
        const length = response.headers.get('content-length');
        if (response.body && length !== null && /^\d+$/.test(length) && Number(length) > bodyLimit) {
          throw failure('response_too_large');
        }
        const chunks = [];
        let total = 0;
        if (response.body) {
          reader = response.body.getReader();
          while (true) {
            const { done, value } = await Promise.race([reader.read(), interrupted]);
            if (done) break;
            total += value.byteLength;
            if (total > bodyLimit) throw failure('response_too_large');
            chunks.push(value);
          }
          reader.releaseLock();
          reader = undefined;
        }
        const body = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
        return { status: response.status, headers, body };
      } catch (error) {
        abort();
        if (timedOut) throw failure('timeout');
        throw error?.code === 'response_too_large' ? error : failure('transport_failure');
      } finally {
        finished = true;
        clearTimeout(timer);
      }
    })();
    return { promise, abort };
  };
}
