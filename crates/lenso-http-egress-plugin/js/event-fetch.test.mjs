import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createEventHttpFetch } from './event-fetch.mjs';

const input = (url, overrides = {}) => ({ url, method: 'GET', headers: [], body: new Uint8Array(), limits: { max_response_body_bytes: 32, max_response_head_bytes: 2048, request_timeout_millis: 200 }, ...overrides });
const tracked = (fetchImpl = fetch) => {
  const timers = new Set();
  const transport = createEventHttpFetch({ fetch: fetchImpl,
    setTimeout(fn, ms) { const timer = setTimeout(fn, ms); timers.add(timer); return timer; },
    clearTimeout(timer) { timers.delete(timer); clearTimeout(timer); },
  });
  return { transport, timers };
};

test('real Fetch preserves binary and cookies, exposes redirect without following it', async () => {
  let followed = 0;
  const server = createServer((req, res) => {
    if (req.url === '/redirect') { res.writeHead(307, { location: '/target' }).end(); return; }
    if (req.url === '/target') { followed++; res.end('bad'); return; }
    res.setHeader('set-cookie', ['a=1; Secure; HttpOnly', 'b=2; Secure; HttpOnly']);
    res.end(Buffer.from([0, 255, 128, 1]));
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  try {
    const origin = `http://127.0.0.1:${server.address().port}`;
    const { transport, timers } = tracked();
    const binary = await transport(input(origin)).promise;
    assert.deepEqual([...binary.body], [0, 255, 128, 1]);
    assert.equal(binary.headers.filter(([name]) => name === 'set-cookie').length, 2);
    const redirect = await transport(input(`${origin}/redirect`)).promise;
    assert.equal(redirect.status, 307); assert.equal(followed, 0); assert.equal(timers.size, 0);
  } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});

test('trusted bridge enforces limits, manual mode and timeout even for an uncooperative fetch', async () => {
  let options;
  const { transport, timers } = tracked(async (_url, init) => { options = init; return new Response(new Uint8Array(33)); });
  await assert.rejects(transport(input('https://example.test')).promise, { code: 'response_too_large' });
  assert.equal(options.redirect, 'manual'); assert.equal(options.credentials, 'omit'); assert.equal(options.cache, 'no-store'); assert(options.signal.aborted); assert.equal(timers.size, 0);
  const hanging = tracked(() => new Promise(() => {}));
  const request = input('https://example.test'); request.limits.request_timeout_millis = 10;
  await assert.rejects(hanging.transport(request).promise, { code: 'timeout' });
  assert.equal(hanging.timers.size, 0);
});

test('response stream overflow and explicit abandonment cancel readers and clear timers', async () => {
  let cancelled = 0;
  const { transport, timers } = tracked(async () => new Response(new ReadableStream({
    start(controller) { controller.enqueue(new Uint8Array(20)); controller.enqueue(new Uint8Array(20)); },
    cancel() { cancelled++; },
  })));
  await assert.rejects(transport(input('https://example.test')).promise, { code: 'response_too_large' });
  assert.equal(cancelled, 1); assert.equal(timers.size, 0);
  const hanging = tracked(async () => new Response(new ReadableStream({ cancel() { cancelled++; } })));
  const operation = hanging.transport(input('https://example.test'));
  await Promise.resolve(); await Promise.resolve();
  operation.abort();
  await assert.rejects(operation.promise, { code: 'transport_failure' });
  assert.equal(hanging.timers.size, 0);
});

test('declared response length and header bounds reject before body consumption', async () => {
  const oversized = tracked(async () => new Response('', { headers: { 'content-length': '999' } }));
  await assert.rejects(oversized.transport(input('https://example.test')).promise, { code: 'response_too_large' });
  const headers = tracked(async () => new Response('', { headers: { 'x-large': 'x'.repeat(3000) } }));
  await assert.rejects(headers.transport(input('https://example.test')).promise, { code: 'response_too_large' });
});
