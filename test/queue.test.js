'use strict';

// Runs against a real PostgreSQL. Set DATABASE_URL to a throwaway database.
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const q = require('../src');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 20 });
let n = 0;
const queueName = () => `t${process.pid}_${++n}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function waitFor(fn, timeoutMs = 5000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await fn()) return;
    await sleep(20);
  }
  throw new Error('timed out');
}

const statusOf = async id =>
  (await pool.query('SELECT status, attempts, last_error FROM pgq_jobs WHERE id = $1', [id])).rows[0];

before(async () => {
  await pool.query('DROP TABLE IF EXISTS pgq_jobs');
  await q.migrate(pool);
  await q.migrate(pool); // idempotent
});
after(() => pool.end());

test('enqueue, claim and complete', async () => {
  const queue = queueName();
  const id = await q.enqueue(pool, queue, { hello: 'world' });
  const [job] = await q.claim(pool, queue);
  assert.equal(job.id, id);
  assert.deepEqual(job.payload, { hello: 'world' });
  assert.equal(job.attempts, 1);
  assert.equal(await q.complete(pool, job), true);
  assert.equal((await statusOf(id)).status, 'done');
  assert.deepEqual(await q.claim(pool, queue), []);
});

test('parallel claims never hand out the same job twice', async () => {
  const queue = queueName();
  for (let i = 0; i < 200; i++) await q.enqueue(pool, queue, { i });
  const batches = await Promise.all(
    Array.from({ length: 16 }, () => q.claim(pool, queue, { limit: 20 })));
  const ids = batches.flat().map(j => j.id);
  assert.equal(ids.length, 200);
  assert.equal(new Set(ids).size, 200);
});

test('an expired lease is re-claimed and the old holder cannot finish', async () => {
  const queue = queueName();
  const id = await q.enqueue(pool, queue);
  const [first] = await q.claim(pool, queue, { leaseMs: 50 });
  await sleep(120);
  const [second] = await q.claim(pool, queue, { leaseMs: 30_000 });
  assert.equal(second.id, id);
  assert.notEqual(second.lease_token, first.lease_token);
  assert.equal(second.attempts, 2);
  assert.equal(await q.complete(pool, first), false, 'stale lease must not complete');
  assert.equal(await q.fail(pool, first, 'late'), false, 'stale lease must not fail the job');
  assert.equal(await q.complete(pool, second), true);
});

test('failures back off, then the job goes dead', async () => {
  const queue = queueName();
  const id = await q.enqueue(pool, queue, {}, { maxAttempts: 2 });
  let [job] = await q.claim(pool, queue);
  assert.equal(await q.fail(pool, job, new Error('boom'), { baseDelayMs: 200 }), true);
  assert.equal((await statusOf(id)).status, 'queued');
  assert.deepEqual(await q.claim(pool, queue), [], 'not due during backoff');
  await sleep(250);
  [job] = await q.claim(pool, queue);
  assert.equal(job.attempts, 2);
  await q.fail(pool, job, new Error('boom again'));
  const s = await statusOf(id);
  assert.equal(s.status, 'dead');
  assert.match(s.last_error, /boom again/);
});

test('a lease that expires on the last attempt is reaped as dead', async () => {
  const queue = queueName();
  const id = await q.enqueue(pool, queue, {}, { maxAttempts: 1 });
  await q.claim(pool, queue, { leaseMs: 30 });
  await sleep(80);
  assert.deepEqual(await q.claim(pool, queue), []);
  assert.equal(await q.reapExpired(pool, queue), 1);
  assert.equal((await statusOf(id)).status, 'dead');
});

test('dedupe key allows one live job, and frees up once it is done', async () => {
  const queue = queueName();
  const a = await q.enqueue(pool, queue, {}, { dedupeKey: 'order-42' });
  const b = await q.enqueue(pool, queue, {}, { dedupeKey: 'order-42' });
  assert.ok(a);
  assert.equal(b, null);
  const [job] = await q.claim(pool, queue);
  await q.complete(pool, job);
  assert.ok(await q.enqueue(pool, queue, {}, { dedupeKey: 'order-42' }));
});

test('a job enqueued in a rolled-back transaction never runs', async () => {
  const queue = queueName();
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await q.enqueue(c, queue);
    await c.query('ROLLBACK');
  } finally { c.release(); }
  assert.deepEqual(await q.claim(pool, queue), []);
});

test('runAt delays a job', async () => {
  const queue = queueName();
  await q.enqueue(pool, queue, {}, { runAt: new Date(Date.now() + 300) });
  assert.deepEqual(await q.claim(pool, queue), []);
  await sleep(350);
  assert.equal((await q.claim(pool, queue)).length, 1);
});

test('worker wakes on NOTIFY, long before its safety poll', async () => {
  const queue = queueName();
  const done = [];
  const w = new q.Worker(pool, queue, async p => { done.push(p.i); }, { pollMs: 60_000, concurrency: 4 });
  await w.start();
  try {
    const t0 = Date.now();
    for (let i = 0; i < 25; i++) await q.enqueue(pool, queue, { i });
    await waitFor(() => done.length === 25, 3000);
    assert.ok(Date.now() - t0 < 3000);
    assert.deepEqual([...done].sort((x, y) => x - y), Array.from({ length: 25 }, (_, i) => i));
  } finally { await w.stop(); }
});

test('worker retries a failing handler and respects concurrency', async () => {
  const queue = queueName();
  let calls = 0, inFlight = 0, peak = 0;
  const w = new q.Worker(pool, queue, async () => {
    inFlight++; peak = Math.max(peak, inFlight);
    await sleep(30);
    inFlight--;
    if (++calls === 1) throw new Error('first try fails');
  }, { pollMs: 100, concurrency: 3, baseDelayMs: 50 });
  await w.start();
  try {
    const ids = [];
    for (let i = 0; i < 9; i++) ids.push(await q.enqueue(pool, queue));
    await waitFor(async () => {
      const { rows } = await pool.query(
        "SELECT count(*)::int AS c FROM pgq_jobs WHERE queue = $1 AND status = 'done'", [queue]);
      return rows[0].c === 9;
    });
    assert.equal(calls, 10);
    assert.ok(peak <= 3, `peak concurrency ${peak}`);
  } finally { await w.stop(); }
});
