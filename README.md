# pg-lease-queue

A durable job queue that lives in your PostgreSQL database. About 250 lines of Node.js, one table, and `pg` as the only dependency.

I wrote it as a standalone, open version of the job-queue design I use in production for ride-hailing dispatch ([case study](https://github.com/ywtzdev/callme-taxi-showcase)). There, the queue sends order offers and push notifications to about 1,000 drivers, and a worker that dies halfway through a job must never cause an order to be sent twice or lost.

[![test](https://github.com/ywtzdev/pg-lease-queue/actions/workflows/test.yml/badge.svg)](https://github.com/ywtzdev/pg-lease-queue/actions/workflows/test.yml)

## Why a queue in Postgres

If your data is already in Postgres, a job queue there has two useful properties:

- **Enqueue in the same transaction as the business write.** If the order insert rolls back, the "notify drivers" job never existed. With Redis or a separate broker you have to solve that yourself.
- **One less system to run, back up and monitor.**

It fits workloads of up to a few thousand jobs per second. Beyond that, use a dedicated broker.

## What it guarantees

| Situation | What happens |
|---|---|
| Two workers poll at the same moment | `FOR UPDATE SKIP LOCKED`: each job goes to exactly one of them |
| A worker crashes mid-job | Its lease expires and another worker picks the job up |
| The crashed worker comes back and tries to finish | Refused. Every claim gets a new `lease_token`, and `complete`/`fail` only succeed with the current one |
| The handler throws | Retried with exponential backoff, then marked `dead` after `maxAttempts` |
| The same job is enqueued twice | With a `dedupeKey`, only one live copy is kept |
| No jobs for hours | Workers sleep on `LISTEN` instead of polling, with one slow safety poll for delayed jobs and expired leases |

Delivery is **at least once**: a handler can run again if its worker dies after doing the work but before recording it. Make handlers idempotent, for example with a unique key on what they write.

## Usage

```bash
npm install pg-lease-queue pg
```

```js
const { Pool } = require('pg');
const { migrate, enqueue, Worker } = require('pg-lease-queue');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool); // creates pgq_jobs; safe on every start

// Enqueue inside your own transaction
const client = await pool.connect();
try {
  await client.query('BEGIN');
  const { rows } = await client.query('INSERT INTO orders (pickup) VALUES ($1) RETURNING id', ['Sule Pagoda']);
  await enqueue(client, 'dispatch', { orderId: rows[0].id }, { dedupeKey: `dispatch:${rows[0].id}` });
  await client.query('COMMIT');
} catch (e) {
  await client.query('ROLLBACK');
  throw e;
} finally {
  client.release();
}

// Process
const worker = new Worker(pool, 'dispatch', async (payload, job) => {
  await sendOffers(payload.orderId);
}, { concurrency: 8, leaseMs: 30_000 });

worker.on('job:failed', (job, err) => console.warn('job', job.id, 'attempt', job.attempts, err.message));
worker.on('lease:lost', job => console.warn('lease lost for job', job.id));
await worker.start();

process.on('SIGTERM', () => worker.stop()); // finishes running jobs first
```

### Options

`enqueue(db, queue, payload, { runAt, maxAttempts = 5, dedupeKey })`

`new Worker(pool, queue, handler, { concurrency = 4, leaseMs = 30000, pollMs = 15000, baseDelayMs = 1000, maxDelayMs = 300000 })`

Pick `leaseMs` well above your slowest handler. If a handler can run longer than the lease, another worker may start the same job.

Lower-level functions are exported too: `claim`, `complete`, `fail`, `reapExpired`.

## How it works

```
enqueue ──INSERT + pg_notify──▶ pgq_jobs ◀──UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)── worker
                                   │                      sets status='running', attempts+1,
                                   │                      lease_token=gen_random_uuid(), lease_expires_at
                                   │
          complete/fail ──UPDATE … WHERE id=$1 AND lease_token=$2──▶ 'done' | 'queued' (backoff) | 'dead'
```

- The claim and the status change are one statement, so there is no window where a job is selected but not yet marked.
- `pg_notify` inside the enqueue transaction is only delivered on commit, so a worker is never woken for a job it cannot see.
- Partial indexes keep the claim fast however many finished jobs the table holds. Delete old `done` rows on a schedule if you don't need them.

## Tests

The tests run against a real PostgreSQL (13 and 16 in CI), including 16 parallel claimers over 200 jobs, a lease that expires and is re-claimed, and a rolled-back enqueue.

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/scratch npm test
```

The test run drops and recreates `pgq_jobs`, so point it at a throwaway database.

## License

MIT
