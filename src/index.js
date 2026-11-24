'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { EventEmitter } = require('node:events');

const CHANNEL = 'pgq_jobs';
const SCHEMA_SQL = fs.readFileSync(path.join(__dirname, 'schema.sql'), 'utf8');

/** Create the table and indexes. Safe to call on every start. */
async function migrate(db) {
  await db.query(SCHEMA_SQL);
}

/**
 * Add a job. `db` can be a Pool or a client inside your own transaction:
 * the job and its wake-up become visible only when that transaction commits.
 *
 * Returns the job id, or null when `dedupeKey` matches a job that is still
 * queued or running.
 */
async function enqueue(db, queue, payload = {}, opts = {}) {
  const { runAt = null, maxAttempts = 5, dedupeKey = null } = opts;
  const { rows } = await db.query(
    `WITH ins AS (
       INSERT INTO pgq_jobs (queue, payload, run_at, max_attempts, dedupe_key)
       VALUES ($1, $2, COALESCE($3::timestamptz, now()), $4, $5)
       ON CONFLICT (queue, dedupe_key)
         WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running')
       DO NOTHING
       RETURNING id
     )
     SELECT id, pg_notify($6, $1) FROM ins`,
    [queue, JSON.stringify(payload), runAt, maxAttempts, dedupeKey, CHANNEL],
  );
  return rows.length ? Number(rows[0].id) : null;
}

/**
 * Lease up to `limit` due jobs. Each claim gets a fresh lease token, so a
 * worker whose lease expired can no longer finish the job it once held.
 * A job whose lease expired is claimable again (its worker is presumed dead).
 */
async function claim(db, queue, { limit = 1, leaseMs = 30_000 } = {}) {
  const { rows } = await db.query(
    `UPDATE pgq_jobs j
        SET status = 'running',
            attempts = j.attempts + 1,
            lease_token = gen_random_uuid(),
            lease_expires_at = now() + ($3::int * interval '1 millisecond'),
            updated_at = now()
      WHERE j.id IN (
        SELECT id FROM pgq_jobs
         WHERE queue = $1
           AND attempts < max_attempts
           AND ((status = 'queued'  AND run_at <= now())
             OR (status = 'running' AND lease_expires_at < now()))
         ORDER BY run_at, id
         LIMIT $2
         FOR UPDATE SKIP LOCKED)
      RETURNING j.id, j.queue, j.payload, j.attempts, j.max_attempts, j.lease_token`,
    [queue, limit, leaseMs],
  );
  return rows.map(r => ({ ...r, id: Number(r.id) }));
}

/** Mark a job done. Returns false if the lease was lost (expired and re-claimed). */
async function complete(db, job) {
  const r = await db.query(
    `UPDATE pgq_jobs
        SET status = 'done', lease_token = NULL, lease_expires_at = NULL, updated_at = now()
      WHERE id = $1 AND lease_token = $2 AND status = 'running'`,
    [job.id, job.lease_token],
  );
  return r.rowCount === 1;
}

/**
 * Record a failure. The job is retried with exponential backoff until it has
 * used `max_attempts`, then it becomes `dead`. Returns false if the lease was lost.
 */
async function fail(db, job, error, { baseDelayMs = 1_000, maxDelayMs = 300_000 } = {}) {
  const message = String(error && error.stack ? error.stack : error).slice(0, 4000);
  const r = await db.query(
    `UPDATE pgq_jobs
        SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
            run_at = now() + (LEAST($4::bigint, $3::bigint * (2 ^ (attempts - 1))::bigint)
                              * interval '1 millisecond'),
            lease_token = NULL, lease_expires_at = NULL,
            last_error = $5, updated_at = now()
      WHERE id = $1 AND lease_token = $2 AND status = 'running'
      RETURNING status`,
    [job.id, job.lease_token, baseDelayMs, maxDelayMs, message],
  );
  if (r.rowCount === 1 && r.rows[0].status === 'queued') {
    await db.query('SELECT pg_notify($1, $2)', [CHANNEL, job.queue]);
  }
  return r.rowCount === 1;
}

/** Mark jobs dead whose lease expired on their last allowed attempt. */
async function reapExpired(db, queue) {
  const r = await db.query(
    `UPDATE pgq_jobs
        SET status = 'dead', lease_token = NULL, lease_expires_at = NULL,
            last_error = COALESCE(last_error, 'lease expired on last attempt'), updated_at = now()
      WHERE queue = $1 AND status = 'running'
        AND lease_expires_at < now() AND attempts >= max_attempts`,
    [queue],
  );
  return r.rowCount;
}

/**
 * A worker that sleeps until NOTIFY says there is work, with a slow safety
 * poll for delayed jobs and expired leases. It does not hold a pool client
 * while idle, apart from the one LISTEN connection.
 *
 * Events: 'error' (err), 'job:done' (job), 'job:failed' (job, err), 'lease:lost' (job)
 */
class Worker extends EventEmitter {
  constructor(pool, queue, handler, opts = {}) {
    super();
    this.pool = pool;
    this.queue = queue;
    this.handler = handler;
    this.concurrency = opts.concurrency ?? 4;
    this.leaseMs = opts.leaseMs ?? 30_000;
    this.pollMs = opts.pollMs ?? 15_000;
    this.retry = { baseDelayMs: opts.baseDelayMs ?? 1_000, maxDelayMs: opts.maxDelayMs ?? 300_000 };
    this.active = 0;
    this.running = false;
    this.wakeRequested = false;
    this.listener = null;
    this.timer = null;
    this.idle = new Set();
  }

  async start() {
    this.running = true;
    this.listener = await this.pool.connect();
    this.listener.on('notification', msg => { if (msg.payload === this.queue) this.wake(); });
    this.listener.on('error', err => this.emit('error', err));
    await this.listener.query(`LISTEN ${CHANNEL}`);
    this.timer = setInterval(() => {
      reapExpired(this.pool, this.queue).catch(err => this.emit('error', err));
      this.wake();
    }, this.pollMs);
    this.wake();
  }

  wake() {
    if (!this.running) return;
    if (this.filling) { this.wakeRequested = true; return; }
    this.fill().catch(err => this.emit('error', err));
  }

  async fill() {
    this.filling = true;
    try {
      do {
        this.wakeRequested = false;
        const free = this.concurrency - this.active;
        if (free <= 0) break;
        const jobs = await claim(this.pool, this.queue, { limit: free, leaseMs: this.leaseMs });
        for (const job of jobs) this.run(job);
        if (jobs.length === free) this.wakeRequested = true; // there may be more
      } while (this.wakeRequested && this.running);
    } finally {
      this.filling = false;
    }
  }

  run(job) {
    this.active++;
    const p = (async () => {
      try {
        await this.handler(job.payload, job);
        if (await complete(this.pool, job)) this.emit('job:done', job);
        else this.emit('lease:lost', job);
      } catch (err) {
        if (await fail(this.pool, job, err, this.retry)) this.emit('job:failed', job, err);
        else this.emit('lease:lost', job);
      }
    })().catch(err => this.emit('error', err)).finally(() => {
      this.active--;
      this.idle.delete(p);
      this.wake();
    });
    this.idle.add(p);
  }

  /** Stop claiming, wait for running jobs, release the LISTEN connection. */
  async stop() {
    this.running = false;
    clearInterval(this.timer);
    await Promise.allSettled([...this.idle]);
    if (this.listener) {
      try { await this.listener.query(`UNLISTEN ${CHANNEL}`); } catch { /* connection may be gone */ }
      this.listener.release();
      this.listener = null;
    }
  }
}

module.exports = { migrate, enqueue, claim, complete, fail, reapExpired, Worker, CHANNEL };
