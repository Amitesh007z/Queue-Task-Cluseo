/* One-shot local verification against http://localhost:3000 — not for production load testing. */
const WebSocket = require('ws');
const { Queue } = require('bullmq');
const { createRedisConnection, QUEUE_NAME, waitForRedis } = require('../queue');

const BASE = process.env.VERIFY_BASE || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

async function waitForJobState(queue, jobId, targetState, timeoutMs = 60_000) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const job = await queue.getJob(jobId);
		if (!job) throw new Error(`Job ${jobId} disappeared`);
		const state = await job.getState();
		if (state === targetState) return state;
		if (state === 'failed') throw new Error(`Job ${jobId} failed: ${job.failedReason}`);
		await new Promise((resolve) => setTimeout(resolve, 250));
	}
	throw new Error(`Job ${jobId} did not reach ${targetState} within ${timeoutMs}ms`);
}

async function testWebSocket() {
	return new Promise((resolve, reject) => {
		const socket = new WebSocket('ws://localhost:3000/ws');
		const timer = setTimeout(() => {
			socket.terminate();
			reject(new Error('WebSocket snapshot timeout'));
		}, 10_000);
		socket.once('message', (data) => {
			clearTimeout(timer);
			const payload = JSON.parse(String(data));
			if (payload.type !== 'snapshot' || !payload.queue || !Array.isArray(payload.jobs)) {
				socket.close();
				reject(new Error('Invalid WebSocket snapshot shape'));
				return;
			}
			socket.close();
			resolve(payload);
		});
		socket.once('error', reject);
	});
}

async function runLoadProbe(count, concurrency) {
	const runId = `verify-load-${Date.now()}`;
	let next = 0;
	let accepted = 0;
	let rejected = 0;
	const started = Date.now();

	async function workerLoop() {
		while (next < count) {
			const index = next++;
			const response = await fetch(`${BASE}/api/subscriptions`, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
					'X-Queue-Test': 'true',
					'X-Queue-Test-Run': runId,
				},
				body: '{}',
			});
			if (response.status === 202) accepted += 1;
			else rejected += 1;
			if ((index + 1) % Math.max(1, Math.floor(count / 5)) === 0) {
				process.stdout.write(`  load progress ${index + 1}/${count}\r`);
			}
		}
	}

	await Promise.all(Array.from({ length: concurrency }, () => workerLoop()));
	const elapsedSec = (Date.now() - started) / 1000;
	return { runId, count, accepted, rejected, elapsedSec, rate: Math.round(count / elapsedSec) };
}

async function main() {
	const results = [];
	process.env.REDIS_URL = REDIS_URL;
	const queueRedis = createRedisConnection({ maxRetriesPerRequest: null, enableOfflineQueue: true });
	await waitForRedis(queueRedis);
	const queue = new Queue(QUEUE_NAME, { connection: queueRedis });

	// Health
	for (const path of ['/livez', '/healthz']) {
		const response = await fetch(`${BASE}${path}`);
		if (!response.ok) throw new Error(`${path} returned ${response.status}`);
		results.push(`${path} OK`);
	}

	// WebSocket initial snapshot
	await testWebSocket();
	results.push('WebSocket snapshot OK');

	// Single queue-test job completes in worker
	const probeRun = `probe-${Date.now()}`;
	const probeRes = await fetch(`${BASE}/api/subscriptions`, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
			'X-Queue-Test': 'true',
			'X-Queue-Test-Run': probeRun,
		},
		body: '{}',
	});
	const probeBody = await probeRes.json();
	if (probeRes.status !== 202) throw new Error(`queue-test enqueue failed: ${JSON.stringify(probeBody)}`);
	await waitForJobState(queue, probeBody.job.id, 'completed', 30_000);
	results.push(`BullMQ queue-test job ${probeBody.job.id} completed`);

	// Subscription + duplicate
	const email = `verify-${Date.now()}@example.com`;
	const subRes = await fetch(`${BASE}/api/subscriptions`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ email }),
	});
	const subBody = await subRes.json();
	if (subRes.status !== 202) throw new Error(`subscription enqueue failed: ${JSON.stringify(subBody)}`);
	await waitForJobState(queue, subBody.job.id, 'completed', 30_000);
	const dupRes = await fetch(`${BASE}/api/subscriptions`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ email }),
	});
	if (dupRes.status !== 409) throw new Error(`expected 409 duplicate, got ${dupRes.status}`);
	results.push('Subscription + duplicate 409 OK');

	// Validation
	const badEmail = await fetch(`${BASE}/api/email`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ to: 'not-an-email', subject: 'x', text: 'y' }),
	});
	if (badEmail.status !== 400) throw new Error(`expected 400 invalid email, got ${badEmail.status}`);
	results.push('API validation OK');

	// HTML dashboard
	const dashboard = await fetch(`${BASE}/`);
	if (!dashboard.ok || !(await dashboard.text()).includes('Queue load test')) {
		throw new Error('Dashboard HTML missing expected content');
	}
	results.push('Dashboard HTML OK');

	// Bounded load probe (mirrors test.html max 500)
	const load = await runLoadProbe(500, 50);
	if (load.rejected > 0) throw new Error(`load probe rejected ${load.rejected}/${load.count}`);
	results.push(`Load probe ${load.accepted}/${load.count} accepted in ${load.elapsedSec.toFixed(2)}s (${load.rate} req/s)`);

	// Drain load jobs (best effort within timeout)
	const countsBefore = await queue.getJobCounts('waiting', 'active', 'delayed');
	const drainDeadline = Date.now() + 120_000;
	while (Date.now() < drainDeadline) {
		const counts = await queue.getJobCounts('waiting', 'active', 'delayed');
		if (counts.waiting + counts.active + counts.delayed === 0) break;
		await new Promise((resolve) => setTimeout(resolve, 500));
	}
	const countsAfter = await queue.getJobCounts('waiting', 'active', 'delayed');
	results.push(`Queue drain: waiting+active+delayed ${countsBefore.waiting + countsBefore.active + countsBefore.delayed} -> ${countsAfter.waiting + countsAfter.active + countsAfter.delayed}`);

	await queue.close();
	await queueRedis.quit();
	console.log('Verification passed:\n- ' + results.join('\n- '));
}

main().catch(async (error) => {
	console.error('Verification failed:', error.message);
	process.exitCode = 1;
});
