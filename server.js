const crypto = require('node:crypto');
const http = require('node:http');
const path = require('node:path');
const express = require('express');
const { Queue } = require('bullmq');
const { createRedisConnection, QUEUE_NAME, jobOptions, waitForRedis } = require('./queue');
const { WebSocketServer, WebSocket } = require('ws');

const HTTP_PORT = Number(process.env.PORT) || 3000;
const MAX_WS_CLIENTS = Number(process.env.MAX_WS_CLIENTS) || 5000;
const MAX_BUFFERED_BYTES = 256 * 1024;
const SNAPSHOT_INTERVAL_MS = Math.max(1000, Number(process.env.SNAPSHOT_INTERVAL_MS) || 1000);

const isEmail = (value) =>
	typeof value === 'string' &&
	value.length <= 254 &&
	/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());

function createApp(queue, redis) {
	const app = express();
	app.disable('x-powered-by');
	app.use(express.json({ limit: '16kb' }));

	app.get('/', (_request, response) => {
		response.sendFile(path.join(__dirname, 'test.html'));
	});

	app.get('/livez', (_request, response) => {
		response.status(200).json({ status: 'alive' });
	});

	app.get('/healthz', async (_request, response) => {
		try {
			await redis.ping();
			response.status(200).json({ status: 'ready' });
		} catch {
			response.status(503).json({ status: 'unavailable' });
		}
	});

	app.post('/api/email', async (request, response, next) => {
		try {
			const { to, subject, text } = request.body || {};
			if (!isEmail(to) || typeof subject !== 'string' || !subject.trim() || subject.length > 200 ||
				typeof text !== 'string' || !text.trim() || text.length > 10000) {
				return response.status(400).json({ error: 'Provide a valid "to" email, subject (max 200 characters), and message (max 10,000 characters).' });
			}

			const suppliedKey = request.get('Idempotency-Key');
			if (suppliedKey && (suppliedKey.length > 128 || !/^[\w.-]+$/.test(suppliedKey))) {
				return response.status(400).json({ error: 'Idempotency-Key must be at most 128 letters, numbers, dots, underscores, or hyphens.' });
			}
			const key = suppliedKey || crypto.randomUUID();
			const jobId = `email-${crypto.createHash('sha256').update(key).digest('hex')}`;
			const job = await queue.add('email', {
				email: to.trim().toLowerCase(),
				subject: subject.trim(),
				text: text.trim(),
			}, { ...jobOptions, jobId });
			return response.status(202).json({ job: { id: job.id, type: 'email', status: await job.getState() } });
		} catch (error) {
			return next(error);
		}
	});

	app.post('/api/subscriptions', async (request, response, next) => {
		try {
			if (request.get('X-Queue-Test') === 'true') {
				if (process.env.ENABLE_QUEUE_TEST !== 'true') {
					return response.status(403).json({ error: 'Queue testing is disabled on this server.' });
				}
				const runId = request.get('X-Queue-Test-Run') || crypto.randomUUID();
				if (!/^[\w-]{1,64}$/.test(runId)) {
					return response.status(400).json({ error: 'Invalid queue test run ID.' });
				}
				const job = await queue.add('queue-test', { runId }, {
					...jobOptions,
					jobId: `probe-${runId}-${crypto.randomUUID()}`,
				});
				return response.status(202).json({ job: { id: job.id, type: 'queue-test', status: await job.getState() } });
			}

			const { email } = request.body || {};
			if (!isEmail(email)) return response.status(400).json({ error: 'Provide a valid email address.' });

			const normalizedEmail = email.trim().toLowerCase();
			const alreadySubscribed = await redis.sismember('relay:subscribers', normalizedEmail);
			if (alreadySubscribed) return response.status(409).json({ error: 'This email is already subscribed.' });

			const jobId = `subscription-${crypto.createHash('sha256').update(normalizedEmail).digest('hex')}`;
			const existing = await queue.getJob(jobId);
			if (existing && await existing.getState() === 'failed') await existing.remove();
			const job = await queue.add('subscription', { email: normalizedEmail }, { ...jobOptions, jobId });
			return response.status(202).json({ job: { id: job.id, type: 'subscription', status: await job.getState() } });
		} catch (error) {
			return next(error);
		}
	});

	app.use((error, _request, response, _next) => {
		const tooLarge = error.status === 413 || error.type === 'entity.too.large';
		const invalidJson = error.type === 'entity.parse.failed';
		const status = tooLarge ? 413 : invalidJson ? 400 : 503;
		const message = tooLarge
			? 'Request body is too large.'
			: invalidJson
				? 'Request body must be valid JSON.'
				: 'Queue service is temporarily unavailable.';
		response.status(status).json({ error: message });
	});

	return app;
}

async function buildSnapshot(queue, redis) {
	const [counts, subscribed, jobs] = await Promise.all([
		queue.getJobCounts('waiting', 'active', 'delayed', 'failed', 'completed'),
		redis.scard('relay:subscribers'),
		queue.getJobs(['waiting', 'active', 'delayed', 'failed', 'completed'], 0, 9, false),
	]);
	const recentJobs = await Promise.all(jobs.map(async (job) => ({
		id: job.id,
		type: job.name,
		email: job.data.email,
		subject: job.data.subject,
		runId: job.data.runId,
		status: await job.getState(),
		attempts: job.attemptsMade,
		createdAt: new Date(job.timestamp).toISOString(),
		updatedAt: new Date(job.finishedOn || job.processedOn || job.timestamp).toISOString(),
		error: job.failedReason || undefined,
	})));
	recentJobs.sort((left, right) => right.createdAt.localeCompare(left.createdAt));

	return {
		type: 'snapshot',
		queue: { waiting: counts.waiting + counts.delayed, processing: counts.active, failed: counts.failed, subscribed },
		jobs: recentJobs.slice(0, 10),
	};
}

async function start() {
	const redis = createRedisConnection();
	const queue = new Queue(QUEUE_NAME, { connection: createRedisConnection(), defaultJobOptions: jobOptions });
	await waitForRedis(redis);
	await redis.ping();
	let currentSnapshot = await buildSnapshot(queue, redis);
	let previousSnapshot = JSON.stringify(currentSnapshot);
	let refreshing = false;
	let redisHealthy = true;
	const app = createApp(queue, redis);
	const httpServer = http.createServer(app);
	const webSocketServer = new WebSocketServer({ noServer: true, maxPayload: 1024, perMessageDeflate: false });

	function broadcastSnapshot() {
		const message = JSON.stringify(currentSnapshot);
		for (const client of webSocketServer.clients) {
			if (client.readyState !== WebSocket.OPEN) continue;
			if (client.bufferedAmount > MAX_BUFFERED_BYTES) {
				client.close(1013, 'Client is too slow; reconnect for a fresh snapshot.');
				continue;
			}
			client.send(message);
		}
	}

	async function refreshSnapshot() {
		if (refreshing) return;
		refreshing = true;
		try {
			const nextSnapshot = await buildSnapshot(queue, redis);
			const nextValue = JSON.stringify(nextSnapshot);
			if (nextValue !== previousSnapshot) {
				currentSnapshot = nextSnapshot;
				previousSnapshot = nextValue;
				broadcastSnapshot();
			}
			if (!redisHealthy) console.log('Redis connection restored.');
			redisHealthy = true;
		} catch (error) {
			if (redisHealthy) console.error('Queue snapshot unavailable:', error.message);
			redisHealthy = false;
			for (const client of webSocketServer.clients) client.close(1012, 'Queue service is reconnecting.');
		}
		refreshing = false;
	}

	httpServer.on('upgrade', (request, socket, head) => {
		if (new URL(request.url, 'http://localhost').pathname !== '/ws') {
			socket.destroy();
			return;
		}
		if (webSocketServer.clients.size >= MAX_WS_CLIENTS) {
			socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
			socket.destroy();
			return;
		}
		webSocketServer.handleUpgrade(request, socket, head, (client) => {
			webSocketServer.emit('connection', client, request);
		});
	});

	webSocketServer.on('connection', (client) => {
		client.send(JSON.stringify(currentSnapshot));
		client.on('pong', () => { client.isAlive = true; });
	});

	const snapshotTimer = setInterval(() => void refreshSnapshot(), SNAPSHOT_INTERVAL_MS);
	const heartbeatTimer = setInterval(() => {
		for (const client of webSocketServer.clients) {
			if (client.isAlive === false) {
				client.terminate();
				continue;
			}
			client.isAlive = false;
			client.ping();
		}
	}, 30000);

	await new Promise((resolve, reject) => {
		httpServer.once('error', reject);
		httpServer.listen(HTTP_PORT, '0.0.0.0', resolve);
	});
	console.log(`Dashboard, API, and WebSocket: http://localhost:${HTTP_PORT}`);

	let closing = false;
	async function shutdown() {
		if (closing) return;
		closing = true;
		clearInterval(snapshotTimer);
		clearInterval(heartbeatTimer);
		for (const client of webSocketServer.clients) client.terminate();
		await new Promise((resolve) => httpServer.close(resolve));
		webSocketServer.close();
		await Promise.all([queue.close(), redis.quit()]);
	}
	process.once('SIGINT', () => void shutdown().then(() => process.exit(0)));
	process.once('SIGTERM', () => void shutdown().then(() => process.exit(0)));
}

if (require.main === module) {
	start().catch((error) => {
		console.error('API server failed to start:', error);
		process.exitCode = 1;
	});
}

module.exports = { buildSnapshot, createApp, start };
