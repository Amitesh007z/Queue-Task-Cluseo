const { UnrecoverableError, Worker } = require('bullmq');
const nodemailer = require('nodemailer');
const { createRedisConnection, QUEUE_NAME } = require('./queue');

const concurrency = Number(process.env.WORKER_CONCURRENCY) || 20;
if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 1000) {
	throw new Error('WORKER_CONCURRENCY must be an integer between 1 and 1000.');
}

const smtpConfigured = Boolean(process.env.SMTP_HOST && process.env.SMTP_FROM);
const mailer = smtpConfigured
	? nodemailer.createTransport({
			host: process.env.SMTP_HOST,
			port: Number(process.env.SMTP_PORT) || 587,
			secure: process.env.SMTP_SECURE === 'true',
			...(process.env.SMTP_USER && process.env.SMTP_PASS
				? { auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
				}
				: {}),
		})
	: null;

async function processJob(job) {
	if (job.name === 'queue-test') return;

	if (job.name === 'subscription') {
		await redis.sadd('relay:subscribers', job.data.email);
		return;
	}

	if (job.name === 'email') {
		if (!mailer) throw new UnrecoverableError('SMTP is not configured on the worker.');
		await mailer.sendMail({
			from: process.env.SMTP_FROM,
			to: job.data.email,
			subject: job.data.subject,
			text: job.data.text,
		});
		return;
	}

	throw new UnrecoverableError(`Unsupported job type: ${job.name}`);
}

const workerRedisOptions = { maxRetriesPerRequest: null, enableOfflineQueue: true };
const redis = createRedisConnection(workerRedisOptions);
const worker = new Worker(QUEUE_NAME, processJob, {
	connection: createRedisConnection(workerRedisOptions),
	concurrency,
	limiter: process.env.WORKER_RATE_MAX && process.env.WORKER_RATE_DURATION_MS
		? { max: Number(process.env.WORKER_RATE_MAX), duration: Number(process.env.WORKER_RATE_DURATION_MS) }
		: undefined,
});

worker.on('ready', () => console.log(`Worker ready (concurrency ${concurrency}).`));
worker.on('completed', (job) => console.log(`Completed ${job.name} job ${job.id}.`));
worker.on('failed', (job, error) => console.error(`Job ${job?.id || 'unknown'} failed:`, error.message));
worker.on('error', (error) => console.error('Worker error:', error));

let closing = false;
async function shutdown() {
	if (closing) return;
	closing = true;
	await worker.close();
	await redis.quit();
}

process.once('SIGINT', () => void shutdown().then(() => process.exit(0)));
process.once('SIGTERM', () => void shutdown().then(() => process.exit(0)));