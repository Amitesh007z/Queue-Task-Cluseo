const IORedis = require('ioredis');

const QUEUE_NAME = 'relay-jobs';
const jobOptions = {
	attempts: 5,
	backoff: { type: 'exponential', delay: 1000, jitter: 0.5 },
	removeOnComplete: { age: 86400, count: 1000 },
	removeOnFail: { age: 604800, count: 10000 },
};

function createRedisConnection(overrides = {}) {
	return new IORedis(process.env.REDIS_URL || 'redis://127.0.0.1:6379', {
		maxRetriesPerRequest: 1,
		enableOfflineQueue: false,
		enableReadyCheck: true,
		connectTimeout: 5000,
		...overrides,
	});
}

async function waitForRedis(client) {
	if (client.status === 'ready') return;
	await new Promise((resolve, reject) => {
		const onReady = () => {
			client.off('error', onError);
			resolve();
		};
		const onError = (error) => {
			client.off('ready', onReady);
			reject(error);
		};
		client.once('ready', onReady);
		client.once('error', onError);
	});
}

module.exports = { QUEUE_NAME, createRedisConnection, jobOptions, waitForRedis };