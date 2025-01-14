import {
  buildProfilerServer,
  isProdEnv,
  logger,
  registerShutdownConfig,
} from '@hirosystems/api-toolkit';
import { buildApiServer, buildPromServer } from './api/init';
import { ENV } from './env';
import { ApiMetrics } from './metrics/metrics';
import { PgStore } from './pg/pg-store';
import { Brc20PgStore } from './pg/brc20/brc20-pg-store';

async function initApiService(db: PgStore, brc20Db: Brc20PgStore) {
  logger.info('Initializing API service...');
  const fastify = await buildApiServer({ db, brc20Db });
  registerShutdownConfig({
    name: 'API Server',
    forceKillable: false,
    handler: async () => {
      await fastify.close();
    },
  });

  await fastify.listen({ host: ENV.API_HOST, port: ENV.API_PORT });

  if (isProdEnv) {
    const promServer = await buildPromServer({ metrics: fastify.metrics });
    registerShutdownConfig({
      name: 'Prometheus Server',
      forceKillable: false,
      handler: async () => {
        await promServer.close();
      },
    });

    ApiMetrics.configure(db);
    await promServer.listen({ host: ENV.API_HOST, port: 9153 });

    const profilerServer = await buildProfilerServer();
    registerShutdownConfig({
      name: 'Profiler Server',
      forceKillable: false,
      handler: async () => {
        await profilerServer.close();
      },
    });
    await profilerServer.listen({ host: ENV.API_HOST, port: ENV.PROFILER_PORT });
  }
}

async function initApp() {
  logger.info(`Initializing Ordinals API...`);
  const db = await PgStore.connect();
  const brc20Db = await Brc20PgStore.connect();
  await initApiService(db, brc20Db);
  registerShutdownConfig({
    name: 'DB',
    forceKillable: false,
    handler: async () => {
      await db.close();
      await brc20Db.close();
    },
  });
}

registerShutdownConfig();
initApp()
  .then(() => {
    logger.info('App initialized');
  })
  .catch(error => {
    logger.error(error, `App failed to start`);
    process.exit(1);
  });
