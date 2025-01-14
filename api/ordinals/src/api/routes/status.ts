import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { FastifyPluginCallback } from 'fastify';
import { Server } from 'http';
import { ApiStatusResponse } from '../schemas';
import { SERVER_VERSION } from '@hirosystems/api-toolkit';
import { handleInscriptionTransfersCache } from '../util/cache';

export const StatusRoutes: FastifyPluginCallback<
  Record<never, never>,
  Server,
  TypeBoxTypeProvider
> = (fastify, options, done) => {
  fastify.addHook('preHandler', handleInscriptionTransfersCache);
  fastify.get(
    '/',
    {
      schema: {
        operationId: 'getApiStatus',
        summary: 'API Status',
        description: 'Displays the status of the API',
        tags: ['Status'],
        response: {
          200: ApiStatusResponse,
        },
      },
    },
    async (request, reply) => {
      const result = await fastify.db.sqlTransaction(async sql => {
        const block_height = await fastify.db.getChainTipBlockHeight();
        const max_inscription_number = await fastify.db.getMaxInscriptionNumber();
        const max_cursed_inscription_number = await fastify.db.getMaxCursedInscriptionNumber();
        return {
          server_version: `ordinals-api ${SERVER_VERSION.tag} (${SERVER_VERSION.branch}:${SERVER_VERSION.commit})`,
          status: 'ready',
          block_height,
          max_inscription_number,
          max_cursed_inscription_number,
        };
      });
      await reply.send(result);
    }
  );
  done();
};
