import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { Type } from '@sinclair/typebox';
import { FastifyPluginAsync, FastifyPluginCallback } from 'fastify';
import { Server } from 'http';
import { BlockHeightParam, InscriptionsPerBlockResponse, NotFoundResponse } from '../schemas';
import { handleInscriptionsPerBlockCache } from '../util/cache';
import { blockParam } from '../util/helpers';

const IndexRoutes: FastifyPluginCallback<Record<never, never>, Server, TypeBoxTypeProvider> = (
  fastify,
  options,
  done
) => {
  fastify.addHook('preHandler', handleInscriptionsPerBlockCache);
  fastify.get(
    '/stats/inscriptions',
    {
      schema: {
        operationId: 'getStatsInscriptionCount',
        summary: 'Inscription Count per Block',
        description: 'Retrieves statistics on the number of inscriptions revealed per block',
        tags: ['Statistics'],
        querystring: Type.Object({
          from_block_height: Type.Optional(BlockHeightParam),
          to_block_height: Type.Optional(BlockHeightParam),
        }),
        response: {
          200: InscriptionsPerBlockResponse,
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const inscriptions = await fastify.db.counts.getInscriptionCountPerBlock({
        ...blockParam(request.query.from_block_height, 'from_block'),
        ...blockParam(request.query.to_block_height, 'to_block'),
      });
      await reply.send({
        results: inscriptions,
      });
    }
  );
  done();
};

export const StatsRoutes: FastifyPluginAsync<
  Record<never, never>,
  Server,
  TypeBoxTypeProvider
> = async fastify => {
  await fastify.register(IndexRoutes);
};
