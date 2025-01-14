import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { Type } from '@sinclair/typebox';
import { FastifyPluginCallback } from 'fastify';
import { Server } from 'http';
import {
  InscriptionResponse,
  InvalidSatoshiNumberResponse,
  LimitParam,
  OffsetParam,
  OrdinalParam,
  PaginatedResponse,
  SatoshiResponse,
} from '../schemas';
import { OrdinalSatoshi } from '../util/ordinal-satoshi';
import { DEFAULT_API_LIMIT, parseDbInscriptions } from '../util/helpers';

export const SatRoutes: FastifyPluginCallback<Record<never, never>, Server, TypeBoxTypeProvider> = (
  fastify,
  options,
  done
) => {
  fastify.get(
    '/sats/:ordinal',
    {
      schema: {
        operationId: 'getSatoshi',
        summary: 'Satoshi Ordinal',
        description: 'Retrieves ordinal information for a single satoshi',
        tags: ['Satoshis'],
        params: Type.Object({
          ordinal: OrdinalParam,
        }),
        response: {
          200: SatoshiResponse,
          400: InvalidSatoshiNumberResponse,
        },
      },
    },
    async (request, reply) => {
      let sat: OrdinalSatoshi;
      try {
        sat = new OrdinalSatoshi(request.params.ordinal);
      } catch (error) {
        await reply.code(400).send({ error: 'Invalid satoshi ordinal number' });
        return;
      }
      const inscriptions = await fastify.db.getInscriptions(
        { limit: 1, offset: 0 },
        { sat_ordinal: BigInt(request.params.ordinal) }
      );
      await reply.send({
        coinbase_height: sat.blockHeight,
        cycle: sat.cycle,
        epoch: sat.epoch,
        period: sat.period,
        offset: sat.offset,
        decimal: sat.decimal,
        degree: sat.degree,
        name: sat.name,
        rarity: sat.rarity,
        percentile: sat.percentile,
        inscription_id: inscriptions.results[0]?.genesis_id,
      });
    }
  );

  fastify.get(
    '/sats/:ordinal/inscriptions',
    {
      schema: {
        operationId: 'getSatoshiInscriptions',
        summary: 'Satoshi Inscriptions',
        description: 'Retrieves all inscriptions associated with a single satoshi',
        tags: ['Satoshis'],
        params: Type.Object({
          ordinal: OrdinalParam,
        }),
        querystring: Type.Object({
          // Pagination
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
        }),
        response: {
          200: PaginatedResponse(InscriptionResponse, 'Paginated Satoshi Inscriptions Response'),
          400: InvalidSatoshiNumberResponse,
        },
      },
    },
    async (request, reply) => {
      let sat: OrdinalSatoshi;
      try {
        sat = new OrdinalSatoshi(request.params.ordinal);
      } catch (error) {
        await reply.code(400).send({ error: 'Invalid satoshi ordinal number' });
        return;
      }
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const inscriptions = await fastify.db.getInscriptions(
        { limit, offset },
        { sat_ordinal: BigInt(sat.ordinal) }
      );
      await reply.send({
        limit,
        offset,
        total: inscriptions.total,
        results: parseDbInscriptions(inscriptions.results),
      });
    }
  );

  done();
};
