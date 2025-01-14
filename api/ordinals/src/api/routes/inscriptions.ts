import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { Type } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import { FastifyPluginAsync, FastifyPluginCallback } from 'fastify';
import { Server } from 'http';
import {
  AddressesParam,
  BlockHeightParam,
  BlockInscriptionTransferSchema,
  BlockParam,
  CursedParam,
  InscriptionIdParamCType,
  InscriptionIdentifierParam,
  InscriptionIdsParam,
  InscriptionLocationResponseSchema,
  InscriptionNumberParam,
  InscriptionNumbersParam,
  InscriptionResponse,
  LimitParam,
  MimeTypesParam,
  NotFoundResponse,
  OffsetParam,
  Order,
  OrderBy,
  OrderByParam,
  OrderParam,
  OrdinalParam,
  OutputParam,
  PaginatedResponse,
  RecursiveParam,
  SatoshiRaritiesParam,
  TimestampParam,
} from '../schemas';
import { handleInscriptionCache, handleInscriptionTransfersCache } from '../util/cache';
import {
  DEFAULT_API_LIMIT,
  blockParam,
  hexToBuffer,
  parseBlockTransfers,
  parseDbInscription,
  parseDbInscriptions,
  parseInscriptionLocations,
} from '../util/helpers';

function inscriptionIdArrayParam(param: string | number) {
  return InscriptionIdParamCType.Check(param) ? { genesis_id: [param] } : { number: [param] };
}

function inscriptionIdParam(param: string | number) {
  return InscriptionIdParamCType.Check(param) ? { genesis_id: param } : { number: param };
}

function bigIntParam(param: number | undefined) {
  return param ? BigInt(param) : undefined;
}

const IndexRoutes: FastifyPluginCallback<Record<never, never>, Server, TypeBoxTypeProvider> = (
  fastify,
  options,
  done
) => {
  fastify.addHook('preHandler', handleInscriptionTransfersCache);
  fastify.get(
    '/inscriptions',
    {
      schema: {
        operationId: 'getInscriptions',
        summary: 'List of Inscriptions',
        description: 'Retrieves a list of inscriptions with options to filter and sort results',
        tags: ['Inscriptions'],
        querystring: Type.Object({
          genesis_block: Type.Optional(BlockParam),
          from_genesis_block_height: Type.Optional(BlockHeightParam),
          to_genesis_block_height: Type.Optional(BlockHeightParam),
          from_genesis_timestamp: Type.Optional(TimestampParam),
          to_genesis_timestamp: Type.Optional(TimestampParam),
          from_sat_ordinal: Type.Optional(OrdinalParam),
          to_sat_ordinal: Type.Optional(OrdinalParam),
          from_sat_coinbase_height: Type.Optional(BlockHeightParam),
          to_sat_coinbase_height: Type.Optional(BlockHeightParam),
          from_number: Type.Optional(InscriptionNumberParam),
          to_number: Type.Optional(InscriptionNumberParam),
          id: Type.Optional(InscriptionIdsParam),
          number: Type.Optional(InscriptionNumbersParam),
          output: Type.Optional(OutputParam),
          address: Type.Optional(AddressesParam),
          genesis_address: Type.Optional(AddressesParam),
          mime_type: Type.Optional(MimeTypesParam),
          rarity: Type.Optional(SatoshiRaritiesParam),
          recursive: Type.Optional(RecursiveParam),
          cursed: Type.Optional(CursedParam),
          // Pagination
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
          // Ordering
          order_by: Type.Optional(OrderByParam),
          order: Type.Optional(OrderParam),
        }),
        response: {
          200: PaginatedResponse(InscriptionResponse, 'Paginated Inscriptions Response'),
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const inscriptions = await fastify.db.getInscriptions(
        { limit, offset },
        {
          ...blockParam(request.query.genesis_block, 'genesis_block'),
          ...blockParam(request.query.from_genesis_block_height, 'from_genesis_block'),
          ...blockParam(request.query.to_genesis_block_height, 'to_genesis_block'),
          ...blockParam(request.query.from_sat_coinbase_height, 'from_sat_coinbase'),
          ...blockParam(request.query.to_sat_coinbase_height, 'to_sat_coinbase'),
          from_genesis_timestamp: request.query.from_genesis_timestamp,
          to_genesis_timestamp: request.query.to_genesis_timestamp,
          from_sat_ordinal: bigIntParam(request.query.from_sat_ordinal),
          to_sat_ordinal: bigIntParam(request.query.to_sat_ordinal),
          from_number: request.query.from_number,
          to_number: request.query.to_number,
          genesis_id: request.query.id,
          number: request.query.number,
          output: request.query.output,
          address: request.query.address,
          genesis_address: request.query.genesis_address,
          mime_type: request.query.mime_type,
          sat_rarity: request.query.rarity,
          recursive: request.query.recursive,
          cursed: request.query.cursed,
        },
        {
          order_by: request.query.order_by ?? OrderBy.genesis_block_height,
          order: request.query.order ?? Order.desc,
        }
      );
      await reply.send({
        limit,
        offset,
        total: inscriptions.total,
        results: parseDbInscriptions(inscriptions.results),
      });
    }
  );

  fastify.get(
    '/inscriptions/transfers',
    {
      schema: {
        operationId: 'getTransfersPerBlock',
        summary: 'Transfers per block',
        description:
          'Retrieves a list of inscription transfers that ocurred at a specific Bitcoin block',
        tags: ['Inscriptions'],
        querystring: Type.Object({
          block: BlockParam,
          // Pagination
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
        }),
        response: {
          200: PaginatedResponse(
            BlockInscriptionTransferSchema,
            'Paginated Block Transfers Response'
          ),
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const transfers = await fastify.db.getTransfersPerBlock({
        limit,
        offset,
        ...blockParam(request.query.block, 'block'),
      });
      await reply.send({
        limit,
        offset,
        total: transfers.total,
        results: parseBlockTransfers(transfers.results),
      });
    }
  );

  done();
};

const ShowRoutes: FastifyPluginCallback<Record<never, never>, Server, TypeBoxTypeProvider> = (
  fastify,
  options,
  done
) => {
  fastify.addHook('preHandler', handleInscriptionCache);
  fastify.get(
    '/inscriptions/:id',
    {
      schema: {
        operationId: 'getInscription',
        summary: 'Specific Inscription',
        description: 'Retrieves a single inscription',
        tags: ['Inscriptions'],
        params: Type.Object({
          id: InscriptionIdentifierParam,
        }),
        response: {
          200: InscriptionResponse,
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const inscription = await fastify.db.getInscriptions(
        { limit: 1, offset: 0 },
        { ...inscriptionIdArrayParam(request.params.id) }
      );
      if (inscription.total > 0) {
        await reply.send(parseDbInscription(inscription.results[0]));
      } else {
        await reply.code(404).send(Value.Create(NotFoundResponse));
      }
    }
  );

  fastify.get(
    '/inscriptions/:id/content',
    {
      schema: {
        operationId: 'getInscriptionContent',
        summary: 'Inscription content',
        description: 'Retrieves the contents of a single inscription',
        tags: ['Inscriptions'],
        params: Type.Object({
          id: InscriptionIdentifierParam,
        }),
        response: {
          200: Type.Uint8Array(),
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const inscription = await fastify.db.getInscriptionContent(
        inscriptionIdParam(request.params.id)
      );
      if (inscription) {
        const bytes = hexToBuffer(inscription.content);
        await reply
          .headers({
            'content-type': inscription.content_type,
            'content-length': inscription.content_length,
          })
          .send(bytes);
      } else {
        await reply.code(404).send(Value.Create(NotFoundResponse));
      }
    }
  );

  fastify.get(
    '/inscriptions/:id/transfers',
    {
      schema: {
        operationId: 'getInscriptionTransfers',
        summary: 'Inscription transfers',
        description: 'Retrieves all transfers for a single inscription',
        tags: ['Inscriptions'],
        params: Type.Object({
          id: InscriptionIdentifierParam,
        }),
        querystring: Type.Object({
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
        }),
        response: {
          200: PaginatedResponse(
            InscriptionLocationResponseSchema,
            'Paginated Inscription Locations Response'
          ),
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const locations = await fastify.db.getInscriptionLocations({
        ...inscriptionIdParam(request.params.id),
        limit,
        offset,
      });
      await reply.send({
        limit,
        offset,
        total: locations.total,
        results: parseInscriptionLocations(locations.results),
      });
    }
  );

  done();
};

export const InscriptionsRoutes: FastifyPluginAsync<
  Record<never, never>,
  Server,
  TypeBoxTypeProvider
> = async fastify => {
  await fastify.register(IndexRoutes);
  await fastify.register(ShowRoutes);
};
