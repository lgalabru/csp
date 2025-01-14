import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { Type } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import { FastifyPluginCallback } from 'fastify';
import { Server } from 'http';
import {
  AddressParam,
  BlockHeightParam,
  Brc20TokensOrderByParam,
  Brc20ActivityResponseSchema,
  Brc20BalanceResponseSchema,
  Brc20HolderResponseSchema,
  Brc20OperationsParam,
  Brc20TickerParam,
  Brc20TickersParam,
  Brc20TokenDetailsSchema,
  Brc20TokenResponseSchema,
  LimitParam,
  NotFoundResponse,
  OffsetParam,
  PaginatedResponse,
} from '../schemas';
import { handleInscriptionTransfersCache } from '../util/cache';
import {
  DEFAULT_API_LIMIT,
  parseBrc20Activities,
  parseBrc20Balances,
  parseBrc20Holders,
  parseBrc20Supply,
  parseBrc20Tokens,
} from '../util/helpers';

export const Brc20Routes: FastifyPluginCallback<
  Record<never, never>,
  Server,
  TypeBoxTypeProvider
> = (fastify, options, done) => {
  fastify.addHook('preHandler', handleInscriptionTransfersCache);

  fastify.get(
    '/brc-20/tokens',
    {
      schema: {
        operationId: 'getBrc20Tokens',
        summary: 'BRC-20 Tokens',
        description: 'Retrieves information for BRC-20 tokens',
        tags: ['BRC-20'],
        querystring: Type.Object({
          ticker: Type.Optional(Brc20TickersParam),
          // Sorting
          order_by: Type.Optional(Brc20TokensOrderByParam),
          // Pagination
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
        }),
        response: {
          200: PaginatedResponse(Brc20TokenResponseSchema, 'Paginated BRC-20 Token Response'),
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const response = await fastify.brc20Db.getTokens({
        limit,
        offset,
        ticker: request.query.ticker,
        order_by: request.query.order_by,
      });
      await reply.send({
        limit,
        offset,
        total: response.total,
        results: parseBrc20Tokens(response.results),
      });
    }
  );

  fastify.get(
    '/brc-20/tokens/:ticker',
    {
      schema: {
        operationId: 'getBrc20TokenDetails',
        summary: 'BRC-20 Token Details',
        description: 'Retrieves information for a BRC-20 token including supply and holders',
        tags: ['BRC-20'],
        params: Type.Object({
          ticker: Brc20TickerParam,
        }),
        response: {
          200: Brc20TokenDetailsSchema,
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const token = await fastify.brc20Db.getToken({ ticker: request.params.ticker });
      if (!token) {
        await reply.code(404).send(Value.Create(NotFoundResponse));
      } else {
        await reply.send({
          token: parseBrc20Tokens([token])[0],
          supply: parseBrc20Supply(token),
        });
      }
    }
  );

  fastify.get(
    '/brc-20/tokens/:ticker/holders',
    {
      schema: {
        operationId: 'getBrc20TokenHolders',
        summary: 'BRC-20 Token Holders',
        description: 'Retrieves a list of holders and their balances for a BRC-20 token',
        tags: ['BRC-20'],
        params: Type.Object({
          ticker: Brc20TickerParam,
        }),
        querystring: Type.Object({
          // Pagination
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
        }),
        response: {
          200: PaginatedResponse(Brc20HolderResponseSchema, 'Paginated BRC-20 Holders Response'),
          404: NotFoundResponse,
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const holders = await fastify.brc20Db.getTokenHolders({
        limit,
        offset,
        ticker: request.params.ticker,
      });
      if (!holders) {
        await reply.code(404).send(Value.Create(NotFoundResponse));
        return;
      }
      await reply.send({
        limit,
        offset,
        total: holders.total,
        results: parseBrc20Holders(holders.results),
      });
    }
  );

  fastify.get(
    '/brc-20/balances/:address',
    {
      schema: {
        operationId: 'getBrc20Balances',
        summary: 'BRC-20 Balances',
        description: 'Retrieves BRC-20 token balances for a Bitcoin address',
        tags: ['BRC-20'],
        params: Type.Object({
          address: AddressParam,
        }),
        querystring: Type.Object({
          ticker: Type.Optional(Brc20TickersParam),
          block_height: Type.Optional(BlockHeightParam),
          // Pagination
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
        }),
        response: {
          200: PaginatedResponse(Brc20BalanceResponseSchema, 'Paginated BRC-20 Balance Response'),
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const balances = await fastify.brc20Db.getBalances({
        limit,
        offset,
        address: request.params.address,
        ticker: request.query.ticker,
        block_height: request.query.block_height ? parseInt(request.query.block_height) : undefined,
      });
      await reply.send({
        limit,
        offset,
        total: balances.total,
        results: parseBrc20Balances(balances.results),
      });
    }
  );

  fastify.get(
    '/brc-20/activity',
    {
      schema: {
        operationId: 'getBrc20Activity',
        summary: 'BRC-20 Activity',
        description:
          'Retrieves BRC-20 activity filtered by ticker, address, operation, or at a specific block height',
        tags: ['BRC-20'],
        querystring: Type.Object({
          ticker: Type.Optional(Brc20TickersParam),
          block_height: Type.Optional(BlockHeightParam),
          operation: Type.Optional(Brc20OperationsParam),
          address: Type.Optional(AddressParam),
          // Pagination
          offset: Type.Optional(OffsetParam),
          limit: Type.Optional(LimitParam),
        }),
        response: {
          200: PaginatedResponse(Brc20ActivityResponseSchema, 'Paginated BRC-20 Activity Response'),
        },
      },
    },
    async (request, reply) => {
      const limit = request.query.limit ?? DEFAULT_API_LIMIT;
      const offset = request.query.offset ?? 0;
      const balances = await fastify.brc20Db.getActivity(
        { limit, offset },
        {
          ticker: request.query.ticker,
          block_height: request.query.block_height
            ? parseInt(request.query.block_height)
            : undefined,
          operation: request.query.operation,
          address: request.query.address,
        }
      );
      await reply.send({
        limit,
        offset,
        total: balances.total,
        results: parseBrc20Activities(balances.results),
      });
    }
  );

  done();
};
