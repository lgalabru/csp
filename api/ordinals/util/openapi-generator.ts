import Fastify, { FastifyPluginAsync } from 'fastify';
import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { Api } from '../src/api/init';
import FastifySwagger from '@fastify/swagger';
import { existsSync, mkdirSync, writeFileSync } from 'fs';
import { Server } from 'http';
import { OpenApiSchemaOptions } from '../src/api/schemas';

/**
 * Generates `openapi.yaml` based on current Swagger definitions.
 */
export const ApiGenerator: FastifyPluginAsync<
  Record<never, never>,
  Server,
  TypeBoxTypeProvider
> = async (fastify, options) => {
  await fastify.register(FastifySwagger, OpenApiSchemaOptions);
  await fastify.register(Api, { prefix: '/ordinals/v1' });
  if (!existsSync('./tmp')) {
    mkdirSync('./tmp');
  }
  writeFileSync('./tmp/openapi.yaml', fastify.swagger({ yaml: true }));
  writeFileSync('./tmp/openapi.json', JSON.stringify(fastify.swagger(), null, 2));
};

const fastify = Fastify({
  trustProxy: true,
  logger: true,
}).withTypeProvider<TypeBoxTypeProvider>();

void fastify.register(ApiGenerator).then(async () => {
  await fastify.close();
});
