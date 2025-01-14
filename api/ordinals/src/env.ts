import { Static, Type } from '@sinclair/typebox';
import envSchema from 'env-schema';

const schema = Type.Object({
  /** Hostname of the API server */
  API_HOST: Type.String({ default: '0.0.0.0' }),
  /** Port in which to serve the API */
  API_PORT: Type.Number({ default: 3000, minimum: 0, maximum: 65535 }),
  /** Port in which to serve the profiler */
  PROFILER_PORT: Type.Number({ default: 9119 }),

  ORDINALS_PGHOST: Type.String(),
  ORDINALS_PGPORT: Type.Number({ default: 5432, minimum: 0, maximum: 65535 }),
  ORDINALS_PGUSER: Type.String(),
  ORDINALS_PGPASSWORD: Type.String(),
  ORDINALS_PGDATABASE: Type.String(),
  ORDINALS_SCHEMA: Type.Optional(Type.String()),

  BRC20_PGHOST: Type.String(),
  BRC20_PGPORT: Type.Number({ default: 5432, minimum: 0, maximum: 65535 }),
  BRC20_PGUSER: Type.String(),
  BRC20_PGPASSWORD: Type.String(),
  BRC20_PGDATABASE: Type.String(),
  BRC20_SCHEMA: Type.Optional(Type.String()),

  /** Limit to how many concurrent connections can be created */
  PG_CONNECTION_POOL_MAX: Type.Number({ default: 10 }),
  PG_IDLE_TIMEOUT: Type.Number({ default: 30 }),
  PG_MAX_LIFETIME: Type.Number({ default: 60 }),
  PG_STATEMENT_TIMEOUT: Type.Number({ default: 60_000 }),
});
type Env = Static<typeof schema>;

export const ENV = envSchema<Env>({
  schema: schema,
  dotenv: true,
});
