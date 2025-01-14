import * as postgres from 'postgres';
import { PgSqlClient } from '@hirosystems/api-toolkit';

export function sqlOr(
  sql: PgSqlClient,
  partials: postgres.PendingQuery<postgres.Row[]>[] | undefined
) {
  return partials?.reduce((acc, curr) => sql`${acc} OR ${curr}`);
}
