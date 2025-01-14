import { BasePgStore, connectPostgres, PgConnectionVars } from '@hirosystems/api-toolkit';
import { DbInscriptionIndexPaging, DbPaginatedResult } from '../types';
import {
  DbBrc20Activity,
  DbBrc20Balance,
  DbBrc20Holder,
  DbBrc20Token,
  DbBrc20TokenWithSupply,
} from './types';
import { Brc20TokenOrderBy } from '../../api/schemas';
import { objRemoveUndefinedValues } from '../helpers';
import { sqlOr } from './helpers';
import { ENV } from '../../env';

export class Brc20PgStore extends BasePgStore {
  static async connect(): Promise<Brc20PgStore> {
    const pgConfig: PgConnectionVars = {
      host: ENV.BRC20_PGHOST,
      port: ENV.BRC20_PGPORT,
      user: ENV.BRC20_PGUSER,
      password: ENV.BRC20_PGPASSWORD,
      database: ENV.BRC20_PGDATABASE,
      schema: ENV.BRC20_SCHEMA,
    };
    const sql = await connectPostgres({
      usageName: 'brc20-pg-store',
      connectionArgs: pgConfig,
      connectionConfig: {
        poolMax: ENV.PG_CONNECTION_POOL_MAX,
        idleTimeout: ENV.PG_IDLE_TIMEOUT,
        maxLifetime: ENV.PG_MAX_LIFETIME,
        statementTimeout: ENV.PG_STATEMENT_TIMEOUT,
      },
    });
    return new Brc20PgStore(sql);
  }

  async getTokens(
    args: { ticker?: string[]; order_by?: Brc20TokenOrderBy } & DbInscriptionIndexPaging
  ): Promise<DbPaginatedResult<DbBrc20Token>> {
    const tickerPrefixCondition = sqlOr(
      this.sql,
      args.ticker?.map(t => this.sql`d.ticker LIKE LOWER(${t}) || '%'`)
    );
    const orderBy =
      args.order_by === Brc20TokenOrderBy.tx_count
        ? this.sql`d.tx_count DESC` // tx_count
        : this.sql`d.block_height DESC, d.tx_index DESC`; // default: `index`
    const results = await this.sql<(DbBrc20Token & { total: number })[]>`
      ${
        args.ticker === undefined
          ? this.sql`WITH global_count AS (
              SELECT COALESCE(count, 0) AS count
              FROM counts_by_operation
              WHERE operation = 'deploy'
            )`
          : this.sql``
      }
      SELECT
        d.*,
        ${
          args.ticker ? this.sql`COUNT(*) OVER()` : this.sql`(SELECT count FROM global_count)`
        } AS total
      FROM tokens AS d
      ${tickerPrefixCondition ? this.sql`WHERE ${tickerPrefixCondition}` : this.sql``}
      ORDER BY ${orderBy}
      OFFSET ${args.offset}
      LIMIT ${args.limit}
    `;
    return {
      total: results[0]?.total ?? 0,
      results: results ?? [],
    };
  }

  async getBalances(
    args: {
      address: string;
      ticker?: string[];
      block_height?: number;
    } & DbInscriptionIndexPaging
  ): Promise<DbPaginatedResult<DbBrc20Balance>> {
    const ticker = sqlOr(
      this.sql,
      args.ticker?.map(t => this.sql`d.ticker LIKE LOWER(${t}) || '%'`)
    );
    // Change selection table depending if we're filtering by block height or not.
    const results = await this.sql<(DbBrc20Balance & { total: number })[]>`
      ${
        args.block_height
          ? this.sql`
              SELECT
                d.ticker, d.decimals,
                SUM(b.avail_balance) AS avail_balance,
                SUM(b.trans_balance) AS trans_balance,
                SUM(b.avail_balance + b.trans_balance) AS total_balance,
                COUNT(*) OVER() as total
              FROM operations AS b
              INNER JOIN tokens AS d ON d.ticker = b.ticker
              WHERE
                b.address = ${args.address}
                AND b.block_height <= ${args.block_height}
                ${ticker ? this.sql`AND ${ticker}` : this.sql``}
              GROUP BY d.ticker, d.decimals
              HAVING SUM(b.avail_balance + b.trans_balance) > 0
            `
          : this.sql`
              SELECT d.ticker, d.decimals, b.avail_balance, b.trans_balance, b.total_balance, COUNT(*) OVER() as total
              FROM balances AS b
              INNER JOIN tokens AS d ON d.ticker = b.ticker
              WHERE
                b.total_balance > 0
                AND b.address = ${args.address}
                ${ticker ? this.sql`AND ${ticker}` : this.sql``}
            `
      }
      LIMIT ${args.limit}
      OFFSET ${args.offset}
    `;
    return {
      total: results[0]?.total ?? 0,
      results: results ?? [],
    };
  }

  async getToken(args: { ticker: string }): Promise<DbBrc20TokenWithSupply | undefined> {
    const result = await this.sql<DbBrc20TokenWithSupply[]>`
      WITH token AS (
        SELECT d.*
        FROM tokens AS d
        WHERE d.ticker = LOWER(${args.ticker})
      ),
      holders AS (
        SELECT COUNT(*) AS count
        FROM balances
        WHERE ticker = (SELECT ticker FROM token) AND total_balance > 0
      )
      SELECT *, COALESCE((SELECT count FROM holders), 0) AS holders
      FROM token
    `;
    if (result.count) return result[0];
  }

  async getTokenHolders(
    args: {
      ticker: string;
    } & DbInscriptionIndexPaging
  ): Promise<DbPaginatedResult<DbBrc20Holder> | undefined> {
    return await this.sqlTransaction(async sql => {
      const token = await sql<{ id: string; decimals: number }[]>`
        SELECT ticker FROM tokens WHERE ticker = LOWER(${args.ticker})
      `;
      if (token.count === 0) return;
      const results = await sql<(DbBrc20Holder & { total: number })[]>`
        SELECT
          b.address, d.decimals, b.total_balance, COUNT(*) OVER() AS total
        FROM balances AS b
        INNER JOIN tokens AS d USING (ticker)
        WHERE b.ticker = LOWER(${args.ticker})
        ORDER BY b.total_balance DESC
        LIMIT ${args.limit}
        OFFSET ${args.offset}
      `;
      return {
        total: results[0]?.total ?? 0,
        results: results ?? [],
      };
    });
  }

  async getActivity(
    page: DbInscriptionIndexPaging,
    filters: {
      ticker?: string[];
      block_height?: number;
      operation?: string[];
      address?: string;
    }
  ): Promise<DbPaginatedResult<DbBrc20Activity>> {
    // Do we need a specific result count such as total activity or activity per address?
    objRemoveUndefinedValues(filters);
    const filterLength = Object.keys(filters).length;
    const needsGlobalEventCount =
      filterLength === 0 ||
      (filterLength === 1 && filters.operation && filters.operation.length > 0);
    const needsAddressEventCount =
      (filterLength === 1 && filters.address != undefined && filters.address != '') ||
      (filterLength === 2 &&
        filters.operation &&
        filters.operation.length > 0 &&
        filters.address != undefined &&
        filters.address != '');
    const needsTickerCount = filterLength === 1 && filters.ticker && filters.ticker.length > 0;
    const operationsFilter = filters.operation?.filter(i => i !== 'transfer_receive');

    return this.sqlTransaction(async sql => {
      const results = await sql<(DbBrc20Activity & { total: number })[]>`
        WITH event_count AS (${
          needsGlobalEventCount
            ? sql`
                SELECT COALESCE(SUM(count), 0) AS count
                FROM counts_by_operation
                ${operationsFilter ? sql`WHERE operation IN ${sql(operationsFilter)}` : sql``}
              `
            : needsAddressEventCount
            ? sql`
                SELECT SUM(count) AS count
                FROM counts_by_address_operation
                WHERE address = ${filters.address}
                ${operationsFilter ? sql`AND operation IN ${sql(operationsFilter)}` : sql``}
              `
            : needsTickerCount && filters.ticker !== undefined
            ? sql`
                SELECT COALESCE(SUM(tx_count), 0) AS count
                FROM tokens AS d
                WHERE ticker IN ${sql(filters.ticker)}
              `
            : sql`SELECT NULL AS count`
        })
        SELECT
          e.*,
          d.max AS deploy_max,
          d.limit AS deploy_limit,
          d.decimals AS deploy_decimals,
          ${
            needsGlobalEventCount || needsAddressEventCount || needsTickerCount
              ? sql`(SELECT count FROM event_count)`
              : sql`COUNT(*) OVER()`
          } AS total
        FROM operations AS e
        INNER JOIN tokens AS d ON d.ticker = e.ticker
        WHERE TRUE
          ${
            operationsFilter
              ? sql`AND e.operation IN ${sql(operationsFilter)}`
              : sql`AND e.operation <> 'transfer_receive'`
          }
          ${filters.ticker ? sql`AND e.ticker IN ${sql(filters.ticker)}` : sql``}
          ${filters.block_height ? sql`AND e.block_height = ${filters.block_height}` : sql``}
          ${
            filters.address
              ? sql`AND (e.address = ${filters.address} OR e.to_address = ${filters.address})`
              : sql``
          }
        ORDER BY e.block_height DESC, e.tx_index DESC
        LIMIT ${page.limit}
        OFFSET ${page.offset}
      `;
      return {
        total: results[0]?.total ?? 0,
        results: results ?? [],
      };
    });
  }
}
