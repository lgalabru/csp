import {
  BasePgStore,
  PgConnectionVars,
  PgSqlClient,
  connectPostgres,
} from '@hirosystems/api-toolkit';
import { Order, OrderBy } from '../api/schemas';
import { ENV } from '../env';
import { CountsPgStore } from './counts/counts-pg-store';
import { getIndexResultCountType } from './counts/helpers';
import {
  DbFullyLocatedInscriptionResult,
  DbInscriptionContent,
  DbInscriptionIndexFilters,
  DbInscriptionIndexOrder,
  DbInscriptionIndexPaging,
  DbInscriptionLocationChange,
  DbLocation,
  DbPaginatedResult,
} from './types';

type InscriptionIdentifier = { genesis_id: string } | { number: number };

export class PgStore extends BasePgStore {
  readonly counts: CountsPgStore;

  static async connect(): Promise<PgStore> {
    const pgConfig: PgConnectionVars = {
      host: ENV.ORDINALS_PGHOST,
      port: ENV.ORDINALS_PGPORT,
      user: ENV.ORDINALS_PGUSER,
      password: ENV.ORDINALS_PGPASSWORD,
      database: ENV.ORDINALS_PGDATABASE,
      schema: ENV.ORDINALS_SCHEMA,
    };
    const sql = await connectPostgres({
      usageName: 'ordinals-pg-store',
      connectionArgs: pgConfig,
      connectionConfig: {
        poolMax: ENV.PG_CONNECTION_POOL_MAX,
        idleTimeout: ENV.PG_IDLE_TIMEOUT,
        maxLifetime: ENV.PG_MAX_LIFETIME,
        statementTimeout: ENV.PG_STATEMENT_TIMEOUT,
      },
    });
    return new PgStore(sql);
  }

  constructor(sql: PgSqlClient) {
    super(sql);
    this.counts = new CountsPgStore(this);
  }

  async getChainTipBlockHeight(): Promise<number> {
    const result = await this.sql<{ block_height: string }[]>`SELECT block_height FROM chain_tip`;
    return parseInt(result[0].block_height);
  }

  async getMaxInscriptionNumber(): Promise<number | undefined> {
    const result = await this.sql<{ max: string }[]>`
      SELECT MAX(number) FROM inscriptions WHERE number >= 0
    `;
    if (result[0].max) {
      return parseInt(result[0].max);
    }
  }

  async getMaxCursedInscriptionNumber(): Promise<number | undefined> {
    const result = await this.sql<{ min: string }[]>`
      SELECT MIN(number) FROM inscriptions WHERE number < 0
    `;
    if (result[0].min) {
      return parseInt(result[0].min);
    }
  }

  async getInscriptionsIndexETag(): Promise<string> {
    const result = await this.sql<{ etag: string }[]>`
      SELECT date_part('epoch', MAX(updated_at))::text AS etag FROM inscriptions
    `;
    return result[0].etag;
  }

  async getInscriptionsPerBlockETag(): Promise<string> {
    const result = await this.sql<{ block_hash: string; inscription_count: string }[]>`
      SELECT block_hash, inscription_count
      FROM counts_by_block
      ORDER BY block_height DESC
      LIMIT 1
    `;
    return `${result[0].block_hash}:${result[0].inscription_count}`;
  }

  async getInscriptionContent(
    args: InscriptionIdentifier
  ): Promise<DbInscriptionContent | undefined> {
    const result = await this.sql<DbInscriptionContent[]>`
      WITH content_id AS (
        SELECT
          CASE
            WHEN delegate IS NOT NULL THEN delegate
            ELSE inscription_id
          END AS genesis_id
        FROM inscriptions
        WHERE ${
          'genesis_id' in args
            ? this.sql`inscription_id = ${args.genesis_id}`
            : this.sql`number = ${args.number}`
        }
      )
      SELECT content, content_type, content_length
      FROM inscriptions
      WHERE inscription_id = (SELECT genesis_id FROM content_id)
    `;
    if (result.count > 0) {
      return result[0];
    }
  }

  async getInscriptionETag(args: InscriptionIdentifier): Promise<string | undefined> {
    const result = await this.sql<{ etag: string }[]>`
      SELECT date_part('epoch', updated_at)::text AS etag
      FROM inscriptions
      WHERE ${
        'genesis_id' in args
          ? this.sql`genesis_id = ${args.genesis_id}`
          : this.sql`number = ${args.number}`
      }
    `;
    if (result.count > 0) {
      return result[0].etag;
    }
  }

  async getInscriptions(
    page: DbInscriptionIndexPaging,
    filters?: DbInscriptionIndexFilters,
    sort?: DbInscriptionIndexOrder
  ): Promise<DbPaginatedResult<DbFullyLocatedInscriptionResult>> {
    return await this.sqlTransaction(async sql => {
      const order = sort?.order === Order.asc ? sql`ASC` : sql`DESC`;
      let orderBy = sql`i.number ${order}`;
      switch (sort?.order_by) {
        case OrderBy.genesis_block_height:
          orderBy = sql`i.block_height ${order}, i.tx_index ${order}`;
          break;
        case OrderBy.ordinal:
          orderBy = sql`i.ordinal_number ${order}`;
          break;
        case OrderBy.rarity:
          orderBy = sql`ARRAY_POSITION(ARRAY['common','uncommon','rare','epic','legendary','mythic'], s.rarity) ${order}, i.number DESC`;
          break;
      }
      // Do we need a filtered `COUNT(*)`? If so, try to use the pre-calculated counts we have in
      // cached tables to speed up these queries.
      const countType = getIndexResultCountType(filters);
      const total = await this.counts.fromResults(countType, filters);
      const results = await sql<(DbFullyLocatedInscriptionResult & { total: number })[]>`
        WITH results AS (
          SELECT
            i.inscription_id AS genesis_id,
            i.number,
            i.mime_type,
            i.content_type,
            i.content_length,
            i.fee AS genesis_fee,
            i.curse_type,
            i.ordinal_number AS sat_ordinal,
            i.parent,
            i.metadata,
            s.rarity AS sat_rarity,
            s.coinbase_height AS sat_coinbase_height,
            i.recursive,
            (
              SELECT STRING_AGG(ir.ref_inscription_id, ',')
              FROM inscription_recursions AS ir
              WHERE ir.inscription_id = i.inscription_id
            ) AS recursion_refs,
            i.block_height AS genesis_block_height,
            i.tx_index AS genesis_tx_index,
            i.timestamp AS genesis_timestamp,
            i.address AS genesis_address,
            cur.address,
            cur.tx_index,
            cur.block_height,
            ${total === undefined ? sql`COUNT(*) OVER() AS total` : sql`0 AS total`},
            ROW_NUMBER() OVER(ORDER BY ${orderBy}) AS row_num
          FROM inscriptions AS i
          INNER JOIN current_locations AS cur ON cur.ordinal_number = i.ordinal_number
          INNER JOIN satoshis AS s ON s.ordinal_number = i.ordinal_number
          WHERE TRUE
            ${
              filters?.genesis_id?.length
                ? sql`AND i.inscription_id IN ${sql(filters.genesis_id)}`
                : sql``
            }
            ${
              filters?.genesis_block_height
                ? sql`AND i.block_height = ${filters.genesis_block_height}`
                : sql``
            }
            ${
              filters?.genesis_block_hash
                ? sql`AND i.block_hash = ${filters.genesis_block_hash}`
                : sql``
            }
            ${
              filters?.from_genesis_block_height
                ? sql`AND i.block_height >= ${filters.from_genesis_block_height}`
                : sql``
            }
            ${
              filters?.to_genesis_block_height
                ? sql`AND i.block_height <= ${filters.to_genesis_block_height}`
                : sql``
            }
            ${
              filters?.from_sat_coinbase_height
                ? sql`AND s.coinbase_height >= ${filters.from_sat_coinbase_height}`
                : sql``
            }
            ${
              filters?.to_sat_coinbase_height
                ? sql`AND s.coinbase_height <= ${filters.to_sat_coinbase_height}`
                : sql``
            }
            ${
              filters?.from_genesis_timestamp
                ? sql`AND i.timestamp >= to_timestamp(${filters.from_genesis_timestamp})`
                : sql``
            }
            ${
              filters?.to_genesis_timestamp
                ? sql`AND i.timestamp <= to_timestamp(${filters.to_genesis_timestamp})`
                : sql``
            }
            ${
              filters?.from_sat_ordinal
                ? sql`AND i.ordinal_number >= ${filters.from_sat_ordinal}`
                : sql``
            }
            ${
              filters?.to_sat_ordinal
                ? sql`AND i.ordinal_number <= ${filters.to_sat_ordinal}`
                : sql``
            }
            ${filters?.number?.length ? sql`AND i.number IN ${sql(filters.number)}` : sql``}
            ${
              filters?.from_number !== undefined
                ? sql`AND i.number >= ${filters.from_number}`
                : sql``
            }
            ${filters?.to_number !== undefined ? sql`AND i.number <= ${filters.to_number}` : sql``}
            ${filters?.address?.length ? sql`AND cur.address IN ${sql(filters.address)}` : sql``}
            ${
              filters?.mime_type?.length ? sql`AND i.mime_type IN ${sql(filters.mime_type)}` : sql``
            }
            ${filters?.output ? sql`AND cur.output = ${filters.output}` : sql``}
            ${filters?.sat_rarity?.length ? sql`AND s.rarity IN ${sql(filters.sat_rarity)}` : sql``}
            ${filters?.sat_ordinal ? sql`AND i.ordinal_number = ${filters.sat_ordinal}` : sql``}
            ${
              filters?.recursive !== undefined ? sql`AND i.recursive = ${filters.recursive}` : sql``
            }
            ${filters?.cursed === true ? sql`AND i.number < 0` : sql``}
            ${filters?.cursed === false ? sql`AND i.number >= 0` : sql``}
            ${
              filters?.genesis_address?.length
                ? sql`AND i.address IN ${sql(filters.genesis_address)}`
                : sql``
            }
            ORDER BY ${orderBy} LIMIT ${page.limit} OFFSET ${page.offset}
          )
        SELECT
          r.*,
          gen_l.block_hash AS genesis_block_hash,
          gen_l.tx_id AS genesis_tx_id,
          cur_l.tx_id,
          cur_l.output,
          cur_l.offset,
          cur_l.timestamp,
          cur_l.value
        FROM results AS r
        INNER JOIN locations AS cur_l ON cur_l.ordinal_number = r.sat_ordinal AND cur_l.block_height = r.block_height AND cur_l.tx_index = r.tx_index
        INNER JOIN locations AS gen_l ON gen_l.ordinal_number = r.sat_ordinal AND gen_l.block_height = r.genesis_block_height AND gen_l.tx_index = r.genesis_tx_index
        ORDER BY r.row_num ASC
      `;
      return {
        total: total ?? results[0]?.total ?? 0,
        results: results ?? [],
      };
    });
  }

  async getInscriptionLocations(
    args: InscriptionIdentifier & { limit: number; offset: number }
  ): Promise<DbPaginatedResult<DbLocation>> {
    const results = await this.sql<({ total: number } & DbLocation)[]>`
      SELECT l.*, COUNT(*) OVER() as total
      FROM locations AS l
      INNER JOIN inscriptions AS i ON i.ordinal_number = l.ordinal_number
      WHERE ${
        'number' in args
          ? this.sql`i.number = ${args.number}`
          : this.sql`i.genesis_id = ${args.genesis_id}`
      }
        AND (
          (l.block_height > i.block_height)
          OR (l.block_height = i.block_height AND l.tx_index >= i.tx_index)
        )
      ORDER BY l.block_height DESC, l.tx_index DESC
      LIMIT ${args.limit}
      OFFSET ${args.offset}
    `;
    return {
      total: results[0]?.total ?? 0,
      results: results ?? [],
    };
  }

  async getTransfersPerBlock(
    args: { block_height?: number; block_hash?: string } & DbInscriptionIndexPaging
  ): Promise<DbPaginatedResult<DbInscriptionLocationChange>> {
    const results = await this.sql<({ total: number } & DbInscriptionLocationChange)[]>`
      WITH transfer_total AS (
        SELECT MAX(block_transfer_index) AS total FROM inscription_transfers WHERE ${
          'block_height' in args
            ? this.sql`block_height = ${args.block_height}`
            : this.sql`block_hash = ${args.block_hash}`
        }
      ),
      transfer_data AS (
        SELECT
          t.number,
          t.inscription_id AS genesis_id,
          t.ordinal_number,
          t.block_height,
          t.tx_index,
          t.block_transfer_index,
          (
            SELECT l.block_height || ',' || l.tx_index
            FROM locations AS l
            WHERE l.ordinal_number = t.ordinal_number AND (
              l.block_height < t.block_height OR
              (l.block_height = t.block_height AND l.tx_index < t.tx_index)
            )
            ORDER BY l.block_height DESC, l.tx_index DESC
            LIMIT 1
          ) AS from_data
        FROM inscription_transfers AS t
        WHERE
          ${
            'block_height' in args
              ? this.sql`t.block_height = ${args.block_height}`
              : this.sql`t.block_hash = ${args.block_hash}`
          }
          AND t.block_transfer_index <= ((SELECT total FROM transfer_total) - ${args.offset}::int)
          AND t.block_transfer_index >
            ((SELECT total FROM transfer_total) - (${args.offset}::int + ${args.limit}::int))
      )
      SELECT
        td.genesis_id,
        td.number,
        lf.block_height AS from_block_height,
        lf.block_hash AS from_block_hash,
        lf.tx_id AS from_tx_id,
        lf.address AS from_address,
        lf.output AS from_output,
        lf.offset AS from_offset,
        lf.value AS from_value,
        lf.timestamp AS from_timestamp,
        lt.block_height AS to_block_height,
        lt.block_hash AS to_block_hash,
        lt.tx_id AS to_tx_id,
        lt.address AS to_address,
        lt.output AS to_output,
        lt.offset AS to_offset,
        lt.value AS to_value,
        lt.timestamp AS to_timestamp,
        (SELECT total FROM transfer_total) + 1 AS total
      FROM transfer_data AS td
      INNER JOIN locations AS lf ON td.ordinal_number = lf.ordinal_number AND lf.block_height = SPLIT_PART(td.from_data, ',', 1)::int AND lf.tx_index = SPLIT_PART(td.from_data, ',', 2)::int
      INNER JOIN locations AS lt ON td.ordinal_number = lt.ordinal_number AND td.block_height = lt.block_height AND td.tx_index = lt.tx_index
      ORDER BY td.block_height DESC, td.block_transfer_index DESC
    `;
    return {
      total: results[0]?.total ?? 0,
      results: results ?? [],
    };
  }
}
