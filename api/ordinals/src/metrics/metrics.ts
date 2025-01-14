import * as prom from 'prom-client';
import { PgStore } from '../pg/pg-store';

export class ApiMetrics {
  /** The most recent Bitcoin block height ingested by the API */
  readonly ordinals_api_block_height: prom.Gauge;
  /** Maximum blessed inscription number */
  readonly ordinals_api_max_inscription_number: prom.Gauge;
  /** Maximum cursed inscription number */
  readonly ordinals_api_max_cursed_inscription_number: prom.Gauge;

  static configure(db: PgStore): ApiMetrics {
    return new ApiMetrics(db);
  }

  private constructor(db: PgStore) {
    this.ordinals_api_block_height = new prom.Gauge({
      name: `ordinals_api_block_height`,
      help: 'The most recent Bitcoin block height ingested by the API',
      async collect() {
        const height = await db.getChainTipBlockHeight();
        this.set(height);
      },
    });
    this.ordinals_api_max_inscription_number = new prom.Gauge({
      name: `ordinals_api_max_inscription_number`,
      help: 'Maximum blessed inscription number',
      async collect() {
        const max = await db.getMaxInscriptionNumber();
        if (max) this.set(max);
      },
    });
    this.ordinals_api_max_cursed_inscription_number = new prom.Gauge({
      name: `ordinals_api_max_cursed_inscription_number`,
      help: 'Maximum cursed inscription number',
      async collect() {
        const max = await db.getMaxCursedInscriptionNumber();
        if (max) this.set(max);
      },
    });
  }
}
