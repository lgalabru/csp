import BigNumber from 'bignumber.js';
import {
  DbBrc20Activity,
  DbBrc20Balance,
  DbBrc20EventOperation,
  DbBrc20Holder,
  DbBrc20Token,
  DbBrc20TokenWithSupply,
} from '../../pg/brc20/types';
import {
  DbFullyLocatedInscriptionResult,
  DbInscriptionLocationChange,
  DbLocation,
} from '../../pg/types';
import {
  BlockHashParamCType,
  BlockHeightParamCType,
  BlockInscriptionTransfer,
  Brc20ActivityResponse,
  Brc20BalanceResponse,
  Brc20HolderResponse,
  Brc20Supply,
  Brc20TokenResponse,
  InscriptionLocationResponse,
  InscriptionResponseType,
} from '../schemas';

export const DEFAULT_API_LIMIT = 20;

export function parseDbInscriptions(
  items: DbFullyLocatedInscriptionResult[]
): InscriptionResponseType[] {
  return items.map(i => ({
    id: i.genesis_id,
    number: parseInt(i.number),
    address: i.address,
    genesis_address: i.genesis_address,
    genesis_block_height: parseInt(i.genesis_block_height),
    genesis_block_hash: i.genesis_block_hash,
    genesis_tx_id: i.genesis_tx_id,
    genesis_fee: i.genesis_fee.toString(),
    genesis_timestamp: i.genesis_timestamp.valueOf(),
    tx_id: i.tx_id,
    location: `${i.output}:${i.offset}`,
    output: i.output,
    value: i.value,
    offset: i.offset,
    sat_ordinal: i.sat_ordinal.toString(),
    sat_rarity: i.sat_rarity,
    sat_coinbase_height: parseInt(i.sat_coinbase_height),
    mime_type: i.mime_type,
    content_type: i.content_type,
    content_length: parseInt(i.content_length),
    timestamp: i.timestamp.valueOf(),
    curse_type: i.curse_type,
    recursive: i.recursive,
    recursion_refs: i.recursion_refs?.split(',') ?? null,
    parent: i.parent,
    metadata: i.metadata ? JSON.parse(i.metadata) : null,
    delegate: i.delegate ?? null,
    meta_protocol: i.metaprotocol ?? null,
  }));
}
export function parseDbInscription(item: DbFullyLocatedInscriptionResult): InscriptionResponseType {
  return parseDbInscriptions([item])[0];
}

export function parseInscriptionLocations(items: DbLocation[]): InscriptionLocationResponse[] {
  return items.map(i => ({
    block_height: parseInt(i.block_height),
    block_hash: i.block_hash,
    address: i.address,
    tx_id: i.tx_id,
    location: `${i.output}:${i.offset}`,
    output: i.output,
    value: i.value,
    offset: i.offset,
    timestamp: i.timestamp.valueOf(),
  }));
}

export function parseBlockTransfers(
  items: DbInscriptionLocationChange[]
): BlockInscriptionTransfer[] {
  return items.map(i => ({
    id: i.genesis_id,
    number: parseInt(i.number),
    from: {
      block_height: parseInt(i.from_block_height),
      block_hash: i.from_block_hash,
      address: i.from_address,
      tx_id: i.from_tx_id,
      location: `${i.from_output}:${i.from_offset}`,
      output: i.from_output,
      value: i.from_value,
      offset: i.from_offset,
      timestamp: i.from_timestamp.valueOf(),
    },
    to: {
      block_height: parseInt(i.to_block_height),
      block_hash: i.to_block_hash,
      address: i.to_address,
      tx_id: i.to_tx_id,
      location: `${i.to_output}:${i.to_offset}`,
      output: i.to_output,
      value: i.to_value,
      offset: i.to_offset,
      timestamp: i.to_timestamp.valueOf(),
    },
  }));
}

export function parseBrc20Tokens(items: DbBrc20Token[]): Brc20TokenResponse[] {
  return items.map(i => ({
    id: i.inscription_id,
    number: parseInt(i.inscription_number),
    block_height: parseInt(i.block_height),
    tx_id: i.tx_id,
    address: i.address,
    ticker: i.ticker,
    max_supply: decimals(i.max, i.decimals),
    mint_limit: i.limit ? decimals(i.limit, i.decimals) : null,
    decimals: i.decimals,
    deploy_timestamp: i.timestamp.valueOf(),
    minted_supply: decimals(i.minted_supply, i.decimals),
    tx_count: parseInt(i.tx_count),
    self_mint: i.self_mint,
  }));
}

export function parseBrc20Supply(item: DbBrc20TokenWithSupply): Brc20Supply {
  return {
    max_supply: decimals(item.max, item.decimals),
    minted_supply: decimals(item.minted_supply, item.decimals),
    holders: parseInt(item.holders),
  };
}

export function parseBrc20Balances(items: DbBrc20Balance[]): Brc20BalanceResponse[] {
  return items.map(i => ({
    ticker: i.ticker,
    available_balance: decimals(i.avail_balance, i.decimals),
    transferrable_balance: decimals(i.trans_balance, i.decimals),
    overall_balance: decimals(i.total_balance, i.decimals),
  }));
}

export function parseBrc20Activities(items: DbBrc20Activity[]): Brc20ActivityResponse[] {
  return items.map(i => {
    const activity = {
      operation: i.operation,
      ticker: i.ticker,
      address: i.to_address ?? i.address,
      tx_id: i.tx_id,
      inscription_id: i.inscription_id,
      location: `${i.output}:${i.offset}`,
      block_hash: i.block_hash,
      block_height: parseInt(i.block_height),
      timestamp: i.timestamp.valueOf(),
    };
    switch (i.operation) {
      case DbBrc20EventOperation.deploy: {
        return {
          ...activity,
          deploy: {
            max_supply: decimals(i.deploy_max, i.deploy_decimals),
            mint_limit: i.deploy_limit ? decimals(i.deploy_limit, i.deploy_decimals) : null,
            decimals: i.deploy_decimals,
          },
        };
      }
      case DbBrc20EventOperation.mint: {
        return {
          ...activity,
          mint: {
            amount: decimals(i.amount, i.deploy_decimals),
          },
        };
      }
      case DbBrc20EventOperation.transfer: {
        return {
          ...activity,
          transfer: {
            amount: decimals(i.amount, i.deploy_decimals),
            from_address: i.address,
          },
        };
      }
      case DbBrc20EventOperation.transferSend: {
        return {
          ...activity,
          transfer_send: {
            amount: decimals(i.amount, i.deploy_decimals),
            from_address: i.address,
            to_address: i.to_address ?? i.address,
          },
        };
      }
    }
  });
}

export function parseBrc20Holders(items: DbBrc20Holder[]): Brc20HolderResponse[] {
  return items.map(i => ({
    address: i.address,
    overall_balance: decimals(i.total_balance, i.decimals),
  }));
}

export function parseSatPoint(satpoint: string): {
  tx_id: string;
  vout: string;
  offset?: string;
} {
  const [tx_id, vout, offset] = satpoint.split(':');
  return { tx_id: normalizedHexString(tx_id), vout: vout, offset };
}

function decimals(num: string, decimals: number): string {
  return new BigNumber(num).dividedBy(10 ** decimals).toFixed(decimals);
}

/**
 * Decodes a `0x` prefixed hex string to a buffer.
 * @param hex - A hex string with a `0x` prefix.
 */
export function hexToBuffer(hex: string): Buffer {
  if (hex.length === 0) {
    return Buffer.alloc(0);
  }
  if (!hex.startsWith('0x')) {
    throw new Error(`Hex string is missing the "0x" prefix: "${hex}"`);
  }
  if (hex.length % 2 !== 0) {
    throw new Error(`Hex string is an odd number of digits: ${hex}`);
  }
  return Buffer.from(hex.substring(2), 'hex');
}

const has0xPrefix = (id: string) => id.substr(0, 2).toLowerCase() === '0x';

export function normalizedHexString(hex: string): string {
  return has0xPrefix(hex) ? hex.substring(2) : hex;
}

export function blockParam(param: string | undefined, name: string) {
  const out: Record<string, string> = {};
  if (BlockHashParamCType.Check(param)) {
    out[`${name}_hash`] = param;
  } else if (BlockHeightParamCType.Check(param)) {
    out[`${name}_height`] = param;
  }
  return out;
}
