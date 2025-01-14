import { Order, OrderBy } from '../api/schemas';
import { SatoshiRarity } from '../api/util/ordinal-satoshi';

export type DbPaginatedResult<T> = {
  total: number;
  results: T[];
};

export type DbFullyLocatedInscriptionResult = {
  genesis_id: string;
  genesis_block_height: string;
  genesis_block_hash: string;
  genesis_tx_id: string;
  genesis_fee: bigint;
  genesis_timestamp: Date;
  genesis_address: string;
  number: string;
  address: string | null;
  tx_id: string;
  tx_index: number;
  output: string;
  offset: string | null;
  value: string | null;
  sat_ordinal: string;
  sat_rarity: string;
  sat_coinbase_height: string;
  mime_type: string;
  content_type: string;
  content_length: string;
  timestamp: Date;
  curse_type: string | null;
  recursive: boolean;
  recursion_refs: string | null;
  parent: string | null;
  metadata: string | null;
  input_index: number;
  pointer: number | null;
  metaprotocol: string | null;
  delegate: string | null;
};

export enum DbLocationTransferType {
  transferred = 'transferred',
  spentInFees = 'spent_in_fees',
  burnt = 'burnt',
}

export type DbLocation = {
  genesis_id: string;
  block_height: string;
  block_hash: string;
  tx_id: string;
  tx_index: number;
  address: string;
  output: string;
  offset: string | null;
  prev_output: string | null;
  prev_offset: string | null;
  value: string | null;
  timestamp: Date;
};

export type DbInscriptionLocationChange = {
  genesis_id: string;
  number: string;
  from_block_height: string;
  from_block_hash: string;
  from_tx_id: string;
  from_address: string;
  from_output: string;
  from_offset: string | null;
  from_value: string | null;
  from_timestamp: Date;
  to_block_height: string;
  to_block_hash: string;
  to_tx_id: string;
  to_address: string;
  to_output: string;
  to_offset: string | null;
  to_value: string | null;
  to_timestamp: Date;
};

export type DbInscriptionContent = {
  content_type: string;
  content_length: string;
  content: string;
};

export type DbInscriptionIndexPaging = {
  limit: number;
  offset: number;
};

export type DbInscriptionIndexFilters = {
  genesis_id?: string[];
  genesis_block_height?: number;
  from_genesis_block_height?: number;
  to_genesis_block_height?: number;
  genesis_block_hash?: string;
  from_genesis_timestamp?: number;
  to_genesis_timestamp?: number;
  from_sat_coinbase_height?: number;
  to_sat_coinbase_height?: number;
  number?: number[];
  from_number?: number;
  to_number?: number;
  address?: string[];
  genesis_address?: string[];
  mime_type?: string[];
  output?: string;
  sat_rarity?: SatoshiRarity[];
  sat_ordinal?: bigint;
  from_sat_ordinal?: bigint;
  to_sat_ordinal?: bigint;
  recursive?: boolean;
  cursed?: boolean;
};

export type DbInscriptionIndexOrder = {
  order_by?: OrderBy;
  order?: Order;
};

export enum DbInscriptionType {
  blessed = 'blessed',
  cursed = 'cursed',
}

export type DbInscriptionCountPerBlockFilters = {
  from_block_height?: number;
  to_block_height?: number;
};

export type DbInscriptionCountPerBlock = {
  block_height: string;
  block_hash: string;
  inscription_count: string;
  inscription_count_accum: string;
  timestamp: number;
};
