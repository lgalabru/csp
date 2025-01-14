import { objRemoveUndefinedValues } from '../helpers';
import { DbInscriptionIndexFilters } from '../types';
import { DbInscriptionIndexResultCountType } from './types';

/**
 * Returns which inscription count is required based on filters sent to the index endpoint.
 * @param filters - DbInscriptionIndexFilters
 * @returns DbInscriptionIndexResultCountType
 */
export function getIndexResultCountType(
  filters?: DbInscriptionIndexFilters
): DbInscriptionIndexResultCountType {
  if (!filters) return DbInscriptionIndexResultCountType.all;
  // How many filters do we have?
  objRemoveUndefinedValues(filters);
  switch (Object.keys(filters).length) {
    case 0:
      return DbInscriptionIndexResultCountType.all;
    case 1:
      if (filters.mime_type) return DbInscriptionIndexResultCountType.mimeType;
      if (filters.sat_rarity) return DbInscriptionIndexResultCountType.satRarity;
      if (filters.address) return DbInscriptionIndexResultCountType.address;
      if (filters.genesis_address) return DbInscriptionIndexResultCountType.genesisAddress;
      if (filters.genesis_block_height) return DbInscriptionIndexResultCountType.blockHeight;
      if (filters.from_genesis_block_height)
        return DbInscriptionIndexResultCountType.fromblockHeight;
      if (filters.to_genesis_block_height) return DbInscriptionIndexResultCountType.toblockHeight;
      if (filters.genesis_block_hash) return DbInscriptionIndexResultCountType.blockHash;
      if (filters.cursed !== undefined) return DbInscriptionIndexResultCountType.cursed;
      if (filters.recursive !== undefined) return DbInscriptionIndexResultCountType.recursive;
      if (filters.number || filters.genesis_id || filters.output || filters.sat_ordinal)
        return DbInscriptionIndexResultCountType.singleResult;
    case 2:
      if (filters.from_genesis_block_height && filters.to_genesis_block_height)
        return DbInscriptionIndexResultCountType.blockHeightRange;
  }
  return DbInscriptionIndexResultCountType.custom;
}
