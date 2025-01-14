import { PgBytea, logger } from '@hirosystems/api-toolkit';
import { hexToBuffer } from '../api/util/helpers';
import { BitcoinInscriptionRevealed } from '@hirosystems/chainhook-client';
import { DbLocationTransferType } from './types';

/**
 * Returns a list of referenced inscription ids from inscription content.
 * @param content - Inscription content
 * @returns List of IDs
 */
export function getInscriptionRecursion(content: PgBytea): string[] {
  const buf = typeof content === 'string' ? hexToBuffer(content) : content;
  const strContent = buf.toString('utf-8');
  const result: string[] = [];
  for (const match of strContent.matchAll(/\/content\/([a-fA-F0-9]{64}i\d+)/g)) {
    result.push(match[1]);
  }
  return result;
}

export function objRemoveUndefinedValues(obj: object) {
  Object.keys(obj).forEach(key => (obj as any)[key] === undefined && delete (obj as any)[key]);
}

/**
 * Replace null bytes on a string with an empty string
 * @param input - String
 * @returns Sanitized string
 */
export function removeNullBytes(input: string): string {
  return input.replace(/\x00/g, '');
}

export function getTransferType(reveal: BitcoinInscriptionRevealed) {
  let transfer_type = DbLocationTransferType.transferred;
  if (reveal.inscriber_address == null || reveal.inscriber_address == '') {
    if (reveal.inscription_output_value == 0) {
      if (reveal.inscription_pointer !== 0 && reveal.inscription_pointer !== null) {
        logger.warn(
          `Detected inscription reveal with no address and no output value but a valid pointer ${reveal.inscription_id}`
        );
      }
      transfer_type = DbLocationTransferType.spentInFees;
    } else {
      transfer_type = DbLocationTransferType.burnt;
    }
  }
  return transfer_type;
}
