/** Type of row count required for an inscription index endpoint call */
export enum DbInscriptionIndexResultCountType {
  /** All inscriptions */
  all,
  /** Filtered by cursed or blessed */
  cursed,
  /** Filtered by mime type */
  mimeType,
  /** Filtered by sat rarity */
  satRarity,
  /** Filtered by address */
  address,
  genesisAddress,
  /** Filtered by block height */
  blockHeight,
  fromblockHeight,
  toblockHeight,
  blockHeightRange,
  /** Filtered by block hash */
  blockHash,
  /** Filtered by recursive */
  recursive,
  /** Filtered by some other param that yields a single result (easy to count) */
  singleResult,
  /** Filtered by custom arguments (tough to count) */
  custom,
}
