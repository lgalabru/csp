import BigNumber from 'bignumber.js';

const HALVING_BLOCKS = 210_000;
const DIFFICULTY_ADJUST_BLOCKS = 2016;
const INITIAL_SUBSIDY = 50;
const SATS_PER_BTC = 100_000_000;
export const SAT_SUPPLY = 2099999997690000;

export enum SatoshiRarity {
  common = 'common',
  uncommon = 'uncommon',
  rare = 'rare',
  epic = 'epic',
  legendary = 'legendary',
  mythic = 'mythic',
}

/**
 * Ordinal Satoshi calculator. Mostly translated from the original Rust implementation at
 * https://github.com/casey/ord/blob/master/src/sat.rs
 */
export class OrdinalSatoshi {
  public blockHeight: number;
  public cycle: number;
  public ordinal: number;
  public epoch: number;
  public period: number;
  public offset: number;
  private hour: number;
  private minute: number;
  private second: number;
  private third: number;

  constructor(ordinal: number) {
    if (ordinal > SAT_SUPPLY || ordinal < 0) throw Error('Invalid satoshi ordinal number');
    let satAccum = 0;
    let subsidy = INITIAL_SUBSIDY;
    let epoch = 0;
    while (true) {
      const satHalvingMax = HALVING_BLOCKS * subsidy * SATS_PER_BTC;
      if (satAccum + satHalvingMax > ordinal) {
        break;
      }
      satAccum += satHalvingMax;
      subsidy /= 2;
      epoch++;
    }
    const halvingOffset = ordinal - satAccum;
    const epochBoundary = epoch * HALVING_BLOCKS;
    const exactHeight = halvingOffset / (subsidy * SATS_PER_BTC) + epochBoundary;

    this.ordinal = ordinal;
    this.blockHeight = Math.floor(exactHeight);
    this.cycle = this.hour = Math.floor(epoch / 6);
    this.minute = epochBoundary === 0 ? this.blockHeight : this.blockHeight % epochBoundary;
    this.second = this.blockHeight % DIFFICULTY_ADJUST_BLOCKS;
    this.third = this.offset = Math.round(
      (exactHeight - this.blockHeight) * subsidy * Math.pow(10, 8)
    );
    this.epoch = epoch;
    this.period = Math.floor(this.blockHeight / DIFFICULTY_ADJUST_BLOCKS);
  }

  public get degree(): string {
    return `${this.hour}°${this.minute}′${this.second}″${this.third}‴`;
  }

  public get decimal(): string {
    return `${this.blockHeight}.${this.third}`;
  }

  public get name(): string {
    let x = SAT_SUPPLY - this.ordinal;
    const name: string[] = [];
    const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('');
    while (x > 0) {
      const index = Math.floor((x - 1) % 26);
      name.push(alphabet[index]);
      x = (x - 1) / 26;
    }
    return name.reverse().join('');
  }

  public get percentile(): string {
    const percentile = new BigNumber((this.ordinal / (SAT_SUPPLY - 1)) * 100.0);
    return `${percentile.toFixed()}%`;
  }

  public get rarity(): SatoshiRarity {
    if (this.hour === 0 && this.minute === 0 && this.second === 0 && this.third === 0) {
      return SatoshiRarity.mythic;
    }
    if (this.minute === 0 && this.second === 0 && this.third === 0) {
      return SatoshiRarity.legendary;
    }
    if (this.minute === 0 && this.third === 0) {
      return SatoshiRarity.epic;
    }
    if (this.second === 0 && this.third === 0) {
      return SatoshiRarity.rare;
    }
    if (this.third === 0) {
      return SatoshiRarity.uncommon;
    }
    return SatoshiRarity.common;
  }
}
