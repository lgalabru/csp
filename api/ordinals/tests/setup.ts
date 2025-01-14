// ts-unused-exports:disable-next-line
export default (): void => {
  process.env.ORDINALS_PGDATABASE = 'postgres';
  process.env.ORDINALS_SCHEMA = 'ordinals';
  process.env.BRC20_PGDATABASE = 'postgres';
  process.env.BRC20_SCHEMA = 'brc20';
};
