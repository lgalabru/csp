import { buildApiServer } from '../../src/api/init';
import { Brc20PgStore } from '../../src/pg/brc20/brc20-pg-store';
import { PgStore } from '../../src/pg/pg-store';
import {
  TestFastifyServer,
  clearDb,
  insertTestInscription,
  insertTestLocation,
  runMigrations,
} from '../helpers';

describe('ETag cache', () => {
  let db: PgStore;
  let brc20Db: Brc20PgStore;
  let fastify: TestFastifyServer;

  beforeEach(async () => {
    db = await PgStore.connect();
    await runMigrations(db.sql, './migrations/ordinals');
    brc20Db = await Brc20PgStore.connect();
    await runMigrations(db.sql, './migrations/ordinals-brc20');
    fastify = await buildApiServer({ db, brc20Db });
  });

  afterEach(async () => {
    await fastify.close();
    await clearDb(db.sql);
    await db.close();
    await clearDb(brc20Db.sql);
    await brc20Db.close();
  });

  test('inscription cache control', async () => {
    await insertTestInscription(db.sql, {
      inscription_id: '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dci0',
      ordinal_number: '257418248345364',
      number: '0',
      classic_number: '0',
      block_height: '775617',
      block_hash: '000000000000000000016bcbcc915c68bce367e18f09d0945dc6aacc0ee20121',
      tx_id: '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dc',
      tx_index: 0,
      address: 'bc1p3cyx5e2hgh53w7kpxcvm8s4kkega9gv5wfw7c4qxsvxl0u8x834qf0u2td',
      mime_type: 'image/png',
      content_type: 'image/png',
      content_length: 5,
      content: '0x48656C6C6F',
      fee: '2805',
      curse_type: null,
      recursive: false,
      input_index: 0,
      pointer: null,
      metadata: null,
      metaprotocol: null,
      parent: null,
      delegate: null,
      timestamp: 10000,
    });
    await insertTestLocation(db.sql, {
      ordinal_number: '257418248345364',
      block_height: '775617',
      tx_index: 0,
      tx_id: '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dc',
      block_hash: '000000000000000000016bcbcc915c68bce367e18f09d0945dc6aacc0ee20121',
      address: 'bc1p3cyx5e2hgh53w7kpxcvm8s4kkega9gv5wfw7c4qxsvxl0u8x834qf0u2td',
      output: '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dc:0',
      offset: '0',
      prev_output: null,
      prev_offset: null,
      value: '10000',
      transfer_type: 'transferred',
      timestamp: 10000,
    });
    const response = await fastify.inject({
      method: 'GET',
      url: '/ordinals/v1/inscriptions/38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dci0',
    });
    expect(response.statusCode).toBe(200);
    expect(response.headers.etag).not.toBeUndefined();
    const etag1 = response.headers.etag;

    // Check on numbered id too
    const nResponse = await fastify.inject({
      method: 'GET',
      url: '/ordinals/v1/inscriptions/0',
    });
    expect(nResponse.statusCode).toBe(200);
    expect(nResponse.headers.etag).not.toBeUndefined();
    const nEtag = nResponse.headers.etag;
    expect(nEtag).toBe(etag1);

    // Cached response
    const cached = await fastify.inject({
      method: 'GET',
      url: '/ordinals/v1/inscriptions/38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dci0',
      headers: { 'if-none-match': etag1 },
    });
    expect(cached.statusCode).toBe(304);
    const nCached = await fastify.inject({
      method: 'GET',
      url: '/ordinals/v1/inscriptions/0',
      headers: { 'if-none-match': etag1 },
    });
    expect(nCached.statusCode).toBe(304);

    // Perform transfer and check cache
    await insertTestLocation(db.sql, {
      ordinal_number: '257418248345364',
      block_height: '775618',
      tx_index: 0,
      tx_id: 'bdda0d240132bab2af7f797d1507beb1acab6ad43e2c0ef7f96291aea5cc3444',
      block_hash: '00000000000000000000a9db2c5d6c5445e7191927d6981ec580ed3c8112e342',
      address: 'bc1p3xqwzmddceqrd6x9yxplqzkl5vucta2gqm5szpkmpuvcvgs7g8psjf8htd',
      output: 'bdda0d240132bab2af7f797d1507beb1acab6ad43e2c0ef7f96291aea5cc3444:0',
      offset: '0',
      prev_output: 'da2da520f055e9fadaf1a78b3e01bc53596dcbb88e9c9f53bcb61b98310b1006:0',
      prev_offset: '0',
      value: '8000',
      transfer_type: 'transferred',
      timestamp: 10001,
    });
    const cached2 = await fastify.inject({
      method: 'GET',
      url: '/ordinals/v1/inscriptions/38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dci0',
      headers: { 'if-none-match': etag1 },
    });
    expect(cached2.statusCode).toBe(200);
    const nCached2 = await fastify.inject({
      method: 'GET',
      url: '/ordinals/v1/inscriptions/0',
      headers: { 'if-none-match': etag1 },
    });
    expect(nCached2.statusCode).toBe(200);
  });

  // test('inscriptions index cache control', async () => {
  //   const block1 = new TestChainhookPayloadBuilder()
  //     .streamingBlocks(true)
  //     .apply()
  //     .block({ height: 778575 })
  //     .transaction({ hash: '0x9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201' })
  //     .inscriptionRevealed({
  //       content_bytes: '0x48656C6C6F',
  //       content_type: 'text/plain',
  //       content_length: 5,
  //       inscription_number: {
  //         classic: 0,
  //         jubilee: 0,
  //       },
  //       inscription_fee: 705,
  //       inscription_id: '9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201i0',
  //       inscription_output_value: 10000,
  //       inscriber_address: 'bc1pscktlmn99gyzlvymvrezh6vwd0l4kg06tg5rvssw0czg8873gz5sdkteqj',
  //       ordinal_number: 257418248345364,
  //       ordinal_block_height: 650000,
  //       ordinal_offset: 0,
  //       satpoint_post_inscription:
  //         '9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201:0:0',
  //       inscription_input_index: 0,
  //       transfers_pre_inscription: 0,
  //       tx_index: 0,
  //       curse_type: null,
  //       inscription_pointer: null,
  //       delegate: null,
  //       metaprotocol: null,
  //       metadata: null,
  //       parent: null,
  //     })
  //     .build();
  //   await db.updateInscriptions(block1);
  //   const block2 = new TestChainhookPayloadBuilder()
  //     .streamingBlocks(true)
  //     .apply()
  //     .block({ height: 778576 })
  //     .transaction({ hash: '0x00000000000000000002a90330a99f67e3f01eb2ce070b45930581e82fb7a91d' })
  //     .inscriptionRevealed({
  //       content_bytes: '0x48656C6C6F',
  //       content_type: 'image/png',
  //       content_length: 5,
  //       inscription_number: {
  //         classic: 1,
  //         jubilee: 1,
  //       },
  //       inscription_fee: 2805,
  //       inscription_id: '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dci0',
  //       inscription_output_value: 10000,
  //       inscriber_address: 'bc1p3cyx5e2hgh53w7kpxcvm8s4kkega9gv5wfw7c4qxsvxl0u8x834qf0u2td',
  //       ordinal_number: 1676913207,
  //       ordinal_block_height: 650000,
  //       ordinal_offset: 0,
  //       satpoint_post_inscription:
  //         '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dc:0:0',
  //       inscription_input_index: 0,
  //       transfers_pre_inscription: 0,
  //       tx_index: 0,
  //       curse_type: null,
  //       inscription_pointer: null,
  //       delegate: null,
  //       metaprotocol: null,
  //       metadata: null,
  //       parent: null,
  //     })
  //     .build();
  //   await db.updateInscriptions(block2);

  //   // ETag response
  //   const response = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/inscriptions',
  //   });
  //   expect(response.statusCode).toBe(200);
  //   expect(response.headers.etag).not.toBeUndefined();
  //   const etag = response.headers.etag;

  //   // Cached
  //   const cached = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/inscriptions',
  //     headers: { 'if-none-match': etag },
  //   });
  //   expect(cached.statusCode).toBe(304);

  //   // New location
  //   const block3 = new TestChainhookPayloadBuilder()
  //     .streamingBlocks(true)
  //     .apply()
  //     .block({ height: 778577 })
  //     .transaction({ hash: 'ae9d273a10e899f0d2cad47ee2b0e77ab8a9addd9dd5bb5e4b03d6971c060d52' })
  //     .inscriptionTransferred({
  //       ordinal_number: 257418248345364,
  //       destination: {
  //         type: 'transferred',
  //         value: 'bc1p3cyx5e2hgh53w7kpxcvm8s4kkega9gv5wfw7c4qxsvxl0u8x834qf0u2td',
  //       },
  //       satpoint_pre_transfer:
  //         '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dc:0:0',
  //       satpoint_post_transfer:
  //         'ae9d273a10e899f0d2cad47ee2b0e77ab8a9addd9dd5bb5e4b03d6971c060d52:0:0',
  //       post_transfer_output_value: 100,
  //       tx_index: 0,
  //     })
  //     .build();
  //   await db.updateInscriptions(block3);
  //   const cached2 = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/inscriptions',
  //     headers: { 'if-none-match': etag },
  //   });
  //   expect(cached2.statusCode).toBe(200);
  // });

  // test('inscriptions stats per block cache control', async () => {
  //   const block1 = new TestChainhookPayloadBuilder()
  //     .streamingBlocks(true)
  //     .apply()
  //     .block({ height: 778575, hash: randomHash() })
  //     .transaction({ hash: '0x9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201' })
  //     .inscriptionRevealed({
  //       content_bytes: '0x48656C6C6F',
  //       content_type: 'text/plain',
  //       content_length: 5,
  //       inscription_number: {
  //         classic: 0,
  //         jubilee: 0,
  //       },
  //       inscription_fee: 705,
  //       inscription_id: '9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201i0',
  //       inscription_output_value: 10000,
  //       inscriber_address: 'bc1pscktlmn99gyzlvymvrezh6vwd0l4kg06tg5rvssw0czg8873gz5sdkteqj',
  //       ordinal_number: 257418248345364,
  //       ordinal_block_height: 650000,
  //       ordinal_offset: 0,
  //       satpoint_post_inscription:
  //         '9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201:0:0',
  //       inscription_input_index: 0,
  //       transfers_pre_inscription: 0,
  //       tx_index: 0,
  //       curse_type: null,
  //       inscription_pointer: null,
  //       delegate: null,
  //       metaprotocol: null,
  //       metadata: null,
  //       parent: null,
  //     })
  //     .build();
  //   await db.updateInscriptions(block1);

  //   // ETag response
  //   const response = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/stats/inscriptions',
  //   });
  //   expect(response.statusCode).toBe(200);
  //   expect(response.headers.etag).not.toBeUndefined();
  //   const etag = response.headers.etag;

  //   // Cached
  //   const cached = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/stats/inscriptions',
  //     headers: { 'if-none-match': etag },
  //   });
  //   expect(cached.statusCode).toBe(304);

  //   // New block
  //   const block2 = new TestChainhookPayloadBuilder()
  //     .streamingBlocks(true)
  //     .apply()
  //     .block({ height: 778576, hash: randomHash() })
  //     .transaction({ hash: '0x00000000000000000002a90330a99f67e3f01eb2ce070b45930581e82fb7a91d' })
  //     .inscriptionRevealed({
  //       content_bytes: '0x48656C6C6F',
  //       content_type: 'image/png',
  //       content_length: 5,
  //       inscription_number: {
  //         classic: 1,
  //         jubilee: 1,
  //       },
  //       inscription_fee: 2805,
  //       inscription_id: '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dci0',
  //       inscription_output_value: 10000,
  //       inscriber_address: 'bc1p3cyx5e2hgh53w7kpxcvm8s4kkega9gv5wfw7c4qxsvxl0u8x834qf0u2td',
  //       ordinal_number: 1676913207,
  //       ordinal_block_height: 650000,
  //       ordinal_offset: 0,
  //       satpoint_post_inscription:
  //         '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dc:0:0',
  //       inscription_input_index: 0,
  //       transfers_pre_inscription: 0,
  //       tx_index: 0,
  //       curse_type: null,
  //       inscription_pointer: null,
  //       delegate: null,
  //       metaprotocol: null,
  //       metadata: null,
  //       parent: null,
  //     })
  //     .build();
  //   await db.updateInscriptions(block2);

  //   // Cache busted
  //   const cacheBusted = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/stats/inscriptions',
  //     headers: { 'if-none-match': etag },
  //   });
  //   expect(cacheBusted.statusCode).toBe(200);
  // });

  // test('status etag changes with new block', async () => {
  //   const block1 = new TestChainhookPayloadBuilder()
  //     .streamingBlocks(true)
  //     .apply()
  //     .block({ height: 778575, hash: randomHash() })
  //     .transaction({ hash: '0x9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201' })
  //     .inscriptionRevealed({
  //       content_bytes: '0x48656C6C6F',
  //       content_type: 'text/plain',
  //       content_length: 5,
  //       inscription_number: {
  //         classic: 0,
  //         jubilee: 0,
  //       },
  //       inscription_fee: 705,
  //       inscription_id: '9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201i0',
  //       inscription_output_value: 10000,
  //       inscriber_address: 'bc1pscktlmn99gyzlvymvrezh6vwd0l4kg06tg5rvssw0czg8873gz5sdkteqj',
  //       ordinal_number: 257418248345364,
  //       ordinal_block_height: 650000,
  //       ordinal_offset: 0,
  //       satpoint_post_inscription:
  //         '9f4a9b73b0713c5da01c0a47f97c6c001af9028d6bdd9e264dfacbc4e6790201:0:0',
  //       inscription_input_index: 0,
  //       transfers_pre_inscription: 0,
  //       tx_index: 0,
  //       curse_type: null,
  //       inscription_pointer: null,
  //       delegate: null,
  //       metaprotocol: null,
  //       metadata: null,
  //       parent: null,
  //     })
  //     .build();
  //   await db.updateInscriptions(block1);

  //   // ETag response
  //   const response = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/',
  //   });
  //   expect(response.statusCode).toBe(200);
  //   expect(response.headers.etag).not.toBeUndefined();
  //   const etag = response.headers.etag;

  //   // Cached
  //   const cached = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/',
  //     headers: { 'if-none-match': etag },
  //   });
  //   expect(cached.statusCode).toBe(304);

  //   // New block
  //   const block2 = new TestChainhookPayloadBuilder()
  //     .streamingBlocks(true)
  //     .apply()
  //     .block({ height: 778576, hash: randomHash() })
  //     .transaction({ hash: '0x00000000000000000002a90330a99f67e3f01eb2ce070b45930581e82fb7a91d' })
  //     .inscriptionRevealed({
  //       content_bytes: '0x48656C6C6F',
  //       content_type: 'image/png',
  //       content_length: 5,
  //       inscription_number: {
  //         classic: 1,
  //         jubilee: 1,
  //       },
  //       inscription_fee: 2805,
  //       inscription_id: '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dci0',
  //       inscription_output_value: 10000,
  //       inscriber_address: 'bc1p3cyx5e2hgh53w7kpxcvm8s4kkega9gv5wfw7c4qxsvxl0u8x834qf0u2td',
  //       ordinal_number: 1676913207,
  //       ordinal_block_height: 650000,
  //       ordinal_offset: 0,
  //       satpoint_post_inscription:
  //         '38c46a8bf7ec90bc7f6b797e7dc84baa97f4e5fd4286b92fe1b50176d03b18dc:0:0',
  //       inscription_input_index: 0,
  //       transfers_pre_inscription: 0,
  //       tx_index: 0,
  //       curse_type: null,
  //       inscription_pointer: null,
  //       delegate: null,
  //       metaprotocol: null,
  //       metadata: null,
  //       parent: null,
  //     })
  //     .build();
  //   await db.updateInscriptions(block2);

  //   // Cache busted
  //   const cacheBusted = await fastify.inject({
  //     method: 'GET',
  //     url: '/ordinals/v1/',
  //     headers: { 'if-none-match': etag },
  //   });
  //   expect(cacheBusted.statusCode).toBe(200);
  // });
});
