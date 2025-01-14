## [4.0.4](https://github.com/hirosystems/ordinals-api/compare/v4.0.3...v4.0.4) (2024-06-17)


### Bug Fixes

* select correct block hash upon migration ([#366](https://github.com/hirosystems/ordinals-api/issues/366)) ([aea1a19](https://github.com/hirosystems/ordinals-api/commit/aea1a19463e82e7fa1f2574df0730a2626eeacc4))

## [4.0.3](https://github.com/hirosystems/ordinals-api/compare/v4.0.2...v4.0.3) (2024-06-17)


### Bug Fixes

* add output to current locations and block_hash to inscriptions ([#365](https://github.com/hirosystems/ordinals-api/issues/365)) ([3d75deb](https://github.com/hirosystems/ordinals-api/commit/3d75deb664906eb1c4392ab18d5c77332047a875))

## [4.0.2](https://github.com/hirosystems/ordinals-api/compare/v4.0.1...v4.0.2) (2024-06-16)


### Bug Fixes

* optimize brc-20 activity query ([#362](https://github.com/hirosystems/ordinals-api/issues/362)) ([e8a4fea](https://github.com/hirosystems/ordinals-api/commit/e8a4fea29a49cf55bd2291c2bf8349a756130028))

## [4.0.1](https://github.com/hirosystems/ordinals-api/compare/v4.0.0...v4.0.1) (2024-06-16)


### Bug Fixes

* optimize base inscriptions query ([#364](https://github.com/hirosystems/ordinals-api/issues/364)) ([cbcc830](https://github.com/hirosystems/ordinals-api/commit/cbcc830a675e5c4dcd29650f1d5e8783ce096989))

## [4.0.0](https://github.com/hirosystems/ordinals-api/compare/v3.1.0...v4.0.0) (2024-06-11)


### ⚠ BREAKING CHANGES

* support reinscription transfers (#348)
* ingest BRC20 data from ordhook (#347)

### Features

* ingest BRC20 data from ordhook ([#347](https://github.com/hirosystems/ordinals-api/issues/347)) ([56a8851](https://github.com/hirosystems/ordinals-api/commit/56a88518b1ffe549524941e4d94d6347d11c98f3))
* return `parent` and `metadata` in inscription responses ([#350](https://github.com/hirosystems/ordinals-api/issues/350)) ([939286e](https://github.com/hirosystems/ordinals-api/commit/939286e3a036063835206c533ce7f9a66cee0ba7))
* support reinscription transfers ([#348](https://github.com/hirosystems/ordinals-api/issues/348)) ([5422156](https://github.com/hirosystems/ordinals-api/commit/5422156e9919f0c5870c9571ea9f591852c98b69))


### Bug Fixes

* detect block gaps when streaming from ordhook ([#349](https://github.com/hirosystems/ordinals-api/issues/349)) ([3c1480f](https://github.com/hirosystems/ordinals-api/commit/3c1480f5bfb8bec4993fffd50245c345d71cdf08))
* ordhook ingestion ([#356](https://github.com/hirosystems/ordinals-api/issues/356)) ([dfc003e](https://github.com/hirosystems/ordinals-api/commit/dfc003ee65198a35bac1e0fc723068bde90c63c4))

## [4.0.0-beta.2](https://github.com/hirosystems/ordinals-api/compare/v4.0.0-beta.1...v4.0.0-beta.2) (2024-06-11)


### Bug Fixes

* ordhook ingestion ([#356](https://github.com/hirosystems/ordinals-api/issues/356)) ([dfc003e](https://github.com/hirosystems/ordinals-api/commit/dfc003ee65198a35bac1e0fc723068bde90c63c4))

## [4.0.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v3.1.0...v4.0.0-beta.1) (2024-04-26)


### ⚠ BREAKING CHANGES

* support reinscription transfers (#348)
* ingest BRC20 data from ordhook (#347)

### Features

* ingest BRC20 data from ordhook ([#347](https://github.com/hirosystems/ordinals-api/issues/347)) ([56a8851](https://github.com/hirosystems/ordinals-api/commit/56a88518b1ffe549524941e4d94d6347d11c98f3))
* return `parent` and `metadata` in inscription responses ([#350](https://github.com/hirosystems/ordinals-api/issues/350)) ([939286e](https://github.com/hirosystems/ordinals-api/commit/939286e3a036063835206c533ce7f9a66cee0ba7))
* support reinscription transfers ([#348](https://github.com/hirosystems/ordinals-api/issues/348)) ([5422156](https://github.com/hirosystems/ordinals-api/commit/5422156e9919f0c5870c9571ea9f591852c98b69))


### Bug Fixes

* detect block gaps when streaming from ordhook ([#349](https://github.com/hirosystems/ordinals-api/issues/349)) ([3c1480f](https://github.com/hirosystems/ordinals-api/commit/3c1480f5bfb8bec4993fffd50245c345d71cdf08))

## [3.1.0](https://github.com/hirosystems/ordinals-api/compare/v3.0.1...v3.1.0) (2024-04-23)


### Features

* support self minted 5-byte wide BRC-20 tokens ([#338](https://github.com/hirosystems/ordinals-api/issues/338)) ([60f46d3](https://github.com/hirosystems/ordinals-api/commit/60f46d3533e837843e8aa6094b4673a3bc84c124))


### Bug Fixes

* calculate transfer type correctly upon reveal ([#326](https://github.com/hirosystems/ordinals-api/issues/326)) ([6c4c54b](https://github.com/hirosystems/ordinals-api/commit/6c4c54b45a74744c4e61a2437632f390080a9624))
* check only the first blessed inscription in next block on gap detection ([#325](https://github.com/hirosystems/ordinals-api/issues/325)) ([9cad6c1](https://github.com/hirosystems/ordinals-api/commit/9cad6c16d34fdd11c1d9f473b2f3802a8da464d8))
* filter correct content types for brc20 ([#323](https://github.com/hirosystems/ordinals-api/issues/323)) ([4d52b48](https://github.com/hirosystems/ordinals-api/commit/4d52b4820e5f1a36264977dc4a6c4ca324864108))
* ignore spent as fee on gap check ([#328](https://github.com/hirosystems/ordinals-api/issues/328)) ([a1277cf](https://github.com/hirosystems/ordinals-api/commit/a1277cf39eb61e548f55bd8e524054db6a11c843))
* remove gap detection ([#330](https://github.com/hirosystems/ordinals-api/issues/330)) ([040ee04](https://github.com/hirosystems/ordinals-api/commit/040ee04b0906106fdece3f00d34fb4817d7f318e))
* use brc20_total_balances table when inserting new transfer ([#321](https://github.com/hirosystems/ordinals-api/issues/321)) ([925fb0e](https://github.com/hirosystems/ordinals-api/commit/925fb0e05a982eeec802bee6f53a957bc5ea3acf))

## [3.1.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v3.0.2-beta.6...v3.1.0-beta.1) (2024-03-27)


### Features

* support self minted 5-byte wide BRC-20 tokens ([#338](https://github.com/hirosystems/ordinals-api/issues/338)) ([60f46d3](https://github.com/hirosystems/ordinals-api/commit/60f46d3533e837843e8aa6094b4673a3bc84c124))

## [3.0.2-beta.6](https://github.com/hirosystems/ordinals-api/compare/v3.0.2-beta.5...v3.0.2-beta.6) (2024-03-09)


### Bug Fixes

* remove gap detection ([#330](https://github.com/hirosystems/ordinals-api/issues/330)) ([040ee04](https://github.com/hirosystems/ordinals-api/commit/040ee04b0906106fdece3f00d34fb4817d7f318e))

## [3.0.2-beta.5](https://github.com/hirosystems/ordinals-api/compare/v3.0.2-beta.4...v3.0.2-beta.5) (2024-03-09)


### Bug Fixes

* ignore spent as fee on gap check ([#328](https://github.com/hirosystems/ordinals-api/issues/328)) ([a1277cf](https://github.com/hirosystems/ordinals-api/commit/a1277cf39eb61e548f55bd8e524054db6a11c843))

## [3.0.2-beta.4](https://github.com/hirosystems/ordinals-api/compare/v3.0.2-beta.3...v3.0.2-beta.4) (2024-03-08)


### Bug Fixes

* calculate transfer type correctly upon reveal ([#326](https://github.com/hirosystems/ordinals-api/issues/326)) ([6c4c54b](https://github.com/hirosystems/ordinals-api/commit/6c4c54b45a74744c4e61a2437632f390080a9624))

## [3.0.2-beta.3](https://github.com/hirosystems/ordinals-api/compare/v3.0.2-beta.2...v3.0.2-beta.3) (2024-03-07)


### Bug Fixes

* check only the first blessed inscription in next block on gap detection ([#325](https://github.com/hirosystems/ordinals-api/issues/325)) ([9cad6c1](https://github.com/hirosystems/ordinals-api/commit/9cad6c16d34fdd11c1d9f473b2f3802a8da464d8))

## [3.0.2-beta.2](https://github.com/hirosystems/ordinals-api/compare/v3.0.2-beta.1...v3.0.2-beta.2) (2024-03-07)


### Bug Fixes

* filter correct content types for brc20 ([#323](https://github.com/hirosystems/ordinals-api/issues/323)) ([4d52b48](https://github.com/hirosystems/ordinals-api/commit/4d52b4820e5f1a36264977dc4a6c4ca324864108))

## [3.0.2-beta.1](https://github.com/hirosystems/ordinals-api/compare/v3.0.1...v3.0.2-beta.1) (2024-03-07)


### Bug Fixes

* use brc20_total_balances table when inserting new transfer ([#321](https://github.com/hirosystems/ordinals-api/issues/321)) ([925fb0e](https://github.com/hirosystems/ordinals-api/commit/925fb0e05a982eeec802bee6f53a957bc5ea3acf))

## [3.0.1](https://github.com/hirosystems/ordinals-api/compare/v3.0.0...v3.0.1) (2024-03-04)


### Bug Fixes

* transfers for inscriptions created immediately before ([#320](https://github.com/hirosystems/ordinals-api/issues/320)) ([66ba744](https://github.com/hirosystems/ordinals-api/commit/66ba744f96a101c6d0e23122ae7aa41fb1933ba4))

## [3.0.0](https://github.com/hirosystems/ordinals-api/compare/v2.2.0...v3.0.0) (2024-02-28)


### ⚠ BREAKING CHANGES

* rename chainhook env vars to ordhook (#314)

### Features

* add `metadata` and `parent` columns to inscriptions table ([#305](https://github.com/hirosystems/ordinals-api/issues/305)) ([d71e93a](https://github.com/hirosystems/ordinals-api/commit/d71e93a54ec8018c15bbfc2f88d18bab6606949e))
* add ordhook debug server ([#306](https://github.com/hirosystems/ordinals-api/issues/306)) ([88ad130](https://github.com/hirosystems/ordinals-api/commit/88ad1302924b8da27b8d98caf3d3351149a45f91))
* shutdown gracefully after finishing replaying blocks ([#315](https://github.com/hirosystems/ordinals-api/issues/315)) ([72fd3fd](https://github.com/hirosystems/ordinals-api/commit/72fd3fda24919f14218cbbf02a552dfcf4d8ea52))


### Bug Fixes

* batch inscription location updates ([47525c9](https://github.com/hirosystems/ordinals-api/commit/47525c93a0e71bd9a7df2ea1596d7c90f59ccaac))
* batch location pointer inserts ([#308](https://github.com/hirosystems/ordinals-api/issues/308)) ([33f8cb2](https://github.com/hirosystems/ordinals-api/commit/33f8cb2576115695d8e54fcc989086afef42ddc7))
* only update timestamp when streaming blocks ([#309](https://github.com/hirosystems/ordinals-api/issues/309)) ([2c9ff17](https://github.com/hirosystems/ordinals-api/commit/2c9ff17ebe8805f3de59046b25387b871debee49))
* remove unused pg indexes ([#311](https://github.com/hirosystems/ordinals-api/issues/311)) ([94d98d4](https://github.com/hirosystems/ordinals-api/commit/94d98d4e945aef28be7ec19fdb07d121c0ba2ea2))
* rename chainhook env vars to ordhook ([#314](https://github.com/hirosystems/ordinals-api/issues/314)) ([ae4ec01](https://github.com/hirosystems/ordinals-api/commit/ae4ec01326779af00b89907751a6946effe60536))
* standardize insert batch size ([b9b2448](https://github.com/hirosystems/ordinals-api/commit/b9b2448a1fa1bef16f0a1eed80158ca9b8ca9133))

## [3.0.0-beta.2](https://github.com/hirosystems/ordinals-api/compare/v3.0.0-beta.1...v3.0.0-beta.2) (2024-02-21)


### Features

* shutdown gracefully after finishing replaying blocks ([#315](https://github.com/hirosystems/ordinals-api/issues/315)) ([72fd3fd](https://github.com/hirosystems/ordinals-api/commit/72fd3fda24919f14218cbbf02a552dfcf4d8ea52))

## [3.0.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v2.3.0-beta.6...v3.0.0-beta.1) (2024-02-21)


### ⚠ BREAKING CHANGES

* rename chainhook env vars to ordhook (#314)

### Bug Fixes

* rename chainhook env vars to ordhook ([#314](https://github.com/hirosystems/ordinals-api/issues/314)) ([ae4ec01](https://github.com/hirosystems/ordinals-api/commit/ae4ec01326779af00b89907751a6946effe60536))

## [2.3.0-beta.6](https://github.com/hirosystems/ordinals-api/compare/v2.3.0-beta.5...v2.3.0-beta.6) (2024-02-20)


### Bug Fixes

* remove unused pg indexes ([#311](https://github.com/hirosystems/ordinals-api/issues/311)) ([94d98d4](https://github.com/hirosystems/ordinals-api/commit/94d98d4e945aef28be7ec19fdb07d121c0ba2ea2))

## [2.3.0-beta.5](https://github.com/hirosystems/ordinals-api/compare/v2.3.0-beta.4...v2.3.0-beta.5) (2024-02-19)


### Bug Fixes

* only update timestamp when streaming blocks ([#309](https://github.com/hirosystems/ordinals-api/issues/309)) ([2c9ff17](https://github.com/hirosystems/ordinals-api/commit/2c9ff17ebe8805f3de59046b25387b871debee49))

## [2.3.0-beta.4](https://github.com/hirosystems/ordinals-api/compare/v2.3.0-beta.3...v2.3.0-beta.4) (2024-02-17)


### Bug Fixes

* standardize insert batch size ([b9b2448](https://github.com/hirosystems/ordinals-api/commit/b9b2448a1fa1bef16f0a1eed80158ca9b8ca9133))

## [2.3.0-beta.3](https://github.com/hirosystems/ordinals-api/compare/v2.3.0-beta.2...v2.3.0-beta.3) (2024-02-17)


### Bug Fixes

* batch inscription location updates ([47525c9](https://github.com/hirosystems/ordinals-api/commit/47525c93a0e71bd9a7df2ea1596d7c90f59ccaac))

## [2.3.0-beta.2](https://github.com/hirosystems/ordinals-api/compare/v2.3.0-beta.1...v2.3.0-beta.2) (2024-02-17)


### Bug Fixes

* batch location pointer inserts ([#308](https://github.com/hirosystems/ordinals-api/issues/308)) ([33f8cb2](https://github.com/hirosystems/ordinals-api/commit/33f8cb2576115695d8e54fcc989086afef42ddc7))

## [2.3.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v2.2.0...v2.3.0-beta.1) (2024-02-12)


### Features

* add `metadata` and `parent` columns to inscriptions table ([#305](https://github.com/hirosystems/ordinals-api/issues/305)) ([d71e93a](https://github.com/hirosystems/ordinals-api/commit/d71e93a54ec8018c15bbfc2f88d18bab6606949e))
* add ordhook debug server ([#306](https://github.com/hirosystems/ordinals-api/issues/306)) ([88ad130](https://github.com/hirosystems/ordinals-api/commit/88ad1302924b8da27b8d98caf3d3351149a45f91))

## [2.2.0](https://github.com/hirosystems/ordinals-api/compare/v2.1.1...v2.2.0) (2024-02-02)


### Features

* support transfers by ordinal number instead of inscription id ([#296](https://github.com/hirosystems/ordinals-api/issues/296)) ([b0c0ffd](https://github.com/hirosystems/ordinals-api/commit/b0c0ffd198a9a98a88b5123927a8fdc5b68ef22d))


### Bug Fixes

* include token in address index on brc20_total_balances ([#299](https://github.com/hirosystems/ordinals-api/issues/299)) ([d8271eb](https://github.com/hirosystems/ordinals-api/commit/d8271eb9fc23d6262fab68c59a0710420ebcb8bf))
* offload ticker lookup from BRC-20 activity query ([#293](https://github.com/hirosystems/ordinals-api/issues/293)) ([e70c222](https://github.com/hirosystems/ordinals-api/commit/e70c2229b03a563f0218c5fcfabb2875451bb77c))

## [2.2.0-beta.2](https://github.com/hirosystems/ordinals-api/compare/v2.2.0-beta.1...v2.2.0-beta.2) (2024-02-02)


### Bug Fixes

* include token in address index on brc20_total_balances ([#299](https://github.com/hirosystems/ordinals-api/issues/299)) ([d8271eb](https://github.com/hirosystems/ordinals-api/commit/d8271eb9fc23d6262fab68c59a0710420ebcb8bf))

## [2.2.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v2.1.2-beta.1...v2.2.0-beta.1) (2024-02-02)


### Features

* support transfers by ordinal number instead of inscription id ([#296](https://github.com/hirosystems/ordinals-api/issues/296)) ([b0c0ffd](https://github.com/hirosystems/ordinals-api/commit/b0c0ffd198a9a98a88b5123927a8fdc5b68ef22d))

## [2.1.2-beta.1](https://github.com/hirosystems/ordinals-api/compare/v2.1.1...v2.1.2-beta.1) (2024-01-16)


### Bug Fixes

* offload ticker lookup from BRC-20 activity query ([#293](https://github.com/hirosystems/ordinals-api/issues/293)) ([e70c222](https://github.com/hirosystems/ordinals-api/commit/e70c2229b03a563f0218c5fcfabb2875451bb77c))

## [2.1.1](https://github.com/hirosystems/ordinals-api/compare/v2.1.0...v2.1.1) (2024-01-09)


### Bug Fixes

* optimize BRC-20 activity count by ticker ([#289](https://github.com/hirosystems/ordinals-api/issues/289)) ([0a7648e](https://github.com/hirosystems/ordinals-api/commit/0a7648ee899e5167a72422c1c0870accad4c2423))
* remove problematic migration ([a82d078](https://github.com/hirosystems/ordinals-api/commit/a82d078fa9c30dddf15ec79746599c6dd527b7f1))

## [2.1.1-beta.2](https://github.com/hirosystems/ordinals-api/compare/v2.1.1-beta.1...v2.1.1-beta.2) (2024-01-09)


### Bug Fixes

* optimize BRC-20 activity count by ticker ([#289](https://github.com/hirosystems/ordinals-api/issues/289)) ([0a7648e](https://github.com/hirosystems/ordinals-api/commit/0a7648ee899e5167a72422c1c0870accad4c2423))

## [2.1.1-beta.1](https://github.com/hirosystems/ordinals-api/compare/v2.1.0...v2.1.1-beta.1) (2024-01-05)


### Bug Fixes

* remove problematic migration ([a82d078](https://github.com/hirosystems/ordinals-api/commit/a82d078fa9c30dddf15ec79746599c6dd527b7f1))

## [2.1.0](https://github.com/hirosystems/ordinals-api/compare/v2.0.1...v2.1.0) (2024-01-04)


### Features

* support ordinals jubilee, ignore classic cursed inscriptions for BRC-20 ([#286](https://github.com/hirosystems/ordinals-api/issues/286)) ([fde705c](https://github.com/hirosystems/ordinals-api/commit/fde705c9a3def85bac0b4f07a3cfefe24fbebc09))

## [2.1.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v2.0.1...v2.1.0-beta.1) (2024-01-04)


### Features

* support ordinals jubilee, ignore classic cursed inscriptions for BRC-20 ([#286](https://github.com/hirosystems/ordinals-api/issues/286)) ([fde705c](https://github.com/hirosystems/ordinals-api/commit/fde705c9a3def85bac0b4f07a3cfefe24fbebc09))

## [2.0.1](https://github.com/hirosystems/ordinals-api/compare/v2.0.0...v2.0.1) (2023-12-05)


### Bug Fixes

* add ENV to toggle inscription gap detection ([56ab283](https://github.com/hirosystems/ordinals-api/commit/56ab283cdfaf95efe058e5f057fb5559b9dfede0))
* optimize block rollback logic for re-orgs ([#279](https://github.com/hirosystems/ordinals-api/issues/279)) ([293c323](https://github.com/hirosystems/ordinals-api/commit/293c32391d5e71a3d6570526be3565a4e75572b7))
* select only the first inscription id match ([106368e](https://github.com/hirosystems/ordinals-api/commit/106368e9fa415db0657988de28a541906f74d28a))
* upgrade chainhook client to v1.4.2 ([#277](https://github.com/hirosystems/ordinals-api/issues/277)) ([67ba3d4](https://github.com/hirosystems/ordinals-api/commit/67ba3d4f48e6ce8e955c08fd75cbf009786c0160))

## [2.0.1-beta.4](https://github.com/hirosystems/ordinals-api/compare/v2.0.1-beta.3...v2.0.1-beta.4) (2023-12-05)


### Bug Fixes

* optimize block rollback logic for re-orgs ([#279](https://github.com/hirosystems/ordinals-api/issues/279)) ([293c323](https://github.com/hirosystems/ordinals-api/commit/293c32391d5e71a3d6570526be3565a4e75572b7))

## [2.0.1-beta.3](https://github.com/hirosystems/ordinals-api/compare/v2.0.1-beta.2...v2.0.1-beta.3) (2023-11-29)


### Bug Fixes

* upgrade chainhook client to v1.4.2 ([#277](https://github.com/hirosystems/ordinals-api/issues/277)) ([67ba3d4](https://github.com/hirosystems/ordinals-api/commit/67ba3d4f48e6ce8e955c08fd75cbf009786c0160))

## [2.0.1-beta.2](https://github.com/hirosystems/ordinals-api/compare/v2.0.1-beta.1...v2.0.1-beta.2) (2023-11-23)


### Bug Fixes

* select only the first inscription id match ([106368e](https://github.com/hirosystems/ordinals-api/commit/106368e9fa415db0657988de28a541906f74d28a))

## [2.0.1-beta.1](https://github.com/hirosystems/ordinals-api/compare/v2.0.0...v2.0.1-beta.1) (2023-11-23)


### Bug Fixes

* add ENV to toggle inscription gap detection ([56ab283](https://github.com/hirosystems/ordinals-api/commit/56ab283cdfaf95efe058e5f057fb5559b9dfede0))

## [2.0.0](https://github.com/hirosystems/ordinals-api/compare/v1.2.6...v2.0.0) (2023-11-21)


### ⚠ BREAKING CHANGES

* handle transfer types and consider them for BRC-20 indexing (#258)

### Features

* handle transfer types and consider them for BRC-20 indexing ([#258](https://github.com/hirosystems/ordinals-api/issues/258)) ([7b83761](https://github.com/hirosystems/ordinals-api/commit/7b83761818ab4a178796a1867d143ff75ef338b0))


### Bug Fixes

* address location counts ([#273](https://github.com/hirosystems/ordinals-api/issues/273)) ([86efc3a](https://github.com/hirosystems/ordinals-api/commit/86efc3acf8d743b22edc11e137458a51c892c1c7))
* also remove null bytes in blessed inscriptions ([7e8d412](https://github.com/hirosystems/ordinals-api/commit/7e8d4127bc4b27917c59349abe9f5fbf98da98ab))
* batch size to 2000 ([9cf9230](https://github.com/hirosystems/ordinals-api/commit/9cf9230fe193a2075a0cd2e45a5fa316ecc225c5))
* bump docker image ([08688da](https://github.com/hirosystems/ordinals-api/commit/08688da84e7695156aa0e822c6f264e9085c5f1a))
* do not reject unbounded inscriptions that come in disorder ([#264](https://github.com/hirosystems/ordinals-api/issues/264)) ([56e2235](https://github.com/hirosystems/ordinals-api/commit/56e2235a3823a94493bbe978c2f8e5be48c6cf46))
* guarantee gap detection is comparing numbers ([0f3f51a](https://github.com/hirosystems/ordinals-api/commit/0f3f51a0186dd2f677e21669d2359cba02122acf))
* inscription count generation ([13e1f16](https://github.com/hirosystems/ordinals-api/commit/13e1f16f6980b6743abe8d95644703329f07fe3f))
* keep block transfer index ([#260](https://github.com/hirosystems/ordinals-api/issues/260)) ([0e33a64](https://github.com/hirosystems/ordinals-api/commit/0e33a644773990531fa3f68e93ca97f83105a621))
* only advance blessed number after gap detection ([c8f3c81](https://github.com/hirosystems/ordinals-api/commit/c8f3c81a657cd28283bdd89e0a17af06cad93d63))
* only scan BRC-20 after its genesis height ([#263](https://github.com/hirosystems/ordinals-api/issues/263)) ([6381760](https://github.com/hirosystems/ordinals-api/commit/6381760f3860a96c3abfd485b612cd212927dca4))
* only validate gaps for blessed numbers ([#262](https://github.com/hirosystems/ordinals-api/issues/262)) ([29aaeda](https://github.com/hirosystems/ordinals-api/commit/29aaeda39527c88b63b7c82739d8ed858c48a74b))
* reduce batching to 4500 ([8fd770b](https://github.com/hirosystems/ordinals-api/commit/8fd770bc4e83be6ddaa4fdead1d1be55c42f870b))
* reduce brc-20 batch to 5k ([2037647](https://github.com/hirosystems/ordinals-api/commit/20376470455a0887d81f3128047d330e7d97e438))
* reduce brc20 processing batch size to half ([7079dc0](https://github.com/hirosystems/ordinals-api/commit/7079dc079bfec5fbde7018dacbf20c1701d7ccb9))
* reduce chunk size to 3000 ([9556148](https://github.com/hirosystems/ordinals-api/commit/95561481a140685d27e68c0c4e03d879882354f4))
* reduce to 4000 chunk size ([3373422](https://github.com/hirosystems/ordinals-api/commit/337342251987d546600fe331154b3e8cd38dcd18))
* reject blocks that would create blessed inscription gaps ([#259](https://github.com/hirosystems/ordinals-api/issues/259)) ([18cd028](https://github.com/hirosystems/ordinals-api/commit/18cd028b636184a96597fa0b0978ba9e4d23f55c))
* remove null bytes before inserting inscriptions ([#274](https://github.com/hirosystems/ordinals-api/issues/274)) ([cec3de0](https://github.com/hirosystems/ordinals-api/commit/cec3de061a5b475161b2d11369fc13b63ad8bc27))
* try batch iterator generator ([e2ed039](https://github.com/hirosystems/ordinals-api/commit/e2ed039120958fde07297c6760022770dab710a8))
* try returning brc20 batch to normal, use batchIterate everywhere ([1d3a24b](https://github.com/hirosystems/ordinals-api/commit/1d3a24b8bb1d852eb1ae198d84a5940eec997ded))

## [2.0.0-beta.20](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.19...v2.0.0-beta.20) (2023-11-20)


### Bug Fixes

* also remove null bytes in blessed inscriptions ([7e8d412](https://github.com/hirosystems/ordinals-api/commit/7e8d4127bc4b27917c59349abe9f5fbf98da98ab))

## [2.0.0-beta.19](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.18...v2.0.0-beta.19) (2023-11-20)


### Bug Fixes

* remove null bytes before inserting inscriptions ([#274](https://github.com/hirosystems/ordinals-api/issues/274)) ([cec3de0](https://github.com/hirosystems/ordinals-api/commit/cec3de061a5b475161b2d11369fc13b63ad8bc27))

## [2.0.0-beta.18](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.17...v2.0.0-beta.18) (2023-11-17)


### Bug Fixes

* address location counts ([#273](https://github.com/hirosystems/ordinals-api/issues/273)) ([86efc3a](https://github.com/hirosystems/ordinals-api/commit/86efc3acf8d743b22edc11e137458a51c892c1c7))

## [2.0.0-beta.17](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.16...v2.0.0-beta.17) (2023-11-17)


### Bug Fixes

* inscription count generation ([13e1f16](https://github.com/hirosystems/ordinals-api/commit/13e1f16f6980b6743abe8d95644703329f07fe3f))

## [2.0.0-beta.16](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.15...v2.0.0-beta.16) (2023-11-12)


### Bug Fixes

* try returning brc20 batch to normal, use batchIterate everywhere ([1d3a24b](https://github.com/hirosystems/ordinals-api/commit/1d3a24b8bb1d852eb1ae198d84a5940eec997ded))

## [2.0.0-beta.15](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.14...v2.0.0-beta.15) (2023-11-12)


### Bug Fixes

* batch size to 2000 ([9cf9230](https://github.com/hirosystems/ordinals-api/commit/9cf9230fe193a2075a0cd2e45a5fa316ecc225c5))

## [2.0.0-beta.14](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.13...v2.0.0-beta.14) (2023-11-12)


### Bug Fixes

* try batch iterator generator ([e2ed039](https://github.com/hirosystems/ordinals-api/commit/e2ed039120958fde07297c6760022770dab710a8))

## [2.0.0-beta.13](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.12...v2.0.0-beta.13) (2023-11-12)


### Bug Fixes

* reduce brc-20 batch to 5k ([2037647](https://github.com/hirosystems/ordinals-api/commit/20376470455a0887d81f3128047d330e7d97e438))

## [2.0.0-beta.12](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.11...v2.0.0-beta.12) (2023-11-12)


### Bug Fixes

* reduce chunk size to 3000 ([9556148](https://github.com/hirosystems/ordinals-api/commit/95561481a140685d27e68c0c4e03d879882354f4))

## [2.0.0-beta.11](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.10...v2.0.0-beta.11) (2023-11-12)


### Bug Fixes

* bump docker image ([08688da](https://github.com/hirosystems/ordinals-api/commit/08688da84e7695156aa0e822c6f264e9085c5f1a))

## [2.0.0-beta.10](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.9...v2.0.0-beta.10) (2023-11-11)


### Bug Fixes

* reduce brc20 processing batch size to half ([7079dc0](https://github.com/hirosystems/ordinals-api/commit/7079dc079bfec5fbde7018dacbf20c1701d7ccb9))

## [2.0.0-beta.9](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.8...v2.0.0-beta.9) (2023-11-04)


### Bug Fixes

* guarantee gap detection is comparing numbers ([0f3f51a](https://github.com/hirosystems/ordinals-api/commit/0f3f51a0186dd2f677e21669d2359cba02122acf))

## [2.0.0-beta.8](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.7...v2.0.0-beta.8) (2023-11-03)


### Bug Fixes

* do not reject unbounded inscriptions that come in disorder ([#264](https://github.com/hirosystems/ordinals-api/issues/264)) ([56e2235](https://github.com/hirosystems/ordinals-api/commit/56e2235a3823a94493bbe978c2f8e5be48c6cf46))

## [2.0.0-beta.7](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.6...v2.0.0-beta.7) (2023-11-02)


### Bug Fixes

* reduce to 4000 chunk size ([3373422](https://github.com/hirosystems/ordinals-api/commit/337342251987d546600fe331154b3e8cd38dcd18))

## [2.0.0-beta.6](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.5...v2.0.0-beta.6) (2023-11-02)


### Bug Fixes

* only scan BRC-20 after its genesis height ([#263](https://github.com/hirosystems/ordinals-api/issues/263)) ([6381760](https://github.com/hirosystems/ordinals-api/commit/6381760f3860a96c3abfd485b612cd212927dca4))
* reduce batching to 4500 ([8fd770b](https://github.com/hirosystems/ordinals-api/commit/8fd770bc4e83be6ddaa4fdead1d1be55c42f870b))

## [2.0.0-beta.5](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.4...v2.0.0-beta.5) (2023-11-02)


### Bug Fixes

* only advance blessed number after gap detection ([c8f3c81](https://github.com/hirosystems/ordinals-api/commit/c8f3c81a657cd28283bdd89e0a17af06cad93d63))

## [2.0.0-beta.4](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.3...v2.0.0-beta.4) (2023-11-02)


### Bug Fixes

* only validate gaps for blessed numbers ([#262](https://github.com/hirosystems/ordinals-api/issues/262)) ([29aaeda](https://github.com/hirosystems/ordinals-api/commit/29aaeda39527c88b63b7c82739d8ed858c48a74b))

## [2.0.0-beta.3](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.2...v2.0.0-beta.3) (2023-11-02)


### Bug Fixes

* keep block transfer index ([#260](https://github.com/hirosystems/ordinals-api/issues/260)) ([0e33a64](https://github.com/hirosystems/ordinals-api/commit/0e33a644773990531fa3f68e93ca97f83105a621))

## [2.0.0-beta.2](https://github.com/hirosystems/ordinals-api/compare/v2.0.0-beta.1...v2.0.0-beta.2) (2023-11-02)


### Bug Fixes

* reject blocks that would create blessed inscription gaps ([#259](https://github.com/hirosystems/ordinals-api/issues/259)) ([18cd028](https://github.com/hirosystems/ordinals-api/commit/18cd028b636184a96597fa0b0978ba9e4d23f55c))

## [2.0.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v1.2.6...v2.0.0-beta.1) (2023-11-01)


### ⚠ BREAKING CHANGES

* handle transfer types and consider them for BRC-20 indexing (#258)

### Features

* handle transfer types and consider them for BRC-20 indexing ([#258](https://github.com/hirosystems/ordinals-api/issues/258)) ([7b83761](https://github.com/hirosystems/ordinals-api/commit/7b83761818ab4a178796a1867d143ff75ef338b0))

## [1.2.6](https://github.com/hirosystems/ordinals-api/compare/v1.2.5...v1.2.6) (2023-10-11)


### Bug Fixes

* scan brc-20 blocks using a cursor ([#251](https://github.com/hirosystems/ordinals-api/issues/251)) ([a1bf4b4](https://github.com/hirosystems/ordinals-api/commit/a1bf4b45eddc002610c6c6588c5ec45a2b64b0bb))
* use limit/offset instead of a cursor ([#252](https://github.com/hirosystems/ordinals-api/issues/252)) ([e4e9819](https://github.com/hirosystems/ordinals-api/commit/e4e98194973a36936d0adcf5069580e39a4ebbf5))

## [1.2.6-beta.2](https://github.com/hirosystems/ordinals-api/compare/v1.2.6-beta.1...v1.2.6-beta.2) (2023-10-11)


### Bug Fixes

* use limit/offset instead of a cursor ([#252](https://github.com/hirosystems/ordinals-api/issues/252)) ([e4e9819](https://github.com/hirosystems/ordinals-api/commit/e4e98194973a36936d0adcf5069580e39a4ebbf5))

## [1.2.6-beta.1](https://github.com/hirosystems/ordinals-api/compare/v1.2.5...v1.2.6-beta.1) (2023-10-11)


### Bug Fixes

* scan brc-20 blocks using a cursor ([#251](https://github.com/hirosystems/ordinals-api/issues/251)) ([a1bf4b4](https://github.com/hirosystems/ordinals-api/commit/a1bf4b45eddc002610c6c6588c5ec45a2b64b0bb))

## [1.2.5](https://github.com/hirosystems/ordinals-api/compare/v1.2.4...v1.2.5) (2023-10-06)


### Bug Fixes

* use ENV for server body limit ([#249](https://github.com/hirosystems/ordinals-api/issues/249)) ([d00fb8e](https://github.com/hirosystems/ordinals-api/commit/d00fb8e8e702c92232ebd82779a2ed29cf9eeaaa))

## [1.2.4](https://github.com/hirosystems/ordinals-api/compare/v1.2.3...v1.2.4) (2023-10-06)


### Bug Fixes

* same-block inscription location pointer comparison ([#248](https://github.com/hirosystems/ordinals-api/issues/248)) ([6fabb40](https://github.com/hirosystems/ordinals-api/commit/6fabb408373906ffbf3651cfa49121211b2cea98))

## [1.2.3](https://github.com/hirosystems/ordinals-api/compare/v1.2.2...v1.2.3) (2023-09-29)


### Bug Fixes

* save brc20 event correctly when returning to sender ([#240](https://github.com/hirosystems/ordinals-api/issues/240)) ([ea99595](https://github.com/hirosystems/ordinals-api/commit/ea99595e3d123bcb4946c1ad40402965ae726a8c))

## [1.2.2](https://github.com/hirosystems/ordinals-api/compare/v1.2.1...v1.2.2) (2023-09-25)


### Bug Fixes

* create table for recursive inscriptions count ([#237](https://github.com/hirosystems/ordinals-api/issues/237)) ([e27fa41](https://github.com/hirosystems/ordinals-api/commit/e27fa41761ca6eb2b267377b5ac36105fc41c017))

## [1.2.1](https://github.com/hirosystems/ordinals-api/compare/v1.2.0...v1.2.1) (2023-09-22)


### Bug Fixes

* keep count of brc-20 activity per address ([#229](https://github.com/hirosystems/ordinals-api/issues/229)) ([eebe8e7](https://github.com/hirosystems/ordinals-api/commit/eebe8e77360c6fb6a0c546546f11234f3dbc6736))
* optimize inscriptions pagination query ([#231](https://github.com/hirosystems/ordinals-api/issues/231)) ([9c46ade](https://github.com/hirosystems/ordinals-api/commit/9c46adee3080273bbdb8e31623943bfd959a00dc))
* save addresses in brc20 events table for faster queries ([#230](https://github.com/hirosystems/ordinals-api/issues/230)) ([13761f7](https://github.com/hirosystems/ordinals-api/commit/13761f711f1e8fb1029434d58aea507ffab5a215))

## [1.2.1-beta.3](https://github.com/hirosystems/ordinals-api/compare/v1.2.1-beta.2...v1.2.1-beta.3) (2023-09-21)


### Bug Fixes

* optimize inscriptions pagination query ([#231](https://github.com/hirosystems/ordinals-api/issues/231)) ([9c46ade](https://github.com/hirosystems/ordinals-api/commit/9c46adee3080273bbdb8e31623943bfd959a00dc))

## [1.2.1-beta.2](https://github.com/hirosystems/ordinals-api/compare/v1.2.1-beta.1...v1.2.1-beta.2) (2023-09-20)


### Bug Fixes

* save addresses in brc20 events table for faster queries ([#230](https://github.com/hirosystems/ordinals-api/issues/230)) ([13761f7](https://github.com/hirosystems/ordinals-api/commit/13761f711f1e8fb1029434d58aea507ffab5a215))

## [1.2.1-beta.1](https://github.com/hirosystems/ordinals-api/compare/v1.2.0...v1.2.1-beta.1) (2023-09-20)


### Bug Fixes

* keep count of brc-20 activity per address ([#229](https://github.com/hirosystems/ordinals-api/issues/229)) ([eebe8e7](https://github.com/hirosystems/ordinals-api/commit/eebe8e77360c6fb6a0c546546f11234f3dbc6736))

## [1.2.0](https://github.com/hirosystems/ordinals-api/compare/v1.1.1...v1.2.0) (2023-09-18)


### Features

* filter BRC-20 activity by address ([#226](https://github.com/hirosystems/ordinals-api/issues/226)) ([55eaeac](https://github.com/hirosystems/ordinals-api/commit/55eaeacb23daf9b52f29a9f824b9166e09999152))

## [1.1.1](https://github.com/hirosystems/ordinals-api/compare/v1.1.0...v1.1.1) (2023-09-14)


### Bug Fixes

* validation for satoshi ordinal numbers ([#223](https://github.com/hirosystems/ordinals-api/issues/223)) ([c3cf8c0](https://github.com/hirosystems/ordinals-api/commit/c3cf8c09995da5bb26b9c432dea8149cc61de0d6))

## [1.1.0](https://github.com/hirosystems/ordinals-api/compare/v1.0.0...v1.1.0) (2023-09-12)


### Features

* BRC-20 token support ([#78](https://github.com/hirosystems/ordinals-api/issues/78)) ([a39bfd3](https://github.com/hirosystems/ordinals-api/commit/a39bfd352cb4c58d83e0db9b7ad680a87e50b11d)), closes [#147](https://github.com/hirosystems/ordinals-api/issues/147) [#147](https://github.com/hirosystems/ordinals-api/issues/147) [#156](https://github.com/hirosystems/ordinals-api/issues/156) [#154](https://github.com/hirosystems/ordinals-api/issues/154) [#147](https://github.com/hirosystems/ordinals-api/issues/147) [#165](https://github.com/hirosystems/ordinals-api/issues/165) [#147](https://github.com/hirosystems/ordinals-api/issues/147) [#165](https://github.com/hirosystems/ordinals-api/issues/165) [#168](https://github.com/hirosystems/ordinals-api/issues/168) [#168](https://github.com/hirosystems/ordinals-api/issues/168) [#167](https://github.com/hirosystems/ordinals-api/issues/167) [#167](https://github.com/hirosystems/ordinals-api/issues/167) [#174](https://github.com/hirosystems/ordinals-api/issues/174) [#174](https://github.com/hirosystems/ordinals-api/issues/174) [#175](https://github.com/hirosystems/ordinals-api/issues/175) [#175](https://github.com/hirosystems/ordinals-api/issues/175) [#177](https://github.com/hirosystems/ordinals-api/issues/177) [#177](https://github.com/hirosystems/ordinals-api/issues/177) [#178](https://github.com/hirosystems/ordinals-api/issues/178) [#178](https://github.com/hirosystems/ordinals-api/issues/178) [#186](https://github.com/hirosystems/ordinals-api/issues/186) [#129](https://github.com/hirosystems/ordinals-api/issues/129) [#168](https://github.com/hirosystems/ordinals-api/issues/168) [#70](https://github.com/hirosystems/ordinals-api/issues/70) [#186](https://github.com/hirosystems/ordinals-api/issues/186) [#167](https://github.com/hirosystems/ordinals-api/issues/167) [#129](https://github.com/hirosystems/ordinals-api/issues/129) [#177](https://github.com/hirosystems/ordinals-api/issues/177) [#132](https://github.com/hirosystems/ordinals-api/issues/132) [#105](https://github.com/hirosystems/ordinals-api/issues/105) [#178](https://github.com/hirosystems/ordinals-api/issues/178) [#147](https://github.com/hirosystems/ordinals-api/issues/147) [#156](https://github.com/hirosystems/ordinals-api/issues/156) [#175](https://github.com/hirosystems/ordinals-api/issues/175) [#154](https://github.com/hirosystems/ordinals-api/issues/154) [#165](https://github.com/hirosystems/ordinals-api/issues/165) [#174](https://github.com/hirosystems/ordinals-api/issues/174) [#145](https://github.com/hirosystems/ordinals-api/issues/145) [#183](https://github.com/hirosystems/ordinals-api/issues/183)


### Bug Fixes

* lint errors ([#220](https://github.com/hirosystems/ordinals-api/issues/220)) ([f6bc735](https://github.com/hirosystems/ordinals-api/commit/f6bc73545852eae7649e4780d05ef5100e599109))


### Reverts

* Revert "Added H1 to the overview page" ([8fedf86](https://github.com/hirosystems/ordinals-api/commit/8fedf86060d92996bb71ab1bfe1024ced5639569))

## [1.0.0-brc-20.27](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.26...v1.0.0-brc-20.27) (2023-09-12)


### Bug Fixes

* return to sender balance calculation ([#218](https://github.com/hirosystems/ordinals-api/issues/218)) ([226dfe6](https://github.com/hirosystems/ordinals-api/commit/226dfe6f9744f5b6713757fe460897839fe3bb68))

## [1.0.0-brc-20.26](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.25...v1.0.0-brc-20.26) (2023-09-11)


### Bug Fixes

* ticker filter for balances ([#217](https://github.com/hirosystems/ordinals-api/issues/217)) ([1ca3bcd](https://github.com/hirosystems/ordinals-api/commit/1ca3bcdf6a1627f8e06d710b78af7dd5f8f5faca))

## [1.0.0-brc-20.25](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.24...v1.0.0-brc-20.25) (2023-09-11)


### Bug Fixes

* change tx_count to json number ([#216](https://github.com/hirosystems/ordinals-api/issues/216)) ([20f9415](https://github.com/hirosystems/ordinals-api/commit/20f941539fe8faadd00220ca085ff0d32214edd3))

## [1.0.0-brc-20.24](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.23...v1.0.0-brc-20.24) (2023-09-10)


### Features

* add location to activity response ([580861e](https://github.com/hirosystems/ordinals-api/commit/580861e1a74294f2a3a038d9919a0d13927cd8a6))

## [1.0.0-brc-20.23](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.22...v1.0.0-brc-20.23) (2023-09-10)


### Bug Fixes

* only increase counts on valid operations ([01f0865](https://github.com/hirosystems/ordinals-api/commit/01f08656966fa2f9ec00f129d51b700ef87ecec7))

## [1.0.0-brc-20.22](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.21...v1.0.0-brc-20.22) (2023-09-10)


### Bug Fixes

* optimize read queries by keeping count tables, implement rollbacks ([#214](https://github.com/hirosystems/ordinals-api/issues/214)) ([574d349](https://github.com/hirosystems/ordinals-api/commit/574d349667545a66d826aec90ea77f6c8e4d1745)), closes [#215](https://github.com/hirosystems/ordinals-api/issues/215)

## [1.0.0-brc-20.21](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.20...v1.0.0-brc-20.21) (2023-09-07)


### Bug Fixes

* calculate etag correctly on status endpoint ([#213](https://github.com/hirosystems/ordinals-api/issues/213)) ([f0c42e2](https://github.com/hirosystems/ordinals-api/commit/f0c42e2b8b01d34fe26c48e8b3fea80ae1ca418a))

## [1.0.0-brc-20.20](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.19...v1.0.0-brc-20.20) (2023-09-06)


### Bug Fixes

* optimize sent as fee detection ([1c9b0f4](https://github.com/hirosystems/ordinals-api/commit/1c9b0f4b35aa5576f151c4d58d7f7d8083494813))

## [1.0.0-brc-20.19](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.18...v1.0.0-brc-20.19) (2023-09-04)


### Features

* add fast ingestion mode for faster replays ([#211](https://github.com/hirosystems/ordinals-api/issues/211)) ([7996587](https://github.com/hirosystems/ordinals-api/commit/799658761250125c736ece824430f803b660bf25))

## [1.0.0-brc-20.18](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.17...v1.0.0-brc-20.18) (2023-09-01)


### Bug Fixes

* temporarily disable count calculation to speed up requests ([#210](https://github.com/hirosystems/ordinals-api/issues/210)) ([640e406](https://github.com/hirosystems/ordinals-api/commit/640e406ba8befb8c6f9d455869870527a7dae99d))

## [1.0.0-brc-20.17](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.16...v1.0.0-brc-20.17) (2023-09-01)


### Bug Fixes

* holders count query ([#209](https://github.com/hirosystems/ordinals-api/issues/209)) ([254ed75](https://github.com/hirosystems/ordinals-api/commit/254ed759f9be05ba0adbf1156515395d8d24b196))

## [1.0.0-brc-20.16](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.15...v1.0.0-brc-20.16) (2023-09-01)


### Bug Fixes

* optimize read queries, display amounts with correct decimals ([#208](https://github.com/hirosystems/ordinals-api/issues/208)) ([5d6453e](https://github.com/hirosystems/ordinals-api/commit/5d6453eda7a17add8c1f1ec6458781e3b762621a))

## [1.0.0-brc-20.15](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.14...v1.0.0-brc-20.15) (2023-08-31)


### Bug Fixes

* remove migration queries ([f53f889](https://github.com/hirosystems/ordinals-api/commit/f53f8896b55b2d64d4c7aad28e9828ad21d2bea3))

## [1.0.0-brc-20.14](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.13...v1.0.0-brc-20.14) (2023-08-31)


### Features

* add BRC-20 event history endpoint ([#191](https://github.com/hirosystems/ordinals-api/issues/191)) ([d9967b1](https://github.com/hirosystems/ordinals-api/commit/d9967b1b2db54b4e8d0e027592a1e2f27e6678cf))

## [1.0.0-brc-20.13](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.12...v1.0.0-brc-20.13) (2023-08-31)


### Bug Fixes

* go back to individual content fetch from utf8 pg errors ([4fd530d](https://github.com/hirosystems/ordinals-api/commit/4fd530d4ae2e3345c2fc056c4f605af01973fbb5))

## [1.0.0-brc-20.12](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.11...v1.0.0-brc-20.12) (2023-08-30)


### Bug Fixes

* guard against null bytes ([9eddbfc](https://github.com/hirosystems/ordinals-api/commit/9eddbfccfcfd74890f595f47a8ba32f57bf314e2))
* null char escaping ([8bb4954](https://github.com/hirosystems/ordinals-api/commit/8bb495403328ebe4a9e06a9dd6a61c794bdca0e7))

## [1.0.0-brc-20.11](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.10...v1.0.0-brc-20.11) (2023-08-30)


### Bug Fixes

* remove supply view, calculate supply as we mint ([36addce](https://github.com/hirosystems/ordinals-api/commit/36addce6add55ffd4e5180c3c396c964672182e9))

## [1.0.0-brc-20.10](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.9...v1.0.0-brc-20.10) (2023-08-30)


### Bug Fixes

* cast numbers to postgres numeric ([f8ae276](https://github.com/hirosystems/ordinals-api/commit/f8ae2767d6276178488e55ca98c884fc3db17860))

## [1.0.0-brc-20.9](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.8...v1.0.0-brc-20.9) (2023-08-30)


### Bug Fixes

* insert ops in single queries, reduce BigNumber usage ([#207](https://github.com/hirosystems/ordinals-api/issues/207)) ([ef11e34](https://github.com/hirosystems/ordinals-api/commit/ef11e34a2090a7f1b7630600cbee7e1ebb632680))

## [1.0.0-brc-20.8](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.7...v1.0.0-brc-20.8) (2023-08-30)


### Bug Fixes

* optimize brc-20 scan by minimizing string conversions ([#206](https://github.com/hirosystems/ordinals-api/issues/206)) ([98c12c8](https://github.com/hirosystems/ordinals-api/commit/98c12c8457006fd0a03dfcd4aeb43046b8d6189a))

## [1.0.0-brc-20.7](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.6...v1.0.0-brc-20.7) (2023-08-30)


### Bug Fixes

* add recursion backfill and temporary skip ([#198](https://github.com/hirosystems/ordinals-api/issues/198)) ([63571ee](https://github.com/hirosystems/ordinals-api/commit/63571eeb5459165a90cc6b45d35f5f50adb7df79))
* attempt to optimize brc-20 scan ([#204](https://github.com/hirosystems/ordinals-api/issues/204)) ([f6545cd](https://github.com/hirosystems/ordinals-api/commit/f6545cdef64fbf8dce6c522e11eb86d4ee4f69d5))
* do not insert repeated recursions ([#199](https://github.com/hirosystems/ordinals-api/issues/199)) ([9c8508b](https://github.com/hirosystems/ordinals-api/commit/9c8508b1e5ef7b21e99c4174b72c4b98b7990d23))
* dont update cache timestamp during ingestion ([#200](https://github.com/hirosystems/ordinals-api/issues/200)) ([8f973a3](https://github.com/hirosystems/ordinals-api/commit/8f973a30862f5aa869709dcaeec434cbe43903c4))
* guard against empty recursion refs ([71ce1a5](https://github.com/hirosystems/ordinals-api/commit/71ce1a52760c2e101c255e7d691653c180cc73c8))
* optimize inscription backfill indexes ([#197](https://github.com/hirosystems/ordinals-api/issues/197)) ([ab2f7bf](https://github.com/hirosystems/ordinals-api/commit/ab2f7bfc475f2399f72647aeac23b4b9a62d0905))
* semantic release ([26cd2c6](https://github.com/hirosystems/ordinals-api/commit/26cd2c6e8fb1948698052bbf3620fbefbd269bc9))
* split recursion insertion into chunks ([#201](https://github.com/hirosystems/ordinals-api/issues/201)) ([4ebc106](https://github.com/hirosystems/ordinals-api/commit/4ebc10652a345353ec58afc8a09893564e28d94e))

## [1.0.0](https://github.com/hirosystems/ordinals-api/compare/v0.4.15...v1.0.0) (2023-08-29)


### ⚠ BREAKING CHANGES

* optimize transfer replay capability (#129)

### Features

* add inscription number sort option ([#168](https://github.com/hirosystems/ordinals-api/issues/168)) ([9f4cdbc](https://github.com/hirosystems/ordinals-api/commit/9f4cdbc96f2efa4610e771df74b11951803cb8a6))
* add stats endpoint for inscription counts ([#70](https://github.com/hirosystems/ordinals-api/issues/70)) ([ac18e62](https://github.com/hirosystems/ordinals-api/commit/ac18e621ed7e8ea2fc5a5e536d59a152c3a1f345))
* detect and tag recursive inscriptions ([#167](https://github.com/hirosystems/ordinals-api/issues/167)) ([fb36285](https://github.com/hirosystems/ordinals-api/commit/fb362857c2c3cf4c098f6604b49d77efa6f95d8b))
* optimize transfer replay capability ([#129](https://github.com/hirosystems/ordinals-api/issues/129)) ([97874cc](https://github.com/hirosystems/ordinals-api/commit/97874cc1461d4e321d5143c70d68927ace62eec5))


### Bug Fixes

* add address column to genesis and current ([d71e1d4](https://github.com/hirosystems/ordinals-api/commit/d71e1d49dece39df1c19c0bb35a43129ef1a31e9))
* add secondary sorting by inscription number ([#177](https://github.com/hirosystems/ordinals-api/issues/177)) ([99959df](https://github.com/hirosystems/ordinals-api/commit/99959dfe6ec3de9288ce47bd8ef4d72535c19468))
* allow multiple transfers of an inscription in one block ([#132](https://github.com/hirosystems/ordinals-api/issues/132)) ([bc545f0](https://github.com/hirosystems/ordinals-api/commit/bc545f0c1d06ea54ceb5d6ba30a9031d04c7e01e))
* auto predicate registration option ([e1ed7c7](https://github.com/hirosystems/ordinals-api/commit/e1ed7c773dfba99f0b098debb3d865da46d8d10e))
* build beta image ([13f2c13](https://github.com/hirosystems/ordinals-api/commit/13f2c13384a00f9bfd58b7ddd88a49e7abbbe588))
* build event server using chainhook client library ([#105](https://github.com/hirosystems/ordinals-api/issues/105)) ([ab4c795](https://github.com/hirosystems/ordinals-api/commit/ab4c795d1621078950e4defa3330ae597f46d6ac))
* chainhook client upgrades ([9a96492](https://github.com/hirosystems/ordinals-api/commit/9a9649251dd449d6784aa4f6cd448c6f1b6cb687))
* consider `tx_index` in transfers by block endpoint ([#178](https://github.com/hirosystems/ordinals-api/issues/178)) ([ed517d6](https://github.com/hirosystems/ordinals-api/commit/ed517d6eb01b2a780ef0fb89fc5a65582d5e575e))
* introduce materialized view to count address inscriptions ([#147](https://github.com/hirosystems/ordinals-api/issues/147)) ([09a95d5](https://github.com/hirosystems/ordinals-api/commit/09a95d55276be8b52ea19c90d0e7fa8bca73cfc7))
* make etag calculation sensitive to inscription location gap fills and upserts ([#156](https://github.com/hirosystems/ordinals-api/issues/156)) ([5648c9e](https://github.com/hirosystems/ordinals-api/commit/5648c9ea72ee09df4a224937a08f662e78d06edd))
* optimize COUNT calculations via the use of count tables ([#175](https://github.com/hirosystems/ordinals-api/issues/175)) ([31498bd](https://github.com/hirosystems/ordinals-api/commit/31498bdb57203bd6c28eccac4446a9d169a3fe18))
* refresh views in parallel ([#154](https://github.com/hirosystems/ordinals-api/issues/154)) ([a7674a9](https://github.com/hirosystems/ordinals-api/commit/a7674a92efcb580b67c3510a2bf09ffb752e2ef0))
* remove unused json functions ([#165](https://github.com/hirosystems/ordinals-api/issues/165)) ([3eb0e24](https://github.com/hirosystems/ordinals-api/commit/3eb0e248a98913b8e4c56949e8ebd174a3f1faae))
* rename location pointer tables ([b84d27e](https://github.com/hirosystems/ordinals-api/commit/b84d27e3624737e59c949906cafa8d76a329c0a0))
* rollback location pointers ([#174](https://github.com/hirosystems/ordinals-api/issues/174)) ([3c9d7f0](https://github.com/hirosystems/ordinals-api/commit/3c9d7f07d14aed2bad1c07d69f7170d74a85d575))
* save tx_index on locations to support transfers on same block ([#145](https://github.com/hirosystems/ordinals-api/issues/145)) ([30a9635](https://github.com/hirosystems/ordinals-api/commit/30a96358c2b7c4c40f908e116478e3ddd83d8857))
* semantic release process ([#202](https://github.com/hirosystems/ordinals-api/issues/202)) ([1bd3f74](https://github.com/hirosystems/ordinals-api/commit/1bd3f74ed91adf2d0983896afa7f9b468b0c26cb))
* skip db migrations during readonly mode ([d5157f0](https://github.com/hirosystems/ordinals-api/commit/d5157f02646ceb6c58b73575d8ff4afc8833b97e))
* upgrade api-toolkit ([#190](https://github.com/hirosystems/ordinals-api/issues/190)) ([a691b67](https://github.com/hirosystems/ordinals-api/commit/a691b6707eb2b6e2cedfc8a13fe249157660457f))
* upgrade chainhook client ([cbbb951](https://github.com/hirosystems/ordinals-api/commit/cbbb9512734f83c27da91b57fd5825b22c510c33))
* upgrade chainhook client to 1.3.3 ([ee66f93](https://github.com/hirosystems/ordinals-api/commit/ee66f93a1d06c786c2eb7ce415df28c7fa8d0032))
* warn correctly on missing prev locations ([879bf55](https://github.com/hirosystems/ordinals-api/commit/879bf55b0fc7efd830c5cc0e1d742818177e8344))

## [1.0.0-beta.18](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.17...v1.0.0-beta.18) (2023-08-29)


### Bug Fixes

* syntax ([79ff84c](https://github.com/hirosystems/ordinals-api/commit/79ff84c92c14b446a440610810a950144b066c97))
* test locking versions in GH action only ([275bbcc](https://github.com/hirosystems/ordinals-api/commit/275bbcc819fd6025ee8e3889dcdaabe0fb44fc49))

## [1.0.0-beta.16](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.15...v1.0.0-beta.16) (2023-08-24)


### Bug Fixes

* upgrade api-toolkit ([#190](https://github.com/hirosystems/ordinals-api/issues/190)) ([a691b67](https://github.com/hirosystems/ordinals-api/commit/a691b6707eb2b6e2cedfc8a13fe249157660457f))

## [1.0.0-brc-20.6](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.5...v1.0.0-brc-20.6) (2023-08-26)

### Bug Fixes

* place a cap on max insertion size ([bef5f23](https://github.com/hirosystems/ordinals-api/commit/bef5f23891b0a041bc27f54e507fead928306c95))

## [1.0.0-brc-20.5](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.4...v1.0.0-brc-20.5) (2023-08-25)


### Bug Fixes

* change uniqueness constraint in locations table ([9a9c5de](https://github.com/hirosystems/ordinals-api/commit/9a9c5de7ff76557e2728b726363d68833d05a689))

## [1.0.0-brc-20.4](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.3...v1.0.0-brc-20.4) (2023-08-25)


### Bug Fixes

* refresh supplies view only if BRC-20 is enabled ([7d6705a](https://github.com/hirosystems/ordinals-api/commit/7d6705a82f2acec5531b53d2d352f7fa04bf1c51))
* upgrade api-toolkit ([#190](https://github.com/hirosystems/ordinals-api/issues/190)) ([0e673a7](https://github.com/hirosystems/ordinals-api/commit/0e673a7ab327e98b85b3f6289fc7addadee6b1d2))

## [1.0.0-brc-20.3](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.2...v1.0.0-brc-20.3) (2023-08-24)


### Bug Fixes

* add unique indexes for mints and transfers ([b428bb4](https://github.com/hirosystems/ordinals-api/commit/b428bb48ef2b2a3eda6f3366fbd7fd488d090d4d))
* sending transfer as fee returns amt to sender ([e23012a](https://github.com/hirosystems/ordinals-api/commit/e23012a926eef80e1d467ef28f618fae989426fd))

## [1.0.0-brc-20.2](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-brc-20.1...v1.0.0-brc-20.2) (2023-08-24)


### Features

* add more brc20 features ([#183](https://github.com/hirosystems/ordinals-api/issues/183)) ([c1939ce](https://github.com/hirosystems/ordinals-api/commit/c1939cee3a40df5f285d3bb749c8b29cea4bb271))


### Bug Fixes

* optimize inscription and brc-20 inserts ([#189](https://github.com/hirosystems/ordinals-api/issues/189)) ([3807334](https://github.com/hirosystems/ordinals-api/commit/38073341163bc1c5cea44b66ad3da505f2ce4273))

## [1.0.0-brc-20.1](https://github.com/hirosystems/ordinals-api/compare/v0.4.15...v1.0.0-brc-20.1) (2023-08-17)


### ⚠ BREAKING CHANGES

* optimize transfer replay capability (#129)

### Features

* add inscription number sort option ([#168](https://github.com/hirosystems/ordinals-api/issues/168)) ([9f4cdbc](https://github.com/hirosystems/ordinals-api/commit/9f4cdbc96f2efa4610e771df74b11951803cb8a6))
* add stats endpoint for inscription counts ([#70](https://github.com/hirosystems/ordinals-api/issues/70)) ([ac18e62](https://github.com/hirosystems/ordinals-api/commit/ac18e621ed7e8ea2fc5a5e536d59a152c3a1f345))
* brc-20 balance at block ([#186](https://github.com/hirosystems/ordinals-api/issues/186)) ([ced5cb3](https://github.com/hirosystems/ordinals-api/commit/ced5cb3306bd0e242503a86f8c94911c2d57161f))
* detect and tag recursive inscriptions ([#167](https://github.com/hirosystems/ordinals-api/issues/167)) ([fb36285](https://github.com/hirosystems/ordinals-api/commit/fb362857c2c3cf4c098f6604b49d77efa6f95d8b))
* first balance endpoint ([f9c6654](https://github.com/hirosystems/ordinals-api/commit/f9c66540b9d173d2981bc2af5ee13fd082dc5547))
* first balance transfers ([dd8ec07](https://github.com/hirosystems/ordinals-api/commit/dd8ec07d366e6bf15e74b528077c8fa1836958e9))
* holders endpoint ([a01f77e](https://github.com/hirosystems/ordinals-api/commit/a01f77ef6c9c03576a07a7cdc14d0279afc44cbb))
* mint within supply ([c8e5820](https://github.com/hirosystems/ordinals-api/commit/c8e582055956c9381d14d5ec1bae5a70c0a4d4a8))
* mints with balance changes ([32e90f7](https://github.com/hirosystems/ordinals-api/commit/32e90f73696aa403417869f0c71fa76da115048e))
* optimize transfer replay capability ([#129](https://github.com/hirosystems/ordinals-api/issues/129)) ([97874cc](https://github.com/hirosystems/ordinals-api/commit/97874cc1461d4e321d5143c70d68927ace62eec5))
* start storing token deploys ([bf4c7f6](https://github.com/hirosystems/ordinals-api/commit/bf4c7f6f27903f18d30ddb7fc2b1a779cc991114))
* token details ([5d35d5b](https://github.com/hirosystems/ordinals-api/commit/5d35d5b0eefb46eeac91ead52f4909279e39404d))
* token info endpoint ([8fad6b9](https://github.com/hirosystems/ordinals-api/commit/8fad6b96c0fffc302a3e61922677bdfb56b74b85))
* tokens endpoint as paginated index ([ae2049b](https://github.com/hirosystems/ordinals-api/commit/ae2049baf04950d810aa997bc0f31b585aaf3391))


### Bug Fixes

* add address column to genesis and current ([d71e1d4](https://github.com/hirosystems/ordinals-api/commit/d71e1d49dece39df1c19c0bb35a43129ef1a31e9))
* add indexes for fks ([354ddd0](https://github.com/hirosystems/ordinals-api/commit/354ddd0559a32a2aba1d407a2c7486348eb91d1c))
* add secondary sorting by inscription number ([#177](https://github.com/hirosystems/ordinals-api/issues/177)) ([99959df](https://github.com/hirosystems/ordinals-api/commit/99959dfe6ec3de9288ce47bd8ef4d72535c19468))
* allow gap fills for transfers ([026c275](https://github.com/hirosystems/ordinals-api/commit/026c2755483efbc8b54753a9a1bf315a6a833d88))
* allow multiple transfers of an inscription in one block ([#132](https://github.com/hirosystems/ordinals-api/issues/132)) ([bc545f0](https://github.com/hirosystems/ordinals-api/commit/bc545f0c1d06ea54ceb5d6ba30a9031d04c7e01e))
* auto predicate registration option ([e1ed7c7](https://github.com/hirosystems/ordinals-api/commit/e1ed7c773dfba99f0b098debb3d865da46d8d10e))
* balances and rollbacks ([61b4139](https://github.com/hirosystems/ordinals-api/commit/61b413955f6ce1428a6a3b1c6b023ae4464c111d))
* balances/:address ([687c2e4](https://github.com/hirosystems/ordinals-api/commit/687c2e43cc5782a2521c3442c0d7fcfe90943b67))
* build beta image ([13f2c13](https://github.com/hirosystems/ordinals-api/commit/13f2c13384a00f9bfd58b7ddd88a49e7abbbe588))
* build event server using chainhook client library ([#105](https://github.com/hirosystems/ordinals-api/issues/105)) ([ab4c795](https://github.com/hirosystems/ordinals-api/commit/ab4c795d1621078950e4defa3330ae597f46d6ac))
* chainhook client upgrades ([9a96492](https://github.com/hirosystems/ordinals-api/commit/9a9649251dd449d6784aa4f6cd448c6f1b6cb687))
* consider `tx_index` in transfers by block endpoint ([#178](https://github.com/hirosystems/ordinals-api/issues/178)) ([ed517d6](https://github.com/hirosystems/ordinals-api/commit/ed517d6eb01b2a780ef0fb89fc5a65582d5e575e))
* introduce materialized view to count address inscriptions ([#147](https://github.com/hirosystems/ordinals-api/issues/147)) ([09a95d5](https://github.com/hirosystems/ordinals-api/commit/09a95d55276be8b52ea19c90d0e7fa8bca73cfc7))
* invalid decimal count ([aa15b0e](https://github.com/hirosystems/ordinals-api/commit/aa15b0e4843435cacfa12856b881566ba0c2f3a3))
* make etag calculation sensitive to inscription location gap fills and upserts ([#156](https://github.com/hirosystems/ordinals-api/issues/156)) ([5648c9e](https://github.com/hirosystems/ordinals-api/commit/5648c9ea72ee09df4a224937a08f662e78d06edd))
* only consider blessed inscriptions ([2a4700c](https://github.com/hirosystems/ordinals-api/commit/2a4700c5ca851b799fba534ff8060004f7ca2f5d))
* optimize COUNT calculations via the use of count tables ([#175](https://github.com/hirosystems/ordinals-api/issues/175)) ([31498bd](https://github.com/hirosystems/ordinals-api/commit/31498bdb57203bd6c28eccac4446a9d169a3fe18))
* refresh views in parallel ([#154](https://github.com/hirosystems/ordinals-api/issues/154)) ([a7674a9](https://github.com/hirosystems/ordinals-api/commit/a7674a92efcb580b67c3510a2bf09ffb752e2ef0))
* remove old json content tables ([0732048](https://github.com/hirosystems/ordinals-api/commit/07320489889b85c881ab49a4ce10d0d21a750114))
* remove old json schemas ([8cc7f8a](https://github.com/hirosystems/ordinals-api/commit/8cc7f8adcb9d70cd511b09583dd45f9dc770cd92))
* remove unused json functions ([#165](https://github.com/hirosystems/ordinals-api/issues/165)) ([3eb0e24](https://github.com/hirosystems/ordinals-api/commit/3eb0e248a98913b8e4c56949e8ebd174a3f1faae))
* rename location pointer tables ([b84d27e](https://github.com/hirosystems/ordinals-api/commit/b84d27e3624737e59c949906cafa8d76a329c0a0))
* rollback location pointers ([#174](https://github.com/hirosystems/ordinals-api/issues/174)) ([3c9d7f0](https://github.com/hirosystems/ordinals-api/commit/3c9d7f07d14aed2bad1c07d69f7170d74a85d575))
* save tx_index on locations to support transfers on same block ([#145](https://github.com/hirosystems/ordinals-api/issues/145)) ([30a9635](https://github.com/hirosystems/ordinals-api/commit/30a96358c2b7c4c40f908e116478e3ddd83d8857))
* skip db migrations during readonly mode ([d5157f0](https://github.com/hirosystems/ordinals-api/commit/d5157f02646ceb6c58b73575d8ff4afc8833b97e))
* tick must be 4 bytes or less ([f6fd0a6](https://github.com/hirosystems/ordinals-api/commit/f6fd0a656d6520f90eda4d6610c04a077fa70354))
* transfers only usable once ([542ec34](https://github.com/hirosystems/ordinals-api/commit/542ec34292d7535d01f62832b270e11b80b59da4))
* upgrade chainhook client ([cbbb951](https://github.com/hirosystems/ordinals-api/commit/cbbb9512734f83c27da91b57fd5825b22c510c33))
* upgrade chainhook client to 1.3.3 ([ee66f93](https://github.com/hirosystems/ordinals-api/commit/ee66f93a1d06c786c2eb7ce415df28c7fa8d0032))
* warn correctly on missing prev locations ([879bf55](https://github.com/hirosystems/ordinals-api/commit/879bf55b0fc7efd830c5cc0e1d742818177e8344))

## [1.0.0-beta.15](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.14...v1.0.0-beta.15) (2023-08-04)


### Bug Fixes

* consider `tx_index` in transfers by block endpoint ([#178](https://github.com/hirosystems/ordinals-api/issues/178)) ([ed517d6](https://github.com/hirosystems/ordinals-api/commit/ed517d6eb01b2a780ef0fb89fc5a65582d5e575e))

## [1.0.0-beta.14](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.13...v1.0.0-beta.14) (2023-08-02)


### Bug Fixes

* add secondary sorting by inscription number ([#177](https://github.com/hirosystems/ordinals-api/issues/177)) ([99959df](https://github.com/hirosystems/ordinals-api/commit/99959dfe6ec3de9288ce47bd8ef4d72535c19468))

## [1.0.0-beta.13](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.12...v1.0.0-beta.13) (2023-08-01)


### Bug Fixes

* optimize COUNT calculations via the use of count tables ([#175](https://github.com/hirosystems/ordinals-api/issues/175)) ([31498bd](https://github.com/hirosystems/ordinals-api/commit/31498bdb57203bd6c28eccac4446a9d169a3fe18))

## [1.0.0-beta.12](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.11...v1.0.0-beta.12) (2023-07-31)


### Bug Fixes

* rollback location pointers ([#174](https://github.com/hirosystems/ordinals-api/issues/174)) ([3c9d7f0](https://github.com/hirosystems/ordinals-api/commit/3c9d7f07d14aed2bad1c07d69f7170d74a85d575))

## [1.0.0-beta.11](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.10...v1.0.0-beta.11) (2023-07-25)


### Bug Fixes

* warn correctly on missing prev locations ([879bf55](https://github.com/hirosystems/ordinals-api/commit/879bf55b0fc7efd830c5cc0e1d742818177e8344))

## [1.0.0-beta.10](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.9...v1.0.0-beta.10) (2023-07-25)


### Features

* detect and tag recursive inscriptions ([#167](https://github.com/hirosystems/ordinals-api/issues/167)) ([fb36285](https://github.com/hirosystems/ordinals-api/commit/fb362857c2c3cf4c098f6604b49d77efa6f95d8b))

## [1.0.0-beta.9](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.8...v1.0.0-beta.9) (2023-07-25)


### Features

* add inscription number sort option ([#168](https://github.com/hirosystems/ordinals-api/issues/168)) ([9f4cdbc](https://github.com/hirosystems/ordinals-api/commit/9f4cdbc96f2efa4610e771df74b11951803cb8a6))

## [1.0.0-beta.8](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.7...v1.0.0-beta.8) (2023-07-21)


### Bug Fixes

* skip db migrations during readonly mode ([d5157f0](https://github.com/hirosystems/ordinals-api/commit/d5157f02646ceb6c58b73575d8ff4afc8833b97e))

## [1.0.0-beta.7](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.6...v1.0.0-beta.7) (2023-07-21)


### Bug Fixes

* build beta image ([13f2c13](https://github.com/hirosystems/ordinals-api/commit/13f2c13384a00f9bfd58b7ddd88a49e7abbbe588))
* introduce materialized view to count address inscriptions ([#147](https://github.com/hirosystems/ordinals-api/issues/147)) ([2e79311](https://github.com/hirosystems/ordinals-api/commit/2e793117afcafbd5f7578bc6b2435af7f33e7dd7))
* remove unused json functions ([#165](https://github.com/hirosystems/ordinals-api/issues/165)) ([3eb0e24](https://github.com/hirosystems/ordinals-api/commit/3eb0e248a98913b8e4c56949e8ebd174a3f1faae))

## [1.0.0-beta.6](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.5...v1.0.0-beta.6) (2023-07-21)


### Bug Fixes

* make etag calculation sensitive to inscription location gap fills and upserts ([#156](https://github.com/hirosystems/ordinals-api/issues/156)) ([5648c9e](https://github.com/hirosystems/ordinals-api/commit/5648c9ea72ee09df4a224937a08f662e78d06edd))
* refresh views in parallel ([#154](https://github.com/hirosystems/ordinals-api/issues/154)) ([a7674a9](https://github.com/hirosystems/ordinals-api/commit/a7674a92efcb580b67c3510a2bf09ffb752e2ef0))

## [1.0.0-beta.5](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.4...v1.0.0-beta.5) (2023-07-14)


### Bug Fixes

* add address column to genesis and current ([d71e1d4](https://github.com/hirosystems/ordinals-api/commit/d71e1d49dece39df1c19c0bb35a43129ef1a31e9))
* introduce materialized view to count address inscriptions ([#147](https://github.com/hirosystems/ordinals-api/issues/147)) ([09a95d5](https://github.com/hirosystems/ordinals-api/commit/09a95d55276be8b52ea19c90d0e7fa8bca73cfc7))

## [0.4.15](https://github.com/hirosystems/ordinals-api/compare/v0.4.14...v0.4.15) (2023-07-13)


### Bug Fixes

* introduce materialized view to count address inscriptions ([#147](https://github.com/hirosystems/ordinals-api/issues/147)) ([2e79311](https://github.com/hirosystems/ordinals-api/commit/2e793117afcafbd5f7578bc6b2435af7f33e7dd7))

## [1.0.0-beta.4](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.3...v1.0.0-beta.4) (2023-07-12)

### Bug Fixes

* save tx_index on locations to support transfers on same block ([#145](https://github.com/hirosystems/ordinals-api/issues/145)) ([30a9635](https://github.com/hirosystems/ordinals-api/commit/30a96358c2b7c4c40f908e116478e3ddd83d8857))

## [1.0.0-beta.3](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.2...v1.0.0-beta.3) (2023-07-12)


### Bug Fixes

* allow multiple transfers per inscription per block ([#131](https://github.com/hirosystems/ordinals-api/issues/131)) ([9fee5ac](https://github.com/hirosystems/ordinals-api/commit/9fee5acb8185681fc95f0b85e9ae02810cf8b473))
* genesis and current locations using materialized view ([#138](https://github.com/hirosystems/ordinals-api/issues/138)) ([88edee4](https://github.com/hirosystems/ordinals-api/commit/88edee44586880f54c2e7d1c16f31d2b583ce216))
* ignore json content for current build ([#140](https://github.com/hirosystems/ordinals-api/issues/140)) ([909f79f](https://github.com/hirosystems/ordinals-api/commit/909f79f43b9c0a359c62c09858b244b8cba58c75))
* mark intractable `/inscriptions` result counts as 0 to avoid DB bottlenecks ([#142](https://github.com/hirosystems/ordinals-api/issues/142)) ([2d1fa8f](https://github.com/hirosystems/ordinals-api/commit/2d1fa8f6b6062494ecd7900c5d74cd12abe7e32f))
* move to many-to-many genesis and current table ([#139](https://github.com/hirosystems/ordinals-api/issues/139)) ([2b6b6ec](https://github.com/hirosystems/ordinals-api/commit/2b6b6eccd95bc24aa1e0e82a06d3ca36ddc2298f))
* patch ordinal number for transfers replay ([#134](https://github.com/hirosystems/ordinals-api/issues/134)) ([8d3fb5b](https://github.com/hirosystems/ordinals-api/commit/8d3fb5b23ad57e33ba22dea12afa33cb146de558))
* rename location pointer tables ([b84d27e](https://github.com/hirosystems/ordinals-api/commit/b84d27e3624737e59c949906cafa8d76a329c0a0))
* skip normalization for now ([#136](https://github.com/hirosystems/ordinals-api/issues/136)) ([85de25f](https://github.com/hirosystems/ordinals-api/commit/85de25f8c14288f426063da08bfb935d8ffb2aad))


## [0.4.14](https://github.com/hirosystems/ordinals-api/compare/v0.4.13...v0.4.14) (2023-07-11)


### Bug Fixes

* mark intractable `/inscriptions` result counts as 0 to avoid DB bottlenecks ([#142](https://github.com/hirosystems/ordinals-api/issues/142)) ([2d1fa8f](https://github.com/hirosystems/ordinals-api/commit/2d1fa8f6b6062494ecd7900c5d74cd12abe7e32f))

## [0.4.13](https://github.com/hirosystems/ordinals-api/compare/v0.4.12...v0.4.13) (2023-07-09)


### Bug Fixes

* ignore json content for current build ([#140](https://github.com/hirosystems/ordinals-api/issues/140)) ([909f79f](https://github.com/hirosystems/ordinals-api/commit/909f79f43b9c0a359c62c09858b244b8cba58c75))

## [0.4.12](https://github.com/hirosystems/ordinals-api/compare/v0.4.11...v0.4.12) (2023-07-09)


### Bug Fixes

* move to many-to-many genesis and current table ([#139](https://github.com/hirosystems/ordinals-api/issues/139)) ([2b6b6ec](https://github.com/hirosystems/ordinals-api/commit/2b6b6eccd95bc24aa1e0e82a06d3ca36ddc2298f))

## [0.4.11](https://github.com/hirosystems/ordinals-api/compare/v0.4.10...v0.4.11) (2023-07-09)


### Bug Fixes

* genesis and current locations using materialized view ([#138](https://github.com/hirosystems/ordinals-api/issues/138)) ([88edee4](https://github.com/hirosystems/ordinals-api/commit/88edee44586880f54c2e7d1c16f31d2b583ce216))

## [0.4.10](https://github.com/hirosystems/ordinals-api/compare/v0.4.9...v0.4.10) (2023-07-08)


### Bug Fixes

* skip normalization for now ([#136](https://github.com/hirosystems/ordinals-api/issues/136)) ([85de25f](https://github.com/hirosystems/ordinals-api/commit/85de25f8c14288f426063da08bfb935d8ffb2aad))

## [0.4.9](https://github.com/hirosystems/ordinals-api/compare/v0.4.8...v0.4.9) (2023-07-07)


### Bug Fixes

* patch ordinal number for transfers replay ([#134](https://github.com/hirosystems/ordinals-api/issues/134)) ([8d3fb5b](https://github.com/hirosystems/ordinals-api/commit/8d3fb5b23ad57e33ba22dea12afa33cb146de558))

## [0.4.8](https://github.com/hirosystems/ordinals-api/compare/v0.4.7...v0.4.8) (2023-07-07)


### Bug Fixes

* allow multiple transfers per inscription per block ([#131](https://github.com/hirosystems/ordinals-api/issues/131)) ([9fee5ac](https://github.com/hirosystems/ordinals-api/commit/9fee5acb8185681fc95f0b85e9ae02810cf8b473))

## [1.0.0-beta.2](https://github.com/hirosystems/ordinals-api/compare/v1.0.0-beta.1...v1.0.0-beta.2) (2023-07-07)


### Bug Fixes

* allow multiple transfers of an inscription in one block ([#132](https://github.com/hirosystems/ordinals-api/issues/132)) ([bc545f0](https://github.com/hirosystems/ordinals-api/commit/bc545f0c1d06ea54ceb5d6ba30a9031d04c7e01e))

## [1.0.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v0.4.7...v1.0.0-beta.1) (2023-07-06)


### ⚠ BREAKING CHANGES

* optimize transfer replay capability (#129)

### Features

* add stats endpoint for inscription counts ([#70](https://github.com/hirosystems/ordinals-api/issues/70)) ([ac18e62](https://github.com/hirosystems/ordinals-api/commit/ac18e621ed7e8ea2fc5a5e536d59a152c3a1f345))
* optimize transfer replay capability ([#129](https://github.com/hirosystems/ordinals-api/issues/129)) ([97874cc](https://github.com/hirosystems/ordinals-api/commit/97874cc1461d4e321d5143c70d68927ace62eec5))


### Bug Fixes

* auto predicate registration option ([e1ed7c7](https://github.com/hirosystems/ordinals-api/commit/e1ed7c773dfba99f0b098debb3d865da46d8d10e))
* build event server using chainhook client library ([#105](https://github.com/hirosystems/ordinals-api/issues/105)) ([ab4c795](https://github.com/hirosystems/ordinals-api/commit/ab4c795d1621078950e4defa3330ae597f46d6ac))
* chainhook client upgrades ([9a96492](https://github.com/hirosystems/ordinals-api/commit/9a9649251dd449d6784aa4f6cd448c6f1b6cb687))
* upgrade chainhook client ([cbbb951](https://github.com/hirosystems/ordinals-api/commit/cbbb9512734f83c27da91b57fd5825b22c510c33))
* upgrade chainhook client to 1.3.3 ([ee66f93](https://github.com/hirosystems/ordinals-api/commit/ee66f93a1d06c786c2eb7ce415df28c7fa8d0032))

## [0.4.7](https://github.com/hirosystems/ordinals-api/compare/v0.4.6...v0.4.7) (2023-07-03)


### Bug Fixes

* expect any and stringify for curse_type ([#127](https://github.com/hirosystems/ordinals-api/issues/127)) ([0459115](https://github.com/hirosystems/ordinals-api/commit/0459115d44a4ad7143c08eb2a0225d2b1123769d))

## [0.4.6](https://github.com/hirosystems/ordinals-api/compare/v0.4.5...v0.4.6) (2023-07-01)


### Bug Fixes

* process full block even on missing genesis ([#122](https://github.com/hirosystems/ordinals-api/issues/122)) ([edbf217](https://github.com/hirosystems/ordinals-api/commit/edbf217692756f87bc17fc35618ffe37b16fb92e))

## [0.4.5](https://github.com/hirosystems/ordinals-api/compare/v0.4.4...v0.4.5) (2023-07-01)


### Bug Fixes

* remove inscription_number and ordinal_number from transfers ([#121](https://github.com/hirosystems/ordinals-api/issues/121)) ([a4cbeae](https://github.com/hirosystems/ordinals-api/commit/a4cbeaeab697d419a470caf9bf18229cc5dbed8a))

## [0.4.4](https://github.com/hirosystems/ordinals-api/compare/v0.4.3...v0.4.4) (2023-06-30)


### Bug Fixes

* don't ping chainhook node if auto predicate reg is disabled ([#119](https://github.com/hirosystems/ordinals-api/issues/119)) ([f04445e](https://github.com/hirosystems/ordinals-api/commit/f04445efa5df5876d6f22b7d616bcadfe4d95b99))

## [0.4.3](https://github.com/hirosystems/ordinals-api/compare/v0.4.2...v0.4.3) (2023-06-30)


### Bug Fixes

* create unique indexes for views for concurrent refresh ([#118](https://github.com/hirosystems/ordinals-api/issues/118)) ([58123cb](https://github.com/hirosystems/ordinals-api/commit/58123cbec9dda2b3c8cbddd8d8beb7751bed1c2d))

## [0.4.2](https://github.com/hirosystems/ordinals-api/compare/v0.4.1...v0.4.2) (2023-06-30)


### Bug Fixes

* throw error on invalid chainhook payloads ([#117](https://github.com/hirosystems/ordinals-api/issues/117)) ([e639343](https://github.com/hirosystems/ordinals-api/commit/e63934319049bf950c58ef3bc6ba0ebb0b65b9c2))

## [0.4.1](https://github.com/hirosystems/ordinals-api/compare/v0.4.0...v0.4.1) (2023-06-30)


### Bug Fixes

* skip expensive view refreshes when not streaming new blocks ([#116](https://github.com/hirosystems/ordinals-api/issues/116)) ([baec17c](https://github.com/hirosystems/ordinals-api/commit/baec17c51e3b5c4b6cb958b13dabb8c8c3de7a71))

## [0.4.0](https://github.com/hirosystems/ordinals-api/compare/v0.3.3...v0.4.0) (2023-06-29)


### Features

* api ingestion metrics on prometheus ([#113](https://github.com/hirosystems/ordinals-api/issues/113)) ([10ec679](https://github.com/hirosystems/ordinals-api/commit/10ec679ca7607be849dd642178f5f170b104ff9b))


### Bug Fixes

* allow nullable and tagged curse types ([#111](https://github.com/hirosystems/ordinals-api/issues/111)) ([641a627](https://github.com/hirosystems/ordinals-api/commit/641a627453aa313c08c697c8cfa05f8277525b4c))
* check for prod correctly when refreshing materialized views ([#112](https://github.com/hirosystems/ordinals-api/issues/112)) ([4518043](https://github.com/hirosystems/ordinals-api/commit/45180430d20cb77278d452be21963a9e53c5e557))
* send 500 code if payload ingestion fails ([#114](https://github.com/hirosystems/ordinals-api/issues/114)) ([0a3fee2](https://github.com/hirosystems/ordinals-api/commit/0a3fee228d29843454be81ee29284195fbbe45f1))

## [0.4.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v0.3.4-beta.2...v0.4.0-beta.1) (2023-06-28)


### Features

* api ingestion metrics on prometheus ([#113](https://github.com/hirosystems/ordinals-api/issues/113)) ([10ec679](https://github.com/hirosystems/ordinals-api/commit/10ec679ca7607be849dd642178f5f170b104ff9b))


### Bug Fixes

* allow nullable and tagged curse types ([#111](https://github.com/hirosystems/ordinals-api/issues/111)) ([641a627](https://github.com/hirosystems/ordinals-api/commit/641a627453aa313c08c697c8cfa05f8277525b4c))

## [0.3.4-beta.2](https://github.com/hirosystems/ordinals-api/compare/v0.3.4-beta.1...v0.3.4-beta.2) (2023-06-28)


### Bug Fixes

* send 500 code if payload ingestion fails ([#114](https://github.com/hirosystems/ordinals-api/issues/114)) ([0a3fee2](https://github.com/hirosystems/ordinals-api/commit/0a3fee228d29843454be81ee29284195fbbe45f1))

## [0.3.4-beta.1](https://github.com/hirosystems/ordinals-api/compare/v0.3.3...v0.3.4-beta.1) (2023-06-28)


### Bug Fixes

* check for prod correctly when refreshing materialized views ([#112](https://github.com/hirosystems/ordinals-api/issues/112)) ([4518043](https://github.com/hirosystems/ordinals-api/commit/45180430d20cb77278d452be21963a9e53c5e557))

## [0.3.3](https://github.com/hirosystems/ordinals-api/compare/v0.3.2...v0.3.3) (2023-06-26)


### Bug Fixes

* allow genesis with null address ([#108](https://github.com/hirosystems/ordinals-api/issues/108)) ([9769028](https://github.com/hirosystems/ordinals-api/commit/9769028d9fc1366216cc858b503e27e0c6ec8b7d))

## [0.3.2](https://github.com/hirosystems/ordinals-api/compare/v0.3.1...v0.3.2) (2023-06-26)


### Bug Fixes

* remove 0x prefix from outputs ([#107](https://github.com/hirosystems/ordinals-api/issues/107)) ([07c91b9](https://github.com/hirosystems/ordinals-api/commit/07c91b971f218bfb7b9615b282cefcffe0428648))

## [0.3.1](https://github.com/hirosystems/ordinals-api/compare/v0.3.0...v0.3.1) (2023-06-14)


### Bug Fixes

* automatic predicate registration ([#97](https://github.com/hirosystems/ordinals-api/issues/97)) ([73157f1](https://github.com/hirosystems/ordinals-api/commit/73157f13e8ef34b9df69701ac6e251f652f9ffbf))

## [0.3.0](https://github.com/hirosystems/ordinals-api/compare/v0.2.0...v0.3.0) (2023-06-08)


### Features

* support cursed inscriptions ([#85](https://github.com/hirosystems/ordinals-api/issues/85)) ([fb93474](https://github.com/hirosystems/ordinals-api/commit/fb9347452bdf261faa600c4785f67d2c99860ce8))

## [0.2.0](https://github.com/hirosystems/ordinals-api/compare/v0.1.2...v0.2.0) (2023-05-19)


### Features

* add endpoint to retrieve all inscription transfers per block ([#63](https://github.com/hirosystems/ordinals-api/issues/63)) ([e1afa7d](https://github.com/hirosystems/ordinals-api/commit/e1afa7dbaf29407c9ddacc775ce69782138c591f))
* add typescript client library ([#58](https://github.com/hirosystems/ordinals-api/issues/58)) ([23e48f1](https://github.com/hirosystems/ordinals-api/commit/23e48f138b0a92137fb350236e7c988ed3a45d72))


### Bug Fixes

* parse chainhooks satpoint to get offset ([#69](https://github.com/hirosystems/ordinals-api/issues/69)) ([73580fb](https://github.com/hirosystems/ordinals-api/commit/73580fb6c9ddab2bd6a3dde2cc90e408bfa7be7e))

## [0.1.2](https://github.com/hirosystems/ordinals-api/compare/v0.1.1...v0.1.2) (2023-05-15)


### Bug Fixes

* run prometheus server at port 9153 ([#75](https://github.com/hirosystems/ordinals-api/issues/75)) ([33823d2](https://github.com/hirosystems/ordinals-api/commit/33823d249027f351b842bd926dec9bc74ad6c2d1))

## [0.1.1](https://github.com/hirosystems/ordinals-api/compare/v0.1.0...v0.1.1) (2023-05-15)


### Bug Fixes

* allow empty NODE_ENV for prometheus ([#73](https://github.com/hirosystems/ordinals-api/issues/73)) ([e8dcd3b](https://github.com/hirosystems/ordinals-api/commit/e8dcd3bd7ab6adc0fd1d6dd015438545a645504b))

## [0.1.0](https://github.com/hirosystems/ordinals-api/compare/v0.0.1...v0.1.0) (2023-05-15)


### Features

* /ordinals prefix ([38d6c6b](https://github.com/hirosystems/ordinals-api/commit/38d6c6b6275c17e6039a5ac5c9e7724aeb80b5c9))
* accept inscription number on endpoints ([952bf8e](https://github.com/hirosystems/ordinals-api/commit/952bf8e8be7035f48a7252102a4c574e32e4f9ff))
* add rarity filter ([41f9207](https://github.com/hirosystems/ordinals-api/commit/41f92071be2beab390362140f075e248da630c19))
* add status endpoint ([e42b351](https://github.com/hirosystems/ordinals-api/commit/e42b351ce69b953c0aabec8ac29b773f3d7ae761))
* address array param ([5d3bc4b](https://github.com/hirosystems/ordinals-api/commit/5d3bc4bdcb3958e4f7a52bd32b0aaceae5d26b36))
* btc indexer poc, update types and tables ([#7](https://github.com/hirosystems/ordinals-api/issues/7)) ([6ff2bd9](https://github.com/hirosystems/ordinals-api/commit/6ff2bd94c7a48a44764b505a9ea387f63cf798ff))
* chain tip cache for index ([7019f98](https://github.com/hirosystems/ordinals-api/commit/7019f985d99e2d9f9c40b4e593404bd443c14194))
* configurable event server body limit ([1a37769](https://github.com/hirosystems/ordinals-api/commit/1a37769cdb4500e1141224fba953a9133f95a88e))
* consume inscription reveals from chainhook node ([#13](https://github.com/hirosystems/ordinals-api/issues/13)) ([a99b4a4](https://github.com/hirosystems/ordinals-api/commit/a99b4a4a1f0ecb5d7a5c7ee38994abc30bc0e796))
* filter by array of ids or numbers ([34721b0](https://github.com/hirosystems/ordinals-api/commit/34721b00df1e44e43b0366cd026c4da00bddcd51))
* filter by block hash ([ed73b78](https://github.com/hirosystems/ordinals-api/commit/ed73b785746cee0d05b9b14bb89f468e2b586b7d))
* filter by coinbase block height range ([5698ebc](https://github.com/hirosystems/ordinals-api/commit/5698ebce90a253b3f2898fb1e0880c9969e932f6))
* filter by genesis block height range ([4f11c44](https://github.com/hirosystems/ordinals-api/commit/4f11c44eec973ef44c238687c8c16dd9c1fab0c7))
* filter by genesis timestamp range ([67a0e9e](https://github.com/hirosystems/ordinals-api/commit/67a0e9eb6ae3ee78d9428c4b2a344edc51619959))
* filter by inscription number range ([f8453b6](https://github.com/hirosystems/ordinals-api/commit/f8453b6a87ee1fce6f35d67d9e521bf3d69914f5))
* filter by mime type ([f0093c7](https://github.com/hirosystems/ordinals-api/commit/f0093c712512bc3157331152eb5acc31d2a400f9))
* filter by rarity array ([4037af3](https://github.com/hirosystems/ordinals-api/commit/4037af340f811439e1ad7b01c7b5edec47433849))
* filter by sat ordinal range ([377ebb0](https://github.com/hirosystems/ordinals-api/commit/377ebb028c694ddf30a4450c457fd4dc6f27aabb))
* inscription cache control ([b9370d8](https://github.com/hirosystems/ordinals-api/commit/b9370d8d2bb994351f7e3ad54ce6748b4f298f2b))
* inscriptions index with filters ([790cbf7](https://github.com/hirosystems/ordinals-api/commit/790cbf7d848a94224f15ab5dce64e5d1e48f5629))
* move to new inscription_feed predicate ([#41](https://github.com/hirosystems/ordinals-api/issues/41)) ([efa4a62](https://github.com/hirosystems/ordinals-api/commit/efa4a6241db3de70a79b9a228a2c2ffa8e1fecd7))
* order index by ordinal, height, rarity ([aba47af](https://github.com/hirosystems/ordinals-api/commit/aba47af3ae36b6c1614112c189e471564a64b124))
* resume indexing from last observed block height ([fc4a549](https://github.com/hirosystems/ordinals-api/commit/fc4a549041131467cad0f3544a7371a3beafa517))
* return current tx_id ([a503f73](https://github.com/hirosystems/ordinals-api/commit/a503f7308414d99dbb4b7cbc64cd7f3d5bb10f86))
* return genesis_address ([ee1e4ea](https://github.com/hirosystems/ordinals-api/commit/ee1e4ea1e082521cc041971e41614c6efefb9018))
* return genesis_timestamp ([d46eb0f](https://github.com/hirosystems/ordinals-api/commit/d46eb0fcae3fbc7edafb70db5e73bd60a1968438))
* sat endpoint ([ff7608b](https://github.com/hirosystems/ordinals-api/commit/ff7608b63a1cc5d9f16d732bd0e023e48b3c3c69))
* shows inscription_id on sats ([1028a13](https://github.com/hirosystems/ordinals-api/commit/1028a13628249c4c746c9f3ccf011d0f54be3d6c))
* support authorized chainhook events ([a2ff106](https://github.com/hirosystems/ordinals-api/commit/a2ff1065aa4a64ab15d1d74209361707c82e704e))
* support inscription transfers ([#37](https://github.com/hirosystems/ordinals-api/issues/37)) ([ebeb805](https://github.com/hirosystems/ordinals-api/commit/ebeb8054980b18db90e038ecf525e2d93b45de28))


### Bug Fixes

* add inscription number to responses ([d4d36c2](https://github.com/hirosystems/ordinals-api/commit/d4d36c2409abfcc7fe33605fd50b9939590279d0))
* add vercel.json ([ae66a07](https://github.com/hirosystems/ordinals-api/commit/ae66a0728500ccec3351ace232040ad8e37208d5))
* adjust payloads ([#21](https://github.com/hirosystems/ordinals-api/issues/21)) ([d6d91e9](https://github.com/hirosystems/ordinals-api/commit/d6d91e9259f5086565dca4a2f9883698b110947b))
* allow max limit of 60 ([2391a75](https://github.com/hirosystems/ordinals-api/commit/2391a7537df6a8912081b98e8624d224c1976f6b))
* buffers ([ae45e06](https://github.com/hirosystems/ordinals-api/commit/ae45e06caa8a914fa1a29b73bc1f3021d2d42b9b))
* cache inscription counts for mime_type and sat_rarity ([#55](https://github.com/hirosystems/ordinals-api/issues/55)) ([f4fb4c7](https://github.com/hirosystems/ordinals-api/commit/f4fb4c75d8191b71c4d09443f698ec36491841dc))
* complete sat ordinal data ([56389b5](https://github.com/hirosystems/ordinals-api/commit/56389b5be589491be9f53d3f204cf478d456894b))
* env default values ([867fc63](https://github.com/hirosystems/ordinals-api/commit/867fc63764d167d048154da557f129228c100dae))
* expand int column sizes ([0e425ff](https://github.com/hirosystems/ordinals-api/commit/0e425ff42aa0f58c0203f4483acb1114f0e604e5))
* increase out-of-order log level to error ([#42](https://github.com/hirosystems/ordinals-api/issues/42)) ([56ca661](https://github.com/hirosystems/ordinals-api/commit/56ca6610fe707080272274d60672a65574547fa2))
* inscription number filtering ([adb5cf6](https://github.com/hirosystems/ordinals-api/commit/adb5cf6155a724c229e008a23d6dad15091eaa9e))
* set starting block height to 767430 ([84ce95e](https://github.com/hirosystems/ordinals-api/commit/84ce95ede9b6905c69eaff354a7ac331a5a4605a))
* use bignumber.js for sat percentiles ([8914e27](https://github.com/hirosystems/ordinals-api/commit/8914e27d4215470f9bea9a1f1e5339ffe38c31d0))
* use materialized view total count correctly ([#64](https://github.com/hirosystems/ordinals-api/issues/64)) ([939f6b9](https://github.com/hirosystems/ordinals-api/commit/939f6b987202f4c554aedc36a766f41f51bae434))
* use pre-calculated inscription count for unfiltered results ([#48](https://github.com/hirosystems/ordinals-api/issues/48)) ([3e7a4f4](https://github.com/hirosystems/ordinals-api/commit/3e7a4f41b0429a26cca1c1af544fff90ed8a3c33))

## [0.1.0-beta.14](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.13...v0.1.0-beta.14) (2023-05-11)


### Bug Fixes

* use materialized view total count correctly ([#64](https://github.com/hirosystems/ordinals-api/issues/64)) ([939f6b9](https://github.com/hirosystems/ordinals-api/commit/939f6b987202f4c554aedc36a766f41f51bae434))

## [0.1.0-beta.13](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.12...v0.1.0-beta.13) (2023-05-05)


### Bug Fixes

* use bignumber.js for sat percentiles ([8914e27](https://github.com/hirosystems/ordinals-api/commit/8914e27d4215470f9bea9a1f1e5339ffe38c31d0))

## [0.1.0-beta.12](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.11...v0.1.0-beta.12) (2023-05-05)


### Bug Fixes

* cache inscription counts for mime_type and sat_rarity ([#55](https://github.com/hirosystems/ordinals-api/issues/55)) ([f4fb4c7](https://github.com/hirosystems/ordinals-api/commit/f4fb4c75d8191b71c4d09443f698ec36491841dc))

## [0.1.0-beta.11](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.10...v0.1.0-beta.11) (2023-05-02)


### Bug Fixes

* expand int column sizes ([0e425ff](https://github.com/hirosystems/ordinals-api/commit/0e425ff42aa0f58c0203f4483acb1114f0e604e5))

## [0.1.0-beta.10](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.9...v0.1.0-beta.10) (2023-04-30)


### Bug Fixes

* use pre-calculated inscription count for unfiltered results ([#48](https://github.com/hirosystems/ordinals-api/issues/48)) ([3e7a4f4](https://github.com/hirosystems/ordinals-api/commit/3e7a4f41b0429a26cca1c1af544fff90ed8a3c33))

## [0.1.0-beta.9](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.8...v0.1.0-beta.9) (2023-04-24)


### Bug Fixes

* increase out-of-order log level to error ([#42](https://github.com/hirosystems/ordinals-api/issues/42)) ([56ca661](https://github.com/hirosystems/ordinals-api/commit/56ca6610fe707080272274d60672a65574547fa2))

## [0.1.0-beta.8](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.7...v0.1.0-beta.8) (2023-04-18)


### Features

* move to new inscription_feed predicate ([#41](https://github.com/hirosystems/ordinals-api/issues/41)) ([efa4a62](https://github.com/hirosystems/ordinals-api/commit/efa4a6241db3de70a79b9a228a2c2ffa8e1fecd7))

## [0.1.0-beta.7](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.6...v0.1.0-beta.7) (2023-04-12)


### Features

* support inscription transfers ([#37](https://github.com/hirosystems/ordinals-api/issues/37)) ([ebeb805](https://github.com/hirosystems/ordinals-api/commit/ebeb8054980b18db90e038ecf525e2d93b45de28))

## [0.1.0-beta.6](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.5...v0.1.0-beta.6) (2023-03-09)


### Features

* configurable event server body limit ([1a37769](https://github.com/hirosystems/ordinals-api/commit/1a37769cdb4500e1141224fba953a9133f95a88e))

## [0.1.0-beta.5](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.4...v0.1.0-beta.5) (2023-03-08)


### Bug Fixes

* set starting block height to 767430 ([84ce95e](https://github.com/hirosystems/ordinals-api/commit/84ce95ede9b6905c69eaff354a7ac331a5a4605a))

## [0.1.0-beta.4](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.3...v0.1.0-beta.4) (2023-03-08)


### Bug Fixes

* adjust payloads ([#21](https://github.com/hirosystems/ordinals-api/issues/21)) ([d6d91e9](https://github.com/hirosystems/ordinals-api/commit/d6d91e9259f5086565dca4a2f9883698b110947b))

## [0.1.0-beta.3](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.2...v0.1.0-beta.3) (2023-03-06)


### Features

* address array param ([5d3bc4b](https://github.com/hirosystems/ordinals-api/commit/5d3bc4bdcb3958e4f7a52bd32b0aaceae5d26b36))
* filter by array of ids or numbers ([34721b0](https://github.com/hirosystems/ordinals-api/commit/34721b00df1e44e43b0366cd026c4da00bddcd51))
* resume indexing from last observed block height ([fc4a549](https://github.com/hirosystems/ordinals-api/commit/fc4a549041131467cad0f3544a7371a3beafa517))
* return current tx_id ([a503f73](https://github.com/hirosystems/ordinals-api/commit/a503f7308414d99dbb4b7cbc64cd7f3d5bb10f86))

## [0.1.0-beta.2](https://github.com/hirosystems/ordinals-api/compare/v0.1.0-beta.1...v0.1.0-beta.2) (2023-03-03)


### Features

* add status endpoint ([e42b351](https://github.com/hirosystems/ordinals-api/commit/e42b351ce69b953c0aabec8ac29b773f3d7ae761))
* support authorized chainhook events ([a2ff106](https://github.com/hirosystems/ordinals-api/commit/a2ff1065aa4a64ab15d1d74209361707c82e704e))

## [0.1.0-beta.1](https://github.com/hirosystems/ordinals-api/compare/v0.0.1...v0.1.0-beta.1) (2023-03-03)


### Features

* /ordinals prefix ([38d6c6b](https://github.com/hirosystems/ordinals-api/commit/38d6c6b6275c17e6039a5ac5c9e7724aeb80b5c9))
* accept inscription number on endpoints ([952bf8e](https://github.com/hirosystems/ordinals-api/commit/952bf8e8be7035f48a7252102a4c574e32e4f9ff))
* add rarity filter ([41f9207](https://github.com/hirosystems/ordinals-api/commit/41f92071be2beab390362140f075e248da630c19))
* btc indexer poc, update types and tables ([#7](https://github.com/hirosystems/ordinals-api/issues/7)) ([6ff2bd9](https://github.com/hirosystems/ordinals-api/commit/6ff2bd94c7a48a44764b505a9ea387f63cf798ff))
* chain tip cache for index ([7019f98](https://github.com/hirosystems/ordinals-api/commit/7019f985d99e2d9f9c40b4e593404bd443c14194))
* consume inscription reveals from chainhook node ([#13](https://github.com/hirosystems/ordinals-api/issues/13)) ([a99b4a4](https://github.com/hirosystems/ordinals-api/commit/a99b4a4a1f0ecb5d7a5c7ee38994abc30bc0e796))
* filter by block hash ([ed73b78](https://github.com/hirosystems/ordinals-api/commit/ed73b785746cee0d05b9b14bb89f468e2b586b7d))
* filter by coinbase block height range ([5698ebc](https://github.com/hirosystems/ordinals-api/commit/5698ebce90a253b3f2898fb1e0880c9969e932f6))
* filter by genesis block height range ([4f11c44](https://github.com/hirosystems/ordinals-api/commit/4f11c44eec973ef44c238687c8c16dd9c1fab0c7))
* filter by genesis timestamp range ([67a0e9e](https://github.com/hirosystems/ordinals-api/commit/67a0e9eb6ae3ee78d9428c4b2a344edc51619959))
* filter by inscription number range ([f8453b6](https://github.com/hirosystems/ordinals-api/commit/f8453b6a87ee1fce6f35d67d9e521bf3d69914f5))
* filter by mime type ([f0093c7](https://github.com/hirosystems/ordinals-api/commit/f0093c712512bc3157331152eb5acc31d2a400f9))
* filter by rarity array ([4037af3](https://github.com/hirosystems/ordinals-api/commit/4037af340f811439e1ad7b01c7b5edec47433849))
* filter by sat ordinal range ([377ebb0](https://github.com/hirosystems/ordinals-api/commit/377ebb028c694ddf30a4450c457fd4dc6f27aabb))
* inscription cache control ([b9370d8](https://github.com/hirosystems/ordinals-api/commit/b9370d8d2bb994351f7e3ad54ce6748b4f298f2b))
* inscriptions index with filters ([790cbf7](https://github.com/hirosystems/ordinals-api/commit/790cbf7d848a94224f15ab5dce64e5d1e48f5629))
* order index by ordinal, height, rarity ([aba47af](https://github.com/hirosystems/ordinals-api/commit/aba47af3ae36b6c1614112c189e471564a64b124))
* return genesis_address ([ee1e4ea](https://github.com/hirosystems/ordinals-api/commit/ee1e4ea1e082521cc041971e41614c6efefb9018))
* return genesis_timestamp ([d46eb0f](https://github.com/hirosystems/ordinals-api/commit/d46eb0fcae3fbc7edafb70db5e73bd60a1968438))
* sat endpoint ([ff7608b](https://github.com/hirosystems/ordinals-api/commit/ff7608b63a1cc5d9f16d732bd0e023e48b3c3c69))
* shows inscription_id on sats ([1028a13](https://github.com/hirosystems/ordinals-api/commit/1028a13628249c4c746c9f3ccf011d0f54be3d6c))


### Bug Fixes

* add inscription number to responses ([d4d36c2](https://github.com/hirosystems/ordinals-api/commit/d4d36c2409abfcc7fe33605fd50b9939590279d0))
* add vercel.json ([ae66a07](https://github.com/hirosystems/ordinals-api/commit/ae66a0728500ccec3351ace232040ad8e37208d5))
* allow max limit of 60 ([2391a75](https://github.com/hirosystems/ordinals-api/commit/2391a7537df6a8912081b98e8624d224c1976f6b))
* buffers ([ae45e06](https://github.com/hirosystems/ordinals-api/commit/ae45e06caa8a914fa1a29b73bc1f3021d2d42b9b))
* complete sat ordinal data ([56389b5](https://github.com/hirosystems/ordinals-api/commit/56389b5be589491be9f53d3f204cf478d456894b))
* env default values ([867fc63](https://github.com/hirosystems/ordinals-api/commit/867fc63764d167d048154da557f129228c100dae))
* inscription number filtering ([adb5cf6](https://github.com/hirosystems/ordinals-api/commit/adb5cf6155a724c229e008a23d6dad15091eaa9e))
