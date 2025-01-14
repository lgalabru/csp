---
Title: Overview
---

# Ordinals API Overview

The Ordinals API provides a service that indexes Bitcoin Ordinals data and offers a REST API to access and query this data.

> **_NOTE:_**
>
> To explore the detailed documentation for the API endpoints, request and response formats, you can refer to the [OpenAPI specification](https://docs.hiro.so/ordinals).
>
> The source code for this project is available in our [GitHub repository](https://github.com/hirosystems/ordinals-api). You can explore the codebase, [contribute](https://docs.hiro.so/contributors-guide), and raise [issues](https://github.com/hirosystems/ordinals-api/issues) or [pull requests](https://github.com/hirosystems/ordinals-api/pulls).

Here are the key features of the Ordinals API:

**Ordinal Inscription Ingestion**: The API helps with the complete ingestion of ordinal inscriptions. Using our endpoitns, you can retrieve the metadata for a particular inscription, all inscriptions held by a particular address, trading activity for inscriptions, and more.

**BRC-20 Support**: The API offers support for BRC-20 tokens, a fungible token standard built on top of ordinal theory. Retrieve data for a particular BRC-20 token, a user's BRC-20 holdings, marketplace activity, and more.

**REST JSON Endpoints with ETag Caching**: The API provides easy-to-use REST endpoints that return responses in JSON format. It also supports *ETag caching*, which allows you to cache responses based on inscriptions. This helps optimize performance and reduce unnecessary requests.

**Auto-Scale Server Configurations**: The Ordinals API supports three run modes based on the `RUN_MODE` environment variable:

- `default`: This mode runs all background jobs and the API server. It is suitable for running a single instance of the API.
- `readonly`: Only the API server runs in this mode. It is designed for auto-scaled clusters with multiple `readonly` instances and a single `writeonly` instance. The `writeonly` instance is responsible for populating the database.
- `writeonly`: This mode is used in an auto-scaled environment to consume new inscriptions and push that data to a database. It works in conjunction with multiple `readonly` instances.
