## @hirosystems/ordinals-api-client

This is a client library for the [Ordinals API](https://github.com/hirosystems/ordinals-api).

### Installation

```
npm install @hirosystems/ordinals-api-client
```

### Example

```typescript
import { Configuration, InscriptionsApi } from "@hirosystems/ordinals-api-client";

const config = new Configuration();
const api = new InscriptionsApi(config);
const result = await api.getInscription("200000")
```
