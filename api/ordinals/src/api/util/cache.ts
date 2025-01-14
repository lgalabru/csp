import { FastifyReply, FastifyRequest } from 'fastify';
import { InscriptionIdParamCType, InscriptionNumberParamCType } from '../schemas';
import { CACHE_CONTROL_MUST_REVALIDATE, parseIfNoneMatchHeader } from '@hirosystems/api-toolkit';

enum ETagType {
  inscriptionsIndex,
  inscription,
  inscriptionsPerBlock,
}

export async function handleInscriptionCache(request: FastifyRequest, reply: FastifyReply) {
  return handleCache(ETagType.inscription, request, reply);
}

export async function handleInscriptionTransfersCache(
  request: FastifyRequest,
  reply: FastifyReply
) {
  return handleCache(ETagType.inscriptionsIndex, request, reply);
}

export async function handleInscriptionsPerBlockCache(
  request: FastifyRequest,
  reply: FastifyReply
) {
  return handleCache(ETagType.inscriptionsPerBlock, request, reply);
}

async function handleCache(type: ETagType, request: FastifyRequest, reply: FastifyReply) {
  const ifNoneMatch = parseIfNoneMatchHeader(request.headers['if-none-match']);
  let etag: string | undefined;
  switch (type) {
    case ETagType.inscription:
      etag = await getInscriptionLocationEtag(request);
      break;
    case ETagType.inscriptionsIndex:
      etag = await getInscriptionsIndexEtag(request);
      break;
    case ETagType.inscriptionsPerBlock:
      etag = await request.server.db.getInscriptionsPerBlockETag();
      break;
  }
  if (etag) {
    if (ifNoneMatch && ifNoneMatch.includes(etag)) {
      await reply.header('Cache-Control', CACHE_CONTROL_MUST_REVALIDATE).code(304).send();
    } else {
      void reply.headers({ 'Cache-Control': CACHE_CONTROL_MUST_REVALIDATE, ETag: `"${etag}"` });
    }
  }
}

/**
 * Retrieve the inscriptions's location timestamp as a UNIX epoch so we can use it as the response
 * ETag.
 * @param request - Fastify request
 * @returns Etag string
 */
async function getInscriptionLocationEtag(request: FastifyRequest): Promise<string | undefined> {
  try {
    const components = request.url.split('/');
    do {
      const lastElement = components.pop();
      if (lastElement && lastElement.length) {
        if (InscriptionIdParamCType.Check(lastElement)) {
          return await request.server.db.getInscriptionETag({ genesis_id: lastElement });
        } else if (InscriptionNumberParamCType.Check(parseInt(lastElement))) {
          return await request.server.db.getInscriptionETag({ number: lastElement });
        }
      }
    } while (components.length);
  } catch (error) {
    return;
  }
}

/**
 * Get an ETag based on the last state of all inscriptions.
 * @param request - Fastify request
 * @returns ETag string
 */
async function getInscriptionsIndexEtag(request: FastifyRequest): Promise<string | undefined> {
  try {
    return await request.server.db.getInscriptionsIndexETag();
  } catch (error) {
    return;
  }
}
