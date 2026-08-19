"""Response serialization. One rule: the same body always renders to the same bytes."""

import json
from typing import Any

from fastapi.responses import JSONResponse


class CanonicalJSONResponse(JSONResponse):
    """JSON with sorted keys.

    An original response is serialized from the dict the write path just built.
    Its replay is serialized from the same dict after a round trip through JSONB,
    and Postgres does not preserve key order -- it stores keys by length, then
    bytewise. Sorting removes that difference, so the two are byte-identical and
    a retrying client genuinely cannot tell them apart (spec 11).
    """

    def render(self, content: Any) -> bytes:
        # Starlette's defaults, plus sort_keys.
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
