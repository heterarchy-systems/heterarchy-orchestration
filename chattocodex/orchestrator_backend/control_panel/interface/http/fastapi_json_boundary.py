from collections.abc import Awaitable, Callable

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError


def build_strict_json_parser[SchemaT: BaseModel](
    schema_type: type[SchemaT],
) -> Callable[[Request], Awaitable[SchemaT]]:
    """Build a FastAPI dependency that preserves Pydantic strict JSON semantics. Args: schema_type. Returns: Callable[[Request], Awaitable[SchemaT]]."""

    async def parse(request: Request) -> SchemaT:
        """Parse. Args: request. Returns: SchemaT."""
        try:
            return schema_type.model_validate_json(await request.body())
        except ValidationError as error:
            raise RequestValidationError(error.errors()) from error

    return parse
