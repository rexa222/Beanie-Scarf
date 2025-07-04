from typing import Type

from beanie import Document


def get_field_proper_key(model: Type[Document], field: str, error_detail: str | None = None):
    """Prevents attr error if methods are used from a parent documents that is not initialized but actually have
    the field."""
    if field not in model.model_fields:
        raise AttributeError(error_detail or f'Required field `{field}` is missing in {model.__name__}.')

    key = getattr(model, field, None)

    if key is None:
        field_info = model.model_fields[field]
        key = field_info.alias or field

    return key
