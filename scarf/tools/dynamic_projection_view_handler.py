from types import GenericAlias
from typing import Type, Any, Optional, Annotated

from annotated_types import BaseMetadata
from pydantic.fields import FieldInfo
from pydantic import BaseModel, create_model, StringConstraints, Field
from beanie import Link, PydanticObjectId as ObjectId

from scarf.tools.annotation_simplifier import simplify_special_annotations


def get_projection_view(
        model: Type[BaseModel],
        desired_fields: list[str] | set[str] | tuple,
        custom_fields_annotations: dict[str, tuple[Any, Any]] | None = None,
        all_fields_as_optional: bool = False,
        must_be_required_fields: list[str] | set[str] | None = None,
        use_aliases: bool = False,
        view_model_name: str | None = None
) -> Type[BaseModel]:
    """Returns a projection model from the model with the desired fields only.

    Args:
        model: The model to inspect.
        desired_fields: Name of the fields from model to exist in the output view model. (FIELD NAMES, not the aliases)
        custom_fields_annotations: If you want some existing fields to have different annotation in the view model
            compared to the main model, or add some new custom fields that do not exist in the main model; pass them as
            a dict with field names as keys and a tuple containing annotation and default value as its value.
        all_fields_as_optional: If True, none of the fields will be required in the output view model.
        must_be_required_fields: Name of the optional fields in the main model to be required in the output view model;
            these fields will be required even if `all_fields_as_optional` is set to True. (FIELD NAMES, not the aliases)
        use_aliases: If True, the aliases of fields (if exist) will be used as field names in the output view model.
        view_model_name: The name of the returned class, `View` will be concatenated to the model name if nothing passed.
    """
    if not desired_fields:
        raise ValueError('desired_fields cannot be empty.')

    desired_fields = set(desired_fields)
    must_be_required_fields = set(must_be_required_fields) if must_be_required_fields else set()

    def must_be_optional(field_name: str) -> bool:
        return all_fields_as_optional and field_name not in must_be_required_fields

    projection_model_fields = {
        get_proper_key(field_name, field_info, use_aliases): get_proper_value(field_info, must_be_optional(field_name))
        for field_name, field_info in model.model_fields.items()
        if field_name in desired_fields or field_info.alias in desired_fields
    }

    if all_fields_as_optional and 'id' in desired_fields:
        projection_model_fields['id'] = (ObjectId, ...)

    if len(projection_model_fields) != len(desired_fields):
        invalid_fields = ', '.join([field for field in desired_fields if field not in model.model_fields])
        raise KeyError(
            f'{invalid_fields} field(s) does not exist in {model.__name__} to include in projection view model.'
        )

    view_model_name = view_model_name or (model.__name__ + 'View')
    view_model_fields = projection_model_fields | (custom_fields_annotations or {})
    View = create_model(view_model_name, **view_model_fields)

    return View


def get_proper_key(field_name: str, field_info: FieldInfo, use_alias: bool):
    if not use_alias:
        return field_name
    else:
        if field_info.alias and field_info.alias.startswith('_'):
            return field_name  # pydantic will raise error if field name starts with underscore
        else:
            return field_info.alias or field_name


def get_proper_value(field_info: FieldInfo, must_be_optional: bool):
    default_value = field_info.default if not must_be_optional else None
    if not field_info.metadata:
        return get_proper_annotation(field_info.annotation, must_be_optional), default_value

    elif isinstance(field_info.metadata[0], StringConstraints):
        return Annotated[str, field_info.metadata[0]], default_value

    else:
        return get_proper_annotation(field_info.annotation, must_be_optional), Field(
            default=default_value, **parse_metadata(field_info.metadata)
        )


def get_proper_annotation(annotation: Type | Link | GenericAlias, get_as_optional: bool = False) -> Type:
    """Handles beanie.odm.fields.Link annotations."""
    annotation_info = simplify_special_annotations(annotation, filter_object_id=False)
    simplified_annotation = annotation_info.simplified

    if simplified_annotation == 'link':
        proper_annotation = ObjectId

    elif simplified_annotation == 'link_list':
        annotation = annotation.__args__[0] if annotation_info.is_optional else annotation
        proper_annotation = annotation.__origin__[ObjectId]

    else:
        proper_annotation = annotation

    if get_as_optional or annotation_info.is_optional:
        return Optional[proper_annotation]

    else:
        return proper_annotation


def parse_metadata(metadata_list: list[BaseMetadata]):
    def get_metadata_key(metadata: BaseMetadata):
        return list(metadata.__annotations__)[0]

    return {get_metadata_key(metadata): getattr(metadata, get_metadata_key(metadata)) for metadata in metadata_list}
