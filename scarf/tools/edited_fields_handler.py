from typing import Type
from types import GenericAlias

from beanie import Link
from pydantic import BaseModel


def get_edited_fields_data(
        model: Type[BaseModel], current_record: BaseModel, new_data: BaseModel, fields_to_check: set = None
) -> dict:
    fields_to_check = fields_to_check - {'id'} if fields_to_check is not None else new_data.model_fields_set - {'id'}

    if not fields_to_check.issubset(new_data.model_fields):
        missing_fields = {field for field in fields_to_check if field not in new_data.model_fields}
        raise AttributeError(
            f'{new_data.__name__} does not have the following fields to check if they were edited: {missing_fields}'
        )

    if not fields_to_check.issubset(model.model_fields):
        missing_fields = {field for field in fields_to_check if field not in model.model_fields}
        raise AttributeError(
            f'{model.__name__} does not have the following fields to check if they were edited: {missing_fields}'
        )

    return {
        field_name: getattr(new_data, field_name)
        for field_name in fields_to_check
        if check_difference_with_field_value(
            current_value=getattr(current_record, field_name),
            new_value=getattr(new_data, field_name),
            field_annotation=model.model_fields[field_name].annotation
        )
    }


def check_difference_with_field_value(
        current_value, new_value, field_annotation: Type | Link | GenericAlias
) -> bool:
    """
    Checks if current value is different with new_value and handles beanie Link type in comparison.

    Returns:
        bool: True if current value is different with new_value, False otherwise.
    """
    if 'beanie.odm.fields.Link' in str(field_annotation):
        if isinstance(current_value, list):
            current_value = {link.ref.id for link in current_value}
            new_value = set(new_value)

        elif isinstance(current_value, set):
            current_value = {link.ref.id for link in current_value}

        elif isinstance(current_value, Link):
            current_value = current_value.ref.id

        elif current_value is not None:
            raise TypeError(f'Unexpected type for current_value: {field_annotation}.')

    return new_value != current_value
