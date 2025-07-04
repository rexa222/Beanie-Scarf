from types import GenericAlias
from typing import Type, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from beanie import Document
from beanie.odm.operators.find.array import BaseFindArrayOperator
from beanie.odm.operators.find.element import BaseFindElementOperator
from beanie.odm.operators.find.evaluation import BaseFindEvaluationOperator
from beanie.odm.operators.find.comparison import BaseFindComparisonOperator


class SearchDetails(BaseModel):
    value: str
    case_sensitive: bool = False
    starts_with_value: bool = False


class FilterableFieldInfo(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    field: str
    annotation: type | GenericAlias
    operator: Union[
        Type[BaseFindArrayOperator],
        Type[BaseFindElementOperator],
        Type[BaseFindEvaluationOperator],
        Type[BaseFindComparisonOperator]
    ]
    is_link: bool = False

    belongs_to_linked_class: Type[Document] | None = None  # must be set if the field belongs to another class that is linked
    linked_field: str | None = None  # the field in main class that is linked to external class

    compile_dynamically: bool = True

    @model_validator(mode='after')
    def validate_field_of_linked_class(self):
        special_fields_mapper = {
            'belongs_to_linked_class': self.belongs_to_linked_class, 'linked_field': self.linked_field
        }
        if any(special_fields_mapper.values()) and not all(special_fields_mapper.values()):
            raise ValueError(
                f'Either all or none of these variables must be passed: {tuple(special_fields_mapper.keys())}'
            )
        return self


class AdvancedFilters(BaseModel):
    pre_fetch: dict = Field(default={})
    post_fetch: dict = Field(default={})
