from typing import Type, TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from scarf.scarf_document import ScarfDocument


class LinkInfo(BaseModel):
    linked_document: Type[ScarfDocument]
    field_name: str
    is_list: bool = Field(default=False, description='The linked field annotation is a list of links or not')
    validate_existence: bool = Field(default=True, description='The existence must be validated dynamically in '
                                                               '`validate_linked_values_existence` or not')


class DependantDocInfo(BaseModel):
    document_name: str = Field(description='Name of the document that has link to the current document')
    field_name: str = Field(description='Name of the field in the dependant document that is linked')
    module_address: str = Field(description='Address of the module where the dependant document can be imported from')