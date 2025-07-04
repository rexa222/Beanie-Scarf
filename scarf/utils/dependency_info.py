from typing import Type, TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from scarf.scarf_document import ScarfDocument


class LinkInfo(BaseModel):
    linked_class: Type[ScarfDocument] | Type[BaseModel] | str
    field_name: str
    is_list: bool = False
    validate_dynamically: bool = True


class DependantDocInfo(BaseModel):
    document_name: str = Field(description='Name of the document that has link to the current document')
    field_name: str = Field(description='Name of the field in the dependant document that is linked')
    module_address: str = Field(description='Address of the module where the dependant document can be imported from')