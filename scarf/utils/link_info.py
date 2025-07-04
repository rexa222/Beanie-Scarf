from typing import Type, TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from scarf.scarf_document import ScarfDocument


class LinkInfo(BaseModel):
    linked_class: Type[ScarfDocument] | Type[BaseModel] | str
    field_name: str
    is_list: bool = False
    validate_dynamically: bool = True
