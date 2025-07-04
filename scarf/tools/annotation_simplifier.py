from types import GenericAlias, NoneType
from typing import Type, Literal, get_origin

from beanie import Link
from pydantic import BaseModel

SIMPLIFIED_ANNOTATIONS = Literal['normal', 'link', 'link_list', 'oid', 'oid_list']


class AnnotationInfo(BaseModel):
    simplified: SIMPLIFIED_ANNOTATIONS
    is_optional: bool = False


def simplify_special_annotations(annotation: Type | Link | GenericAlias, filter_object_id: bool) -> AnnotationInfo:
    annotation_str = str(annotation)

    if filter_object_id and 'ObjectId' in annotation_str:
        # sometimes ObjectId type is not recognized! so checking the str method is used
        if 'ObjectId' in annotation_str and get_origin(annotation) in [list, set]:
            return AnnotationInfo(simplified='oid_list')

        elif NoneType in getattr(annotation, '__args__', []):
            main_ann = annotation.__args__[0]
            simplified_ann = 'oid_list' if isinstance(main_ann, GenericAlias) else 'oid'
            return AnnotationInfo(simplified=simplified_ann, is_optional=True)

        else:
            return AnnotationInfo(simplified='oid')

    if 'beanie.odm.fields.Link' not in annotation_str:
        return AnnotationInfo(simplified='normal')

    # sometimes Link type is not recognized! so checking the str method is used
    elif isinstance(annotation, Link) or annotation_str.startswith('beanie.odm.fields.Link['):
        return AnnotationInfo(simplified='link')

    # list (or other possible generics) of beanie Link annotations
    elif get_origin(annotation) is list:
        return AnnotationInfo(simplified='link_list')

    elif NoneType in getattr(annotation, '__args__', []):
        main_ann = annotation.__args__[0]
        simplified_ann = 'link_list' if isinstance(main_ann, GenericAlias) else 'link'
        return AnnotationInfo(simplified=simplified_ann, is_optional=True)

    elif get_origin(annotation) is set:
        raise TypeError(
            'Defining a field with `set[Link[...]]` will not work properly and will cause unexpected behaviour. '
            'Define it as `list[Link[...]]` and analyze the values to be linked before creating `beanie.Link` objects.'
        )

    else:
        raise TypeError(f'Unexpected annotation: {annotation}')
