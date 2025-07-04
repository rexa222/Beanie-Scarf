from datetime import date

from pydantic.fields import FieldInfo

from dataflake.tools import simplify_special_annotations


def get_projection_value_by_annotation(
        field_info: FieldInfo,
        field_prefix: str | None = None
) -> str | dict:
    annotation = field_info.annotation
    simplified_annotation = simplify_special_annotations(annotation, filter_object_id=True).simplified

    field_name = field_info.alias
    field_referer = f'${field_prefix}.{field_name}' if field_prefix else '$' + field_name
    field_id_referer = field_referer + '.$id'

    if annotation == date:
        return {'$dateToString': {
            'format': '%Y-%m-%d',
            'date': field_referer
        }}

    elif simplified_annotation == 'normal':
        return field_referer

    elif simplified_annotation == 'oid':
        return {'$toString': field_referer}

    elif simplified_annotation == 'oid_list':
        return {
            '$map': {
                'input': field_referer,
                'as': 'oid',
                'in': {'$toString': '$$oid'},
            }
        }

    elif simplified_annotation == 'link':
        return {'$toString': field_id_referer}

    elif simplified_annotation == 'link_list':
        return {
            '$map': {
                'input': field_referer,
                'as': 'link',
                'in': {'$toString': '$$link.$id'},
            }
        }
