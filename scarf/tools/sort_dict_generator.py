from typing import Literal

from pymongo import ASCENDING, DESCENDING

SORT_VALUE_MAPPER = {'asc': ASCENDING, 'desc': DESCENDING}


def get_sort_dict_for_pipeline(sort_key_order_mapper: dict[str, Literal['asc', 'desc']]) -> dict[str, int]:
    """Get MongoDB sort dict for aggregation pipeline."""
    return {
        sort_key: SORT_VALUE_MAPPER[sort_order] for sort_key, sort_order in sort_key_order_mapper.items()
    }
