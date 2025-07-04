from re import escape as escape_re_special_characters

from dataflake.data_models import SearchDetails


def compile_search_details_to_pattern(search_details: SearchDetails | list[SearchDetails]) -> str:
    """Compiles search details to regex pattern."""
    if isinstance(search_details, SearchDetails):
        search_details = [search_details]

    patterns = []

    for sd in search_details:
        search_value = escape_re_special_characters(sd.value)

        if sd.starts_with_value:
            pattern = f'^{search_value}'

        else:
            pattern = search_value

        if not sd.case_sensitive:
            pattern = '(?i)' + pattern

        patterns.append(pattern)

    return '|'.join(patterns)
