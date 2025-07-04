import logging

from pymongo.errors import BulkWriteError


def handle_bulk_write_error(bwe: BulkWriteError) -> dict:
    error = bwe.details

    duplication_errors = [str(e['keyValue']) for e in error['writeErrors'] if e['code'] == 11000]
    logging.warning(f'Duplicate records existed and were skipped. Details: {duplication_errors}')

    non_duplication_errors = [e['errmsg'] for e in error['writeErrors'] if e['code'] != 11000]
    if non_duplication_errors:
        logging.error(f'-- UNKNOWN ERROR(S) IN BULK ADD --\nNon-duplication errors: {non_duplication_errors}')

    return {
        'duplication_errors': duplication_errors, 'non_duplication_errors': non_duplication_errors
    }
