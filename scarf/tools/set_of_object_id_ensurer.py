from bson import ObjectId


def get_set_of_object_ids(object_id_or_list: ObjectId | list[ObjectId] | set[ObjectId]) -> set[ObjectId]:
    """Ensures to return a set of ObjectIds after receiving either an ObjectId or a list or set of ObjectIds."""
    if type(object_id_or_list) in [set, list]:
        return set(object_id_or_list)

    elif isinstance(object_id_or_list, ObjectId):
        return {object_id_or_list}

    else:
        raise ValueError('object_id_or_list must be an ObjectId or a list of ObjectIds.')
