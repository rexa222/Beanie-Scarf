import logging
from collections import defaultdict
from typing import Any, Self, Type, Literal, Optional

from beanie import Document as BeanieDocument, PydanticObjectId as ObjectId
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.operators import In, All, Eq
from pydantic import BaseModel, create_model, model_validator

from scarf.tools import get_projection_view, get_projection_value_by_annotation, get_sort_dict_for_pipeline, \
    compile_search_details_to_pattern, get_set_of_object_ids, get_class
from scarf.utils import LinkInfo, DependantDocInfo, AdvancedFilters, FilterableFieldInfo, SearchDetails


class ScarfDocument(BeanieDocument):
    __db_name__: str

    __fields_to_exclude__: tuple[str] = ('revision_id',)  # MODEL FIELD NAMES, not the aliases

    # When the class is linked in another class and is fetched, only these fields will be shown:
    __main_fields_for_compact_view__: tuple[str] | None = None  # MODEL FIELD NAMES, not the aliases

    # All fields that are linked to other documents info must be contained in this list for dynamic projections to work:
    __linked_fields_info__: list[LinkInfo] = None

    # The info of the classes that have links to this class; necessary for `get_dependent_records_count_per_model`:
    __dependent_models_info__: list[DependantDocInfo] = None

    __sortable_fields__: tuple[str] = None  # Include ALIASES if exists (DB field not model field)
    __special_sortable_fields__: tuple[str] = None  # Fields' existence in the model won't be checked if included here

    __filterable_fields_info__: dict[str, FilterableFieldInfo] | None = None

    __never_fetch_fields__: tuple[str] = tuple()
    __always_fetch_fields__: tuple[str] = tuple()

    @model_validator(mode='after')
    def validate_linked_values_existence(self):
        for link_info in self.__linked_fields_info__:
            if link_info.validate_dynamically:
                link_info.linked_document.validate_records_existence(getattr(self, link_info.field_name))

    # ----- HIERARCHICAL ATTRIBUTES -----

    @classmethod
    def get_fields_to_exclude(cls, temp_exclude: str | set[str] | None = None) -> set[str]:
        """Field names that should not be included in document projections.

        Combines all `__fields_to_exclude__` attribute in every child class in the hierarchy.
        The mentioned attribute must contain MODEL FIELD NAMES, not the aliases; or else they will not be excluded.

        Args:
            temp_exclude: Temporary adds the given value(s) to the default fields to be excluded.
                make sure to pass MODEL FIELD NAMES, not the aliases.

        Returns:
            A set of field names that should not be included in document projections
        """
        fields_to_exclude = {
            field
            for _cls in reversed(cls.mro()[:-10])
            if _cls.__fields_to_exclude__
            for field in _cls.__fields_to_exclude__
        }

        if isinstance(temp_exclude, str):
            return fields_to_exclude | {temp_exclude}

        return fields_to_exclude | temp_exclude if temp_exclude else fields_to_exclude

    @classmethod
    def get_linked_fields_info(cls) -> list[LinkInfo]:
        """Returns info about fields in the document model that are linked to other documents."""
        prevent_duplicate = set()
        return [
            link_info
            for _cls in reversed(cls.mro()[:-10])
            if _cls.__linked_fields_info__
            for link_info in _cls.__linked_fields_info__
            if link_info.field_name not in prevent_duplicate and not prevent_duplicate.add(link_info.field_name)
        ]

    @classmethod
    def get_sortable_fields(cls):
        """Returns sortable fields as a literal."""
        sortable_fields = tuple(dict.fromkeys(
            field
            for _cls in reversed(cls.mro()[:-10])
            if _cls.__sortable_fields__
            for field in _cls.__sortable_fields__
        ))

        model_db_fields = {'time'} | {field.alias for field in cls.model_fields.values()}
        if not model_db_fields.issuperset(sortable_fields):
            invalid_fields = set(sortable_fields) - model_db_fields
            raise AttributeError(
                f'{cls.__name__} does not have following attribute(s): {invalid_fields}. \n'
                'If the field exists but has an alias, the alias must be included instead of the model field.\n'
                'If it is a special sort param that is handled, include it in `__special_sortable_fields__` attribute.'
            )

        if cls.__special_sortable_fields__:
            sortable_fields += cls.__special_sortable_fields__

        return Literal[sortable_fields]

    @classmethod
    def get_filterable_fields_info(cls) -> dict[str, FilterableFieldInfo]:
        """Returns info about fields in the document model that are filterable in finding records."""
        return {
            k: v
            for _cls in reversed(cls.mro()[:-10])
            if _cls.__filterable_fields_info__
            for k, v in _cls.__filterable_fields_info__.items()
        }

    # ----- SCHEMAS & VIEWS -----

    @classmethod
    def get_schema_for_filters(cls) -> Type[BaseModel]:
        """Returns a model for dynamic filters in getting records based on defined filterable fields info."""
        schema_model_fields = {
            field_name: (Optional[field_info.annotation], None)
            for field_name, field_info in cls.get_filterable_fields_info().items()
        }

        cls.__filters_schema__ = create_model(cls.__name__ + 'FiltersSchema', **schema_model_fields)

        return cls.__filters_schema__

    @classmethod
    def get_projection_view(
            cls,
            desired_fields: list[str] | set[str] | tuple,
            custom_fields_annotations: dict[str, tuple[Any, Any]] | None = None,
            all_fields_as_optional: bool = False,
            must_be_required_fields: list[str] | set[str] | None = None,
            use_aliases: bool = False,
            view_model_name: str | None = None
    ) -> Type[BaseModel]:
        """Creates a projection model that contains only the desired fields from the document.

        Args:
            desired_fields: Name of the fields from model to exist in the output view model.
                (MODEL FIELD NAMES, not the aliases)
            custom_fields_annotations: If you want some existing fields to have different annotation in the view model
                compared to the main model, or add some new custom fields that do not exist in the main model; pass them
                as a dict with field names as keys and a tuple containing annotation and default value as its value.
            all_fields_as_optional: If True, none of the fields will be required in the output view model.
            must_be_required_fields: Name of the optional fields in the main model to be required in the output view
                model; these fields will be required even if `all_fields_as_optional` is set to True.
                (MODEL FIELD NAMES, not the aliases)
            use_aliases: If True, the aliases of fields (if exist) will be used as field names in the output view model.
            view_model_name: The name of the returned class, `View` will be concatenated to the model name if nothing
                passed.

        Returns:
            A pydantic base model that contains only the desired fields.
        """
        return get_projection_view(
            cls, desired_fields, custom_fields_annotations, all_fields_as_optional, must_be_required_fields,
            use_aliases, view_model_name
        )

    # ----- PROJECTION TOOLS -----

    @classmethod
    def get_projection_pipeline_for_linked_field(
            cls,
            desired_fields: list[str] | set[str],
            linked_field_name: str,
            is_list_of_links: bool,
    ) -> dict[str, dict]:
        """Returns a MongoDB aggregation pipeline for the fetched linked documents."""
        model_fields = cls.model_fields.copy()
        model_fields.pop('revision_id')

        if is_list_of_links:
            set_fields_dict = {
                linked_field_name: {
                    '$map': {
                        'input': f'${linked_field_name}',
                        'as': 'link',
                        'in': {
                            field_info.alias: get_projection_value_by_annotation(
                                field_info, field_prefix='$link'
                            )
                            for field_name, field_info in model_fields.items()
                            if field_name in desired_fields
                        } if len(desired_fields) > 1
                        else  # Append the single value to the list directly, rather than inside a dict
                        get_projection_value_by_annotation(
                            model_fields[desired_fields[0]], field_prefix='$link'
                        ),
                    }
                }
            }
        else:
            if len(desired_fields) > 1:
                set_fields_dict = {
                    f'{linked_field_name}.{field_info.alias}': get_projection_value_by_annotation(
                        field_info, field_prefix=linked_field_name
                    )
                    for field_name, field_info in model_fields.items()
                    if field_name in desired_fields
                }
            else:  # Assign the single value to the linked field key directly, rather than inside a dict
                set_fields_dict = {linked_field_name: get_projection_value_by_annotation(
                    model_fields[desired_fields[0]], field_prefix=linked_field_name
                )}

        # Excluding all non-desired fields
        projection_dict = {
            f'{linked_field_name}.{field_info.alias}': 0
            for field_name, field_info in model_fields.items()
            if field_name not in desired_fields
        }

        return {'$addFields': set_fields_dict, '$project': projection_dict}

    @classmethod
    def get_projection_pipeline(
            cls,
            desired_fields: list[str] | set[str],
            links_are_fetched: bool = True,
            ignore_always_and_never_fetch_fields: bool = False,
    ) -> list[dict[str, dict]]:
        """Returns a MongoDB projection dict from the model with the desired fields only."""
        desired_fields = set(desired_fields)

        projection_dict = {
            field_info.alias: get_projection_value_by_annotation(field_info)
            for field_name, field_info in cls.model_fields.items()
            if field_name in desired_fields
        }

        pipeline = [{'$project': projection_dict}]

        fields_to_be_fetched = desired_fields if links_are_fetched else set()
        if not ignore_always_and_never_fetch_fields:
            fields_to_be_fetched.update(desired_fields.intersection(cls.__always_fetch_fields__))
            fields_to_be_fetched -= set(cls.__never_fetch_fields__)

        if fields_to_be_fetched:
            links_fields_addition_dict = {}
            links_projection_dict = {}
            for link_info in cls.get_linked_fields_info():
                if (
                        link_info.field_name in fields_to_be_fetched and
                        cls.__db_name__ == getattr(link_info.linked_document, '__db_name__', None)
                ):
                    compact_fields = link_info.linked_document.__main_fields_for_compact_view__

                    link_projection_pipeline = link_info.linked_document.get_projection_pipeline_for_linked_field(
                        compact_fields or link_info.linked_document.get_default_projection_fields(),
                        link_info.field_name,
                        link_info.is_list
                    )

                    projection_dict[link_info.field_name] = 1
                    links_fields_addition_dict.update(link_projection_pipeline['$addFields'])
                    links_projection_dict.update(link_projection_pipeline['$project'])

            if links_fields_addition_dict or links_projection_dict:
                add_fields_stage = {'$addFields': links_fields_addition_dict} if links_fields_addition_dict else None
                project_stage = {'$project': links_projection_dict} if links_projection_dict else None
                pipeline = [stage for stage in [add_fields_stage, project_stage] if stage] + pipeline

        return pipeline

    @classmethod
    def get_default_projection_fields(cls, temp_exclude: str | set[str] | None = None) -> set[str]:
        """Returns all fields of the model except default excluded ones.

        Args:
            temp_exclude: Temporary adds the given value(s) on top of the default fields to be excluded.
                make sure to pass MODEL FIELD NAMES, not the aliases.
        """
        return cls.model_fields.keys() - cls.get_fields_to_exclude(temp_exclude)

    @classmethod
    def get_default_projection_view(cls, temp_exclude: str | set[str] | None = None) -> Type[BaseModel]:
        """Returns a MongoDB style projection dict from the model with the desired fields only.

        Args:
            temp_exclude: Temporary adds the given value(s) on top of the default fields to be excluded.
                make sure to pass MODEL FIELD NAMES, not the aliases.
        """
        return cls.get_projection_view(cls.get_default_projection_fields(temp_exclude), use_aliases=True,
                                       view_model_name=f'{cls.__name__}DefaultView')

    @classmethod
    def get_default_projection_pipeline(
            cls, temp_exclude: str | set[str] | None = None, links_are_fetched: bool = True
    ) -> list[dict[str, dict]]:
        """Returns a MongoDB aggregation projection dict from the model with the desired fields only.

        Args:
            temp_exclude: Temporary adds the given value(s) on top of the default fields to be excluded.
                make sure to pass MODEL FIELD NAMES, not the aliases.
            links_are_fetched: If links are fetched, the output will also handle their proper projection.
        """
        return cls.get_projection_pipeline(cls.get_default_projection_fields(temp_exclude), links_are_fetched)

    # ----- PIPELINE TOOLS -----

    @classmethod
    def get_skip_limit_pipeline(cls, skip: int | None, limit: int | None) -> list[dict[str, int]]:
        """Returns a MongoDB aggregation pipeline for skip and limit stages."""
        return cls.find().skip(skip).limit(limit).build_aggregation_pipeline()

    @classmethod
    def get_sort_pipeline(
            cls, sort_order: Literal['asc', 'desc'] = None, sort_key: str = None
    ) -> list[dict[str, dict]]:
        """Get sort pipeline for aggregation."""
        if not sort_order or (sort_key is None and sort_order == 'asc'):  # the second part will be MongoDB default sort
            return []

        if sort_key in [None, 'time']:
            mapper = {'_id': sort_order}
            mapper = {cls.time: sort_order} | mapper if 'time' in cls.model_fields else mapper
            sort_dict = get_sort_dict_for_pipeline(sort_key_order_mapper=mapper)
        else:
            sort_dict = get_sort_dict_for_pipeline({sort_key: sort_order})

        return [{'$sort': sort_dict}]

    @classmethod
    def get_group_all_pipeline(cls, target_field: str) -> list[dict[str, dict]]:
        """Pipeline for gathering all values of target field in a list with `results` key."""
        return [{'$group': {
            '_id': None,
            'results': {'$push': f'${target_field}'}
        }}]

    @classmethod
    def get_random_sample_pipeline(cls, count: int = 1) -> list[dict[str, dict]]:
        return [{'$sample': {'size': count}}]

    # ----- TOOLS -----

    @classmethod
    async def advanced_find(
            cls,
            filters: AdvancedFilters | dict | None = None,
            sort_key: str | None = None,
            sort_order: Literal['asc', 'desc'] | None = None,
            skip: int | None = None,
            limit: int | None = None,
            projection_pipeline: list[dict[str, Any]] | None = None,
            fetch_links: bool = False,
            nesting_depth: int | None = None,
            nesting_depths_per_field: dict[str, int] | None = None,
            ignore_always_and_never_fetch_fields: bool = False,
            random_sample: bool = False,
            get_as_objects: bool = True,
            run_query: bool = True,
    ) -> list[Self | dict] | AggregationQuery:
        """Finds records in the most optimized and fastest way with all finding options in one method."""
        if random_sample and not limit:
            raise ValueError('`limit` arg must be passed when `random_sample` is True.')
        projection_pipeline = projection_pipeline or []
        filters = AdvancedFilters(pre_fetch=filters) if isinstance(filters, dict) else filters

        if not fetch_links and filters.post_fetch:
            raise ValueError('AdvancedFilters.post_fetch must be empty when fetch_links is False.')

        sort_pipeline = cls.get_sort_pipeline(sort_order, sort_key)
        skip_limit_pipeline = cls.get_skip_limit_pipeline(skip, limit if not random_sample else None)
        specify_desired_records_pipeline = sort_pipeline + skip_limit_pipeline
        if random_sample:
            specify_desired_records_pipeline += cls.get_random_sample_pipeline(limit)

        special_nesting_depths_per_field = None if ignore_always_and_never_fetch_fields else (
            {f: 0 for f in cls.__never_fetch_fields__} | {f: 1 for f in cls.__always_fetch_fields__}
        ) | (nesting_depths_per_field or {})

        if not fetch_links and not special_nesting_depths_per_field:
            query = cls.find(
                filters.pre_fetch | filters.post_fetch
            ).aggregate(
                specify_desired_records_pipeline + projection_pipeline,
                projection_model=cls if get_as_objects else None, allowDiskUse=True
            )

        else:
            pre_fetch_pipeline = cls.find(filters.pre_fetch).build_aggregation_pipeline()

            fetch_pipeline = cls.find(
                filters.post_fetch,
                fetch_links=True,
                nesting_depth=nesting_depth if fetch_links else 0,
                nesting_depths_per_field=special_nesting_depths_per_field or nesting_depths_per_field
            ).build_aggregation_pipeline()

            final_pipeline = (
                pre_fetch_pipeline + fetch_pipeline + specify_desired_records_pipeline + projection_pipeline
                if filters.post_fetch else
                pre_fetch_pipeline + specify_desired_records_pipeline + fetch_pipeline + projection_pipeline
            )

            mongo_db_pipeline_str = (str(final_pipeline).replace('True', 'true').replace('False', 'false')
                                     .replace('None', 'null'))
            logging.debug('MongoDB pipeline for fetching results:\n' + mongo_db_pipeline_str)
            query = cls.aggregate(final_pipeline, projection_model=cls if get_as_objects else None, allowDiskUse=True)

        return await query.to_list() if run_query else query

    @classmethod
    async def find_ids(
            cls,
            filters: dict | None = None,
            sort_key: str | None = None,
            sort_order: Literal['asc', 'desc'] | None = None,
            skip: int | None = None,
            limit: int | None = None,
            random_sample: bool = False,
            return_as_str: bool = False,
            id_field: str = '_id'
    ) -> list[ObjectId] | list[str]:
        """Returns only the ids of found records.

        Finds the target records based on the args and groups them all to produce a list that contains
        all of their IDs from the target ID field.

        Args:
            filters: MongoDB filters to use for finding records.
            sort_key: Field name to sort the records based on it.
            sort_order: Ascending or descending sort order.
            skip: Number of records to skip.
            limit: Number of records to return.
            random_sample: Return random records.
            return_as_str: Convert the ids to string in the output.
                (useful when they are in ObjectId or int format, but you want string output)
            id_field: Target field to get the ids from.

        Returns:
            List of found ids in form of ObjectIds or converted to string if get_as_str is True.
        """
        if random_sample and not limit:
            raise ValueError('`limit` arg must be passed when `random_sample` is True.')

        sort_pipeline = cls.get_sort_pipeline(sort_order, sort_key)
        skip_limit_pipeline = cls.get_skip_limit_pipeline(skip, limit if not random_sample else None)
        specify_desired_records_pipeline = sort_pipeline + skip_limit_pipeline
        if random_sample:
            specify_desired_records_pipeline += cls.get_random_sample_pipeline(limit)

        if return_as_str:
            final_pipeline = specify_desired_records_pipeline \
                             + cls.get_projection_pipeline({'id'}, links_are_fetched=False) \
                             + cls.get_group_all_pipeline(id_field)
        else:
            final_pipeline = specify_desired_records_pipeline + cls.get_group_all_pipeline(id_field)

        filters = filters or {}
        results = await cls.find(filters).aggregate(final_pipeline, allowDiskUse=True).to_list()
        if results:
            results = results[0]['results']

        return results

    @classmethod
    async def compile_dynamic_filters(cls, dynamic_filters: BaseModel) -> dict:
        """Converts a dynamic filters BaseModel object to a MongoDB filter dict."""
        filters = dict()
        filterable_fields_info = cls.get_filterable_fields_info()
        fields_to_be_excluded = {k for k, v in filterable_fields_info.items() if not v.compile_dynamically}

        filter_keys = set(dynamic_filters.model_dump(exclude_none=True, exclude=fields_to_be_excluded))

        filters_on_linked_classes: dict[Type[BeanieDocument], dict] = defaultdict(dict)
        linked_classes_fields: dict[Type[BeanieDocument], str] = dict()

        for filter_key in filter_keys:
            field_info = filterable_fields_info[filter_key]
            new_filter_key = field_info.field

            if field_info.is_link:
                new_filter_key = f'{field_info.field}.$id'
                filter_value = getattr(dynamic_filters, filter_key)

                if field_info.operator in [In, All] and isinstance(filter_value, ObjectId):
                    new_filter = Eq(new_filter_key, filter_value)
                elif field_info.operator in [In, All] and len(filter_value) == 1:
                    new_filter = Eq(new_filter_key, filter_value[0])
                else:
                    new_filter = field_info.operator(new_filter_key, filter_value)

            elif field_info.annotation in [SearchDetails, list[SearchDetails]]:
                pattern = compile_search_details_to_pattern(getattr(dynamic_filters, filter_key))
                new_filter = field_info.operator(field_info.field, pattern)

            else:
                new_filter = field_info.operator(field_info.field, getattr(dynamic_filters, filter_key))

            if field_info.belongs_to_linked_class:
                linked_classes_fields[field_info.belongs_to_linked_class] = field_info.linked_field
                filters_to_be_added_to = filters_on_linked_classes[field_info.belongs_to_linked_class]
            else:
                filters_to_be_added_to = filters

            if new_filter_key in filters:
                filters_to_be_added_to[new_filter_key].update(new_filter[new_filter_key])

            else:
                filters_to_be_added_to.update(new_filter)

        if filters_on_linked_classes:
            for linked_class, linked_class_filters in filters_on_linked_classes.items():
                linked_records_ids = await linked_class.find_ids(linked_class_filters)
                linked_classes_field = linked_classes_fields[linked_class]
                filters.update(In(f'{linked_classes_field}.$id', linked_records_ids))

        return filters

    @classmethod
    async def check_records_existence(
            cls, record_id_or_list: ObjectId | list[ObjectId] | set[ObjectId], filters: dict | None = None
    ) -> list[ObjectId] | None:
        """Checks existence of records from this document in the database.

        Can be used to validate existence of ids that are linked to this document in other documents; and more.

        Args:
            record_id_or_list: An ObjectId or a list of them to check.
            filters: Extra filters for finding documents under those conditions.

        Returns:
            List of the passed ids that was not found in case there were any, None otherwise.
        """
        if not record_id_or_list:
            return None

        if filters is None:
            filters = dict()

        records_list = get_set_of_object_ids(record_id_or_list)
        filters.update(In(cls.id, records_list))

        found_records_ids = set(await cls.find_ids(filters))
        if found_records_ids:
            missing_records = list(records_list - found_records_ids) or None
        else:
            missing_records = list(records_list)

        return missing_records

    @classmethod
    async def validate_records_existence(
            cls, record_id_or_list: ObjectId | list[ObjectId] | set[ObjectId], filters: dict | None = None
    ) -> None:
        """Checks existence of records from this document in the database and raises ValueError in case of missing
        records.

        Args:
            record_id_or_list: Linked object id or a list of them.
            filters: Extra filters to pass for finding linked documents.

        Raises:
            ValueError: If any of the records with given IDs are not found in the database.
        """
        missing_records = await cls.check_records_existence(record_id_or_list, filters)

        if missing_records:
            raise ValueError(f'{cls.__name__} instances with _ids {missing_records} do not exist in the database.')

    @classmethod
    async def get_dependent_records_count_per_model(cls, record_id: ObjectId) -> dict[str, int]:
        """Finds count of dependant records in dependant documents.

        Finds the count of links to the record with given ID in each document that has links to the current document.

        Args:
            record_id: ID of the record to find links to.

        Returns:
            A dictionary with the dependant document names as key and the count of links to the record as value.
        """
        if not cls.__dependent_models_info__:
            return {}

        linked_records = {}

        for dependant_doc_info in cls.__dependent_models_info__:
            linked_cls = get_class(dependant_doc_info.module_address, dependant_doc_info.document_name)

            pipeline = [
                {'$match': {
                    f'{dependant_doc_info.field_name}.$id': {'$in': [record_id]}
                }},
                {'$count': 'count'}
            ]

            count_aggregation_result = await linked_cls.aggregate(pipeline).to_list()
            if count_aggregation_result:
                linked_records[dependant_doc_info.linked_document] = count_aggregation_result[0]['count']

        return linked_records
