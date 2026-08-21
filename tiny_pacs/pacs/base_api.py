"""Base API for PACS Query/Retrieve implementations.

Builds peewee queries from C-FIND request datasets and encodes query
results back into C-FIND response datasets.
"""
import logging
from collections.abc import Iterator
from typing import Any, Protocol, TypeAlias, TypeVar

import peewee
import pydicom
import trolleybus
from pydicom.tag import BaseTag

TM = TypeVar('TM', bound=peewee.Model)


#: Model with a DICOM tag to ``(attribute name, VR)`` mapping, used for
#: building C-FIND queries
class MappedModel(Protocol):
    """Protocol of a peewee model providing a DICOM tag mapping."""

    mapping: dict[int, tuple[str, str]]


#: Set of joins between models for a C-FIND query
JoinsSet: TypeAlias = set[tuple[type[peewee.Model], type[peewee.Model]]]

#: C-FIND select columns: model classes and column expressions
SelectColumns: TypeAlias = list[Any]

#: C-FIND response attributes: ``(tag, attribute name or attribute path, VR,
#: encoding function)`` tuples, unpacked positionally by
#: :meth:`BaseAPI.encode_response`
ResponseAttrs: TypeAlias = list[tuple[Any, ...]]

#: Upper C-FIND level filters: ``(tag, attribute, VR, element, attribute
#: name)`` tuples, as yielded by :meth:`BaseAPI.filter_upper_level`
UpperLevelFilters: TypeAlias = list[tuple[Any, ...]]

#: C-FIND request tags skipped by query building
SkippedTags: TypeAlias = set[BaseTag]


#: Set of tags excluded from generating queries based on C-FIND-RQ
EXCLUDED_ATTRS = set([
    0x00080052,  # Query/Retrieve Level
    0x00080005,  # Specific Character Set
    0x00201200,  # Number of Patient Related Studies
    0x00201202,  # Number of Patient Related Series
    0x00201204,  # Number of Patient Related Instances
    0x00080061,  # Modalities in Study
    0x00080062,  # SOP Classes in Study
    0x00201070,  # Other Study Numbers
    0x00201206,  # Number of Study Related Series
    0x00201208,  # Number of Study Related Instances
    0x00201209  # Number of Series Related Instances
])


#: List of text VRs
TEXT_VR = ['AE', 'CS', 'LO', 'LT', 'PN', 'SH', 'ST', 'UC', 'UR', 'UT', 'UI']


class BaseAPI:
    """Base implementation of the PACS level APIs.

    Provides query building and response encoding shared by the patient,
    study, series and instance level APIs.
    """

    @classmethod
    def name(cls) -> str:
        """API name.

        Defaults to class name

        :return: API name
        :rtype: str
        """
        return cls.__name__

    def __init__(self, bus: trolleybus.EventBus):
        """Initializes the API

        :param bus: event bus
        :type bus: trolleybus.EventBus
        """
        self.bus = bus
        self.log = logging.getLogger(self.name())

    def build_filters(self, model: MappedModel,
                      query: 'peewee.ModelSelect[TM]',
                      ds: pydicom.Dataset,
                      skipped: SkippedTags | None = None
                      ) -> tuple['peewee.ModelSelect[TM]', ResponseAttrs]:
        """Build filters for provided model

        :param model: PACS level model
        :type model: peewee.Model
        :param query: C-FIND SQL query
        :type query: peewee.ModelSelect
        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :param skipped: skipped tags, defaults to None
        :type skipped: set[BaseTag], optional
        :return: query and response attributes
        :rtype: tuple
        """
        if skipped is None:
            skipped = set()
        response_attrs: ResponseAttrs = []
        for elem in ds:
            if elem.tag in EXCLUDED_ATTRS or elem.tag in skipped:
                continue

            try:
                attr_name, vr = model.mapping[elem.tag]
            except KeyError:
                response_attrs.append((elem.tag, None, elem.VR, None))
                continue

            response_attrs.append((elem.tag, attr_name, vr, None))
            if elem.is_empty:
                continue

            attr = getattr(model, attr_name)
            query = self.build_filter(query, attr, vr, elem)
        return query, response_attrs

    def build_filter(self, query: 'peewee.ModelSelect[TM]', attr: peewee.Field,
                     vr: str, elem: pydicom.DataElement
                     ) -> 'peewee.ModelSelect[TM]':
        """Build filter for specific attribute

        :param query: current SQL query
        :type query: peewee.ModelSelect
        :param attr: C-FIND request attribute
        :type attr: peewee.Field
        :param vr: element VR
        :type vr: str
        :param elem: DICOM element
        :type elem: pydicom.DataElement
        :raises ValueError: raised for an unsupported VR
        :return: query with the filter added
        :rtype: peewee.ModelSelect
        """
        if vr in TEXT_VR:
            if vr == 'PN':
                value = str(elem.value)
            else:
                value = elem.value
            return self._text_filter(query, attr, value)
        elif vr == 'DA':
            return self._date_filter(query, attr, elem.value)
        elif vr == 'TM':
            return self._time_filter(query, attr, elem.value)
        elif vr == 'DT':
            return self._date_time_filter(query, attr, elem.value)
        raise ValueError(f'Unsupported VR: {vr}')

    def filter_upper_level(self, model: MappedModel,
                           elements: list[pydicom.DataElement]
                           ) -> Iterator[tuple[Any, ...]]:
        """Build filter for upper C-FIND level

        :param model: peewee model
        :type model: peewee.Model
        :param elements: upper level elements
        :type elements: list
        :yield: tuple of tag, attribute, VR, element and attribute name
        :rtype: tuple
        """
        for elem in elements:
            attr_name, vr = model.mapping[elem.tag]
            attr = getattr(model, attr_name)
            yield elem.tag, attr, vr, elem, attr_name

    def encode_response(self, instance: peewee.Model,
                        response_attrs: ResponseAttrs,
                        encoding: str) -> pydicom.Dataset:
        """Creates a C-FIND response dataset

        :param instance: database model instance
        :type instance: peewee.Model
        :param response_attrs: list of response attributes: ``(tag,
                               attribute name or attribute path, VR,
                               encoding function)`` tuples
        :type response_attrs: ResponseAttrs
        :param encoding: response encoding
        :type encoding: str
        :return: C-FIND-RSP dataset
        :rtype: pydicom.Dataset
        """
        rsp = pydicom.Dataset()
        rsp.SpecificCharacterSet = encoding
        for tag, attr_name, vr, func in response_attrs:
            if attr_name is None:
                # Attribute not supported
                rsp.add_new(tag, vr, None)
            else:
                if not isinstance(attr_name, tuple):
                    attr = getattr(instance, attr_name)
                else:
                    attr = instance
                    for field in attr_name:
                        attr = getattr(attr, field)
                if func:
                    attr = func(attr)
                rsp.add_new(tag, vr, attr)
        return rsp

    def _text_filter(self, query: 'peewee.ModelSelect[TM]', attr: peewee.Field,
                     value: str | list[str]) -> 'peewee.ModelSelect[TM]':
        """Adds a filter for a text attribute.

        Single character (``?``) and multiple character (``*``) DICOM
        wildcards are translated to the SQL LIKE wildcards.
        """
        if isinstance(value, list):
            return query.where(attr << value)
        value = value.replace('?', '_')
        value = value.replace('*', '%')
        return query.where(attr ** value)

    def _date_filter(self, query: 'peewee.ModelSelect[TM]', attr: peewee.Field,
                     value: str) -> 'peewee.ModelSelect[TM]':
        """Adds a filter for a date (DA) attribute, range-aware."""
        if '-' in value:
            start, end = value.split('-')
            # TODO: Add normalization for shorter value
            return query.where((attr >= start) & (attr <= end))

        return query.where(attr == value)

    def _time_filter(self, query: 'peewee.ModelSelect[TM]', attr: peewee.Field,
                     value: str) -> 'peewee.ModelSelect[TM]':
        """Adds a filter for a time (TM) attribute, range-aware."""
        if '-' in value:
            start, end = value.split('-')
            # TODO: Add normalization for shorter value
            return query.where((attr >= start) & (attr <= end))

        return query.where(attr == value)

    def _date_time_filter(self, query: 'peewee.ModelSelect[TM]',
                          attr: peewee.Field,
                          value: str) -> 'peewee.ModelSelect[TM]':
        """Adds a filter for a date-time (DT) attribute, range-aware."""
        if '-' in value:
            start, end = value.split('-')
            # TODO: Add normalization for shorter value
            return query.where((attr >= start) & (attr <= end))

        return query.where(attr == value)
