# -*- coding: utf-8 -*-
import logging
import peewee
import pydicom

from .. import event_bus


#: Set of tags excluded from generating queries based on C-FIND-RQ
EXCLUDED_ATTRS = set([
    0x00080052, # Query/Retrieve Level
    0x00080005, # Specific Character Set
    0x00201200, # Number of Patient Related Studies
    0x00201202, # Number of Patient Related Series
    0x00201204, # Number of Patient Related Instances
    0x00080061, # Modalities in Study
    0x00080062, # SOP Classes in Study
    0x00201070, # Other Study Numbers
    0x00201206, # Number of Study Related Series
    0x00201208, # Number of Study Related Instances
    0x00201209  # Number of Series Related Instances
])


#: List of text VRs
TEXT_VR = ['AE', 'CS', 'LO', 'LT', 'PN', 'SH', 'ST', 'UC', 'UR', 'UT', 'UI']


class BaseAPI:
    @classmethod
    def name(cls):
        return cls.__name__

    def __init__(self, bus: event_bus.EventBus):
        self.bus = bus
        self.log = logging.getLogger(self.name())

    def build_filters(self, model, query, ds: pydicom.Dataset, skipped=None):
        """Build filters for provided model

        :param model: PACS level model
        :type model: peewee.Model
        :param query: C-FIND SQL query
        :type query: peewee.Query
        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :param skipped: skipped attributes, defaults to None
        :type skipped: list, optional
        :return: query and response attributes
        :rtype: tuple
        """
        if skipped is None:
            skipped = set()
        response_attrs = []
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


    def build_filter(self, query, attr, vr, elem):
        """Build filter for specific attribute

        :param query: current SQL query
        :type query: peewee.Query
        :param attr: C-FIND request attribute
        :type attr: [type]
        :param vr: element VR
        :type vr: str
        :param elem: DICOM element
        :type elem: [type]
        :raises ValueError: raises ValueError for unsupported VR
        :return: query with added filter
        :rtype: peewee.Query
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


    def filter_upper_level(self, model, elements: list):
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


    def encode_response(self, instance, response_attrs: list, encoding: str):
        """Creates a C-FIND response dataset

        :param instance: database model instance
        :type instance: peewee.Model
        :param response_attrs: list of response attributes (tag, attribute name in
                            the database model and VR)
        :type response_attrs: list
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

    def _text_filter(self, query: peewee.Query, attr, value: str):
        if isinstance(value, list):
            return query.where(attr << value)
        value = value.replace('?', '_')
        value = value.replace('*', '%')
        return query.where(attr ** value)


    def _date_filter(self, query: peewee.Query, attr, value: str):
        if '-' in value:
            start, end = value.split('-')
            # TODO: Add normalization for shorter value
            return query.where((attr >= start) & (attr <= end))

        return query.where(attr == value)


    def _time_filter(self, query: peewee.Query, attr, value: str):
        if '-' in value:
            start, end = value.split('-')
            # TODO: Add normalization for shorter value
            return query.where((attr >= start) & (attr <= end))

        return query.where(attr == value)


    def _date_time_filter(self, query: peewee.Query, attr, value: str):
        if '-' in value:
            start, end = value.split('-')
            # TODO: Add normalization for shorter value
            return query.where((attr >= start) & (attr <= end))

        return query.where(attr == value)
