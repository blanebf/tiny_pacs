# -*- coding: utf-8 -*-
import peewee
import pydicom
from pydicom.tag import Tag

from . import models
from . import base_api


class SeriesAPI(base_api.BaseAPI):
    def c_store(self, study: models.Study, ds: pydicom.Dataset):
        """C-STORE handler

        :param study: study reference
        :type study: models.Study
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: new or existing series record, that matches incoming dataset
        :rtype: Series
        """
        series_instance_uid = ds.SeriesInstanceUID
        try:
            return models.Series.get(models.Series.series_instance_uid == series_instance_uid)
        except models.Series.DoesNotExist:  # pylint: disable=no-member
            modality = getattr(ds, 'Modality', None)
            series_number = getattr(ds, 'SeriesNumber', None)
            series = models.Series.create(
                study=study,
                series_instance_uid=series_instance_uid,
                modality=modality,
                series_number=series_number
            )
            self.log.debug('Created new series, Series Instance UID: %s', series_instance_uid)
            return series

    def c_find(self, ds: pydicom.Dataset):
        """C-FIND handler

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND results
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []
        select = [models.Series]
        upper_level_filters = []

        skipped = set()

        patient_attrs = [e for e in ds if e.tag in models.Patient.mapping]
        skipped.update(e.tag for e in patient_attrs)
        if patient_attrs:
            _upper_level_filters = list(self.filter_upper_level(models.Patient, patient_attrs))
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append(
                    (tag, ('study', 'patient', attr_name), vr, None)
                )
            joins.update([(models.Series, models.Study), (models.Study, models.Patient)])

        study_attrs = [e for e in ds if e.tag in models.Study.mapping]
        skipped.update(e.tag for e in study_attrs)
        if study_attrs:
            _upper_level_filters = list(self.filter_upper_level(models.Study, study_attrs))
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('study', attr_name), vr, None))
            joins.update([(models.Series, models.Study)])

        if 'NumberOfSeriesRelatedInstances' in ds:
            _tag = Tag((0x0020, 0x1209))
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(models.Instance.id)\
                    .alias('number_of_study_related_series')  # pylint: disable=no-member
            )
            response_attrs.append(
                (_tag, 'number_of_series_related_instances', 'IS', None)
            )
            joins.add((models.Series, models.Instance))

        query = models.Series.select(*select)

        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = self.build_filters(models.Series, query, ds, skipped)
        response_attrs.extend(_response_attrs)
        for _, attr, vr, elem, _ in upper_level_filters:
            if not elem.value:
                continue
            query = self.build_filter(query, attr, vr, elem)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        if not query.count():
            return []

        yield from (self.encode_response(s, response_attrs, encoding) for s in query)
