# -*- coding: utf-8 -*-
import peewee
import pydicom
from . import models
from . import base_api

class InstanceAPI(base_api.BaseAPI):
    def c_store(self, series: models.Series, ds: pydicom.Dataset):
        """C-STORE handler

        :param series: series reference
        :type series: Series
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: new or existing instance record, that matches incoming dataset
        :rtype: Instance
        """
        sop_instance_uid = ds.SOPInstanceUID
        try:
            return models.Instance.get(models.Instance.sop_instance_uid == sop_instance_uid)
        except models.Instance.DoesNotExist:  # pylint: disable=no-member
            instance_number = getattr(ds, 'InstanceNumber', None)
            sop_class_uid = getattr(ds, 'SOPClassUID', None)
            container_identifier = getattr(ds, 'ContainerIdentifier', None)
            meta = getattr(ds, 'file_meta', None)
            if meta:
                transfer_syntax_uid = getattr(meta, 'TransferSyntaxUID')
            else:
                transfer_syntax_uid = None
            instance = models.Instance.create(
                series=series,
                sop_instance_uid=sop_instance_uid,
                instance_number=instance_number,
                sop_class_uid=sop_class_uid,
                container_identifier=container_identifier,
                transfer_syntax_uid=transfer_syntax_uid
            )
            self.log.debug('Created new instance, SOP Instance UID: %s', sop_instance_uid)
            return instance

    def c_find(self, ds: pydicom.Dataset):
        """C-FIND handler

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND results
        :rtype: pydicom.Dataset
        """
        joins = set()

        response_attrs = []
        select = [models.Instance]
        upper_level_filters = []

        skipped = set()

        patient_attrs = [e for e in ds if e.tag in models.Patient.mapping]
        skipped.update(e.tag for e in patient_attrs)
        if patient_attrs:
            _upper_level_filters = list(
                self.filter_upper_level(models.Patient, patient_attrs)
            )
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append(
                    (tag, ('series', 'study', 'patient', attr_name), vr, None)
                )
            joins.update(
                [(models.Instance, models.Series), (models.Series, models.Study), (models.Study, models.Patient)]
            )

        study_attrs = [e for e in ds if e.tag in models.Study.mapping]
        skipped.update(e.tag for e in study_attrs)
        if study_attrs:
            _upper_level_filters = list(self.filter_upper_level(models.Study, study_attrs))
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append(
                    (tag, ('series', 'study', attr_name), vr, None)
                )
            joins.update([(models.Instance, models.Series), (models.Series, models.Study)])

        series_attrs = [e for e in ds if e.tag in models.Series.mapping]
        skipped.update(e.tag for e in series_attrs)
        if series_attrs:
            _upper_level_filters = list(
                self.filter_upper_level(models.Series, series_attrs)
            )
            upper_level_filters.extend(_upper_level_filters)
            for tag, attr, vr, _, attr_name in _upper_level_filters:
                select.append(attr)
                response_attrs.append((tag, ('series', attr_name), vr, None))
            joins.add((models.Instance, models.Series))

        query = models.Instance.select(*select)

        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = self.build_filters(models.Instance, query, ds, skipped)
        response_attrs.extend(_response_attrs)
        for _, attr, vr, elem, _ in upper_level_filters:
            if not elem.value:
                continue
            query = self.build_filter(query, attr, vr, elem)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        if not query.count():
            return []

        yield from (self.encode_response(s, response_attrs, encoding) for s in query)
