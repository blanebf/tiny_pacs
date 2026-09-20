"""Patient level Query/Retrieve API."""
from collections.abc import Iterator, MutableSequence, Sequence

import peewee
import pydicom
import trolleybus
from pydicom.tag import Tag

from . import base_api
from .models import Instance, Patient, Series, Study

#: Patient ID values (compared case-insensitively) treated as
#: de-identification placeholders even without the normative PS3.15 E
#: de-identification attributes
DEFAULT_ANONYMOUS_PATIENT_IDS: Sequence[str] = ('ANONYMOUS',)

#: Dataset attributes compared against the stored record to report
#: demographic conflicts: ``(dataset attribute, model column)``
CONFLICT_ATTRS: tuple[tuple[str, str], ...] = (
    ('PatientName', 'patient_name'),
    ('PatientSex', 'patient_sex'),
    ('PatientBirthDate', 'patient_birth_date')
)


def _value_str(ds: pydicom.Dataset, attribute: str) -> str:
    """Returns the string value of a dataset attribute.

    Missing attributes and empty (Type 2 zero-length) values map to an
    empty string. Values are never stripped or otherwise rewritten, so
    the stored identity key matches the raw value the C-FIND and
    C-MOVE/C-GET query paths compare against.

    :param ds: dataset
    :type ds: pydicom.Dataset
    :param attribute: attribute keyword
    :type attribute: str
    :return: string value or an empty string
    :rtype: str
    """
    value = getattr(ds, attribute, None)
    if value is None:
        return ''
    return str(value)


def identity_key(patient_id: str, issuer: str) -> peewee.Expression:
    """Returns the query expression of the patient identity key.

    :param patient_id: normalized Patient ID
    :type patient_id: str
    :param issuer: normalized Issuer of Patient ID
    :type issuer: str
    :return: expression matching the identity key of a patient record
    :rtype: peewee.Expression
    """
    return (
        (Patient.patient_id == patient_id)
        & (Patient.issuer_of_patient_id == issuer)
    )


class PatientAPI(base_api.BaseAPI):
    """API for the PATIENT Query/Retrieve level.

    :ivar anonymous_patient_ids: normalized (upper-case) Patient ID
        values recognized as de-identification placeholders, see
        :meth:`is_anonymized`
    """

    def __init__(
            self,
            bus: trolleybus.EventBus,
            anonymous_patient_ids: Sequence[str] = (
                DEFAULT_ANONYMOUS_PATIENT_IDS
            )
    ) -> None:
        """Initializes the API

        :param bus: event bus
        :type bus: trolleybus.EventBus
        :param anonymous_patient_ids: Patient ID values treated as
            de-identification placeholders (case-insensitive), defaults
            to :data:`DEFAULT_ANONYMOUS_PATIENT_IDS`
        :type anonymous_patient_ids: Sequence[str], optional
        """
        super().__init__(bus)
        self.anonymous_patient_ids = frozenset(
            value.strip().upper()
            for value in anonymous_patient_ids
            if value.strip()
        )

    def c_store(self, ds: pydicom.Dataset) -> peewee.Model:
        """Gets or creates patient record for storage request

        The record is matched on the DICOM patient identity key — the
        pair of Patient ID (0010,0020) and Issuer of Patient ID
        (0010,0021), PS3.3 C.7.1.1 — and never on demographics: within
        one assigning authority the identifier takes precedence over
        attributes, so a dataset conflicting by name, sex or birth date
        attaches to the existing record and the conflict is reported as a
        warning only. Issuerless datasets share the empty-issuer
        namespace; datasets without a Patient ID share one record.

        De-identified datasets — Patient Identity Removed (0012,0062)
        = YES, a non-empty De-identification Method (0012,0063) or Code
        Sequence (0012,0064) per PS3.15 E, an empty Patient ID or a
        configured placeholder from :attr:`anonymous_patient_ids` —
        attach silently: their demographics carry no identity semantics,
        so conflicting values are expected and never warned about.

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: new or existing patient record for the identity key of
                 the incoming dataset
        :rtype: Patient
        """
        # TODO: Issue locally generated Patient IDs for datasets
        #  without one; until then they share the empty-ID patient record
        patient_id = _value_str(ds, 'PatientID')
        issuer = _value_str(ds, 'IssuerOfPatientID')
        anonymized = self.is_anonymized(ds, patient_id)
        patient = Patient.get_or_none(identity_key(patient_id, issuer))
        if patient is None:
            try:
                patient = self.create_patient(ds, patient_id, issuer)
            except peewee.IntegrityError:
                # Lost a creation race against a concurrent store of the
                # same identity key; the rolled-back insert is retried as
                # an attach to the record the winner created
                patient = Patient.get_or_none(
                    identity_key(patient_id, issuer)
                )
                if patient is None:
                    raise
                self.attach_patient(patient, ds, anonymized)
            else:
                self.log.debug(
                    'Created new patient, Patient ID: %r, '
                    'Issuer of Patient ID: %r', patient_id, issuer
                )
        else:
            self.attach_patient(patient, ds, anonymized)
        return patient

    def is_anonymized(self, ds: pydicom.Dataset, patient_id: str) -> bool:
        """Detects a de-identified dataset.

        The normative markers come first: Patient Identity Removed
        (0012,0062) = YES, or a non-empty De-identification Method
        (0012,0063) / De-identification Method Code Sequence
        (0012,0064) as produced by the PS3.15 E profiles. Without them
        the dataset still counts as de-identified when its Patient ID is
        empty or one of the configured placeholders
        (:attr:`anonymous_patient_ids`).

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :param patient_id: normalized Patient ID of the dataset
        :type patient_id: str
        :return: True when the dataset carries no usable patient identity
        :rtype: bool
        """
        if _value_str(ds, 'PatientIdentityRemoved').upper() == 'YES':
            return True
        if _value_str(ds, 'DeidentificationMethod'):
            return True
        if ds.get('DeidentificationMethodCodeSequence', None):
            return True
        return (not patient_id
                or patient_id.upper() in self.anonymous_patient_ids)

    def create_patient(
            self, ds: pydicom.Dataset, patient_id: str, issuer: str
    ) -> Patient:
        """Creates the patient record of an incoming dataset.

        Demographics are stored as received (empty values as NULL) and
        the de-identification attributes of the dataset are recorded on
        the record for :meth:`attach_patient` and administrative tools.

        The insert runs inside a savepoint of the ambient transaction,
        so a lost creation race (the composite unique index of the
        identity key) surfaces as a contained
        :class:`peewee.IntegrityError` that :meth:`c_store` retries as
        an attach instead of failing the whole C-STORE.

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :param patient_id: normalized Patient ID of the dataset
        :type patient_id: str
        :param issuer: normalized Issuer of Patient ID of the dataset
        :type issuer: str
        :return: the new patient record
        :rtype: Patient
        :raises peewee.IntegrityError: when a concurrent store already
            created the identity key
        """
        other_patient_names = getattr(ds, 'OtherPatientNames', '') or ''
        if isinstance(other_patient_names, MutableSequence):
            # Wire-decoded multi-valued PN attributes are pydicom
            # MultiValue objects (not plain lists); join every sequence
            # like a multi-valued DICOM string
            other_patient_names = '\\'.join(
                str(name) for name in other_patient_names
            )
        else:
            other_patient_names = str(other_patient_names)
        with Patient._meta.database.atomic():
            return Patient.create(
                patient_id=patient_id,
                issuer_of_patient_id=issuer,
                patient_name=_value_str(ds, 'PatientName') or None,
                patient_sex=_value_str(ds, 'PatientSex') or None,
                patient_birth_date=_value_str(ds, 'PatientBirthDate')
                or None,
                patient_birth_time=_value_str(ds, 'PatientBirthTime')
                or None,
                other_patient_names=other_patient_names,
                ethnic_group=_value_str(ds, 'EthnicGroup') or None,
                patient_comments=_value_str(ds, 'PatientComments'),
                patient_identity_removed=_value_str(
                    ds, 'PatientIdentityRemoved'
                ).upper(),
                deidentification_method=self.deidentification_method(ds)
            )

    def attach_patient(
            self, patient: Patient, ds: pydicom.Dataset, anonymized: bool
    ) -> None:
        """Attaches an incoming dataset to an existing patient record.

        The stored demographics always win (first-seen values are kept);
        de-identification attributes the record does not carry yet are
        recorded. Conflicting demographics are reported as a warning
        unless the dataset or the record is de-identified: identity
        attributes of anonymized data carry no identity semantics, so
        differences there are expected, not conflicts.

        :param patient: existing patient record
        :type patient: Patient
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :param anonymized: whether the dataset is de-identified, see
                           :meth:`is_anonymized`
        :type anonymized: bool
        """
        changed = False
        identity_removed = _value_str(ds, 'PatientIdentityRemoved').upper()
        if (identity_removed == 'YES'
                and patient.patient_identity_removed != 'YES'):
            patient.patient_identity_removed = 'YES'
            changed = True
        method = self.deidentification_method(ds)
        if method and not patient.deidentification_method:
            patient.deidentification_method = method
            changed = True
        if changed:
            patient.save()
        if anonymized or patient.patient_identity_removed == 'YES':
            return
        conflicts = self.conflicts(patient, ds)
        if conflicts:
            self.log.warning(
                'Demographic conflict for Patient ID %r (Issuer of '
                'Patient ID %r) on: %s. The identity key takes '
                'precedence; the dataset is attached to the existing '
                'patient and the stored values are kept',
                patient.patient_id, patient.issuer_of_patient_id,
                ', '.join(conflicts)
            )

    def conflicts(self, patient: Patient, ds: pydicom.Dataset) -> list[str]:
        """Compares incoming demographics with the stored record.

        Only attributes that are non-empty on both sides participate.
        The keywords of the conflicting attributes are returned for the
        warning; the conflicting values themselves are deliberately
        left out so no PHI beyond the identity key reaches the logs.

        :param patient: existing patient record
        :type patient: Patient
        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: keyword of every conflicting attribute
        :rtype: list[str]
        """
        result = []
        for attribute, column in CONFLICT_ATTRS:
            incoming = _value_str(ds, attribute)
            stored = str(getattr(patient, column) or '')
            if incoming and stored and incoming != stored:
                result.append(attribute)
        return result

    def deidentification_method(self, ds: pydicom.Dataset) -> str:
        """Extracts the de-identification method of a dataset.

        De-identification Method (0012,0063) when non-empty; otherwise
        the code meanings of the De-identification Method Code Sequence
        (0012,0064), joined like a multi-valued LO.

        :param ds: incoming dataset
        :type ds: pydicom.Dataset
        :return: method description or an empty string
        :rtype: str
        """
        method = _value_str(ds, 'DeidentificationMethod')
        if method:
            return method
        return '\\'.join(
            _value_str(item, 'CodeMeaning')
            for item in ds.get('DeidentificationMethodCodeSequence', [])
            if _value_str(item, 'CodeMeaning')
        )

    def c_find(self, ds: pydicom.Dataset) -> Iterator[pydicom.Dataset]:
        """C-FIND request handler for Patient level

        :param ds: C-FIND request
        :type ds: pydicom.Dataset
        :yield: C-FIND result
        :rtype: pydicom.Dataset
        """
        joins: base_api.JoinsSet = set()

        response_attrs: base_api.ResponseAttrs = []

        select: base_api.SelectColumns = [Patient]
        skipped: base_api.SkippedTags = set()
        if 'NumberOfPatientRelatedStudies' in ds:
            _tag = Tag(0x0020, 0x1200)
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(Study.id)
                .alias('number_of_patient_related_studies')
            )
            response_attrs.append(
                (_tag, 'number_of_patient_related_studies', 'IS', None)
            )
            joins.add((Patient, Study))
        if 'NumberOfPatientRelatedSeries' in ds:
            _tag = Tag(0x0020, 0x1202)
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(Series.id)
                .alias('number_of_patient_related_series')
            )
            response_attrs.append(
                (_tag, 'number_of_patient_related_series', 'IS', None)
            )
            joins.update([(Patient, Study), (Study, Series)])
        if 'NumberOfPatientRelatedInstances' in ds:
            _tag = Tag(0x0020, 0x1204)
            skipped.add(_tag)
            select.append(
                peewee.fn.Count(Instance.id)
                .alias('number_of_patient_related_instances')
            )
            response_attrs.append(
                (_tag, 'number_of_patient_related_instances', 'IS', None)
            )
            joins.update(
                [(Patient, Study), (Study, Series), (Series, Instance)]
            )

        query = Patient.select(*select)
        for join in joins:
            query = query.join_from(*join)

        query, _response_attrs = self.build_filters(Patient, query, ds)
        response_attrs.extend(_response_attrs)

        encoding = getattr(ds, 'SpecificCharacterSet', 'ISO-IR 6')
        yield from (
            self.encode_response(p, response_attrs, encoding) for p in query
        )
