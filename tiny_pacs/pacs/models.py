# -*- coding: utf-8 -*-
import peewee


class Patient(peewee.Model):
    """Patient model.

    Stores all C-FIND relevant patient attributes.
    """
    mapping = {
        0x00100010: ('patient_name', 'PN'),
        0x00100020: ('patient_id', 'LO'),
        0x00100021: ('issuer_of_patient_id', 'LO'),
        0x00100030: ('patient_birth_date', 'DA'),
        0x00100032: ('patient_birth_time', 'TM'),
        0x00100040: ('patient_sex', 'CS'),
        0x00101001: ('other_patient_names', 'PN'),
        0x00102160: ('ethnic_group', 'SH'),
        0x00104000: ('patient_comments', 'LT')
    }

    #: Patinet's Name (0010, 0010) PN
    patient_name = peewee.CharField(max_length=64*5+4, index=True, null=True)

    #: Patient's ID (0010, 0020) LO
    patient_id = peewee.CharField(max_length=64, unique=True)

    #: Issuer of Patient's ID (0010, 0021) LO
    issuer_of_patient_id = peewee.CharField(max_length=64, index=True,
                                            null=True)

    #: Patient's Birth Date (0010, 0030) DA
    patient_birth_date = peewee.CharField(max_length=8, index=True, null=True)

    #: Patient's Birth Time (0010, 0032) TM
    patient_birth_time = peewee.CharField(max_length=14, index=True, null=True)

    #: Patient's Sex (0010, 0040) CS
    patient_sex = peewee.CharField(max_length=16, index=True, null=True)

    #: Other Patient's Names (0010, 1001) PN
    other_patient_names = peewee.TextField(default='')

    #: Ethnic Group (0010, 2160) SH
    ethnic_group = peewee.CharField(max_length=16, index=True, null=True)

    #: Patient Comments (0010, 4000) LT
    patient_comments = peewee.TextField(default='')

    # Number of Patient Related Studies (0020,1200)
    # Number of Patient Related Series (0020,1202)
    # Number of Patient Related Instances (0020,1204)


class Study(peewee.Model):
    """Study model.

    Stores all relevant C-FIND attributes.
    """
    mapping = {
        0x00080020: ('study_date', 'DA'),
        0x00080030: ('study_time', 'TM'),
        0x00080050: ('accession_number', 'SH'),
        0x00200010: ('study_id', 'SH'),
        0x0020000D: ('study_instance_uid', 'UI'),
        0x00081030: ('study_description', 'LO'),
        0x00080090: ('referring_physician_name', 'PN'),
        0x00081060: ('name_of_physicians_reading_study', 'PN'),
        0x00081080: ('admitting_diagnoses_description', 'LO'),
        0x00101010: ('patient_age', 'AS'),
        0x00101020: ('patient_size', 'DS'),
        0x00101030: ('patient_weight', 'DS'),
        0x00102180: ('occupation', 'SH'),
        0x001021B0: ('additional_patient_history', 'LT')
    }

    #: Reference to Patient
    patient = peewee.ForeignKeyField(Patient)

    #: Study Date (0008, 0020) DA
    study_date = peewee.CharField(max_length=8, index=True, null=True)

    #: Study Time (0008, 0030) TM
    study_time = peewee.CharField(max_length=14, index=True, null=True)

    #: Accession Number (0008, 0050) SH
    accession_number = peewee.CharField(max_length=16, index=True, null=True)

    #: Study ID (0020, 0010) SH
    study_id = peewee.CharField(max_length=16, index=True, null=True)

    #: Study Instance UID (0020, 000D) UI
    study_instance_uid = peewee.CharField(max_length=64, unique=True)

    #: Study Description (0008,1030) LO
    study_description = peewee.CharField(max_length=64, index=True, null=True)

    #: Referring Physician Name (0008, 0090) PN
    referring_physician_name = peewee.CharField(max_length=5*64+4, index=True,
                                                null=True)

    #: Name Of Physicians Reading Study (0008, 1060) PN
    name_of_physicians_reading_study = peewee.TextField(default='')

    #: Admitting Diagnoses Description (0008, 1080) LO
    admitting_diagnoses_description = peewee.CharField(
        max_length=64, index=True, null=True
    )

    #: Patient Age (0010, 1010) AS
    patient_age = peewee.CharField(max_length=4, index=True, null=True)

    #: Patient Size (0010, 1020) DS
    patient_size = peewee.CharField(max_length=16, index=True, null=True)

    #: Patient Weight (0010, 1030) DS
    patient_weight = peewee.CharField(max_length=16, index=True, null=True)

    #: Occupation (0010, 2180) SH
    occupation = peewee.CharField(max_length=16, index=True, null=True)

    #: Additional Patient History (0010, 21B0) LT
    additional_patient_history = peewee.TextField(default='')

    # Modalities in Study (0008,0061)
    # SOP Classes in Study (0008,0062)
    # Other Study Numbers (0020,1070)
    # Number of Study Related Series (0020,1206)
    # Number of Study Related Instances (0020,1208)


class Series(peewee.Model):
    """Series model.

    Stores all relevant C-FIND attributes.
    """
    mapping = {
        0x00080060: ('modality', 'CS'),
        0x00200011: ('series_number', 'IS'),
        0x0020000E: ('series_instance_uid', 'UI')
    }

    #: Reference to Study
    study = peewee.ForeignKeyField(Study)

    #: Modality (0008, 0060) CS
    modality = peewee.CharField(max_length=16, index=True, null=True)

    #: Series Number (0020, 0011) IS
    series_number = peewee.CharField(max_length=12, index=True, null=True)

    #: Series Instance UID (0020, 000E) UI
    series_instance_uid = peewee.CharField(max_length=64, unique=True)

    # Number of Series Related Instances (0020,1209)


class Instance(peewee.Model):
    """Instance model.

    Stores all relevant C-FIND attributes.
    """
    mapping = {
        0x00020010: ('transfer_syntax_uid', 'UI'),
        0x00200013: ('instance_number', 'IS'),
        0x00080018: ('sop_instance_uid', 'UI'),
        0x00080016: ('sop_class_uid', 'UI'),
        0x00400512: ('container_identifier', 'LO')
    }

    #: Series reference
    series = peewee.ForeignKeyField(Series)

    #: Instance Number (0020, 0013) IS
    instance_number = peewee.CharField(max_length=12, index=True, null=True)

    #: SOP Instance UID (0008, 0018) UI
    sop_instance_uid = peewee.CharField(max_length=64, unique=True)

    #: SOP Class UID (0008, 0016) UI
    sop_class_uid = peewee.CharField(max_length=64, index=True, null=True)

    #: Container Identifier (0040, 0512) LO
    container_identifier = peewee.CharField(max_length=64, index=True, null=True)

    # Transfer Syntax UID (0002, 0010) UI
    transfer_syntax_uid = peewee.CharField(max_length=64, index=True, null=True)

    # Available Transfer Syntax UID (0008,3002)
    # Related General SOP Class UID (0008,001A)
