from tiny_pacs.db import DBQuestionnaire
from tiny_pacs.interactive import AEQuestionnaire
from tiny_pacs.questions import Question


def test_value_default() -> None:
    question = Question('port', 'port', int, default=11112)
    assert question.value == 11112


def test_value_set() -> None:
    question = Question('port', 'port', int, default=11112)
    question.value = '104'
    assert question.value == 104


def test_empty_value_keeps_default() -> None:
    question = Question('port', 'port', int, default=11112)
    question.value = ''
    assert question.value == 11112
    question.value = None
    assert question.value == 11112


def test_falsy_value_accepted() -> None:
    question = Question('port', 'port', int, default=11112)
    question.value = 0
    assert question.value == 0

    toggle = Question('dump_ds', 'dump', lambda v: str(v).lower() == 'y', default='Y')
    toggle.value = False
    assert toggle.value is False


def test_repeatable_appends() -> None:
    question = Question('ae_title', 'aet', lambda v: v, True, ['TINY_PACS'])
    question.value = 'FIRST'
    question.value = 'SECOND'
    assert question.value == ['FIRST', 'SECOND']
    assert isinstance(question._value, list)


def test_repeatable_default() -> None:
    question = Question('ae_title', 'aet', lambda v: v, True, ['TINY_PACS'])
    assert question.value == ['TINY_PACS']


def test_ae_questionnaire_interactive_flow() -> None:
    questionnaire = AEQuestionnaire()
    ae_title, port, max_pdu, dump_ds = questionnaire.questions
    ae_title.value = 'AET1'
    ae_title.value = 'AET2'
    port.value = 104
    max_pdu.value = 16384
    dump_ds.value = 'Y'
    assert questionnaire.value() == {
        'ae_title': ['AET1', 'AET2'],
        'port': 104,
        'max_pdu_length': 16384,
        'dump_ds': True
    }


def test_db_questionnaire_password() -> None:
    questionnaire = DBQuestionnaire()
    questionnaire.db_driver.value = 'postgres'
    questionnaire.postgres_db_name.value = 'tiny_pacs_db'
    questionnaire.postgres_db_host.value = 'localhost'
    questionnaire.postgres_port.value = 5432
    questionnaire.postgres_user.value = 'postgres'
    questionnaire.postgres_password.value = 'secret'
    assert questionnaire.value() == {
        'db_name': 'tiny_pacs_db',
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'secret'
    }
