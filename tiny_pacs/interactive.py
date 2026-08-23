"""Interactive configuration front-ends.

Provides the questionnaires for every configuration section and the
front-ends that run them.
"""
from collections.abc import Iterator
from typing import Any

from . import config
from .questions import Question, Questionnaire


class AEQuestionnaire(Questionnaire):
    """Questionnaire for the ``ae`` configuration section."""

    key = 'ae'

    def __init__(self) -> None:
        questions = [
            Question(
                'ae_title', 'Enter AE Titles for your service',
                lambda v: v, True, ['TINY_PACS']
            ),
            Question(
                'port', 'Enter SCP port for your DICOM service',
                int, False, 11112
            ),
            Question(
                'max_pdu_length', 'Enter max PDU size',
                int, False, 65536
            ),
            Question(
                'dump_ds', 'Should your DICOM service dump Datasets and '
                'association PDU',
                lambda v: v.lower() == 'y', default='Y'
            )
        ]
        super().__init__(questions)


class LogginQuestionnaire(Questionnaire):
    """Questionnaire for the ``log`` configuration section."""

    key = 'log'
    stream_handler = 'logging.StreamHandler'
    rotating_handler = 'logging.handlers.RotatingFileHandler'
    file_handler = 'logging.FileHandler'

    def __init__(self) -> None:
        self.logging_handler = Question(
            'logging_handler',
            'Select logging handler: StreamHandler(1), FileHandler(2), '
            'RotatingFileHandler(3)',
            self.select_logging_handler, False, '1'
        )
        self.logging_level = Question(
            'logging_level',
            'Select logging level: DEBUG(1), INFO(2), WARNING(3), ERROR(4)',
            self.select_logging_level, False, '1'
        )
        self.log_file = Question(
            'log_file',
            'Enter log file name',
            lambda v: v, False, './tiny_pacs.log'
        )
        self.log_file_size = Question(
            'log_file_size',
            'Enter max log file size',
            int, False, f'{10 * 1024 * 1024}'
        )
        self.log_backup_count = Question(
            'log_backup_count',
            'Enter backup count',
            int, False, f'{10}'
        )
        super().__init__([
            self.logging_handler, self.logging_level, self.log_file,
            self.log_file_size, self.log_backup_count
        ])

    def __iter__(self) -> Iterator[Question]:
        """Yields questions relevant for the selected logging handler"""
        yield self.logging_handler
        if self.logging_handler.value != self.stream_handler:
            if self.logging_handler.value == self.rotating_handler:
                yield self.log_file_size
                yield self.log_backup_count
            yield self.log_file
        yield self.logging_level

    def value(self) -> dict[str, Any]:
        """Builds the logging configuration from the collected answers

        :return: logging configuration dictionary
        :rtype: dict[str, Any]
        """
        if self.logging_handler.value == self.stream_handler:
            return {
                'handlers': {
                    'console': {
                        'class': self.stream_handler,
                        'level': self.logging_level.value,
                        'formatter': 'simple',
                        'stream': 'ext://sys.stdout'
                    }
                }
            }
        if self.logging_handler.value == self.file_handler:
            return {
                'handlers': {
                    'console': {
                        'class': self.file_handler,
                        'level': self.logging_level.value,
                        'formatter': 'simple',
                        'filename': self.log_file.value
                    }
                }
            }
        return {
            'handlers': {
                'console': {
                    'class': self.rotating_handler,
                    'level': self.logging_level.value,
                    'formatter': 'simple',
                    'filename': self.log_file.value,
                    'maxBytes': self.log_file_size.value,
                    'backupCount': self.log_backup_count.value
                }
            }
        }

    def select_logging_handler(self, value: str) -> str:
        """Maps the selected index to a logging handler class name

        :param value: selected index ("1", "2" or "3")
        :type value: str
        :return: logging handler class name
        :rtype: str
        :raises ValueError: raised for an unsupported index
        """
        index = int(value)
        if index == 3:
            return self.rotating_handler
        if index == 2:
            return self.file_handler
        if index == 1:
            return self.stream_handler
        raise ValueError('Unsupported logging handler')

    def select_logging_level(self, value: str) -> str:
        """Maps the selected index to a logging level name

        :param value: selected index ("1" to "4")
        :type value: str
        :return: logging level name
        :rtype: str
        :raises ValueError: raised for an unsupported index
        """
        index = int(value)
        if index == 1:
            return 'DEBUG'
        if index == 2:
            return 'INFO'
        if index == 3:
            return 'WARNING'
        if index == 4:
            return 'ERROR'
        raise ValueError('Unsupported logging level')


class ComponentsQuestionnaire(Questionnaire):
    """Questionnaire that selects and configures components."""

    key = 'components'

    def __init__(self) -> None:
        super().__init__([
            Question(k, f'Use component {k}?',
                     lambda v: v.lower() == 'y',
                     default='N')
            for k in config.COMPONENT_REGISTRY.keys()
        ])
        self._value: dict[str, Any] = {}

    def __iter__(self) -> Iterator[Question]:
        """Yields the questions and nested component questionnaires"""
        for question in self.questions:
            yield question
            value = question.value
            if not value:
                continue
            component = config.COMPONENT_REGISTRY[question.key]
            questionnaire = component.interactive()
            yield from questionnaire
            component_config = {'on': True}
            component_config.update(questionnaire.value())
            self._value[question.key] = component_config

    def value(self) -> dict[str, Any]:
        """Returns the collected component configurations

        :return: component configurations by component name
        :rtype: dict[str, Any]
        """
        return self._value


class InteractiveFront:
    """Base class for interactive configuration front-ends."""

    def __init__(self) -> None:
        self.questionnairies = [
            AEQuestionnaire(),
            LogginQuestionnaire(),
            ComponentsQuestionnaire()
        ]
        self.save_config = Question(
            'save_config', 'Do you want to save config in a file? (Y,N)',
            lambda v: v.lower() == 'y', default='N'
        )
        self.start_server = Question(
            'start_server', 'Do you want to start the server? (Y,N)',
            lambda v: v.lower() == 'y', default='Y'
        )
        self.config_filename = Question(
            'config_filename', 'Enter config file name',
            lambda v: v
        )

    def run(self) -> tuple[dict[str, Any], bool]:
        """Runs the full configuration wizard.

        Asks every configuration question, optionally saves the resulting
        configuration to a file and finally asks whether the server should
        be started.

        :return: collected configuration and whether the server should start
        """
        _config = self.run_questionnairies()
        self.save_config.value = self.request_value(self.save_config, False)
        if self.save_config.value:
            self.config_filename.value = self.request_value(
                self.config_filename, False
            )
            self.save_config_to_file(_config, self.config_filename.value)
        self.start_server.value = self.request_value(self.start_server, False)
        return _config, self.start_server.value

    def run_questionnairies(self) -> dict[str, Any]:
        """Runs all the questionnaires without the save/start questions.

        :return: collected configuration
        """
        _config: dict[str, Any] = {}
        for questionnaire in self.questionnairies:
            for question in questionnaire:
                if question.repeatable:
                    while True:
                        value = self.request_value(question, True)
                        if not value:
                            break
                        question.value = value
                else:
                    value = self.request_value(question, False)
                    question.value = value
            _config[questionnaire.key] = questionnaire.value()
        return _config

    def save_config_to_file(
            self, _config: dict[str, Any], file_name: str
    ) -> None:
        """Saves the collected configuration merged with the defaults.

        :param _config: configuration collected from the questionnaires
        :param file_name: name of the file to write the YAML configuration to
        """
        conf = config.Config()
        conf.update_config(_config)
        config.write_yaml(conf, file_name)

    def request_value(self, question: Question, repeatable: bool) -> str:
        """Asks one question and returns the raw answer

        :param question: question to ask
        :type question: Question
        :param repeatable: whether the question accepts multiple answers
        :type repeatable: bool
        :raises NotImplementedError: always, subclasses must implement this
        """
        raise NotImplementedError()


class TerminalFront(InteractiveFront):
    """Interactive front-end that asks questions on the terminal."""

    def request_value(self, question: Question, repeatable: bool) -> str:
        """Asks one question on the terminal

        :param question: question to ask
        :type question: Question
        :param repeatable: whether the question accepts multiple answers
        :type repeatable: bool
        :return: raw answer entered by the user
        :rtype: str
        """
        _default = question.default_repr or question.default
        if not repeatable:
            return input(f'{question.prompt}[{_default}]: ')

        return input(f'{question.prompt}[{_default}, empy to skip]:')
