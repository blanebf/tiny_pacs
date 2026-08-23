"""Interactive configuration questions.

Provides :class:`Question` and :class:`Questionnaire` building blocks used
by the interactive configuration front-ends and by component
:meth:`~tiny_pacs.component.Component.interactive` questionnaires.
"""
from collections.abc import Callable, Iterator
from typing import Any


class Question:
    """Single interactive configuration question.

    :ivar key: configuration key the answer is stored under
    :ivar prompt: prompt shown to the user
    :ivar handler: converts the raw answer to a configuration value
    :ivar repeatable: whether the question can be answered multiple times
    :ivar default: default answer when nothing is entered
    :ivar default_repr: displayed default, if different from ``default``
    """

    def __init__(
            self,
            key: str,
            prompt: str,
            handler: Callable[[Any], Any],
            repeatable: bool = False,
            default: Any = None,
            default_repr: str | None = None
    ) -> None:
        """Initializes the question

        :param key: configuration key for the answer
        :type key: str
        :param prompt: prompt shown to the user
        :type prompt: str
        :param handler: converts the raw answer to a configuration value
        :type handler: Callable[[Any], Any]
        :param repeatable: allow multiple answers, defaults to False
        :type repeatable: bool, optional
        :param default: default answer, defaults to None
        :type default: Any, optional
        :param default_repr: displayed default, defaults to None
        :type default_repr: str, optional
        """
        self.key = key
        self.prompt = prompt
        self.handler = handler
        self.repeatable = repeatable
        self.default = default
        self.default_repr = default_repr
        self._value: Any
        if repeatable:
            self._value = []
        else:
            self._value = default

    @property
    def value(self) -> Any:
        """Answer or answers processed by the handler"""
        if self.repeatable:
            if not self._value:
                return self.default
            return [self.handler(v) for v in self._value]
        return self.handler(self._value)

    @value.setter
    def value(self, _value: Any) -> None:
        self._set_value(_value)

    def _set_value(self, _value: Any) -> None:
        if _value is None or _value == '':
            return

        if self.repeatable:
            self._value.append(_value)
        else:
            self._value = _value


class Questionnaire:
    """Ordered set of questions for one configuration section."""

    #: Configuration section name the questionnaire values belong to
    key: str

    def __init__(self, questions: list[Question]):
        """Initializes the questionnaire

        :param questions: questions of this questionnaire
        :type questions: list[Question]
        """
        self.questions = questions

    def __iter__(self) -> Iterator[Question]:
        yield from self.questions

    def value(self) -> dict[str, Any]:
        """Returns the collected answers as a configuration dictionary

        :return: configuration values keyed by question keys
        :rtype: dict[str, Any]
        """
        return {q.key: q.value for q in self.questions}
