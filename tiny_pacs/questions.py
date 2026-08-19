# -*- coding: utf-8 -*-
from typing import Any, Dict, Iterator, List, Callable, Union

class Question:
    def __init__(self, key, prompt: str, handler: Callable[[Any], Any],
                 repeatable=False, default: Any = None,
                 default_repr: Union[str, None] = None):
        self.key = key
        self.prompt = prompt
        self.handler = handler
        self.repeatable = repeatable
        self.default = default
        self.default_repr = default_repr
        if repeatable:
            self._value = []
        else:
            self._value = default

    @property
    def value(self) -> Any:
        if self.repeatable:
            if not self._value:
                return self.default
            return [self.handler(v) for v in self._value]
        return self.handler(self._value)

    @value.setter
    def value(self, _value: Any):
        if not _value:
            return

        if self.repeatable:
            self._value.append(_value)
        self._value = _value


class Questionnaire:
    def __init__(self, questions: List[Question]):
        self.questions = questions

    def __iter__(self) -> Iterator[Question]:
        yield from self.questions

    def value(self) -> Dict[str, Any]:
        return {q.key: q.value for q in self.questions}
