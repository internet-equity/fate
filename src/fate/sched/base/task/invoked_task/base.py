from __future__ import annotations

import abc
import typing

from .. import ext

if typing.TYPE_CHECKING:
    from .event import TaskEvents


class InvokedTask(ext.TaskConfExt):
    """Abstract base to task classes extended for invocation of their
    commands by the operating system.

    """
    @abc.abstractmethod
    def ready_(self) -> bool:
        pass

    @abc.abstractmethod
    def events_(self) -> typing.Optional[TaskEvents]:
        pass
