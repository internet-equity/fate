"""Extension of task configuration with specific functionality."""
from fate.common.ext import TaskConfExt


class BoundTask(TaskConfExt):
    """Abstract base to classes binding a task to its persisted state."""

    def __init__(self, data, /, state):
        super().__init__(data)

        self._state_ = state
