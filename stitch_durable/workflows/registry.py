class WorkflowRegistry:
    """Maps workflow names to ``Workflow`` objects so recovery can find them.

    Every ``Workflow`` registers itself in ``WorkflowRegistry.default()`` when it is created. Pass your own registry only to keep workflows apart, for example in tests.
    """

    _default = None

    def __init__(self):
        """Create an empty registry."""
        self._workflows: dict[str, object] = {}

    @classmethod
    def default(cls) -> "WorkflowRegistry":
        """Return the process-wide registry, creating it on first use."""
        if cls._default is None:
            cls._default = cls()
        return cls._default

    def register(self, workflow) -> None:
        """Add ``workflow`` under ``workflow.name``, replacing any workflow with that name."""
        self._workflows[workflow.name] = workflow

    def unregister(self, name: str) -> None:
        """Remove the workflow called ``name``; does nothing if it is not registered."""
        self._workflows.pop(name, None)

    def find(self, name: str):
        """Return the workflow called ``name``, or ``None``."""
        return self._workflows.get(name)
