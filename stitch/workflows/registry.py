class WorkflowRegistry:
    _default = None

    def __init__(self):
        self._workflows: dict[str, object] = {}

    @classmethod
    def default(cls) -> "WorkflowRegistry":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    def register(self, workflow) -> None:
        self._workflows[workflow.name] = workflow

    def unregister(self, name: str) -> None:
        self._workflows.pop(name, None)

    def find(self, name: str):
        return self._workflows.get(name)
