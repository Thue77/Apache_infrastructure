import dataclasses

@dataclasses.dataclass
class File:
    name: str
    path: str
    type: str