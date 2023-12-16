import dataclasses
from typing import List, Tuple, Any

@dataclasses.dataclass
class File:
    name: str
    path: str
    type: str
    partitions: List[Tuple[str,Any]] = None