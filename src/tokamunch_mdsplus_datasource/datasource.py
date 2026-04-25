from __future__ import annotations

from typing import Any

import numpy as np


class MDSplusDataSource:
    def __init__(self, config: dict[str, Any]):
        self.config = dict(config)

    def get(self, args: dict[str, Any]) -> np.ndarray:
        self._require_args(args, "signal")
        raise NotImplementedError("Implement backend-specific data fetching in get().")

    @staticmethod
    def _require_args(args: dict[str, Any], *required: str) -> None:
        missing = [name for name in required if name not in args]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(f"Missing required args: {missing_list}")
