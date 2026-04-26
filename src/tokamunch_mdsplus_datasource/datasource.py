from __future__ import annotations

import libtokamap
from typing_extensions import override

import logging
import string
import time
from typing import Any

import numpy as np
import mdsthin


logger = logging.getLogger(__name__)


class MDSplusDataSource(libtokamap.DataSource):
    def __init__(self, config: dict[str, Any]):
        self.config = dict(config or {})
        self.connection = None

        required = {"connection_string"}
        self._require_args(self.config, *required)

        server = self.config["connection_string"]
        logger.debug("Opening MDSplus connection: server=%s", server)
        self.connection = mdsthin.Connection(server)

        self.default_template = str(self.config.get("template", "{signal}"))
        self.default_suffix = str(self.config.get("suffix", ""))

        self.trees = set({})

    def close(self) -> None:
        if self.connection is None:
            return

        try:
            self.connection.closeAllTrees()
        except Exception:
            logger.exception("Failed to close MDSplus trees")

        try:
            self.connection.disconnect()
        except Exception:
            logger.exception("Failed to disconnect MDSplus connection")

        self.connection = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            # Never let destructor cleanup errors escape.
            pass

    # def __del__(self):
    #     self.connection.closeAllTrees()
    #     self.connection.disconnect()

    @override
    def get(self, args: dict[str, Any]) -> np.ndarray:
        # required = {"shot", "signal"}
        # self._require_args(args, *required)
        args = dict(args or {})

        # Global defaults first, per-mapping args win.
        context = {
            **self.config,
            **args,
        }

        template = str(context.get("template", self.default_template))
        suffix = str(context.get("suffix", self.default_suffix))

        # Ensure {suffix} resolves to the final selected suffix.
        context["suffix"] = suffix

        # Only require fields actually used in the template.
        # self._require_template_args(template, context)
        required = fields_used_by_template(template)
        self._require_args(context, *required)

        shot = int(context["shot"])
        signal = str(context["signal"])

        if "tree" in context:
            tree = str(context["tree"])
            logger.debug(f"Opening MDSplus tree: tree={tree} shot={shot}")
            if (tree, shot) not in self.trees:
                self.connection.openTree(tree, shot)
                self.trees.add((tree, shot))

        # template = str(args.get("template", self.default_template))
        # suffix = str(args.get("suffix", self.default_suffix))
        #
        # context = {
        #     **args,
        #     "suffix": suffix,
        # }

        expression = format_template(template, context)
        logger.debug(f"Rendered expression for mds request: {expression}")
        t0 = time.perf_counter()
        result = self.connection.get(expression).data()
        dt = time.perf_counter() - t0
        logger.debug(f"mds request returned without exception in {dt}s")
        return np.asarray(result)

    @staticmethod
    def _require_args(args: dict[str, Any], *required: str) -> None:
        missing = [name for name in required if name not in args]
        if missing:
            missing_list = ", ".join(missing)
            logger.error(
                "Missing required MDSplus datasource config: %s",
                ", ".join(sorted(missing_list)),
            )
            raise ValueError(f"Missing required args: {missing_list}")


def format_template(template: str, context: dict[str, Any]) -> str:
        fields = fields_used_by_template(template)

        missing = sorted(field for field in fields if field not in context)
        if missing:
            raise ValueError(
                f"MDSplus template requires missing arg(s): {', '.join(missing)}. "
                f"Template was: {template!r}"
            )

        try:
            return template.format_map(_StrictFormatContext(context))
        except Exception as exc:
            raise ValueError(
                f"Failed to format MDSplus template {template!r} "
                f"with args {sorted(context.keys())}"
            ) from exc


def fields_used_by_template(template: str) -> set[str]:
        formatter = string.Formatter()
        fields: set[str] = set()
    
        for _, field_name, _, _ in formatter.parse(template):
            if field_name is None:
                continue
    
            # Handles simple fields like {signal}, {shot}, {suffix}.
            # For future-proofing, also reduce {foo.bar} or {foo[0]} to "foo".
            root = field_name.split(".", 1)[0].split("[", 1)[0]
            fields.add(root)
    
        return fields
    
    
class _StrictFormatContext(dict):
        def __missing__(self, key: str) -> Any:
            raise KeyError(key)
