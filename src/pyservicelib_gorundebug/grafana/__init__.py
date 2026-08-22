#  Copyright (c) 2024 Sergey Alexeev
#  Email: sergeyalexeev@yahoo.com
#
#   Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT)
#   file for details.

"""Grafana dashboard templates for pyservicelib.

Equivalent to Go's grafana.DashboardFiles (embed.FS).

Usage — copy dashboards to a project directory and generate JSON::

    import shutil
    from pathlib import Path
    from pyservicelib_gorundebug.grafana import get_files

    dest = Path("grafana")
    pkg = get_files()
    shutil.copytree(str(pkg / "dashboards"), str(dest / "dashboards"))
    shutil.copy2(str(pkg / "Dockerfile"), str(dest / "Dockerfile"))
    shutil.copy2(str(pkg / "generate.sh"), str(dest / "generate.sh"))
    # then: cd grafana && bash generate.sh
"""

import importlib.resources as _res
from importlib.resources.abc import Traversable


def get_files() -> Traversable:
    """Return a Traversable rooted at the grafana package directory.

    Contains: Dockerfile, generate.sh, dashboards/*.jsonnet, dashboards/_lib.libsonnet
    """
    return _res.files(__package__)
