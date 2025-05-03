import json
import os.path
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.metadata import distribution, version
from pathlib import Path
from typing import Any


class PackageAnalyzer:
    """
    A utility class for analyzing Python package installation details and converting them to source specifications.

    This class provides functionality to:
    - Check if a package is installed in editable mode
    - Determine if a package is managed by version control (VCS)
    - Retrieve package source URLs
    - Convert package information to pip-compatible source specifications

    The source specifications can be used for package requirements or dependency management.
    """

    def __init__(self, package_name: str) -> None:
        self.package_name = package_name
        self.package_version = version(package_name)

        self.direct_url = self._get_direct_url(package_name)

    @property
    def editable(self) -> bool:
        """
        Check if a package is installed in editable mode.

        :return: True if the package is installed in editable mode, False otherwise.
        """
        return bool(self.direct_url and self.direct_url.get("dir_info", {}).get("editable"))

    @property
    def vcs(self) -> bool:
        """
        Check if a package's source is managed by VCS.

        :return: True if the package's source is e.g. a Git repository, False otherwise.
        """
        return bool(
            self.direct_url and "vcs_info" in self.direct_url and self.direct_url["vcs_info"].get("vcs") == "git"
        )

    @property
    def url(self) -> str | None:
        """
        Retrieve the source URL of a package.

        :return: The source URL of the package if available, None otherwise.
        """
        return self.direct_url.get("url") if self.direct_url else None

    def to_source(self) -> str:
        """
        Convert package information to a source specification string.

        For packages installed in editable mode from VCS, returns "package_name @ url".
        For packages installed in editable mode from local directory, builds a wheel and returns its path.
        For regular installations, returns "package_name==version".

        :return: A string representing the package source specification
        """

        if self.editable:
            if self.vcs:
                return f"{self.package_name} @ {self.url}"
            else:
                assert self.url is not None  # make mypy happy
                return self._build_wheel(self.url)
        else:
            return f"{self.package_name}=={self.package_version}"

    @classmethod
    def _build_wheel(cls, project_dir: str) -> str:
        import build

        project_dir = project_dir.replace("file://", "")
        builder = build.ProjectBuilder(project_dir)
        wheel_file = builder.build("wheel", os.getcwd())
        return os.path.basename(wheel_file)

    @classmethod
    def _get_direct_url(cls, package_name: str) -> dict[str, Any] | None:
        dist = distribution(package_name)
        try:
            content = dist.read_text("direct_url.json")
            return json.loads(content) if content else None
        except FileNotFoundError:
            return None


@contextmanager
def change_dir(destination: Path) -> Iterator[None]:
    """
    A context manager that temporarily changes the current working directory.

    This function allows you to temporarily change the working directory and ensures
    that the original directory is restored when exiting the context, even if an
    exception occurs.

    :param destination: The path to the directory to change to.
    """
    prev_cwd = os.getcwd()
    try:
        os.chdir(destination)
        yield
    finally:
        os.chdir(prev_cwd)
