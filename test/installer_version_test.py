"""Exercise the Python checks embedded in both standalone device installers."""

import contextlib
import importlib.metadata
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

INSTALLERS = ("rpi_installer.sh", "zero_installer.sh")
SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "expidite_rpi" / "scripts"
GIT_URL = "https://github.com/oxford-bee-ops/expidite.git"
OLD_COMMIT = "a" * 40
NEW_COMMIT = "b" * 40


def _embedded_python(installer: str, function: str) -> str:
    script = (SCRIPTS / installer).read_text(encoding="utf-8")
    body = script.split(f"{function}() {{", maxsplit=1)[1].split("\n}", maxsplit=1)[0]
    return body.split('bin/python" -c "', maxsplit=1)[1].split('\n" "$', maxsplit=1)[0]


def _shell_function(installer: str, function: str) -> str:
    script = (SCRIPTS / installer).read_text(encoding="utf-8")
    return (
        f"{function}() {{"
        + script.split(f"{function}() {{", maxsplit=1)[1].split("\n}", maxsplit=1)[0]
        + "\n}"
    )


def _requirement(installer: str, requirements: list[str]) -> str:
    output = io.StringIO()
    with (
        mock.patch.object(importlib.metadata, "requires", return_value=requirements),
        mock.patch.object(sys, "argv", ["python", "user-package"]),
        contextlib.redirect_stdout(output),
    ):
        exec(_embedded_python(installer, "get_user_expidite_requirement"), {"__name__": "__main__"})  # noqa: S102
    return output.getvalue().strip()


class _Distribution:
    def __init__(self, version: str, direct_url: dict[str, object] | None) -> None:
        self.version = version
        self.direct_url = direct_url

    def read_text(self, filename: str) -> str | None:
        assert filename == "direct_url.json"
        return json.dumps(self.direct_url) if self.direct_url is not None else None


def _installed_matches(installer: str, requirement: str, distribution: _Distribution) -> bool:
    with (
        mock.patch.object(importlib.metadata, "distribution", return_value=distribution),
        mock.patch.object(sys, "argv", ["python", requirement]),
        pytest.raises(SystemExit) as result,
    ):
        exec(_embedded_python(installer, "expidite_requirement_is_installed"), {"__name__": "__main__"})  # noqa: S102
    return result.value.code == 0


def _git_distribution(commit: str, revision: str) -> _Distribution:
    return _Distribution(
        "0.1.305",
        {"url": GIT_URL, "vcs_info": {"vcs": "git", "requested_revision": revision, "commit_id": commit}},
    )


@pytest.mark.parametrize("installer", INSTALLERS)
def test_selects_only_applicable_base_requirement(installer: str) -> None:
    pin = f"expidite @ git+{GIT_URL}@{NEW_COMMIT}"
    assert (
        _requirement(
            installer,
            [
                "expidite",
                'expidite==0.1.304; python_version < "3.0"',
                'expidite==0.1.304; extra == "camera"',
                f'{pin} ; python_version >= "3.0"',
            ],
        )
        == f'{pin} ; python_version >= "3.0"'
    )
    assert _requirement(installer, ['expidite==0.1.304; python_version < "3.0"']) == ""
    assert _requirement(installer, ["expidite"]) == ""


@pytest.mark.parametrize("installer", INSTALLERS)
def test_same_version_different_commit_requires_install(installer: str) -> None:
    requirement = f"expidite @ git+{GIT_URL}@{NEW_COMMIT}"
    assert not _installed_matches(installer, requirement, _git_distribution(OLD_COMMIT, OLD_COMMIT))
    assert _installed_matches(installer, requirement, _git_distribution(NEW_COMMIT, NEW_COMMIT))
    assert _installed_matches(
        installer,
        f"expidite @ git+{GIT_URL}@{NEW_COMMIT[:12]}",
        _git_distribution(NEW_COMMIT, NEW_COMMIT[:12]),
    )
    wrong_url = _git_distribution(NEW_COMMIT, NEW_COMMIT)
    assert wrong_url.direct_url is not None
    wrong_url.direct_url["url"] = "https://github.com/another-team/expidite.git"
    assert not _installed_matches(installer, requirement, wrong_url)


@pytest.mark.parametrize("installer", INSTALLERS)
def test_exact_release_pin_replaces_git_install_of_same_version(installer: str) -> None:
    assert not _installed_matches(installer, "expidite==0.1.305", _git_distribution(NEW_COMMIT, NEW_COMMIT))
    assert _installed_matches(installer, "expidite==0.1.305", _Distribution("0.1.305", None))


@pytest.mark.parametrize("installer", INSTALLERS)
def test_branch_pin_keeps_installed_branch_until_user_changes_it(installer: str) -> None:
    assert _installed_matches(
        installer, f"expidite @ git+{GIT_URL}@main", _git_distribution(OLD_COMMIT, "main")
    )


def _run_install_expidite(
    installer: str, home: Path, requirement: str, installed: str, install_rc: int = 0
) -> str:
    scripts = home / "venv" / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    (scripts / "rpi_installer.sh").touch()
    (scripts / "dummy.py").touch()
    shell = f"""
echo_header() {{ :; }}
get_user_expidite_requirement() {{ printf '%s' "$REQUIREMENT"; }}
get_installed_expidite_commit() {{ printf '%s' "$INSTALLED"; }}
install_expidite_requirement() {{ printf 'INSTALL=%s\\n' "$1"; return "$INSTALL_RC"; }}
pip() {{ printf 'Version: 0.1.305\\n'; }}
sync() {{ :; }}
{_shell_function(installer, "install_expidite")}
install_expidite
"""
    env = {
        **os.environ,
        "HOME": str(home),
        "venv_dir": "venv",
        "expidite_git_branch": "main",
        "EXPIDITE_GIT_URL": GIT_URL,
        "EXP_REMOTE_HASH": NEW_COMMIT,
        "REQUIREMENT": requirement,
        "INSTALLED": installed,
        "INSTALL_RC": str(install_rc),
    }
    return subprocess.run(["bash", "-c", shell], env=env, text=True, capture_output=True, check=True).stdout


@pytest.mark.skipif(os.name == "nt", reason="Bash fixture requires a Unix host")
@pytest.mark.parametrize("installer", INSTALLERS)
def test_unpinned_repository_keeps_tracking_main(installer: str, tmp_path: Path) -> None:
    output = _run_install_expidite(installer, tmp_path, "", OLD_COMMIT)
    assert f"INSTALL=expidite @ git+{GIT_URL}@{NEW_COMMIT}" in output


@pytest.mark.skipif(os.name == "nt", reason="Bash fixture requires a Unix host")
@pytest.mark.parametrize("installer", INSTALLERS)
def test_rollback_retries_until_install_succeeds(installer: str, tmp_path: Path) -> None:
    flag = tmp_path / ".expidite" / "flags" / "expidite-branch-override"
    flag.parent.mkdir(parents=True)
    flag.write_text("test-branch", encoding="utf-8")
    requirement = f"expidite @ git+{GIT_URL}@{NEW_COMMIT}"
    _run_install_expidite(installer, tmp_path, requirement, OLD_COMMIT, install_rc=1)
    assert flag.exists()
    output = _run_install_expidite(installer, tmp_path, requirement, OLD_COMMIT)
    assert f"INSTALL={requirement}" in output
    assert not flag.exists()
