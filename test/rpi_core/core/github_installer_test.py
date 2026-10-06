from unittest.mock import MagicMock

import pytest

from expidite_rpi.scripts import github_installer


@pytest.fixture
def installer(monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    """Stub out GitHub, pip and the post-install script so only the installer's decisions are exercised."""
    mocks = {
        "download": MagicMock(),
        "post_install": MagicMock(),
    }
    monkeypatch.setattr(github_installer, "_get_my_github_pat", lambda: "pat")
    monkeypatch.setattr(github_installer, "Github", MagicMock())
    monkeypatch.setattr(github_installer, "_get_my_package_name", lambda: "my-package")
    monkeypatch.setattr(github_installer, "_download_and_install_package", mocks["download"])
    monkeypatch.setattr(github_installer, "_run_package_post_install", mocks["post_install"])
    return mocks


def _set_versions(monkeypatch: pytest.MonkeyPatch, installed: str, latest: str) -> None:
    monkeypatch.setattr(github_installer, "_get_installed_user_repo_version", lambda: installed)
    monkeypatch.setattr(github_installer, "_get_latest_user_repo_version", lambda _g: (latest, MagicMock()))


class TestInstallUserRepoPackage:
    @pytest.mark.unittest
    @pytest.mark.parametrize(
        "package_name", ["my-package", "my_package", "My-Package", "my.package", "MY--..__PACKAGE"]
    )
    def test_new_version_installs_then_runs_post_install(
        self, installer: dict[str, MagicMock], monkeypatch: pytest.MonkeyPatch, package_name: str
    ) -> None:
        _set_versions(monkeypatch, installed="0.1.0", latest="0.1.1")
        monkeypatch.setattr(github_installer, "_get_my_package_name", lambda: package_name)

        github_installer._install_user_repo_package()

        installer["download"].assert_called_once()
        installer["post_install"].assert_called_once_with("my_package")

    @pytest.mark.unittest
    @pytest.mark.parametrize(
        "package_name", ["my-package", "my_package", "My-Package", "my.package", "MY--..__PACKAGE"]
    )
    def test_up_to_date_still_runs_post_install(
        self, installer: dict[str, MagicMock], monkeypatch: pytest.MonkeyPatch, package_name: str
    ) -> None:
        # A post-install that failed on a previous pass must be retried even though the package itself is
        # already at the latest version.
        _set_versions(monkeypatch, installed="0.1.1", latest="0.1.1")
        monkeypatch.setattr(github_installer, "_get_my_package_name", lambda: package_name)

        github_installer._install_user_repo_package()

        installer["download"].assert_not_called()
        installer["post_install"].assert_called_once_with("my_package")

    @pytest.mark.unittest
    def test_failed_install_skips_post_install(
        self, installer: dict[str, MagicMock], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_versions(monkeypatch, installed="0.1.0", latest="0.1.1")
        installer["download"].side_effect = RuntimeError("pip failed")

        with pytest.raises(RuntimeError):
            github_installer._install_user_repo_package()

        installer["post_install"].assert_not_called()
