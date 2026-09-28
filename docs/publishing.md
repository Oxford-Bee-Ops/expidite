# Publishing expidite

The workflow in [`.github/workflows/publish.yml`](../.github/workflows/publish.yml) builds a wheel and source archive. Run it manually to publish to TestPyPI. Pushing a version tag publishes to PyPI. The two publishing jobs use PyPI Trusted Publishing, so GitHub does not need a PyPI API token or password.

## One-time account setup

1. Check that the name `expidite` is available on [PyPI](https://pypi.org/) and [TestPyPI](https://test.pypi.org/). If an existing project belongs to your account, use its project settings instead of creating a pending publisher. A pending publisher does not reserve a name before its first upload.
2. In the GitHub repository, open **Settings → Environments** and create environments named exactly `testpypi` and `pypi`. For `pypi`, configure required reviewers if your GitHub plan and repository visibility support them. You can also restrict `pypi` deployments to version tags such as `v*` and `testpypi` deployments to the `main` branch. Do not put PyPI credentials in either environment.
3. On [PyPI's Trusted Publishers page](https://pypi.org/manage/account/publishing/), add a pending publisher for the new project, using these exact values:

   | Field | Value |
   | --- | --- |
   | PyPI project name | `expidite` |
   | GitHub owner | `Oxford-Bee-Ops` |
   | GitHub repository | `expidite` |
   | Workflow filename | `publish.yml` |
   | Environment | `pypi` |

4. On [TestPyPI's Trusted Publishers page](https://test.pypi.org/manage/account/publishing/), register the same owner, repository, workflow filename, and project name, but use environment `testpypi`. PyPI and TestPyPI have separate accounts and publisher settings.

If the project already exists in either index under your account, add a Trusted Publisher in that project's **Publishing** settings with the same GitHub values. The workflow filename and environment names must match exactly.

## Release steps

1. Merge the release changes, including `publish.yml`, into `main`. Confirm `[project].version` in `pyproject.toml` has not been published on either index, and change it if needed; an index will not accept a second upload of the same version. Review the README, dependency pins, and package contents before release.
2. In GitHub, open **Actions → Publish Python package → Run workflow**, select `main`, and run it. This uploads the current version to TestPyPI. Check the build and publish jobs and the resulting [TestPyPI project page](https://test.pypi.org/project/expidite/). TestPyPI may not have all the package's dependencies; the build artifact itself is also available in the workflow run.
3. After that TestPyPI run succeeds, copy its commit SHA from the GitHub Actions run. Tag that exact commit and push the tag. For example, if `pyproject.toml` says `version = "0.1.303"`:

   ```sh
   git fetch origin main
   git tag v0.1.303 <tested-commit-sha>
   git push origin v0.1.303
   ```

   Replace `0.1.303` with the actual version. The workflow checks that the pushed tag is `v` followed by `[project].version`. The tag run uploads to PyPI, with approval first if the `pypi` environment requires it.
4. Check the [PyPI project page](https://pypi.org/project/expidite/) and install the published version in a clean Python 3.13 or newer environment. Confirm the Raspberry Pi setup and installer behavior on a device before directing a fleet to the PyPI release.

For subsequent releases, change the version, run TestPyPI once, and push the matching tag. If a TestPyPI upload has already used a version, increment the version before trying that index again. Publishing to TestPyPI and PyPI uses separate indexes, so the same version can be uploaded once to each.

See the [PyPA GitHub Actions publishing guide](https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/) and [PyPI Trusted Publishing documentation](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/) for account setup details.
