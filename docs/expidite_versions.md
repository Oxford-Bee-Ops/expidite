# Which version of expidite a device runs

A device runs two packages: expidite, and your own code (the repo in `my_git_repo_url`). Your code decides which
version of expidite it needs, the same way it pins any other dependency. The one exception is a device you
are using to test an expidite branch, which you set with `expidite_git_branch` in `system.cfg`.

| `expidite_git_branch` in system.cfg | Which expidite is installed | When it changes |
|---|---|---|
| `main`, or not set (the default) | The version your code's `pyproject.toml` asks for | When your code changes |
| Any other branch | The latest commit on that expidite branch | Whenever the branch has a new commit |

The installer (`rpi_installer.sh`, or `zero_installer.sh` on a Pi Zero) applies this on every run. It runs on
every reboot and in the weekly OS update, and you can also start it from `bcli` or the management service.

## Use case 1: devices in the field

Leave `expidite_git_branch` unset, or set to `main`. Declare expidite in your code's `pyproject.toml`:

```toml
dependencies = [
    "expidite @ git+https://github.com/oxford-bee-ops/expidite.git@<commit or tag>",
    ...
]
```

To upgrade expidite on these devices, change that line and commit to the branch the devices use
(`my_git_branch`). On its next run, each device installs your code again, which brings in the expidite
version you asked for, and then reboots. No change to `system.cfg` is needed.

What you can put in the dependency:

| Dependency | What devices run |
|---|---|
| `expidite @ git+https://github.com/oxford-bee-ops/expidite.git@<commit>` | Exactly that commit. Recommended. |
| `expidite @ git+https://github.com/oxford-bee-ops/expidite.git@<tag>` | Exactly that tag. |
| `expidite @ git+https://github.com/oxford-bee-ops/expidite.git@main` | The head of expidite main *as of the last time your code changed*. See the caveat below. |
| `expidite==<version>` | That release from PyPI, once expidite is published there. |

**Caveat for `@main` and other branch names.** pip decides whether to replace an installed package by
comparing version numbers only. When your code is reinstalled and its dependency is `@main`, pip upgrades
expidite only if the version in expidite's `pyproject.toml` has changed. A commit to expidite main without a
version bump therefore does not reach devices. A pin to a commit or tag, or to a PyPI version, does not
have this problem, because changing it is a deliberate change to your code.

**Never pin a version older than the first one with this installer (0.1.304).** The installer that runs on a
device is the one in the installed expidite. Older installers install the head of expidite main on every run,
whatever your code asks for, so the device would move back to main the next time main changes.

## Use case 2: testing an expidite branch on a device

Push your work in progress to a branch of expidite, then on the test device set it in
`~/.expidite/system.cfg`:

```
expidite_git_branch="my-feature"
```

Reboot the device. The installer installs the latest commit on `my-feature` over whatever your code asked
for, and reboots again so that everything runs the new code.

From then on, each time you push to the branch, reboot the device to pick it up. The installer compares the
commit that is installed with the head of the branch, so you don't need to bump expidite's version for a
test commit. If your code changes while the device is on the test branch, the installer puts the test branch
back afterwards.

The device's own code (`my_git_repo_url`, `my_git_branch`) is installed as normal throughout.

## Use case 3: finishing a test

Remove `expidite_git_branch` from `system.cfg`, or set it back to `main`, and reboot. The installer notices
that a test branch was installed (it records it in `~/.expidite/flags/expidite-branch-override`), installs
the version of expidite your code asks for, and reboots.

## Use case 4: setting up a new device

Nothing changes. Copy `system.cfg`, `keys.env` and the installer to the device and run it, as described in
the [README](../README.md). The installer first installs expidite from `expidite_git_branch` (main unless you
set a test branch), because installing your code may need expidite's own scripts. It then installs your code,
which replaces that with the version your code asks for.

## Use case 5: repairing a broken install

If the installed expidite fails to import, for example after a power cut during an install, the installer
deletes it and installs the version your code asks for (or the test branch, on a test device). This happens
on the next run, without any action from you. See `heal_broken_install` in the installer for details.

## Publishing expidite to PyPI

This scheme doesn't depend on PyPI, but works the same when expidite is published there. Change your code's
dependency to `expidite==<version>`; the installer needs no change. Exact versions from PyPI avoid the `@main`
caveat above, and install faster because pip doesn't need to clone the repo. Test branches are still
installed from GitHub.

## How the installer does this

The steps, in order, in `rpi_installer.sh` and `zero_installer.sh`:

1. `prepare_code_install` checks that GitHub is reachable, repairs a broken install, and installs expidite
   if it isn't installed at all.
2. `install_user_code` installs your code when its branch has a new commit. pip installs the expidite version
   your code depends on at the same time.
3. `install_expidite` applies the test branch if one is set. If none is set but one was installed before, it
   reinstalls the version your code asks for.

Every expidite install does a normal `pip install` (to bring in any changed dependencies) followed by
`pip install --force-reinstall --no-deps` of expidite alone, so that code changes are installed even when the
version number hasn't changed.

Before this, `main` meant "install the head of expidite main on every run". That overrode any version your
code pinned, because pip only applied the pin on the runs where your code changed.
`~/.expidite/flags/expidite-repo-last-hash` belongs to that old scheme and is no longer used.
