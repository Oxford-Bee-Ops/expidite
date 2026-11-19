"""Export the retained system journal directly to the requesting portal."""

from __future__ import annotations

import subprocess
import tempfile
import threading
from contextlib import closing
from http import HTTPStatus
from http.client import HTTPSConnection
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from expidite_rpi.core import configuration as root_cfg

if TYPE_CHECKING:
    from typing import IO
    from urllib.parse import SplitResult

logger = root_cfg.setup_logger("expidite")
_EXPORT_LOCK = threading.Lock()


def request_system_logs(upload_url: object, upload_token: object) -> dict[str, str]:
    """Accept one export at a time and let the IoT Hub handler reply immediately."""
    if not isinstance(upload_url, str) or not isinstance(upload_token, str) or not upload_token:
        return {"status": "invalid"}
    try:
        target = urlsplit(upload_url)
        port = target.port
    except ValueError:
        return {"status": "invalid"}
    if (
        target.scheme != "https"
        or not target.hostname
        or target.username is not None
        or target.query
        or target.fragment
        or not target.path
    ):
        return {"status": "invalid"}
    if not _EXPORT_LOCK.acquire(blocking=False):
        return {"status": "busy"}
    try:
        threading.Thread(
            target=_export, args=(target, port, upload_token), name="system-log-export", daemon=True
        ).start()
    except Exception:
        _EXPORT_LOCK.release()
        raise
    return {"status": "accepted"}


def _export(target: SplitResult, port: int | None, upload_token: str) -> None:
    try:
        with tempfile.TemporaryFile() as archive:
            _collect_journal(archive)
            length = archive.tell()
            archive.seek(0)
            _upload(target, port, upload_token, archive, length)
    except Exception as exc:
        # Log exception types without the request headers.
        logger.warning(f"System journal export failed ({type(exc).__name__})")
        try:
            _upload(target, port, upload_token, None, 0)
        except Exception as status_exc:
            logger.warning(
                f"System journal export status could not be uploaded ({type(status_exc).__name__})"
            )
    finally:
        _EXPORT_LOCK.release()


def _upload(
    target: SplitResult, port: int | None, upload_token: str, archive: IO[bytes] | None, length: int
) -> None:
    host = target.hostname
    assert host is not None
    with closing(HTTPSConnection(host, port, timeout=30, blocksize=64 * 1024)) as connection:
        connection.request(
            "PUT",
            target.path,
            body=archive,
            headers={
                "Authorization": f"Bearer {upload_token}",
                "Content-Type": "application/gzip",
                "Content-Length": str(length),
            },
        )
        with connection.getresponse() as response:
            if response.status != HTTPStatus.NO_CONTENT:
                message = f"Log upload returned HTTP {response.status}"
                raise RuntimeError(message)


def _collect_journal(archive: IO[bytes]) -> None:
    # The installer gives the management service user passwordless sudo. --system selects the system journal
    # across every retained boot, and --all preserves fields and lines that journalctl may otherwise shorten.
    with subprocess.Popen(
        ["sudo", "-n", "journalctl", "--system", "--no-pager", "--all", "--quiet"],
        stdout=subprocess.PIPE,
    ) as journal:
        try:
            assert journal.stdout is not None
            with subprocess.Popen(["gzip", "-1"], stdin=journal.stdout, stdout=archive) as compressor:
                journal.stdout.close()
                try:
                    returncode = compressor.wait(timeout=180)
                finally:
                    if compressor.poll() is None:
                        compressor.kill()
                if returncode:
                    raise subprocess.CalledProcessError(returncode, compressor.args)
            if journal.wait(timeout=5):
                msg = "journalctl failed"
                raise RuntimeError(msg)
        finally:
            if journal.poll() is None:
                journal.kill()
