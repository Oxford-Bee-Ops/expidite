##############################################################################################################
# Expidite Web Server
#
# A lightweight Flask web server that exposes a subset of the bcli functionality via a browser.
# Designed to run as a systemd service alongside the main expidite service on a Raspberry Pi.
#
# Endpoints:
#   /             - Dashboard with links to all actions
#   /terminal     - Full interactive BCLI terminal (xterm.js + WebSocket + pty)
#   /status       - View device status
#   /config       - View device configuration
#   /update       - Trigger a software update (runs rpi_installer.sh)
#   /reboot       - Reboot the device (requires confirmation)
##############################################################################################################
from __future__ import annotations

import contextlib
import html
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from flask import Flask, request
from flask_sock import Sock

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.rpi_core import RpiCore

_PTY_SUPPORTED = False
try:
    import fcntl
    import pty
    import select
    import struct
    import termios

    _PTY_SUPPORTED = True
except ImportError:
    pass

logger = root_cfg.setup_logger("expidite")

app = Flask(__name__)
sock = Sock(app)

_init_lock = threading.Lock()
_rpi_core: RpiCore | None = None


def _get_rpi_core() -> RpiCore:
    global _rpi_core
    if _rpi_core is None:
        with _init_lock:
            if _rpi_core is None:
                sc = RpiCore()
                inventory = root_cfg.load_configuration()
                if inventory:
                    sc.configure(inventory)
                _rpi_core = sc
    return _rpi_core


def _page(title: str, body: str) -> str:
    """Wrap *body* HTML in a minimal page shell."""
    return (
        "<!DOCTYPE html><html><head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{html.escape(title)} — Expidite</title>"
        "<style>"
        "body{font-family:system-ui,sans-serif;margin:2rem;background:#f5f5f5;color:#222}"
        "h1{margin-top:0}"
        "pre{background:#fff;padding:1rem;border:1px solid #ddd;border-radius:4px;"
        "overflow-x:auto;white-space:pre-wrap;word-wrap:break-word}"
        "a.btn{display:inline-block;padding:.6rem 1.2rem;margin:.3rem;border-radius:4px;"
        "text-decoration:none;color:#fff;background:#2563eb}"
        "a.btn:hover{background:#1d4ed8}"
        "a.btn.danger{background:#dc2626}"
        "a.btn.danger:hover{background:#b91c1c}"
        "a.btn.warn{background:#d97706}"
        "a.btn.warn:hover{background:#b45309}"
        "nav{margin-bottom:1.5rem}"
        ".confirm{background:#fff;padding:1.5rem;border:2px solid #dc2626;border-radius:8px;"
        "display:inline-block}"
        "</style>"
        "</head><body>"
        "<nav><a class='btn' href='/'>Home</a></nav>"
        f"<h1>{html.escape(title)}</h1>"
        f"{body}"
        "</body></html>"
    )


##############################################################################################################
# Dashboard


@app.route("/")
def index() -> str:
    device_name = root_cfg.my_device.name if root_cfg.my_device else "Unknown"
    device_id = root_cfg.my_device_id
    return _page(
        "Expidite Dashboard",
        f"<p>Device: <strong>{html.escape(device_name)}</strong> "
        f"(<code>{html.escape(device_id)}</code>)</p>"
        "<h2>Terminal</h2>"
        "<a class='btn' href='/terminal'>Open Terminal (BCLI)</a>"
        "<h2>Quick Actions</h2>"
        "<a class='btn warn' href='/update'>Update Software</a>"
        "<a class='btn danger' href='/reboot'>Reboot Device</a>",
    )


##############################################################################################################
# Update Software.


@app.route("/update", methods=["GET", "POST"])
def update() -> str:
    if request.method == "GET":
        return _page(
            "Update Software",
            "<div class='confirm'>"
            "<p>This will run the installer script to pull the latest code and may trigger a reboot.</p>"
            "<form method='POST'>"
            "<button type='submit' class='btn warn' style='border:none;cursor:pointer;font-size:1rem'>"
            "Confirm Update</button>"
            "</form>"
            "</div>",
        )

    if not root_cfg.running_on_rpi:
        return _page("Update Software", "<pre>This command only works on a Raspberry Pi.</pre>")

    if not root_cfg.system_cfg.is_valid:
        return _page("Update Software", "<pre>system.cfg is not set. Please check your installation.</pre>")

    scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
    if not scripts_dir.exists():
        return _page(
            "Update Software",
            f"<pre>Error: scripts directory does not exist at {scripts_dir}.</pre>",
        )

    installer = "zero_installer.sh" if root_cfg.running_on_pi_zero else "rpi_installer.sh"
    cmd = f"sudo -u $USER {scripts_dir}/{installer}"

    def _run_update() -> None:
        try:
            subprocess.run(cmd, shell=True, check=False, capture_output=True)
        except Exception:
            logger.exception("Software update failed")

    threading.Thread(target=_run_update, daemon=True).start()
    return _page(
        "Update Software",
        "<pre>Software update started in the background.\n"
        "The device may reboot automatically if new code was installed.</pre>",
    )


##############################################################################################################
# Reboot Device.


@app.route("/reboot", methods=["GET", "POST"])
def reboot() -> str:
    if request.method == "GET":
        return _page(
            "Reboot Device",
            "<div class='confirm'>"
            "<p>Are you sure you want to reboot this device?</p>"
            "<form method='POST'>"
            "<button type='submit' class='btn danger' style='border:none;cursor:pointer;font-size:1rem'>"
            "Confirm Reboot</button>"
            "</form>"
            " <a class='btn' href='/'>Cancel</a>"
            "</div>",
        )

    if not root_cfg.running_on_rpi:
        return _page("Reboot Device", "<pre>This command only works on a Raspberry Pi.</pre>")

    subprocess.Popen(["sudo", "reboot"])
    return _page("Reboot Device", "<pre>Rebooting now…</pre>")


##############################################################################################################
# Terminal (xterm.js + WebSocket + pty)

_TERMINAL_HTML = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Terminal — Expidite</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.css">
<style>
html,body{margin:0;padding:0;height:100%;overflow:hidden;background:#0d1117}
#top-bar{background:#1a1a2e;color:#e0e0e0;padding:.4rem 1rem;display:flex;align-items:center;
  gap:1rem;font-family:system-ui,sans-serif;font-size:.9rem;border-bottom:1px solid #333}
#top-bar a{color:#7db3ff;text-decoration:none}
#top-bar a:hover{text-decoration:underline}
#terminal-container{position:absolute;top:36px;bottom:0;left:0;right:0;padding:4px}
</style>
</head>
<body>
<div id="top-bar">
  <a href="/">&larr; Dashboard</a>
  <span>Expidite Terminal</span>
</div>
<div id="terminal-container"></div>
<script src="https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xterm-addon-fit@0.8.0/lib/xterm-addon-fit.js"></script>
<script>
var term = new Terminal({
    cursorBlink: true, fontSize: 14,
    fontFamily: "'Cascadia Code','Fira Code',Consolas,monospace",
    theme: {background:'#0d1117', foreground:'#c9d1d9', cursor:'#58a6ff'}
});
var fitAddon = new FitAddon.FitAddon();
term.loadAddon(fitAddon);
term.open(document.getElementById('terminal-container'));
fitAddon.fit();

var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
var ws = new WebSocket(proto + '//' + location.host + '/ws');

function sendResize() {
    if (ws.readyState === WebSocket.OPEN) {
        var msg = JSON.stringify({type:'resize', cols:term.cols, rows:term.rows});
        ws.send(new Blob([msg]));
    }
}

ws.onopen = function() { sendResize(); };
ws.onmessage = function(ev) { term.write(ev.data); };
ws.onclose = function() {
    term.write('\\r\\n\\x1b[31m[Connection closed. Refresh to reconnect.]\\x1b[0m\\r\\n');
};
term.onData(function(data) {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
});
term.onResize(function() { sendResize(); });
window.addEventListener('resize', function() { fitAddon.fit(); });
</script>
</body>
</html>"""


@app.route("/terminal")
def terminal() -> str:
    return _TERMINAL_HTML


@sock.route("/ws")
def terminal_ws(ws: Any) -> None:  # noqa: ANN401
    if not _PTY_SUPPORTED:
        ws.send("Terminal requires Linux (pty not available on this platform).\r\n")
        return

    master_fd, slave_fd = pty.openpty()  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]

    env = os.environ.copy()
    env["TERM"] = "xterm-256color"

    process = subprocess.Popen(
        [sys.executable, "-m", "expidite_rpi.bcli"],
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        env=env,
        start_new_session=True,
    )
    os.close(slave_fd)

    def _pty_reader() -> None:
        try:
            while True:
                ready, _, _ = select.select([master_fd], [], [], 0.5)
                if ready:
                    data = os.read(master_fd, 4096)
                    if not data:
                        break
                    ws.send(data.decode("utf-8", errors="replace"))
                if process.poll() is not None:
                    while True:
                        r, _, _ = select.select([master_fd], [], [], 0.1)
                        if not r:
                            break
                        leftover = os.read(master_fd, 4096)
                        if not leftover:
                            break
                        ws.send(leftover.decode("utf-8", errors="replace"))
                    break
        except OSError:
            pass
        with contextlib.suppress(Exception):
            ws.send("\r\n\x1b[33m[Session ended. Refresh to reconnect.]\x1b[0m\r\n")

    reader_thread = threading.Thread(target=_pty_reader, daemon=True)
    reader_thread.start()

    try:
        while True:
            data = ws.receive()
            if data is None:
                break

            # Binary frames carry JSON control messages (e.g. resize).
            if isinstance(data, bytes):
                try:
                    msg = json.loads(data)
                    if msg.get("type") == "resize":
                        winsize = struct.pack("HHHH", msg["rows"], msg["cols"], 0, 0)
                        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                        continue
                except (json.JSONDecodeError, KeyError, OSError):
                    pass
                os.write(master_fd, data)
            else:
                os.write(master_fd, data.encode("utf-8"))
    except Exception:
        pass
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        with contextlib.suppress(OSError):
            os.close(master_fd)
        reader_thread.join(timeout=2)


##############################################################################################################
# Entry point.


def main() -> None:
    logger.info("Starting Expidite web server on port 5000")
    app.run(host="0.0.0.0", port=5000)  # noqa: S104


if __name__ == "__main__":
    main()
