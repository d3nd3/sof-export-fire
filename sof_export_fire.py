"""sof-export-fire: generic filesystem watcher for SoFPlus ext_trigger events.

In-game, ``ext_trigger.func`` (sof-profiles, vendored into each consumer)
fires named triggers by writing ``sofplus/data/ext_trigger/<name>.cfg`` via
``sp_sc_cvar_save``. This script is the single out-of-game observer for that
contract: it watches ``<SoF root>/user-*/sofplus/data/ext_trigger/*.cfg``
and dispatches each deduplicated event to your code.

File contract (mirrored from the .func side):
- Lines: ``set "key" "value"``, latin-1 encoded.
- Keys: ``_ext_trigger_name``, ``_ext_trigger_seq`` (monotonic per server
  process), ``_ext_trigger_slot``, ``_ext_trigger_arg1..3``, plus
  ``hostname`` / ``mapname`` / ``_sp_sv_info_map_current`` context.
- Server port is extracted from the path via ``user-(\\d+)``.
- ``last.cfg`` is an alias of the latest fire and is ignored by default so
  one fire dispatches once (pass ``ignore_last=False`` to include it).

Dedup: watchdog coalesces rapid writes and re-fires on unrelated mtime
bumps, so each path has a cooldown window and events redeliver only when
``_ext_trigger_seq`` advances.

Use as a library::

    from sof_export_fire import TriggerWatcher

    def on_event(ev):
        print(ev.port, ev.name, ev.seq, ev.slot, ev.args)

    TriggerWatcher("/path/to/sof", on_event=on_event,
                   triggers={"discord"}).run_forever()

Use as a CLI (no consumer code needed)::

    python sof_export_fire.py --root /path/to/sof --trigger discord --print
    python sof_export_fire.py --root /path/to/sof \\
        --exec "mybot --port {port} --mode {arg1}"

``--exec`` placeholders: ``{port} {path} {file} {name} {seq} {slot}``
``{arg1} {arg2} {arg3} {hostname} {map}``.

Run as a socket server so separately-installed consumers subscribe instead
of watching files themselves::

    python sof_export_fire.py --root /path/to/sof --serve 127.0.0.1:8765

Wire protocol (TCP, newline-delimited JSON, UTF-8):

- Server -> client on connect::

      {"hello": {"service": "sof-export-fire", "version": 1}}

- Client -> server: ``{"subscribe": ["discord"]}`` or ``{"subscribe": "*"}``
- Server -> client per event::

      {"event": {"port": "28910", "file": "discord.cfg", "name": "discord",
                 "seq": 7, "slot": 3, "args": [".wantplay", "", ""],
                 "hostname": "...", "map": "dm/x",
                 "data_dir": "/abs/path/user-28910/sofplus/data"}}

- Liveness: client -> server ``{"ping": <id>}``; server replies
  ``{"pong": <id>}``. Unknown lines are ignored.

Requires: watchdog (``pip install watchdog``).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Optional

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError:  # pragma: no cover
    FileSystemEventHandler = object  # type: ignore[assignment,misc]

    Observer = None  # type: ignore[assignment]

CVAR_RE = re.compile(r'set "([^"]+)" "([^"]*)"')
PORT_RE = re.compile(r"user-(\d+)")

TRIGGER_DIR = os.path.join("sofplus", "data", "ext_trigger")
LAST_FILENAME = "last.cfg"

PROTOCOL_VERSION = 1
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765


def parse_trigger_file(path: str) -> Dict[str, Any]:
    """Parse an ext_trigger ``*.cfg`` file into a raw dict (latin-1).

    ``_ext_trigger_seq`` / ``_ext_trigger_slot`` are converted to int when
    numeric; everything else stays a string.
    """
    data: Dict[str, Any] = {}
    with open(path, "r", encoding="latin-1") as file_obj:
        for line in file_obj:
            match = CVAR_RE.match(line)
            if not match:
                continue
            key, value = match.groups()
            if key in ("_ext_trigger_seq", "_ext_trigger_slot"):
                try:
                    data[key] = int(value)
                except ValueError:
                    data[key] = value
            else:
                data[key] = value
    return data


@dataclass
class TriggerEvent:
    """One deduplicated trigger fire."""

    port: str
    path: str
    filename: str
    name: str
    seq: Any
    slot: Any
    args: tuple = ()
    hostname: str = ""
    map: str = ""
    data_dir: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def arg1(self) -> Any:
        return self.args[0] if len(self.args) > 0 else ""

    @property
    def arg2(self) -> Any:
        return self.args[1] if len(self.args) > 1 else ""

    @property
    def arg3(self) -> Any:
        return self.args[2] if len(self.args) > 2 else ""


def build_event(port: str, path: str, raw: Dict[str, Any]) -> Optional[TriggerEvent]:
    """Build a TriggerEvent from parsed cfg data, or None if not a trigger."""
    name = raw.get("_ext_trigger_name")
    if not name:
        return None
    return TriggerEvent(
        port=port,
        path=path,
        filename=os.path.basename(path),
        name=name,
        seq=raw.get("_ext_trigger_seq"),
        slot=raw.get("_ext_trigger_slot"),
        args=(
            raw.get("_ext_trigger_arg1", ""),
            raw.get("_ext_trigger_arg2", ""),
            raw.get("_ext_trigger_arg3", ""),
        ),
        hostname=raw.get("hostname", ""),
        map=raw.get("mapname") or raw.get("_sp_sv_info_map_current", "") or "",
        data_dir=os.path.dirname(os.path.dirname(os.path.abspath(path))),
        raw=raw,
    )


def event_to_dict(event: TriggerEvent) -> Dict[str, Any]:
    """JSON-serializable event payload published to socket subscribers."""
    return {
        "port": event.port,
        "file": event.filename,
        "name": event.name,
        "seq": event.seq,
        "slot": event.slot,
        "args": list(event.args),
        "hostname": event.hostname,
        "map": event.map,
        "data_dir": event.data_dir,
    }


class EventServer:
    """TCP pub/sub server for TriggerEvents (one watcher, many consumers).

    ``publish`` matches the ``on_event`` callback signature, so the server
    plugs straight into ``TriggerWatcher``::

        server = EventServer()
        TriggerWatcher(root, on_event=[watcher_cb, server.publish], ...)
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
        self.host = host
        self.port = port
        self._subs: Dict[object, Optional[set]] = {}
        self._lock = threading.Lock()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]  # honour port 0 (ephemeral)
        self._accept_thread: Optional[threading.Thread] = None

    @property
    def address(self) -> tuple:
        return (self.host, self.port)

    def publish(self, event: TriggerEvent) -> None:
        """Broadcast one event to matching subscribers (on_event-compatible)."""
        line = (json.dumps({"event": event_to_dict(event)}) + "\n").encode("utf-8")
        dead = []
        with self._lock:
            subs = list(self._subs.items())
        for conn, filt in subs:
            if filt is not None and event.name not in filt:
                continue
            try:
                conn.sendall(line)
            except OSError:
                dead.append(conn)
        if dead:
            with self._lock:
                for conn in dead:
                    self._subs.pop(conn, None)

    def start(self) -> "EventServer":
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        return self

    def stop(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def serve_forever(self) -> None:
        self.start()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subs)

    def _accept_loop(self) -> None:
        while True:
            try:
                conn, _addr = self._sock.accept()
            except OSError:
                return  # listener closed
            thread = threading.Thread(target=self._client_loop, args=(conn,), daemon=True)
            thread.start()

    def _send(self, conn: socket.socket, obj: dict) -> None:
        conn.sendall((json.dumps(obj) + "\n").encode("utf-8"))

    def _client_loop(self, conn: socket.socket) -> None:
        try:
            self._send(conn, {"hello": {"service": "sof-export-fire",
                                        "version": PROTOCOL_VERSION}})
            filt: Optional[set] = None  # None = all triggers
            buf = conn.makefile("r", encoding="utf-8")
            for line in buf:
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(msg, dict):
                    continue
                if "subscribe" in msg:
                    want = msg["subscribe"]
                    filt = None if want == "*" else set(want)
                    with self._lock:
                        self._subs[conn] = filt
                elif "ping" in msg:
                    try:
                        self._send(conn, {"pong": msg["ping"]})
                    except OSError:
                        break
        except OSError:
            pass
        finally:
            with self._lock:
                self._subs.pop(conn, None)
            try:
                conn.close()
            except OSError:
                pass


class TriggerHandler(FileSystemEventHandler):
    """Watchdog handler: debounces, dedups on seq, dispatches TriggerEvents."""

    def __init__(
        self,
        on_event: Callable[[TriggerEvent], None],
        triggers: Optional[Iterable[str]] = None,
        cooldown: float = 1.0,
        settle: float = 0.5,
        ignore_last: bool = True,
    ) -> None:
        self.on_event = on_event
        self.triggers = set(triggers) if triggers else None
        self.cooldown = cooldown
        self.settle = settle
        self.ignore_last = ignore_last
        self._last_time: Dict[str, float] = {}
        self._last_seq: Dict[str, Any] = {}

    def _handle(self, src_path: str) -> Optional[TriggerEvent]:
        if not src_path.endswith(".cfg"):
            return None
        if TRIGGER_DIR not in src_path and "ext_trigger" not in src_path:
            return None
        if self.ignore_last and os.path.basename(src_path) == LAST_FILENAME:
            return None
        port_match = PORT_RE.search(src_path)
        if not port_match:
            return None
        port = port_match.group(1)
        now = time.time()
        if now - self._last_time.get(src_path, 0.0) < self.cooldown:
            return None
        self._last_time[src_path] = now
        if self.settle:
            time.sleep(self.settle)
        if not os.path.isfile(src_path):
            return None
        try:
            raw = parse_trigger_file(src_path)
        except OSError:
            return None
        event = build_event(port, src_path, raw)
        if event is None:
            return None
        if self.triggers is not None and event.name not in self.triggers:
            return None
        if event.seq is not None and self._last_seq.get(src_path) == event.seq:
            return None  # duplicate delivery of the same fire
        self._last_seq[src_path] = event.seq
        self.on_event(event)
        return event

    def on_modified(self, event) -> None:  # type: ignore[no-untyped-def]
        if getattr(event, "is_directory", False):
            return
        self._handle(event.src_path)

    def on_created(self, event) -> None:  # type: ignore[no-untyped-def]
        if getattr(event, "is_directory", False):
            return
        self._handle(event.src_path)


class TriggerWatcher:
    """Owns the watchdog Observer; library entry point."""

    def __init__(
        self,
        root: str,
        on_event: Callable[[TriggerEvent], None],
        triggers: Optional[Iterable[str]] = None,
        cooldown: float = 1.0,
        settle: float = 0.5,
        ignore_last: bool = True,
    ) -> None:
        if Observer is None:
            raise RuntimeError("watchdog is required: pip install watchdog")
        if not os.path.isdir(root):
            raise ValueError("SoF root does not exist: %s" % root)
        self.root = root
        self.handler = TriggerHandler(
            on_event,
            triggers=triggers,
            cooldown=cooldown,
            settle=settle,
            ignore_last=ignore_last,
        )
        self._observer = Observer()
        self._observer.schedule(self.handler, root, recursive=True)

    def start(self) -> None:
        self._observer.start()

    def stop(self) -> None:
        self._observer.stop()

    def join(self) -> None:
        self._observer.join()

    def run_forever(self) -> None:
        print("Watching %s for %s/*.cfg ..." % (self.root, TRIGGER_DIR))
        self.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        self.join()


def print_event(event: TriggerEvent) -> None:
    print(
        "trigger port=%s file=%s name=%s seq=%s slot=%s args=%s host=%s map=%s"
        % (
            event.port,
            event.filename,
            event.name,
            event.seq,
            event.slot,
            list(event.args),
            event.hostname,
            event.map,
        )
    )


def exec_hook_factory(template: str) -> Callable[[TriggerEvent], None]:
    """Build an on_event callback that runs a shell command per event."""

    def _hook(event: TriggerEvent) -> None:
        cmd = template.format(
            port=event.port,
            path=event.path,
            file=event.filename,
            name=event.name,
            seq=event.seq,
            slot=event.slot,
            arg1=event.arg1,
            arg2=event.arg2,
            arg3=event.arg3,
            hostname=event.hostname,
            map=event.map,
        )
        subprocess.run(cmd, shell=True, check=False)

    return _hook


def parse_serve_arg(value: str) -> tuple:
    """Parse [HOST:]PORT for --serve (bare port binds DEFAULT_HOST)."""
    if ":" in value:
        host, _, port = value.rpartition(":")
        return (host or DEFAULT_HOST, int(port))
    return (DEFAULT_HOST, int(value))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=os.path.dirname(os.getcwd()),
                        help="SoF root containing user-<port>/ dirs")
    parser.add_argument("--trigger", action="append", default=None,
                        help="Only dispatch this trigger name (repeatable)")
    parser.add_argument("--cooldown", type=float, default=1.0)
    parser.add_argument("--settle", type=float, default=0.5)
    parser.add_argument("--exec", dest="exec_template", default=None,
                        help="Shell command per event; see placeholders above")
    parser.add_argument("--serve", default=None, metavar="[HOST:]PORT",
                        help="Publish events on a TCP socket for separately "
                             "installed subscribers (default %s:%d)"
                             % (DEFAULT_HOST, DEFAULT_PORT))
    parser.add_argument("--include-last", action="store_true",
                        help="Also dispatch ext_trigger/last.cfg (alias file)")
    parser.add_argument("--quiet", action="store_true",
                        help="Don't print events (useful with --exec)")
    args = parser.parse_args()

    callbacks: list[Callable[[TriggerEvent], None]] = []
    if args.exec_template:
        callbacks.append(exec_hook_factory(args.exec_template))
    server: Optional[EventServer] = None
    if args.serve:
        host, port = parse_serve_arg(args.serve)
        server = EventServer(host, port).start()
        callbacks.append(server.publish)
        print("Serving events on %s:%d ..." % (server.host, server.port))
    if not args.quiet or not callbacks:
        callbacks.append(print_event)

    def _dispatch(event: TriggerEvent) -> None:
        for callback in callbacks:
            callback(event)

    TriggerWatcher(
        args.root,
        on_event=_dispatch,
        triggers=args.trigger,
        cooldown=args.cooldown,
        settle=args.settle,
        ignore_last=not args.include_last,
    ).run_forever()


if __name__ == "__main__":
    main()
