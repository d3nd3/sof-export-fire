# sof-export-fire

Generic out-of-game observer for SoFPlus `ext_trigger` events.

In-game, `ext_trigger.func` (canonical source: `sof-profiles`, vendored into
each consumer's `sofplus/addons/`) fires named triggers by writing
`sofplus/data/ext_trigger/<name>.cfg` via `sp_sc_cvar_save`. This project is
the single Python watcher for that contract: it monitors
`<SoF root>/user-*/sofplus/data/ext_trigger/*.cfg` and dispatches each
deduplicated event to your code.

## File contract

- Lines: `set "key" "value"`, latin-1 encoded.
- Keys: `_ext_trigger_name`, `_ext_trigger_seq` (monotonic per server
  process), `_ext_trigger_slot`, `_ext_trigger_arg1..3`, plus `hostname` /
  `mapname` / `_sp_sv_info_map_current` context.
- Server port is extracted from the path via `user-(\d+)`.
- `last.cfg` is an alias of the latest fire and is ignored by default, so
  one fire dispatches once (`--include-last` / `ignore_last=False` to opt in).
- Watchdog coalesces rapid writes and re-fires on unrelated mtime bumps, so
  every path gets a cooldown window and an event redelivers only when
  `_ext_trigger_seq` advances.

## Requirements

- Python 3.9+
- `watchdog` (`pip install watchdog`)

## Use as a library

```python
from sof_export_fire import TriggerWatcher

def on_event(ev):
    print(ev.port, ev.name, ev.seq, ev.slot, ev.args)

TriggerWatcher("/path/to/sof", on_event=on_event,
               triggers={"discord"}).run_forever()
```

`TriggerEvent` fields: `port`, `path`, `filename`, `name`, `seq`, `slot`,
`args` (tuple of arg1..3, also as `arg1`/`arg2`/`arg3` properties),
`hostname`, `map` (`mapname`, falling back to
`_sp_sv_info_map_current`), and `raw` (full parsed cfg dict).
`triggers={...}` restricts dispatch to named triggers; omit for all.

## Use as a CLI (no consumer code)

```bash
python sof_export_fire.py --root /path/to/sof --trigger discord --print
python sof_export_fire.py --root /path/to/sof \
    --exec "mybot --port {port} --mode {arg1}"
```

Options:

- `--trigger NAME` (repeatable): only dispatch this trigger name.
- `--cooldown` (default 1.0), `--settle` (default 0.5): per-path debounce
  seconds and post-event settle sleep before reading.
- `--exec TEMPLATE`: shell command run per event. Placeholders: `{port}`
  `{path}` `{file}` `{name}` `{seq}` `{slot}` `{arg1}` `{arg2}` `{arg3}`
  `{hostname}` `{map}`.
- `--include-last`: also dispatch `ext_trigger/last.cfg`.
- `--quiet`: don't print events (useful with `--exec`).

## Run as a socket server (recommended for installed consumers)

One watcher serves many separately-installed consumers over TCP:

```bash
python sof_export_fire.py --root /path/to/sof --serve 127.0.0.1:8765
```

`--serve [HOST:]PORT` publishes every dispatched event to subscribers while
`--print` / `--exec` keep working alongside for debugging. Wire protocol
(TCP, newline-delimited JSON, UTF-8):

- Server → client on connect:
  `{"hello": {"service": "sof-export-fire", "version": 1}}`
- Client → server: `{"subscribe": ["discord"]}` or `{"subscribe": "*"}`
- Server → client per event:
  `{"event": {"port": "28910", "file": "discord.cfg", "name": "discord",`
  `"seq": 7, "slot": 3, "args": [".wantplay", "", ""],`
  `"hostname": "...", "map": "dm/x",`
  `"data_dir": "/abs/path/user-28910/sofplus/data"}}`
  (`data_dir` lets subscribers find the snapshot without knowing the layout.)
- Liveness: client → server `{"ping": <id>}`; server replies
  `{"pong": <id>}`. Malformed lines are ignored.

Minimal subscriber (any language: connect, send one subscribe line, read
JSON lines, reconnect on EOF):

```python
import json, socket
sock = socket.create_connection(("127.0.0.1", 8765))
fp = sock.makefile("r", encoding="utf-8")
print(json.loads(fp.readline()))  # hello
sock.sendall(b'{"subscribe": ["discord"]}\n')
for line in fp:
    msg = json.loads(line)
    if "event" in msg:
        print(msg["event"])
```

## Install / systemd

```bash
python3 -m venv venv && venv/bin/pip install -r requirements.txt
venv/bin/python sof_export_fire.py --root /path/to/sof --serve 127.0.0.1:8765
```

An example unit (`sof-export-fire.service`, plus the matching bot unit)
ships with `sof-discord-bot` under `systemd/`; adjust `--root` and the
`--serve` bind to the deployment.

## Layout

- `sof_export_fire.py`: the whole watcher (library + CLI, single file).

## Sibling projects

- `sof-profiles`: in-game side (`ext_trigger.func`, canonical source).
- `sof-discord-bot`: domain consumer — snapshots server/client info in-game,
  fires the `discord` trigger, and handles it (mode routing, scoreboard
  render, webhook post). It runs separately and subscribes to this server
  over the socket protocol (`SOF_EXPORT_FIRE_HOST`/`SOF_EXPORT_FIRE_PORT`).

## Checks

```bash
python3 -m py_compile sof_export_fire.py
```

Dispatch, dedup, name filtering, alias ignoring, and `--exec` rendering were
verified against a simulated `user-<port>/sofplus/data/ext_trigger/` tree.

Prefer pictures? Start with **[HUMANS.md](HUMANS.md)**.
