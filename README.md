# WxCC historical transcript fetch

A small Python script that fetches the historical transcript of a Webex Contact
Center conversation from the WxCC AI **Serving API** (gRPC service `AiInsight`).

It opens the `StreamingInsightServing` stream with `historicalTranscripts=true`,
collects the replayed history, closes the stream once it goes quiet (5 s by
default) and prints the transcript. This is the route Cisco documents for
historical transcripts. `--mode unary` tries the single-call `InsightServing`
RPC instead.

Tested setup: macOS, Python 3.15, grpcio / grpcio-tools 1.85.0.

---

## Files

| File | Purpose |
|------|---------|
| `fetch_transcript.py` | The client script |
| `generate_stubs.sh` | Compiles the `.proto` files in `protos/` into Python code in `generated/` |
| `requirements.txt` | Python libraries (grpcio, grpcio-tools) |
| `protos/` | Cisco's `.proto` source files (you add these, see step 3) |
| `Dockerfile`, `.env.example` | **Optional.** Only needed to run the script in a container (e.g. on a server). Not needed on your own machine. |

---

## Setup (macOS)

### 1. Python

```
python3 --version
```

You need 3.9 or newer. Python 3.15 works with the versions pinned in
`requirements.txt` (see *Lessons learned* if the install fails).

### 2. Virtual environment and libraries

```
cd ~/Desktop/wxcc-transcript-fetch
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install --only-binary=:all: -r requirements.txt
python -c "import grpc, grpc_tools; print(grpc.__version__)"   # should print 1.85.0
```

When you open a new terminal later, re-run `cd ...` and `source .venv/bin/activate`
(you'll see `(.venv)` in the prompt).

### 3. Proto files

Download the repo (https://github.com/ralfschiffert/webex-contact-center-ai-sample-code
→ green **Code** button → **Download ZIP**) and unzip it. You need the `.proto`
*source* files, not the generated `serving_pb.js` / `serving_grpc_web_pb.js`
used by the browser sample client.

The import paths inside the protos decide the folder layout. Your `protos/`
folder must look exactly like this:

```
protos/
├── serving.proto
└── com/cisco/wcc/ccai/v1/common/
    ├── messages.proto
    ├── recognize.proto
    ├── suggestions.proto
    └── virtualagent.proto
```

Finding and copying the common files:

```
cd ~/Downloads/webex-contact-center-ai-sample-code-main
find . -path "*ccai/v1/common*" -name "*.proto"
mkdir -p ~/Desktop/wxcc-transcript-fetch/protos/com/cisco/wcc/ccai/v1/common
cp <path-before-com>/com/cisco/wcc/ccai/v1/common/{messages,recognize,suggestions,virtualagent}.proto \
   ~/Desktop/wxcc-transcript-fetch/protos/com/cisco/wcc/ccai/v1/common/
```

Rules (see *Lessons learned* for why):
- Only `serving.proto` sits directly in `protos/`. No other `.proto` at the top level.
- Do **not** include `tts.proto` or `ccai-api.proto`; they aren't used by the Serving API.

### 4. Generate the Python stubs

```
cd ~/Desktop/wxcc-transcript-fetch
chmod +x generate_stubs.sh
./generate_stubs.sh
```

Success looks like `Stubs written to ./generated:` followed by a list that
includes `serving_pb2.py` and `serving_pb2_grpc.py`. The script deletes and
rebuilds `generated/` every run, so it's safe to re-run.

### 5. Credentials and IDs

- **Access token**: sign in at developer.webex.com with your Contact Center
  account and copy your personal access token (expires after ~12 h). It needs the
  `cjp:user` scope; if you get `PERMISSION_DENIED`, use a token from an
  integration or service app with that scope. Treat it like a password.
- **Org ID**: Control Hub → account / organization settings → Organization ID.
- **Conversation ID**: the interaction ID (= task ID) of the call, e.g. from Analyzer.

```
export WXCC_ACCESS_TOKEN="paste-token-here"
export WXCC_ORG_ID="paste-org-id-here"
```

These last only for the current terminal session, on purpose. Optional:
`WXCC_AGENT_ID` (not required; Cisco's sample client never sends it) and
`WXCC_SERVING_HOST` (host:port; normally discovered automatically via the
U2C catalog).

### 6. Run

```
python fetch_transcript.py <conversation-id>
python fetch_transcript.py <conversation-id> --with-va --json transcript.json
```

| Option | Meaning |
|--------|---------|
| `--with-va` | Also fetch the virtual agent (bot) turns, shown as `[BOT]` |
| `--with-messages` | Also fetch digital / chat messages |
| `--json FILE` | Save the full raw results to `FILE` |
| `--idle N` | Seconds of silence before the stream is considered finished (default 5) |
| `--timeout N` | Overall deadline in seconds (default 60) |
| `--include-interim` | Keep non-final results (default: final only) |
| `--mode unary` | Use the single-call `InsightServing` RPC instead of the stream |

Network errors (`UNAVAILABLE`, `DEADLINE_EXCEEDED`) are retried up to 3 times
with 1 s / 3 s / 5 s waits, like Cisco's sample client.

---

## Lessons learned

**`pip install` fails with "Failed to build grpcio grpcio-tools".**
The gRPC version was too old for the Python version, so no pre-built package
existed and pip tried (and failed) to compile from C++ source. Fix: use a gRPC
release that ships wheels for your Python (1.85.0 has macOS wheels for 3.15).
Installing with `--only-binary=:all:` makes pip fail fast with a clear message
instead of attempting a source build. Fallback: create the venv with an older
Python, e.g. `brew install python@3.13` then `python3.13 -m venv .venv`.

**Do I need a new virtual environment after a failed install?**
No. A failed build installs nothing, so just re-run `pip install` in the same
venv. You only need a new venv when switching Python versions, because a venv is
permanently tied to the Python that created it.

**Why isn't `protobuf` in `requirements.txt`?**
It's still required, but `grpcio-tools` depends on it, so pip installs a
compatible version automatically. Pinning it by hand risks a mismatch with the
compiler bundled in grpcio-tools (install conflicts or a version error at
import). Check with `pip show protobuf`. To lock exact versions once everything
works: `pip freeze > requirements.lock`.

**"com/cisco/wcc/ccai/v1/common/....proto: File not found".**
`serving.proto` imports its dependencies by path. The compiler looks for them
relative to `protos/`, so they must live in `protos/com/cisco/wcc/ccai/v1/common/`,
not next to `serving.proto`.

**"... is already defined in file ...".**
The same proto exists twice (e.g. `protos/recognize.proto` and
`protos/com/.../common/recognize.proto`). The generator compiles every `.proto`
it finds, so both copies are read. Keep only the nested copy.

**"google/api/annotations.proto: File not found" from `tts.proto` / `ccai-api.proto`.**
Those files belong to the provider/media side of the API and import Google API
definitions. The Serving API doesn't use them, so delete them.
`grep -rl "google/api" protos` should print nothing. (If a needed file ever
imports them, `pip install googleapis-common-protos` provides the Google files,
and `generate_stubs.sh` would need an extra `-I` path for them.)

**Docker is optional.**
The `Dockerfile` only packages the script for running in a container, e.g. on
a server or shared with a team. On your own machine, the steps above are enough.

---

## Troubleshooting at run time

| Symptom | Fix |
|---------|-----|
| `serving_pb2.py not found` | Re-run step 4; check the `protos/` layout |
| `ModuleNotFoundError: grpc` | Activate the venv: `source .venv/bin/activate` |
| `UNAUTHENTICATED` | Token expired or mis-pasted; get a new one and re-export it |
| `PERMISSION_DENIED` | Token lacks `cjp:user` or belongs to another org |
| `No transcript entries returned` | Check the conversation ID; transcription must have been enabled for that call; try `--idle 15` |
| `UNIMPLEMENTED` / `INTERNAL` / HTTP content-type error immediately | The endpoint may only accept gRPC-Web (what the browser sample uses); a Node.js client based on the sample would be needed |

---

## Docker (optional)

```
cp .env.example .env        # fill in the values
docker build -t wxcc-transcript .
docker run --rm --env-file .env wxcc-transcript <conversation-id>
```

The `protos/` folder must be set up as in step 3 before building.
