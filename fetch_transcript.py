#!/usr/bin/env python3
"""
One-time fetch of the historical transcript for a Webex Contact Center
conversation, using the WxCC AI Serving API (gRPC, service AiInsight).

Two modes:
  stream (default) - StreamingInsightServing with historicalTranscripts=True.
                     This is the route Cisco documents for historical
                     transcripts. The script replays the history, stops once
                     the stream goes quiet, closes it and prints the result.
  unary            - InsightServing with insightType=TRANSCRIPTION.
                     A single request/response, if your org returns
                     transcripts on this RPC.

Configuration comes from environment variables (never hardcode the token):
  WXCC_ACCESS_TOKEN  required  Bearer token with the cjp:user scope
  WXCC_ORG_ID        required  Control Hub org ID
  WXCC_AGENT_ID      optional  agent ID (the sample client omits it)
  WXCC_SERVING_HOST  optional  host:port; if unset it is looked up via U2C
"""
import argparse
import glob
import importlib
import json
import os
import queue
import re
import sys
import threading
import time
import urllib.parse
import urllib.request

import grpc
from google.protobuf.json_format import MessageToDict

HERE = os.path.dirname(os.path.abspath(__file__))
GENERATED = os.path.join(HERE, "generated")
U2C_URL = "https://u2c.wbx2.com/u2c/api/v1/user/catalog"
ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def load_stubs():
    """Import serving_pb2 / serving_pb2_grpc wherever protoc put them."""
    sys.path.insert(0, GENERATED)
    hits = glob.glob(os.path.join(GENERATED, "**", "serving_pb2.py"), recursive=True)
    if not hits:
        sys.exit("serving_pb2.py not found - run ./generate_stubs.sh first.")
    rel = os.path.relpath(hits[0], GENERATED)[:-3].replace(os.sep, ".")
    return importlib.import_module(rel), importlib.import_module(rel + "_grpc")


def require_env(name):
    val = os.environ.get(name, "").strip()
    if not val:
        sys.exit(f"Missing environment variable {name}")
    return val


def check_id(label, value):
    if not ID_RE.match(value):
        sys.exit(f"{label} looks malformed: {value!r}")
    return value


def discover_host(token):
    """Ask the U2C catalog where wccai-serving-api lives for this org."""
    q = urllib.parse.urlencode({"types": "WCC", "services": "wccai-serving-api"})
    req = urllib.request.Request(f"{U2C_URL}?{q}",
                                 headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=15) as resp:  # TLS verified by default
        data = json.load(resp)
    for svc in data.get("services", []):
        if svc.get("serviceName") == "wccai-serving-api":
            urls = sorted(svc.get("serviceUrls", []), key=lambda u: u.get("priority", 99))
            base = (urls[0]["baseUrl"] if urls else svc["logicalNames"][0])
            parsed = urllib.parse.urlparse(base)
            return f"{parsed.hostname}:{parsed.port or 443}"
    sys.exit("U2C catalog did not return wccai-serving-api; set WXCC_SERVING_HOST manually.")


RETRYABLE = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
MAX_RETRIES = 3


def fetch_stream(pb2, stub, md, conv_id, org_id, agent_id, idle_secs, max_secs,
                 with_va=False, with_messages=False):
    # Same request shape as Cisco's sample client: conversationId + orgId +
    # flags. The sample never sets agentDetails, so we only add it if given.
    isr = pb2.InsightServingRequest(
        conversationId=conv_id,
        orgId=org_id,
        historicalTranscripts=True,
        historicalVirtualAgent=with_va,
        historicalMessage=with_messages,
    )
    if agent_id:
        isr.agentDetails.CopyFrom(pb2.AgentDetails(agentId=agent_id))
    req = pb2.StreamingInsightServingRequest(insightServingRequest=isr)

    for attempt in range(MAX_RETRIES + 1):
        try:
            return _collect(stub, req, md, idle_secs, max_secs)
        except grpc.RpcError as e:
            if e.code() not in RETRYABLE or attempt == MAX_RETRIES:
                raise
            wait = attempt * 2 + 1  # same 1s, 3s, 5s backoff as the sample
            print(f"{e.code().name}, retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)


def _collect(stub, req, md, idle_secs, max_secs):
    call = stub.StreamingInsightServing(req, metadata=md, timeout=max_secs)
    q = queue.Queue()

    def pump():
        try:
            for msg in call:
                q.put(("msg", msg))
            q.put(("end", None))
        except grpc.RpcError as e:
            q.put(("err", e))

    threading.Thread(target=pump, daemon=True).start()
    items = []
    while True:
        try:
            kind, val = q.get(timeout=idle_secs)
        except queue.Empty:
            call.cancel()  # history delivered and stream is now idle -> done
            break
        if kind == "msg":
            items.append(MessageToDict(val.insightServingResponse,
                                       preserving_proto_field_name=True))
        elif kind == "end":
            break
        else:
            if val.code() in (grpc.StatusCode.CANCELLED, grpc.StatusCode.DEADLINE_EXCEEDED) and items:
                break
            raise val
    return items


def fetch_unary(pb2, stub, md, conv_id, org_id, max_secs):
    req = pb2.InsightsServingRequest(
        conversationId=conv_id,
        orgId=org_id,
        insightType=pb2.InsightsServingRequest.TRANSCRIPTION,
    )
    resp = stub.InsightServing(req, metadata=md, timeout=max_secs)
    d = MessageToDict(resp, preserving_proto_field_name=True)
    # Normalise to the same shape as the streaming items
    return [{"responseContent": rc, "insightType": "TRANSCRIPTION",
             "isFinal": True} for rc in d.get("responseContent", [])]


def transcript_text(item):
    rc = item.get("responseContent", {})
    if "virtualAgentResult" in rc:  # bot replies (historicalVirtualAgent)
        va = rc["virtualAgentResult"]
        return " ".join(va.get("replyText") or va.get("reply_text") or [])
    if "messageResult" in rc:  # chat/digital messages: structure varies
        return json.dumps(rc["messageResult"], ensure_ascii=False)
    rr = rc.get("recognitionResult", rc)
    alts = rr.get("alternatives") or []
    return alts[0].get("transcript", "") if alts else ""


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("conversation_id", help="Interaction / task / conversation ID")
    ap.add_argument("--mode", choices=["stream", "unary"], default="stream")
    ap.add_argument("--idle", type=float, default=5.0,
                    help="stream mode: stop after this many seconds with no new message")
    ap.add_argument("--timeout", type=float, default=60.0, help="overall RPC deadline (s)")
    ap.add_argument("--include-interim", action="store_true",
                    help="keep non-final results (default: final only)")
    ap.add_argument("--with-va", action="store_true",
                    help="stream mode: also fetch virtual agent (bot) turns")
    ap.add_argument("--with-messages", action="store_true",
                    help="stream mode: also fetch digital/chat messages")
    ap.add_argument("--json", metavar="FILE", help="also write raw results to FILE")
    args = ap.parse_args()

    token = require_env("WXCC_ACCESS_TOKEN")
    org_id = check_id("WXCC_ORG_ID", require_env("WXCC_ORG_ID"))
    conv_id = check_id("conversation_id", args.conversation_id)
    agent_id = os.environ.get("WXCC_AGENT_ID", "").strip()
    if agent_id:
        check_id("WXCC_AGENT_ID", agent_id)
    host = os.environ.get("WXCC_SERVING_HOST") or discover_host(token)

    pb2, pb2_grpc = load_stubs()
    channel = grpc.secure_channel(
        host, grpc.ssl_channel_credentials(),  # system CAs, full TLS verification
        options=[("grpc.max_receive_message_length", 50 * 1024 * 1024)],
    )
    stub = pb2_grpc.AiInsightStub(channel)
    md = [("authorization", f"Bearer {token}")]

    print(f"Fetching transcript for {conv_id} from {host} ({args.mode} mode)...", file=sys.stderr)
    try:
        if args.mode == "stream":
            items = fetch_stream(pb2, stub, md, conv_id, org_id, agent_id, args.idle,
                                 args.timeout, args.with_va, args.with_messages)
        else:
            items = fetch_unary(pb2, stub, md, conv_id, org_id, args.timeout)
    except grpc.RpcError as e:
        sys.exit(f"gRPC error {e.code().name}: {e.details()}")
    finally:
        channel.close()

    if not args.include_interim:
        items = [i for i in items if i.get("isFinal", False)]
    items.sort(key=lambda i: int(i.get("startTimestamp") or i.get("publishTimestamp") or 0))

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2)
        print(f"Raw results written to {args.json}", file=sys.stderr)

    if not items:
        print("No transcript entries returned.", file=sys.stderr)
        return
    for i in items:
        text = transcript_text(i)
        if text:
            label = "BOT" if "virtualAgentResult" in i.get("responseContent", {}) else i.get("role", "?")
            print(f"[{label:6}] {text}")


if __name__ == "__main__":
    main()
