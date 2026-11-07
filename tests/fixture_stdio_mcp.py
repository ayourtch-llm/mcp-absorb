#!/usr/bin/env python3
"""Minimal stdio JSON-RPC MCP fixture for mcp-absorb --dynamic integration
tests: answers initialize / tools/list (2 fake tools) / tools/call (echo,
slow). Notifications (no id) are ignored. stdin lines in, one-line JSON out.
"""
import json
import sys
import time


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    mid = msg.get("id")
    method = msg.get("method", "")
    params = msg.get("params") or {}
    if mid is None:
        continue
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": params.get("protocolVersion", "2024-11-05"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "fixture-stdio-mcp", "version": "0.1.0"}}})
    elif method == "ping":
        send({"jsonrpc": "2.0", "id": mid, "result": {}})
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": mid, "result": {"tools": [
            {"name": "echo", "description": "Echo back the call arguments as JSON",
             "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}}},
            {"name": "slow", "description": "Sleep timeout_s seconds, then answer",
             "inputSchema": {"type": "object", "properties": {"timeout_s": {"type": "number"}}}}]}})
    elif method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        if name == "echo":
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": json.dumps(args)}]}})
        elif name == "slow":
            time.sleep(float(args.get("timeout_s", 5)))
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": "slow done"}]}})
        else:
            send({"jsonrpc": "2.0", "id": mid, "error": {
                "code": -32602, "message": "unknown tool: %s" % name}})
    else:
        send({"jsonrpc": "2.0", "id": mid, "error": {
            "code": -32601, "message": "method not found: %s" % method}})
