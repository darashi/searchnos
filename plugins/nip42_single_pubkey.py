#!/usr/bin/env python3
"""Write policy plugin that requires AUTH for a specific npub."""

import json
import sys
from typing import Any

AUTHORIZED_PUBKEY_HEX = "c0c3fdcef7fc9eb85ceb5f1b899abbcb331cf673d032c4ccb5a473a4af7372f4"


def _decide(request: dict[str, Any]) -> dict[str, Any]:
    if request.get("type") != "new":
        return {}

    authed = set(request.get("authenticatedPubkeys") or [])
    event = request.get("event") or {}
    response: dict[str, Any] = {"id": event.get("id", "")}

    if AUTHORIZED_PUBKEY_HEX not in authed:
        response["action"] = "rejectAndChallenge"
        response["msg"] = "blocked: authenticate"
        return response

    response["action"] = "accept"
    return response


def main() -> None:
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            request = json.loads(raw)
        except json.JSONDecodeError:
            print(
                json.dumps({"id": "", "action": "reject", "msg": "invalid json"}),
                flush=True,
            )
            continue
        response = _decide(request)
        if response:
            print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
