"""Shared Claude API utilities for PS Operations scripts."""

import json
import os
import urllib.request

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = "claude-sonnet-4-6"


def call_claude(system_prompt, user_prompt, max_tokens=2048, model=None):
    """Call Claude API and return the text response, or None on failure.

    Args:
        system_prompt: System-level instructions for Claude.
        user_prompt: The data/question to analyze.
        max_tokens: Max response length.
        model: Override model (defaults to Haiku 4.5).

    Returns:
        str or None: Claude's response text, or None if API unavailable/failed.
    """
    if not ANTHROPIC_API_KEY:
        return None

    payload = json.dumps({
        "model": model or DEFAULT_MODEL,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }).encode("utf-8")

    req = urllib.request.Request(
        ANTHROPIC_API_URL,
        data=payload,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            result = json.loads(resp.read().decode())
        text = result.get("content", [{}])[0].get("text", "")
        return text if text else None
    except Exception as e:
        print(f"  Claude API error: {e}")
        return None
