# ============================================================
# UNOFFICIAL CAUSE LIST PARSER
# ============================================================
# Courts at PHHC paste unofficial notices outside each courtroom
# resequencing the day's ordinary (200-series) items. The admin
# photographs them and uploads via the app (UnofficialCauseList
# entity). This module runs in a background daemon thread: it polls
# Base44 for unparsed photos of today's date, reads each one with
# Claude vision, and writes the structured sequence back to the
# record's parsed_blocks field for human review in the app.
#
# Runs only when ANTHROPIC_API_KEY is set; otherwise inert.
# Parsing runs in its own thread so a 30-60s vision call can never
# starve the 30s display-board loop (lesson from the sweep-starvation
# incident: long work must not block the main cycle).

import base64
import datetime
import json
import os
import re
import threading
import time

import requests

try:
    import anthropic
except ImportError:  # dependency missing — feature stays off
    anthropic = None

# Self-contained config (mirrors scraper.py; avoids a circular import)
APP_ID = os.environ.get("BASE44_APP_ID", "YOUR_APP_ID_HERE")
API_KEY = os.environ.get("BASE44_API_KEY", "YOUR_API_KEY_HERE")
BASE44_URL = f"https://preview--matter-track-pro.base44.app/api/apps/{APP_ID}/entities"
HEADERS = {"api_key": API_KEY, "Content-Type": "application/json"}
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
PARSE_MODEL = os.environ.get("UNOFFICIAL_PARSE_MODEL", "claude-opus-5")
POLL_SECONDS = int(os.environ.get("UNOFFICIAL_PARSE_POLL_SECONDS", "60"))
MAX_ATTEMPTS = 3          # per record, per process lifetime
MAX_IMAGE_BYTES = 4_500_000  # API limit is 5MB per image post-base64 headroom

_attempts = {}  # record id -> failed attempt count (in-memory)

SYSTEM_PROMPT = """You parse photographs of UNOFFICIAL cause-list notices pasted outside individual courtrooms of the Punjab & Haryana High Court. Each notice tells lawyers the order in which that one courtroom will take up the day's cases. Your output drives a "how many matters away is my case" calculator, so the ORDER of what you extract is everything. A human reviews your output against the photo before it is used.

Return ONLY a JSON object (no markdown fences, no commentary) with this exact shape:
{
  "court_no": <int or null>,        // courtroom number printed/handwritten on the sheet
  "date": "YYYY-MM-DD" or null,     // the date the notice applies to
  "blocks": [                        // the taking-up sequence, in the order the sheet defines it
    {
      "label": "<the sheet's own heading for this group, e.g. 'Regular Bails', 'After urgent', 'Incomplete cases'>",
      "rule": "explicit_items" | "by_case_type" | "remaining_serial" | "time_anchor" | "urgent_edit",
      "items": [<int or "A+B" string>, ...],   // in the order printed; empty for remaining_serial
      "case_types": ["FAO", "RSA", ...],        // only for by_case_type
      "time": "HH:MM" or null,                  // 24h; only for time-anchored groups
      "notes": "<anything a reviewer must know about this group>" or null
    }, ...
  ],
  "clubbed": [["117","246"], ...],   // pairs written as A+B anywhere on the sheet
  "adjournment_slips": [<int>, ...], // items listed under 'Adjournment Slips' / marked 'Adj Slip'
  "review_flags": ["<one short, specific doubt the reviewer must verify>", ...],
  "confidence": "high" | "medium" | "low",
  "notes": "<overall context: what the sheet is, what was skipped, surrounding clutter>"
}

review_flags is the reviewer's checklist — it is shown to them in red. EVERY uncertainty that could change a number, its position, or its inclusion MUST appear as its own entry, worded so the reviewer can check it against the photo in seconds, naming the block and the item (e.g. "Incomplete RSA: handwritten 224 may be struck through — confirm it stands", "Third margin pair read as 136+335 — second number smudged, could be 835"). Do not bury number-affecting doubts in notes; notes is for context only. Use an empty array when nothing needs checking.

Domain rules (follow exactly):
- Item numbers 101-199 are the URGENT list; 200+ are the ORDINARY list. These notices normally resequence only the ordinary items; urgent items run first in serial order unless the sheet says otherwise.
- The sheet's top-to-bottom (and within tables, the printed reading order) IS the calling order. Preserve it exactly. Do not sort numbers.
- Expand ranges: "206 to 218" means 206,207,...,218. "206 TO 213, 215 TO 235" expands both ranges in order.
- Tables whose cells contain rows of grouped numbers (e.g. rows "201,223,245" / "203,224,246") are called ROW-WISE exactly as printed: 201,223,245,203,224,246,...
- HANDWRITTEN marks override print: a struck-through number is REMOVED; a handwritten number inserted in a list belongs at that position; handwritten date corrections win. A handwritten column of numbers in the margin of a printed list IS the taking-up sequence.
- "Fresh" categories (e.g. "FRESH Regular Bail") are URGENT-list matters even when the sheet lists them with times.
- Clubbed pairs like "106+219" or "132+228": keep the pair as a single string item ("106+219") at its printed position AND record it in "clubbed". The urgent member is heard together with the ordinary member.
- Adjournment-slip cases are called AT THEIR SHEET POSITION, never earlier. An item marked "Adj Slip" inside the sequence stays exactly where it is printed in its block. A separate "Adjournment Slips" section becomes its OWN block (rule "explicit_items", label "Adjournment Slips") at the position where that section appears on the sheet. In BOTH cases also list the item numbers in the top-level adjournment_slips array — it is informational (used for a tag on the case card), not ordering.
- A group with a stated clock time ("AT 2:00 PM", "at 1:45 pm", "after urgent or at 2:00 PM whichever is later") gets rule "time_anchor" with the time in 24h "HH:MM"; keep its printed position among the blocks and put the exact condition in its notes.
- Statements that modify the URGENT phase (e.g. "except 128 and 130, to be taken up at the end of the list", urgent items displaced to the end, "proceedings stayed matters in urgent list taken at end: 126, 134, 150") get rule "urgent_edit" with those items and a clear note describing what happens to them.
- Category groups that enumerate only some items and say "and many others - please see list" get rule "by_case_type": put the sheet's case-type names in case_types and the enumerated numbers in items, and note that the category expands to all items of that type from the official list.
- A statement like "thereafter remaining cases serial-wise" / "REMAINING CASES: serial wise" is a block with rule "remaining_serial" and empty items. If specific numbers are listed under "Remaining", use explicit_items instead.
- Skip empty categories (no numbers and no expansion rule). Placeholder markers like "****" mean empty.
- Ignore surrounding clutter: other sheets, the wire mesh, mediation pamphlets, hands. Parse only the main notice. If two distinct notices are fully visible, parse the one filling most of the frame and note the other's existence.
- If any region is illegible or cut off, set confidence to "medium"/"low" and describe exactly what is uncertain in notes. Never invent numbers."""


def _b64_image(url):
    """Download the photo and return (base64, media_type) or (None, err)."""
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        return None, f"image fetch HTTP {r.status_code}"
    data = r.content
    if len(data) > MAX_IMAGE_BYTES:
        return None, f"image too large ({len(data)} bytes)"
    ctype = r.headers.get("Content-Type", "")
    if "png" in ctype:
        media = "image/png"
    elif "webp" in ctype:
        media = "image/webp"
    else:
        media = "image/jpeg"
    return (base64.standard_b64encode(data).decode("utf-8"), media), None


def _extract_json(text):
    """The model is told to output bare JSON; tolerate stray fences anyway."""
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    start = t.find("{")
    end = t.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("no JSON object in model output")
    return json.loads(t[start:end + 1])


def _parse_one(client, rec):
    """Parse a single UnofficialCauseList record; returns the parsed dict."""
    img, err = _b64_image(rec["image_url"])
    if err:
        raise RuntimeError(err)
    b64, media = img

    hints = []
    if rec.get("court_no") is not None:
        hints.append(f"The uploader tagged this photo as Court {int(float(rec['court_no']))}.")
    if rec.get("date"):
        hints.append(f"The uploader tagged the date as {rec['date']}.")
    hints.append("If the sheet itself clearly shows a different court number or date, "
                 "report what the sheet says and flag the mismatch in notes.")

    response = client.beta.messages.create(
        model=PARSE_MODEL,
        max_tokens=16000,
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image",
                 "source": {"type": "base64", "media_type": media, "data": b64}},
                {"type": "text", "text": " ".join(hints)},
            ],
        }],
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("model declined the request (refusal)")
    text = "".join(b.text for b in response.content if b.type == "text")
    parsed = _extract_json(text)
    parsed["_meta"] = {
        "model": response.model,
        "parsed_at": datetime.datetime.now(IST).isoformat(timespec="seconds"),
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }
    return parsed


def _pending_records():
    """Today's (IST) uploaded-but-unparsed records."""
    today = datetime.datetime.now(IST).date().isoformat()
    try:
        resp = requests.get(
            f"{BASE44_URL}/UnofficialCauseList",
            params={"date": today, "limit": 500},
            headers=HEADERS, timeout=30,
        )
        if resp.status_code != 200:
            print(f"[UNOFFICIAL] List fetch HTTP {resp.status_code}")
            return []
        rows = resp.json()
    except Exception as e:
        print(f"[UNOFFICIAL] List fetch error: {e}")
        return []
    out = []
    for r in rows:
        if r.get("status") == "superseded":
            continue
        if r.get("parsed_blocks"):  # already parsed (or errored terminally)
            continue
        if _attempts.get(r["id"], 0) >= MAX_ATTEMPTS:
            continue
        out.append(r)
    return out


def _write_back(rec, parsed):
    payload = {"parsed_blocks": parsed}
    # Fill an untagged court number from the sheet itself; never override a tag.
    if rec.get("court_no") is None and isinstance(parsed.get("court_no"), int):
        payload["court_no"] = parsed["court_no"]
    r = requests.put(
        f"{BASE44_URL}/UnofficialCauseList/{rec['id']}",
        headers=HEADERS, json=payload, timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"write-back HTTP {r.status_code}: {r.text[:150]}")


def _loop():
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print(f"[UNOFFICIAL] Parser thread up (model {PARSE_MODEL}, poll {POLL_SECONDS}s)")
    while True:
        try:
            for rec in _pending_records():
                rid = rec["id"]
                tag = f"court {rec.get('court_no')}" if rec.get("court_no") is not None else "untagged"
                try:
                    t0 = time.time()
                    parsed = _parse_one(client, rec)
                    _write_back(rec, parsed)
                    print(f"[UNOFFICIAL] Parsed {rid} ({tag}) in {time.time()-t0:.0f}s: "
                          f"{len(parsed.get('blocks', []))} blocks, "
                          f"confidence {parsed.get('confidence')}")
                except Exception as e:
                    _attempts[rid] = _attempts.get(rid, 0) + 1
                    print(f"[UNOFFICIAL] Parse failed for {rid} ({tag}), "
                          f"attempt {_attempts[rid]}/{MAX_ATTEMPTS}: {e}")
                    if _attempts[rid] >= MAX_ATTEMPTS:
                        # Terminal: surface the failure to the review UI.
                        try:
                            requests.put(
                                f"{BASE44_URL}/UnofficialCauseList/{rid}",
                                headers=HEADERS,
                                json={"parsed_blocks": {"parse_error": str(e)[:300]}},
                                timeout=30,
                            )
                        except Exception:
                            pass
        except Exception as e:
            print(f"[UNOFFICIAL] Poll cycle error: {e}")
        time.sleep(POLL_SECONDS)


def start_unofficial_parser():
    """Start the background parser thread. Inert without an API key."""
    if not ANTHROPIC_API_KEY:
        print("[UNOFFICIAL] ANTHROPIC_API_KEY not set — parser disabled")
        return
    if anthropic is None:
        print("[UNOFFICIAL] anthropic package missing — parser disabled")
        return
    threading.Thread(target=_loop, daemon=True, name="unofficial-parser").start()
