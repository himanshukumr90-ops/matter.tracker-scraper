# ============================================================
# DAILY SITTING-ROSTER PARSER
# ============================================================
# The PHHC daily sitting roster circulates as a photo of a table:
# every judge, their CR (court room) number, and a STATUS column.
# Judges marked "NOT HOLDING COURT" have a dark courtroom that day.
# The admin uploads the photo (SittingRoster entity); this module
# polls for unparsed rows, reads them with Claude vision, and writes
# the structured result back so the app can badge affected cases.
#
# Verified 2026-09-07: the roster's CR numbering IS the same court
# numbering used by CauseListEntry / CourtStatus / the display board.
#
# SAFETY NOTE. The dangerous failure here is telling an advocate their
# court is dark when it is sitting — they stay away and the matter is
# dismissed. So: only an explicit "NOT HOLDING COURT" status counts as
# a closure; "ON VC" (sitting remotely) and Division-Bench labels
# ("1ST DB", "8TH DB") mean the judge IS working. The frontend layers
# a second guard on top: a live display board showing that court
# active overrides the roster.
#
# Runs only when ANTHROPIC_API_KEY is set; otherwise inert. Own daemon
# thread so a 30-60s vision call can never starve the 30s board loop.

import datetime
import re
import threading
import time

import requests

try:
    import anthropic
except ImportError:  # dependency missing — feature stays off
    anthropic = None

from unofficial_parser import (
    ANTHROPIC_API_KEY, BASE44_URL, HEADERS, IST, PARSE_MODEL,
    _b64_image, _extract_json,
)

POLL_SECONDS = 60
MAX_ATTEMPTS = 3

_attempts = {}  # record id -> failed attempt count (in-memory)

SYSTEM_PROMPT = """You read a photograph of the Punjab & Haryana High Court DAILY SITTING ROSTER and return STRICT JSON — no prose, no code fences.

THE SHEET. A title like "PHHC-SITTING ROSTER FOR 02.09.2026 (WEDNESDAY)", then one row per judge. Columns are: S.N. | HON'BLE JUDGE | CR | DOB | DOA | DOR | ROSTER W.E.F. | VC LINKS FOR ZOOM | MEETING ID | STATUS.

The only two columns that matter are CR and STATUS.
- CR is the judge's COURT ROOM NUMBER. It is a small integer (roughly 1-69). It is NOT the serial number in the first column and NOT any of the dates. Read it from the CR column only.
- STATUS is the last column, usually empty.

OUTPUT EXACTLY THIS SHAPE:
{
  "roster_date": "YYYY-MM-DD",
  "day": "<weekday as printed, upper case>",
  "no_judge_on_leave": <true|false>,
  "not_holding": [ {"judge": "<name as printed>", "court_no": <int>}, ... ],
  "other_status": [ {"judge": "<name>", "court_no": <int>, "status": "<text as printed>"}, ... ],
  "review_flags": ["<one short, specific doubt that could change a court number>", ...],
  "confidence": "high" | "medium" | "low",
  "notes": "<what the sheet is, anything unusual, anything skipped>"
}

WHAT COUNTS AS A CLOSURE — BE STRICT.
- Put a judge in "not_holding" ONLY when the STATUS column says "NOT HOLDING COURT" (any capitalisation). Such rows are normally highlighted red as well; the red highlight ALONE, with no such text, is not enough — say so in review_flags instead.
- NEVER treat these as closures, because the judge IS working. They go in "other_status" verbatim:
  * "ON VC" / "ON VIDEO CONFERENCE" — sitting remotely.
  * Any Division-Bench label: "1ST DB", "2ND DB", "8TH DB", "11TH DB", and so on.
  * Anything else printed in STATUS that is not "NOT HOLDING COURT".
- If a status is ambiguous or partly cut off, put it in "other_status" as printed AND add a review_flag. Never guess it into "not_holding".

THE "NOBODY IS ON LEAVE" SHEET. Some days carry a banner such as "*NO HON'BLE JUDGE IS ON LEAVE ON 07.09.26 (MONDAY)*", often sideways down the right-hand side, and no row is marked. Then set "no_judge_on_leave": true and "not_holding": []. Set it false whenever any judge is marked not holding court. If both appear, trust the marked rows, set false, and add a review_flag.

THE DATE. Take roster_date from the title, which is DD.MM.YYYY, and convert to YYYY-MM-DD (so "02.09.2026" becomes "2026-09-02"). This is the date the roster APPLIES to — usually the day after it was circulated. If the title date is unreadable, set roster_date to null and add a review_flag; do not guess it from anything else on the sheet.

ACCURACY RULES.
- Read every row. A missed closure means an advocate is told their court is sitting when it is not; a wrong court number means the wrong advocate is warned. Both are serious.
- Court numbers must be integers. If a CR digit is smudged or ambiguous, still report your best reading BUT add a review_flag naming the judge and both candidate readings.
- Copy judge names as printed, including the "HMJ" prefix.
- review_flags is a checklist for a human, in red in the UI. Every doubt that could change a court number, a status, or the date gets its own short, specific entry. Use an empty array when there is genuinely nothing to check.
- Set confidence "medium" or "low" if any part of the sheet is blurred, cropped or cut off, and say exactly what in notes. Never invent a judge, a court number or a status."""


def _pending_records():
    """SittingRoster rows that have not been parsed yet (any date — rosters
    are normally uploaded the evening before the day they apply to)."""
    try:
        resp = requests.get(
            f"{BASE44_URL}/SittingRoster",
            params={"limit": 200}, headers=HEADERS, timeout=30,
        )
        if resp.status_code != 200:
            print(f"[ROSTER] List fetch HTTP {resp.status_code}")
            return []
        rows = resp.json()
    except Exception as e:
        print(f"[ROSTER] List fetch error: {e}")
        return []
    out = []
    for r in rows if isinstance(rows, list) else []:
        if r.get("status") == "superseded":
            continue
        if r.get("parsed_blocks"):      # already parsed (or errored terminally)
            continue
        if _attempts.get(r["id"], 0) >= MAX_ATTEMPTS:
            continue
        out.append(r)
    return out


def _parse_one(client, rec):
    img, err = _b64_image(rec["image_url"])
    if err:
        raise RuntimeError(err)
    b64, media = img
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
                {"type": "text",
                 "text": "Read this sitting roster and return the JSON."},
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


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _clean(parsed):
    """Normalise and defend against a malformed parse. Anything we cannot
    trust is dropped rather than guessed — a bad court number here warns the
    wrong advocate."""
    if not isinstance(parsed, dict):
        return None
    date = parsed.get("roster_date")
    if not (isinstance(date, str) and _DATE_RE.match(date)):
        parsed["roster_date"] = None
    good, dropped = [], 0
    for e in parsed.get("not_holding") or []:
        if not isinstance(e, dict):
            dropped += 1
            continue
        try:
            cn = int(float(e.get("court_no")))
        except (TypeError, ValueError):
            dropped += 1
            continue
        if not (1 <= cn <= 99):     # outside any real court room
            dropped += 1
            continue
        good.append({"judge": str(e.get("judge") or "").strip()[:120],
                     "court_no": cn})
    parsed["not_holding"] = good
    if dropped:
        flags = parsed.get("review_flags")
        parsed["review_flags"] = (flags if isinstance(flags, list) else []) + [
            f"{dropped} 'not holding court' row(s) had an unreadable court "
            f"number and were DROPPED — check the photo for those courts."
        ]
    # A sheet saying nobody is on leave must not also carry closures.
    if good:
        parsed["no_judge_on_leave"] = False
    return parsed


def _auto_approve(parsed):
    """Same policy as the cause-list parser: a clean parse goes live and the
    admin un-approves anything wrong. Held back: a parse error, the model's
    own low confidence, and — specific to rosters — a missing date, since
    without one we cannot know which day it applies to."""
    if not isinstance(parsed, dict) or parsed.get("parse_error"):
        return False
    if str(parsed.get("confidence", "")).strip().lower() == "low":
        return False
    if not parsed.get("roster_date"):
        return False
    return True


def _write_back(rec, parsed):
    payload = {"parsed_blocks": parsed}
    if parsed.get("roster_date"):
        payload["roster_date"] = parsed["roster_date"]
    if _auto_approve(parsed):
        payload["status"] = "approved"
    r = requests.put(
        f"{BASE44_URL}/SittingRoster/{rec['id']}",
        headers=HEADERS, json=payload, timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"write-back HTTP {r.status_code}: {r.text[:150]}")


def _loop():
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print(f"[ROSTER] Sitting-roster parser up (model {PARSE_MODEL}, poll {POLL_SECONDS}s)")
    while True:
        try:
            for rec in _pending_records():
                rid = rec["id"]
                try:
                    t0 = time.time()
                    parsed = _clean(_parse_one(client, rec))
                    _write_back(rec, parsed)
                    live = "APPROVED" if _auto_approve(parsed) else "held for review"
                    closed = len(parsed.get("not_holding") or [])
                    print(f"[ROSTER] Parsed {rid} for {parsed.get('roster_date')} "
                          f"in {time.time()-t0:.0f}s: {closed} court(s) not holding "
                          f"{[e['court_no'] for e in parsed.get('not_holding') or []]}, "
                          f"no_judge_on_leave={parsed.get('no_judge_on_leave')}, "
                          f"confidence {parsed.get('confidence')} -> {live}")
                except Exception as e:
                    _attempts[rid] = _attempts.get(rid, 0) + 1
                    print(f"[ROSTER] Parse failed for {rid}, "
                          f"attempt {_attempts[rid]}/{MAX_ATTEMPTS}: {e}")
                    if _attempts[rid] >= MAX_ATTEMPTS:
                        try:
                            requests.put(
                                f"{BASE44_URL}/SittingRoster/{rid}",
                                headers=HEADERS,
                                json={"parsed_blocks": {"parse_error": str(e)[:300]}},
                                timeout=30,
                            )
                        except Exception:
                            pass
        except Exception as e:
            print(f"[ROSTER] Poll cycle error: {e}")
        time.sleep(POLL_SECONDS)


def start_roster_parser():
    if not ANTHROPIC_API_KEY:
        print("[ROSTER] ANTHROPIC_API_KEY not set — roster parser disabled")
        return
    if anthropic is None:
        print("[ROSTER] anthropic package missing — roster parser disabled")
        return
    threading.Thread(target=_loop, name="roster-parser", daemon=True).start()
