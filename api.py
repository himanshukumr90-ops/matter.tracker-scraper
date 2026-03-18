"""
MatterTracker — Combined API + Scraper
=======================================
Single process that runs both:
1. Flask web API for case lookups (foreground, serves HTTP)
2. PHHC display board scraper (background thread, every 30s)
"""

import os
import time
import datetime
import threading
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ── Configuration ──────────────────────────────────────────────
APP_ID  = os.environ.get("BASE44_APP_ID",  "YOUR_APP_ID_HERE")
API_KEY = os.environ.get("BASE44_API_KEY", "YOUR_API_KEY_HERE")
BASE44_URL = f"https://api.base44.com/v1/apps/{APP_ID}/entities"
BASE44_HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

DISPLAY_BOARD_URL = "https://www.phhc.gov.in/home.php?search_param=display"
PHHC_API_BASE = "https://livedb9010.digitalls.in/cis_filing/public"
SCRAPE_INTERVAL = 30
THRESHOLDS = [15, 10, 5]

PHHC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://new.phhc.gov.in",
    "Referer": "https://new.phhc.gov.in/",
}


# ── Case Lookup API ────────────────────────────────────────────

def fetch_case(case_type, case_no, case_year):
    try:
        url = f"{PHHC_API_BASE}/getCase"
        params = {"case_no": str(case_no), "case_type": case_type, "case_year": str(case_year)}
        r = requests.get(url, params=params, headers=PHHC_HEADERS, timeout=15)
        print(f"[API] PHHC status: {r.status_code}")
        if r.status_code != 200:
            return None
        data = r.json()
        if isinstance(data, list):
            if len(data) == 0:
                return None
            data = data[0]
        if not data:
            return None

        party_detail = data.get("party_detail") or data.get("partyDetail") or ""
        petitioner, respondent = "", ""
        if " Vs " in party_detail:
            parts = party_detail.split(" Vs ", 1)
            petitioner, respondent = parts[0].strip(), parts[1].strip()
        elif " VS " in party_detail:
            parts = party_detail.split(" VS ", 1)
            petitioner, respondent = parts[0].strip(), parts[1].strip()

        next_date = (data.get("next_date") or data.get("nextDate") or
                     data.get("next_hearing_date") or data.get("nextHearingDate") or "")

        return {
            "found": True,
            "party_detail": party_detail,
            "petitioner_name": petitioner,
            "respondent_name": respondent,
            "next_hearing_date": str(next_date).strip() if next_date else "",
            "cnr_no": data.get("cnr_no") or data.get("cnrNo") or "",
            "status": data.get("status") or data.get("case_status") or "",
            "advocate_name": data.get("advocate_name") or data.get("advocateName") or "",
            "category": data.get("category") or "",
            "diary_number": data.get("diary_number") or data.get("diaryNumber") or "",
            "registration_date": data.get("registration_date") or data.get("registrationDate") or "",
            "district": data.get("district") or "",
        }
    except Exception as e:
        print(f"[API] Error: {e}")
        return None


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "MatterTracker Case Lookup API"})


@app.route("/case", methods=["GET"])
def get_case():
    case_type = request.args.get("type", "").strip().upper()
    case_no   = request.args.get("no",   "").strip()
    case_year = request.args.get("year", "").strip()
    if not case_type or not case_no or not case_year:
        return jsonify({"error": "Missing parameters: type, no, year"}), 400
    print(f"[API] Looking up: {case_type}/{case_no}/{case_year}")
    result = fetch_case(case_type, case_no, case_year)
    if result:
        return jsonify(result)
    return jsonify({"found": False, "message": "Case not found"}), 404


# ── Display Board Scraper (background thread) ──────────────────

def scrape_display_board():
    try:
        r = requests.get(DISPLAY_BOARD_URL, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        if "Court is not in session" in r.text or "not in Session" in r.text:
            return None
        courts = {}
        for tag in soup.find_all("a", onclick=True):
            onclick = tag.get("onclick", "")
            if "court_no_wise_display" not in onclick:
                continue
            import re
            m = re.search(r"court_no_wise_display\('(\d+)'", onclick)
            if not m:
                continue
            court_no = int(m.group(1))
            td = tag.find_parent("td")
            if not td:
                continue
            next_td = td.find_next_sibling("td")
            if not next_td:
                continue
            sr_text = next_td.get_text(strip=True)
            passover_match = re.match(r"(\d+)-P\((\d+)/(\d+)\)", sr_text)
            if passover_match:
                courts[court_no] = {
                    "current_item": int(passover_match.group(1)),
                    "is_passover": True,
                    "passover_current": int(passover_match.group(2)),
                    "passover_total": int(passover_match.group(3)),
                }
            elif sr_text.isdigit():
                courts[court_no] = {
                    "current_item": int(sr_text),
                    "is_passover": False,
                    "passover_current": None,
                    "passover_total": None,
                }
        return courts if courts else None
    except Exception as e:
        print(f"[SCRAPER] Board fetch error: {e}")
        return None


def update_court_status(courts):
    if not courts:
        return
    now = datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    today = datetime.date.today().isoformat()

    try:
        resp = requests.get(f"{BASE44_URL}/CourtStatus", headers=BASE44_HEADERS, timeout=10)
        existing = {r["court_number"]: r for r in resp.json()} if resp.ok else {}
    except Exception as e:
        print(f"[SCRAPER] CourtStatus fetch error: {e}")
        existing = {}

    for court_no, info in courts.items():
        payload = {
            "court_number": court_no,
            "current_item": info["current_item"],
            "is_passover": info["is_passover"],
            "passover_current": info.get("passover_current"),
            "passover_total": info.get("passover_total"),
            "court_date": today,
            "last_updated": now,
            "is_active": True,
        }
        try:
            if court_no in existing:
                requests.put(f"{BASE44_URL}/CourtStatus/{existing[court_no]['id']}",
                             headers=BASE44_HEADERS, json=payload, timeout=10)
            else:
                requests.post(f"{BASE44_URL}/CourtStatus",
                              headers=BASE44_HEADERS, json=payload, timeout=10)
        except Exception as e:
            print(f"[SCRAPER] CourtStatus update error court {court_no}: {e}")


def check_notifications(courts):
    if not courts:
        return
    today = datetime.date.today().isoformat()
    try:
        resp = requests.get(f"{BASE44_URL}/TrackedCase", headers=BASE44_HEADERS, timeout=10)
        if not resp.ok:
            return
        cases = [c for c in resp.json()
                 if c.get("notifications_enabled") and c.get("court_number") and c.get("case_date") == today]
    except Exception as e:
        print(f"[SCRAPER] TrackedCase fetch error: {e}")
        return

    now = datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    for case in cases:
        court_no = int(case["court_number"])
        item_no  = case.get("item_number")
        if not item_no or court_no not in courts:
            continue
        current = courts[court_no]["current_item"]
        items_away = int(item_no) - current
        for threshold in THRESHOLDS:
            flag = f"notify_at_{threshold}"
            if items_away <= threshold and case.get(flag):
                msg = f"Your case is {items_away} items away in Court {court_no}"
                try:
                    requests.post(f"{BASE44_URL}/NotificationLog",
                                  headers=BASE44_HEADERS, timeout=10,
                                  json={"user_id": case["user_id"], "case_id": case["id"],
                                        "notification_type": f"items_away_{threshold}",
                                        "sent_at": now, "message": msg})
                    requests.put(f"{BASE44_URL}/TrackedCase/{case['id']}",
                                 headers=BASE44_HEADERS, json={flag: False}, timeout=10)
                    print(f"[SCRAPER] Notified: {msg}")
                except Exception as e:
                    print(f"[SCRAPER] Notification error: {e}")


def scraper_loop():
    print("[SCRAPER] Background scraper starting...")
    while True:
        try:
            courts = scrape_display_board()
            if courts:
                update_court_status(courts)
                check_notifications(courts)
            else:
                print("[SCRAPER] Court not in session.")
        except Exception as e:
            print(f"[SCRAPER] Loop error: {e}")
        time.sleep(SCRAPE_INTERVAL)


# ── Start background scraper thread ───────────────────────────
scraper_thread = threading.Thread(target=scraper_loop, daemon=True)
scraper_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"[API] Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
