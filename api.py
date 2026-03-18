"""
MatterTracker — PHHC Case Lookup API
=====================================
Flask web server that fetches real case data from the PHHC backend API.
Real endpoint discovered: https://livedb9010.digitalls.in/cis_filing/public/getCase
"""

import os
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

PHHC_API_BASE = "https://livedb9010.digitalls.in/cis_filing/public"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Origin": "https://new.phhc.gov.in",
    "Referer": "https://new.phhc.gov.in/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
}


def fetch_case(case_type, case_no, case_year):
    try:
        url = f"{PHHC_API_BASE}/getCase"
        params = {
            "case_no": str(case_no),
            "case_type": case_type,
            "case_year": str(case_year),
        }
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        print(f"[API] PHHC status: {r.status_code}")

        if r.status_code != 200:
            return None

        data = r.json()
        print(f"[API] Raw response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")

        if isinstance(data, list):
            if len(data) == 0:
                return None
            data = data[0]

        if not data:
            return None

        party_detail = data.get("party_detail") or data.get("partyDetail") or ""
        petitioner = ""
        respondent = ""

        if " Vs " in party_detail:
            parts = party_detail.split(" Vs ", 1)
            petitioner = parts[0].strip()
            respondent = parts[1].strip()
        elif " VS " in party_detail:
            parts = party_detail.split(" VS ", 1)
            petitioner = parts[0].strip()
            respondent = parts[1].strip()

        next_date = (
            data.get("next_date") or
            data.get("nextDate") or
            data.get("next_hearing_date") or
            data.get("nextHearingDate") or ""
        )

        return {
            "found": True,
            "party_detail": party_detail,
            "petitioner_name": petitioner,
            "respondent_name": respondent,
            "next_hearing_date": str(next_date).strip() if next_date else "",
            "cnr_no": data.get("cnr_no") or data.get("cnrNo") or "",
            "status": data.get("status") or data.get("case_status") or "",
            "advocate_name": data.get("advocate_name") or data.get("advocateName") or "",
            "respondent_advocate_name": data.get("respondent_advocate") or data.get("respondentAdvocate") or "",
            "category": data.get("category") or "",
            "diary_number": data.get("diary_number") or data.get("diaryNumber") or "",
            "registration_date": data.get("registration_date") or data.get("registrationDate") or "",
            "list_type": data.get("list_type") or data.get("listType") or "",
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
    case_no = request.args.get("no", "").strip()
    case_year = request.args.get("year", "").strip()

    if not case_type or not case_no or not case_year:
        return jsonify({"error": "Missing parameters: type, no, year"}), 400

    print(f"[API] Looking up: {case_type}/{case_no}/{case_year}")
    result = fetch_case(case_type, case_no, case_year)

    if result:
        return jsonify(result)
    else:
        return jsonify({"found": False, "message": "Case not found"}), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"MatterTracker Case API starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
