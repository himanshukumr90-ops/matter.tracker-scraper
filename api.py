"""
MatterTracker — PHHC Case Lookup API
=====================================
A lightweight Flask web server that runs alongside the scraper on Railway.
Provides a single endpoint that fetches real case data from the PHHC website.

Endpoint: GET /case?type=CRM-M&no=22&year=2026
"""

import os
import re
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allow requests from Base44 app

PHHC_CASE_URL = "https://new.phhc.gov.in/case-status/case-no"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://new.phhc.gov.in/case-status/case-no",
    "Origin": "https://new.phhc.gov.in",
}


def fetch_case_from_phhc(case_type, case_no, case_year):
    """
    Tries to fetch case details from PHHC.
    Returns a dict with case details, or None if not found.
    """

    # Strategy 1: Try the internal Next.js API route
    try:
        api_url = "https://new.phhc.gov.in/api/case-status"
        payload = {
            "case_type": case_type,
            "case_no": str(case_no),
            "case_year": str(case_year),
        }
        r = requests.post(api_url, json=payload, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data:
                return parse_api_response(data)
    except Exception:
        pass

    # Strategy 2: POST form submission to case status page
    try:
        form_url = "https://www.phhc.gov.in/case_status_new.php"
        form_data = {
            "case_type": case_type,
            "case_no": str(case_no),
            "case_year": str(case_year),
            "Submit": "Search",
        }
        r = requests.post(form_url, data=form_data, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            result = parse_html_response(r.text)
            if result:
                return result
    except Exception:
        pass

    # Strategy 3: Try alternate PHHC endpoint
    try:
        alt_url = "https://new.phhc.gov.in/case-status/get-case-details"
        payload = {
            "caseType": case_type,
            "caseNo": str(case_no),
            "caseYear": str(case_year),
        }
        r = requests.post(alt_url, json=payload, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data:
                return parse_api_response(data)
    except Exception:
        pass

    return None


def parse_api_response(data):
    """Parse a JSON response from PHHC API into our standard format."""
    if not data:
        return None

    # Handle list response
    if isinstance(data, list):
        if len(data) == 0:
            return None
        data = data[0]

    # Extract fields — try various key name conventions PHHC might use
    def get(keys, default=""):
        for k in keys:
            if k in data and data[k]:
                return str(data[k]).strip()
        return default

    party_detail = get(["party_detail", "partyDetail", "PARTY_DETAIL", "parties"])
    petitioner = ""
    respondent = ""

    # PHHC party_detail is usually "PETITIONER Vs RESPONDENT"
    if " Vs " in party_detail:
        parts = party_detail.split(" Vs ", 1)
        petitioner = parts[0].strip()
        respondent = parts[1].strip()
    elif " VS " in party_detail:
        parts = party_detail.split(" VS ", 1)
        petitioner = parts[0].strip()
        respondent = parts[1].strip()

    next_date = get([
        "next_date", "nextDate", "NEXT_DATE",
        "next_hearing_date", "nextHearingDate",
        "date_of_hearing", "dateOfHearing"
    ])

    cnr = get(["cnr_no", "cnrNo", "CNR_NO", "cnr"])
    status = get(["status", "STATUS", "case_status", "caseStatus"])
    advocate = get(["advocate_name", "advocateName", "ADVOCATE_NAME"])
    respondent_advocate = get(["respondent_advocate", "respondentAdvocate", "RESPONDENT_ADVOCATE"])
    category = get(["category", "CATEGORY"])
    sr_no = get(["sr_no", "srNo", "SR_NO", "list_sr_no"])
    court_no = get(["court_no", "courtNo", "COURT_NO"])

    return {
        "found": True,
        "party_detail": party_detail,
        "petitioner_name": petitioner,
        "respondent_name": respondent,
        "next_hearing_date": next_date,
        "cnr_no": cnr,
        "status": status,
        "advocate_name": advocate,
        "respondent_advocate_name": respondent_advocate,
        "category": category,
        "court_number": court_no,
        "sr_number": sr_no,
    }


def parse_html_response(html):
    """Parse HTML response from old PHHC site."""
    soup = BeautifulSoup(html, "lxml")

    # Check if case was found — old site shows "No Record Found" on failure
    body_text = soup.get_text()
    if "No Record Found" in body_text or "no record" in body_text.lower():
        return None

    result = {
        "found": True,
        "party_detail": "",
        "petitioner_name": "",
        "respondent_name": "",
        "next_hearing_date": "",
        "cnr_no": "",
        "status": "",
        "advocate_name": "",
        "respondent_advocate_name": "",
        "category": "",
        "court_number": "",
        "sr_number": "",
    }

    # Look for labeled rows in tables — PHHC HTML uses table layout
    rows = soup.find_all("tr")
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).lower()
            value = cells[-1].get_text(strip=True)

            if "party" in label:
                result["party_detail"] = value
                if " Vs " in value:
                    parts = value.split(" Vs ", 1)
                    result["petitioner_name"] = parts[0].strip()
                    result["respondent_name"] = parts[1].strip()
                elif " VS " in value:
                    parts = value.split(" VS ", 1)
                    result["petitioner_name"] = parts[0].strip()
                    result["respondent_name"] = parts[1].strip()
            elif "advocate" in label and "respondent" not in label:
                result["advocate_name"] = value
            elif "respondent" in label and "advocate" in label:
                result["respondent_advocate_name"] = value
            elif "next" in label and "date" in label:
                result["next_hearing_date"] = value
            elif "cnr" in label:
                result["cnr_no"] = value
            elif "status" in label:
                result["status"] = value
            elif "category" in label:
                result["category"] = value
            elif "court" in label and "no" in label:
                result["court_number"] = value

    # Only return if we got at least some meaningful data
    if result["party_detail"] or result["cnr_no"] or result["advocate_name"]:
        return result

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
        return jsonify({"error": "Missing required parameters: type, no, year"}), 400

    result = fetch_case_from_phhc(case_type, case_no, case_year)

    if result:
        return jsonify(result)
    else:
        return jsonify({"found": False, "message": "Case not found on PHHC records"}), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"MatterTracker Case API starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
