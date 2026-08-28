# ============================================================
# UNOFFICIAL SEQUENCE — effective queue builder
# ============================================================
# Pure logic (no HTTP): turns an APPROVED UnofficialCauseList
# parsed_blocks document plus the official queue into the order the
# court will actually call items. Mirrored by the frontend's
# computeItemsAway pipeline — any semantic change here must be made
# there too.
#
# v1 model (documented limitations):
#   - The urgent phase (items < 200) always runs first, in serial
#     order, minus items displaced by urgent_edit blocks. Sheets that
#     run ordinary work BEFORE the urgent list (Court 58's 10-12 bail
#     slots) are not modelled; their ordinary blocks still order the
#     post-urgent phase.
#   - time_anchor blocks keep their sheet position (sequence trumps
#     the clock, per the operator); the time is carried as a tag.
#   - Clubbed pairs: the ordinary member sits at its sheet position;
#     the urgent member stays in the urgent phase too. Distance for a
#     tracked case on either member is the nearest upcoming of the
#     two positions (courts sometimes still call the urgent member
#     serial-wise, so neither position is trusted alone).

import re

REST_MARKER = object()  # placeholder for a remaining_serial block

# Sheet-phrase -> official case-type tokens (exact-match after upper()).
# Only confident aliases; unknown labels fall through to exact equality
# with the label itself (covers FAO, RSA, COCP, TA, ...). Never prefix-
# match ("CR" must not swallow CRM/CRR/CRWP).
_TYPE_ALIASES = {
    "CIVIL REVISION": ["CR"],
    "CIVIL RIVISION": ["CR"],          # a real sheet spelling
    "REGULAR SECOND APPEAL": ["RSA"],
    "CIVIL REVISION CASES": ["CR"],
}


def extract_case_type(case_number):
    """'CRM-M-39427-2025' -> 'CRM-M'; None if the shape is unfamiliar."""
    m = re.match(r"^(.*?)-(\d+)-(\d{4})$", str(case_number or "").strip())
    return m.group(1).upper() if m else None


def _as_int(v):
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def _parse_pair(s):
    """'132+228' -> (132, 228) or None."""
    m = re.match(r"^\s*(\d+)\s*\+\s*(\d+)\s*$", str(s))
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def build_effective_queue(official_items, item_case_types, parsed_blocks):
    """
    official_items: ascending list[int] — the official queue for (court, date)
    item_case_types: dict[int -> str] — case-type token per item (may be partial)
    parsed_blocks:  approved parsed_blocks dict from UnofficialCauseList

    Returns (queue, meta):
      queue: list[int], call order, no duplicates
      meta:  {"resequenced": True,
              "clubbed": {item: partner, ...},        # both directions
              "tags": {item: [str, ...], ...}}
    or (official_items, None) when parsed_blocks is unusable.
    """
    if not isinstance(parsed_blocks, dict) or parsed_blocks.get("parse_error"):
        return official_items, None
    blocks = parsed_blocks.get("blocks")
    if not isinstance(blocks, list) or not blocks:
        return official_items, None

    official_set = set(official_items)
    urgent_official = [i for i in official_items if i < 200]
    ordinary_official = [i for i in official_items if i >= 200]

    clubbed = {}

    def _register_pair(a, b):
        if a is None or b is None or a == b:
            return
        clubbed[a] = b
        clubbed[b] = a

    for pair in parsed_blocks.get("clubbed") or []:
        try:
            a, b = _as_int(pair[0]), _as_int(pair[1])
        except (TypeError, IndexError):
            continue
        _register_pair(a, b)

    placed = set()
    displaced_urgent = set()
    seq = []          # ordinary-phase call order (may contain REST_MARKER)
    tags = {}

    def _tag(item, label):
        tags.setdefault(item, []).append(label)

    def _place(item, block_tag=None):
        if item is None or item in placed:
            return
        placed.add(item)
        seq.append(item)
        if block_tag:
            _tag(item, block_tag)

    for block in blocks:
        if not isinstance(block, dict):
            continue
        rule = block.get("rule")
        items = block.get("items") or []
        block_tag = None
        if rule == "time_anchor" and block.get("time"):
            block_tag = f"listed at {block['time']}"

        if rule == "remaining_serial":
            if REST_MARKER not in seq:
                seq.append(REST_MARKER)
            continue

        for raw in items:
            pair = _parse_pair(raw) if isinstance(raw, str) else None
            if pair:
                a, b = pair
                _register_pair(a, b)
                # place the ordinary member at the pair's sheet position
                ordinary_member = b if b >= 200 else a
                urgent_member = a if ordinary_member == b else b
                _place(ordinary_member, block_tag)
                if urgent_member < 200:
                    _tag(urgent_member, f"clubbed with item {ordinary_member}")
                    _tag(ordinary_member, f"clubbed with item {urgent_member}")
                continue
            item = _as_int(raw)
            if item is None or not (100 <= item <= 999):
                continue
            if rule == "urgent_edit" and item < 200:
                displaced_urgent.add(item)
                _place(item, block_tag or "displaced from urgent list")
                continue
            if item < 200:
                # an urgent item appearing inside an ordinary block without
                # urgent_edit — treat like a displacement to this position
                displaced_urgent.add(item)
            _place(item, block_tag)

        if rule == "by_case_type":
            wanted = []
            for label in block.get("case_types") or []:
                lab = str(label or "").strip().upper()
                if not lab:
                    continue
                wanted.extend(_TYPE_ALIASES.get(lab, [lab]))
            if wanted:
                for item in ordinary_official:
                    if item in placed:
                        continue
                    itype = (item_case_types.get(item) or "").upper()
                    if itype and itype in wanted:
                        _place(item, block_tag)

    # Adjournment slips: kept in the queue, called quickly at the start of
    # the ordinary phase (operator ruling). A slip item explicitly placed
    # by a block keeps its block position instead.
    slips = []
    for raw in parsed_blocks.get("adjournment_slips") or []:
        item = _as_int(raw)
        if item is not None and item not in placed and item >= 200:
            slips.append(item)
            placed.add(item)
            _tag(item, "adjournment slip")
    slips.sort()

    # Leftover ordinary items run serial-wise at the REST marker, or at
    # the end when the sheet named no remaining block (domain default).
    leftovers = [i for i in ordinary_official if i not in placed]
    if REST_MARKER in seq:
        idx = seq.index(REST_MARKER)
        seq = seq[:idx] + leftovers + [x for x in seq[idx + 1:] if x is not REST_MARKER]
    else:
        seq = [x for x in seq if x is not REST_MARKER] + leftovers

    urgent_phase = [i for i in urgent_official if i not in displaced_urgent]

    queue = []
    seen = set()
    for item in urgent_phase + slips + seq:
        if item not in seen:
            seen.add(item)
            queue.append(item)

    # Unofficial-only items (absent from our official cache) stay in the
    # queue — the court will call them; the cache may simply lag.
    meta = {"resequenced": True, "clubbed": clubbed, "tags": tags}
    return queue, meta


def positions_to_watch(queue, item, clubbed):
    """All queue positions relevant to a tracked item: its own, plus its
    clubbed partner's (courts may call the urgent member serial-wise OR
    with the pair). Returns a sorted list of indices (possibly empty)."""
    watch = set()
    if item in queue:
        watch.add(queue.index(item))
    partner = (clubbed or {}).get(item)
    if partner is not None and partner in queue:
        watch.add(queue.index(partner))
    return sorted(watch)
