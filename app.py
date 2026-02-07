import json
import os
import time
import uuid
import math
import fcntl
import tempfile
from datetime import datetime
from contextlib import contextmanager
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# -----------------------------
# CONFIG
# -----------------------------
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"
DATA_FILE = "/var/data/izesquad_data.json"
LOCK_FILE = DATA_FILE + ".lock"

K_BASE = 50  # Elo K-base => ถ้าแรงใกล้กัน delta ~ 25 (ตามที่ขอ "เฉลี่ย 25")
K_CALIB_MULT = 1.8  # calibrate เร็วกว่า
AUTOMATCH_COOLDOWN_SEC = 3  # กัน auto-match ยิงถี่เกินจาก polling

# -----------------------------
# DEFAULT DB
# -----------------------------
default_db = {
    "schema_version": 3,
    "system_settings": {
        "total_courts": 2,
        "is_session_active": False,
        "current_event_id": None,

        # session settings (set ตอนกดเริ่มก๊วน)
        "match_points": 21,      # 11 or 21
        "match_bo": 1,           # 1,2,3
        "notify_enabled": False, # mod เปิด/ปิดตอน start (default ปิด)

        # per-court automatch toggle
        "automatch": {},

        # internal throttle
        "automatch_last_ts": {}
    },
    "mod_ids": [],
    "players": {},
    "events": {},          # sessions list
    "courts": {},          # court_id -> match or None
    "match_history": [],   # newest first
    "billing_history": []
}

# -----------------------------
# UTIL: LOCKED DB IO (atomic)
# -----------------------------
def deep_merge(dst, src):
    for k, v in src.items():
        if k not in dst:
            dst[k] = v
        else:
            if isinstance(dst[k], dict) and isinstance(v, dict):
                deep_merge(dst[k], v)

def ensure_dirs():
    d = os.path.dirname(DATA_FILE)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def init_db_if_missing():
    ensure_dirs()
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(default_db, f, ensure_ascii=False, indent=2)

@contextmanager
def db_lock():
    ensure_dirs()
    # lock file
    lf = open(LOCK_FILE, "w")
    fcntl.flock(lf, fcntl.LOCK_EX)
    try:
        yield
    finally:
        fcntl.flock(lf, fcntl.LOCK_UN)
        lf.close()

def load_db():
    init_db_if_missing()
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    # merge new keys without destroying old data
    deep_merge(data, json.loads(json.dumps(default_db)))
    return data

def save_db(data):
    ensure_dirs()
    # atomic write
    fd, tmp_path = tempfile.mkstemp(prefix="izesquad_", suffix=".json", dir=os.path.dirname(DATA_FILE) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, DATA_FILE)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass

def now_ts():
    return time.time()

# -----------------------------
# RANK TITLE (HTML-only, no emoji)
# -----------------------------
def get_rank_title(mmr: int) -> str:
    try:
        v = int(mmr)
    except:
        v = 1000

    # 8+ tiers + T9 as requested; use "ฉายา" (no emoji)
    if v < 1000:
        return "มือใหม่หัดตี"
    if v < 1200:
        return "จอมวางลูก"
    if v < 1300:
        return "ตบดังแต่ยังหลุด"
    if v < 1400:
        return "สายคุมเกม"
    if v < 1500:
        return "โต้กลับไว"
    if v < 1600:
        return "ตัวตึงหน้าเน็ต"
    if v < 1700:
        return "ราชันย์ก๊วน"
    if v < 1800:
        return "จักรพรรดิ์คอร์ท"
    return "เทพเจ้าก๊วนแบด"

def is_unrank(p):
    return int(p.get("calib_games", 0)) < 10

def player_role(uid, db):
    if uid == SUPER_ADMIN_ID:
        return "super"
    if uid in db.get("mod_ids", []):
        return "mod"
    return "user"

def player_public_view(p, include_hidden_mmr=False):
    """
    - Unrank: hide mmr everywhere for normal users
    - Mod/Super: can see real mmr if include_hidden_mmr True
    """
    unrank = is_unrank(p)
    base = {
        "id": p["id"],
        "nickname": p.get("nickname", "Unknown"),
        "pictureUrl": p.get("pictureUrl", ""),
        "status": p.get("status", "offline"),
        "resting": bool(p.get("resting", False)),
        "auto_rest": bool(p.get("auto_rest", False)),
        "queue_since": p.get("queue_since", None),
        "last_active": float(p.get("last_active", 0)),
        "paired_with": p.get("paired_with", None),
        "pair_outgoing": p.get("pair_outgoing", None),
        "pair_incoming": p.get("pair_incoming", []),
        "current_court": p.get("current_court", None),
        "current_match_id": p.get("current_match_id", None),
        "calib_games": int(p.get("calib_games", 0)),
        "unrank": unrank,
    }

    mmr_val = int(p.get("mmr", 1000))
    if unrank and not include_hidden_mmr:
        base["mmr"] = None
        base["rank_title"] = f"Unrank ({base['calib_games']}/10)"
    else:
        base["mmr"] = mmr_val
        base["rank_title"] = get_rank_title(mmr_val) if not unrank else f"Unrank ({base['calib_games']}/10)"
    return base

# -----------------------------
# COURTS / SESSION HOUSEKEEPING
# -----------------------------
def ensure_courts(db):
    total = int(db["system_settings"].get("total_courts", 2))
    if "courts" not in db or not isinstance(db["courts"], dict):
        db["courts"] = {}
    if "automatch" not in db["system_settings"] or not isinstance(db["system_settings"]["automatch"], dict):
        db["system_settings"]["automatch"] = {}
    if "automatch_last_ts" not in db["system_settings"] or not isinstance(db["system_settings"]["automatch_last_ts"], dict):
        db["system_settings"]["automatch_last_ts"] = {}

    # add
    for i in range(1, total + 1):
        cid = str(i)
        if cid not in db["courts"]:
            db["courts"][cid] = None
        if cid not in db["system_settings"]["automatch"]:
            db["system_settings"]["automatch"][cid] = False
        if cid not in db["system_settings"]["automatch_last_ts"]:
            db["system_settings"]["automatch_last_ts"][cid] = 0

    # remove extra
    for cid in list(db["courts"].keys()):
        if int(cid) > total:
            db["courts"].pop(cid, None)
            db["system_settings"]["automatch"].pop(cid, None)
            db["system_settings"]["automatch_last_ts"].pop(cid, None)

def expire_rest(db):
    t = now_ts()
    for p in db["players"].values():
        until = p.get("rest_until")
        if p.get("resting") and until and t >= float(until):
            p["resting"] = False
            p["rest_until"] = None

def advance_called_to_playing(db):
    t = now_ts()
    for cid, m in db["courts"].items():
        if not m:
            continue
        if m.get("status") == "called":
            start_at = float(m.get("start_at", 0))
            if start_at and t >= start_at:
                m["status"] = "playing"
                m["actual_start"] = start_at

def compute_global_avg_match_minutes(db):
    # average duration last 10 matches
    durations = []
    for m in db.get("match_history", [])[:10]:
        if m.get("duration_min") is not None:
            durations.append(float(m["duration_min"]))
    if not durations:
        return 12.0
    return sum(durations) / len(durations)

def suggested_cooldown_min(db):
    """
    คำนวณอัตโนมัติจากจำนวนคอร์ท + คน check-in ตอนนั้น
    แล้วผู้เล่นเลือกเปิด auto-rest เอง (ตามที่ขอ)
    """
    C = int(db["system_settings"].get("total_courts", 2))
    if C <= 0:
        return 0

    # active players (eligible-ish)
    active = [p for p in db["players"].values() if p.get("status") in ["active"]]
    N = len(active)
    if N <= 4 * C:
        return 0

    avg_m = compute_global_avg_match_minutes(db)
    # expected "overcrowd factor"
    factor = (N / (4 * C)) - 1.0
    cd = max(0.0, avg_m * factor)
    # clamp
    cd = max(0, min(20, int(math.ceil(cd))))
    return cd

# -----------------------------
# PAIR REQUESTS (ตามกติกาที่คุย)
# -----------------------------
def pair_request_send(db, uid, target_id):
    u = db["players"].get(uid)
    t = db["players"].get(target_id)
    if not u or not t:
        return False, "User not found"

    if u.get("status") != "active":
        return False, "ต้อง Check-in ก่อนนะครับ"

    # requester ขอได้แค่คนเดียว
    if u.get("pair_outgoing") and u.get("pair_outgoing") != target_id:
        return False, "คุณส่งคำขอไว้แล้ว ต้องยกเลิกก่อนถึงจะขอคนอื่นได้"

    # ถ้าคุณ paired อยู่แล้ว ส่งเพิ่มไม่ได้
    if u.get("paired_with"):
        return False, "คุณจับคู่เรียบร้อยแล้ว ส่งคำขอเพิ่มไม่ได้"

    # target รับคำขอได้หลายคน (เลยไม่กัน)
    # แต่ถ้า target paired แล้ว ก็ยังรับคำขอได้ตามที่ขอ (รับได้แต่ accept ไม่ได้)
    # (เรายังอนุญาตให้ส่งเข้าไปได้)

    u["pair_outgoing"] = target_id

    inc = t.get("pair_incoming", [])
    if uid not in inc:
        inc.append(uid)
    t["pair_incoming"] = inc
    return True, None

def pair_request_cancel_outgoing(db, uid):
    u = db["players"].get(uid)
    if not u:
        return False, "User not found"

    tgt = u.get("pair_outgoing")
    if not tgt:
        return True, None

    t = db["players"].get(tgt)
    if t:
        inc = t.get("pair_incoming", [])
        if uid in inc:
            inc.remove(uid)
        t["pair_incoming"] = inc

    u["pair_outgoing"] = None
    return True, None

def pair_accept(db, receiver_id, requester_id):
    r = db["players"].get(receiver_id)
    q = db["players"].get(requester_id)
    if not r or not q:
        return False, "User not found"

    # receiver accept ได้แค่ 1 ถ้า already paired -> ห้าม
    if r.get("paired_with"):
        return False, "คุณจับคู่ไว้แล้ว ต้องยกเลิกก่อนถึงจะยอมรับได้"

    # requester ถ้า paired อยู่แล้ว -> รับคำขอได้แต่ accept ไม่ได้
    if q.get("paired_with"):
        return False, "อีกฝ่ายจับคู่ไว้แล้ว"

    # ต้องมีคำขออยู่จริง
    if requester_id not in (r.get("pair_incoming", []) or []):
        return False, "ไม่มีคำขอนี้แล้ว"

    # ถ้า receiver เคยส่ง outgoing ไปหาใครอยู่ แล้วรับคำขอคนอื่น => ยกเลิก outgoing เดิม
    if r.get("pair_outgoing"):
        pair_request_cancel_outgoing(db, receiver_id)

    # และตามกติกา: ถ้า A ส่งให้ B แล้ว A รับของ C => outgoing A->B ต้องถูกยกเลิก
    # (เราทำไปแล้วด้วย cancel_outgoing ของ receiver)

    # set paired
    r["paired_with"] = requester_id
    q["paired_with"] = receiver_id
    r["paired_since"] = now_ts()
    q["paired_since"] = now_ts()

    # clear requester outgoing (ต้องเป็น receiver อยู่แล้ว)
    q["pair_outgoing"] = None

    # remove this requester from receiver incoming (แต่คำขออื่น "ไม่หายไป" -> คงไว้)
    inc = r.get("pair_incoming", [])
    if requester_id in inc:
        inc.remove(requester_id)
    r["pair_incoming"] = inc

    return True, None

def pair_decline(db, receiver_id, requester_id):
    r = db["players"].get(receiver_id)
    q = db["players"].get(requester_id)
    if not r:
        return False, "User not found"

    inc = r.get("pair_incoming", [])
    if requester_id in inc:
        inc.remove(requester_id)
    r["pair_incoming"] = inc

    # requester outgoing ถ้าชี้มาที่ receiver -> เคลียร์
    if q and q.get("pair_outgoing") == receiver_id:
        q["pair_outgoing"] = None

    return True, None

def pair_cancel_pair(db, uid):
    u = db["players"].get(uid)
    if not u:
        return False, "User not found"
    partner = u.get("paired_with")
    if not partner:
        return True, None
    p = db["players"].get(partner)
    u["paired_with"] = None
    if p and p.get("paired_with") == uid:
        p["paired_with"] = None
    return True, None

# -----------------------------
# MATCHMAKING
# -----------------------------
def effective_mmr(p):
    mmr = int(p.get("mmr", 1000))
    # calibrate streak bump: ถ้าชนะรัวๆช่วง calibrate ให้เจอคนแรงขึ้น
    if is_unrank(p):
        streak = int(p.get("calib_win_streak", 0))
        mmr += min(250, streak * 50)
    return mmr

def eligible_player(p, db):
    if not db["system_settings"].get("is_session_active"):
        return False
    if p.get("status") != "active":
        return False
    if p.get("resting"):
        return False
    if p.get("current_match_id"):
        return False
    # ถ้า paired_with แล้วต้องให้คู่พร้อมลงด้วย
    partner_id = p.get("paired_with")
    if partner_id:
        partner = db["players"].get(partner_id)
        if not partner:
            return False
        if partner.get("status") != "active" or partner.get("resting") or partner.get("current_match_id"):
            return False
    return True

def recent_signature_penalty(db, team_a_ids, team_b_ids):
    """
    กันได้ระดับหนึ่ง: ไม่ให้คู่เหมือนเดิมหลัง cancel/จบ
    """
    sigA = tuple(sorted(team_a_ids))
    sigB = tuple(sorted(team_b_ids))
    sig = tuple(sorted([sigA, sigB]))
    recent = db.get("recent_match_signatures", []) or []
    # recent items: {"sig":sig, "ts":...}
    t = now_ts()
    # purge old (30 นาที)
    new_recent = [x for x in recent if (t - float(x.get("ts", 0))) < 1800]
    db["recent_match_signatures"] = new_recent

    for x in new_recent:
        if x.get("sig") == sig:
            return 100000  # massive penalty
    return 0

def best_team_split(db, four_ids):
    """
    เลือกทีมให้:
    - เคารพ paired_with => ต้องอยู่ทีมเดียวกัน
    - เลี่ยงคู่ห่างกันเกิน (fair as partner skill)
    - แล้วค่อยดู total mmr diff
    """
    players = db["players"]
    ids = list(four_ids)

    # possible partitions for 4 players into 2 teams of 2
    # partitions: (a,b)|(c,d), (a,c)|(b,d), (a,d)|(b,c)
    a,b,c,d = ids
    splits = [
        ([a,b],[c,d]),
        ([a,c],[b,d]),
        ([a,d],[b,c]),
    ]

    def violates_pair(team):
        s = set(team)
        for uid in team:
            pw = players[uid].get("paired_with")
            if pw and pw not in s:
                return True
        return False

    best = None
    best_cost = None

    for ta, tb in splits:
        if violates_pair(ta) or violates_pair(tb):
            continue

        mmr_ta = [effective_mmr(players[x]) for x in ta]
        mmr_tb = [effective_mmr(players[x]) for x in tb]

        # partner disparity
        gap_a = abs(mmr_ta[0] - mmr_ta[1])
        gap_b = abs(mmr_tb[0] - mmr_tb[1])
        max_gap = max(gap_a, gap_b)
        gap_balance = abs(gap_a - gap_b)

        # team total diff
        total_diff = abs(sum(mmr_ta) - sum(mmr_tb))

        # avoid repeat signature (cancel / recent)
        rp = recent_signature_penalty(db, ta, tb)

        # cost tuple: prioritize partner fairness first, then total_diff
        cost = (max_gap, gap_balance, total_diff, rp)

        if best_cost is None or cost < best_cost:
            best_cost = cost
            best = (ta, tb)

    if not best:
        # fallback split (shouldn't)
        return ([ids[0], ids[1]], [ids[2], ids[3]])

    return best

def pick_four_players(db):
    """
    First priority: waiting time
    Second: mmr proximity (avoid extremes if possible)
    """
    players = db["players"]
    eligible = [p for p in players.values() if eligible_player(p, db)]
    if len(eligible) < 4:
        return None

    # sort by queue_since oldest first
    def qs(p):
        return float(p.get("queue_since") or p.get("last_active") or now_ts())
    eligible.sort(key=qs)

    # candidate pool first 10 by waiting
    pool = eligible[:10]
    pool_ids = [p["id"] for p in pool]

    oldest_id = pool_ids[0]

    # build combinations of 4 (bounded)
    best_combo = None
    best_tuple = None
    t = now_ts()

    # quick helper: expand pairs closure
    def normalize_combo(ids):
        s = set(ids)
        # ensure pair closure
        changed = True
        while changed:
            changed = False
            for uid in list(s):
                pw = players[uid].get("paired_with")
                if pw and pw not in s:
                    s.add(pw)
                    changed = True
        if len(s) != 4:
            return None
        return tuple(sorted(s))

    combos = set()
    n = len(pool_ids)
    for i in range(n):
        for j in range(i+1, n):
            for k in range(j+1, n):
                for l in range(k+1, n):
                    ids = [pool_ids[i], pool_ids[j], pool_ids[k], pool_ids[l]]
                    if oldest_id not in ids:
                        continue
                    norm = normalize_combo(ids)
                    if norm:
                        combos.add(norm)

    if not combos:
        # fallback greedy from eligible with pair closure
        s = []
        for p in eligible:
            if p["id"] in s:
                continue
            s.append(p["id"])
            pw = players[p["id"]].get("paired_with")
            if pw and pw not in s:
                s.append(pw)
            if len(s) >= 4:
                s = s[:4]
                return s if len(set(s)) == 4 else None
        return None

    for combo in combos:
        ids = list(combo)
        # waiting priority: maximize sum_wait (older => larger wait)
        waits = []
        for uid in ids:
            q0 = float(players[uid].get("queue_since") or players[uid].get("last_active") or t)
            waits.append(max(0.0, t - q0))
        sum_wait = sum(waits)

        # mmr dispersion (avoid mixing extreme)
        mmrs = [effective_mmr(players[uid]) for uid in ids]
        spread = max(mmrs) - min(mmrs)

        # team split cost (partner fairness etc.)
        ta, tb = best_team_split(db, ids)
        # compute split tuple cost again (same as function)
        mmr_ta = [effective_mmr(players[x]) for x in ta]
        mmr_tb = [effective_mmr(players[x]) for x in tb]
        gap_a = abs(mmr_ta[0] - mmr_ta[1])
        gap_b = abs(mmr_tb[0] - mmr_tb[1])
        max_gap = max(gap_a, gap_b)
        gap_balance = abs(gap_a - gap_b)
        total_diff = abs(sum(mmr_ta) - sum(mmr_tb))

        tup = (-sum_wait, spread, max_gap, gap_balance, total_diff)
        if best_tuple is None or tup < best_tuple:
            best_tuple = tup
            best_combo = ids

    return best_combo

def create_match_on_court(db, court_id, initiated_by="auto"):
    ensure_courts(db)
    if not db["system_settings"].get("is_session_active"):
        return False, "Session not active"

    cid = str(court_id)
    if cid not in db["courts"]:
        return False, "Court not found"
    if db["courts"][cid] is not None:
        return False, "Court is busy"

    four = pick_four_players(db)
    if not four:
        return False, "Not enough players"

    team_a, team_b = best_team_split(db, four)

    mid = str(uuid.uuid4())[:8]
    t = now_ts()
    start_at = t + 60  # แจ้งให้ลงภายใน 1 นาที แล้วเริ่มนับเวลาเลย

    m = {
        "id": mid,
        "event_id": db["system_settings"].get("current_event_id"),
        "court_id": cid,
        "team_a_ids": team_a,
        "team_b_ids": team_b,
        "created_at": t,
        "start_at": start_at,
        "actual_start": None,
        "status": "called",   # called -> playing (after 60s)
        "initiated_by": initiated_by,
    }

    db["courts"][cid] = m

    # set players state
    for uid in team_a + team_b:
        p = db["players"][uid]
        p["current_court"] = cid
        p["current_match_id"] = mid
        # keep status 'called' for notify stage
        p["status"] = "called"

    # add attendance to current event roster
    eid = db["system_settings"].get("current_event_id")
    if eid and eid in db["events"]:
        roster = db["events"][eid].get("players", [])
        for uid in team_a + team_b:
            if uid not in roster:
                roster.append(uid)
        db["events"][eid]["players"] = roster

    return True, m

def auto_fill_courts(db):
    ensure_courts(db)
    if not db["system_settings"].get("is_session_active"):
        return

    t = now_ts()
    for cid in sorted(db["courts"].keys(), key=lambda x: int(x)):
        if not db["system_settings"]["automatch"].get(cid):
            continue
        if db["courts"][cid] is not None:
            continue

        last_ts = float(db["system_settings"]["automatch_last_ts"].get(cid, 0))
        if t - last_ts < AUTOMATCH_COOLDOWN_SEC:
            continue

        ok, _ = create_match_on_court(db, cid, initiated_by="auto")
        db["system_settings"]["automatch_last_ts"][cid] = t
        # if no players, stop
        if not ok:
            continue

def housekeeping(db):
    ensure_courts(db)
    expire_rest(db)
    advance_called_to_playing(db)

    # auto-fill idle courts that have automatch enabled
    auto_fill_courts(db)

# -----------------------------
# SCORE VALIDATION
# -----------------------------
def validate_set_score(a, b, target):
    """
    badminton deuce: must win by 2 after reaching target,
    but cap at 30 (30-29 allowed).
    """
    if a is None or b is None:
        return False, "Missing score"
    try:
        a = int(a); b = int(b)
    except:
        return False, "Score must be integer"

    if a < 0 or b < 0:
        return False, "Score must be >= 0"
    if a > 30 or b > 30:
        return False, "Max 30"

    if a == b:
        return False, "Score cannot tie"

    w = max(a, b)
    l = min(a, b)

    # must reach at least target to win
    if w < target:
        return False, f"Winner must reach {target}"

    # if reach 30, 30-29 ok
    if w == 30:
        if l != 29:
            return False, "At 30, must be 30-29"
        return True, None

    # normal: must win by 2
    if (w - l) != 2 and w >= target and l >= (target - 1):
        # Example: 21-20 invalid, must 22-20 or 30-29
        return False, "Must win by 2 (except 30-29)"
    if w >= target and (w - l) >= 2:
        return True, None

    return False, "Invalid score"

def decide_winner_from_sets(sets, bo, target):
    """
    bo1: winner from set1
    bo3: winner by set wins (2 needed)
    bo2: winner by total points (ตามที่ขอ), if tie -> winner of last set
    """
    set_wins_a = 0
    set_wins_b = 0
    total_a = 0
    total_b = 0

    for s in sets:
        a = int(s["a"]); b = int(s["b"])
        total_a += a
        total_b += b
        if a > b:
            set_wins_a += 1
        else:
            set_wins_b += 1

    if bo == 1:
        winner = "A" if sets[0]["a"] > sets[0]["b"] else "B"
        return winner, set_wins_a, set_wins_b, total_a, total_b

    if bo == 3:
        winner = "A" if set_wins_a >= 2 else "B"
        return winner, set_wins_a, set_wins_b, total_a, total_b

    # bo2 by total points
    if total_a > total_b:
        return "A", set_wins_a, set_wins_b, total_a, total_b
    if total_b > total_a:
        return "B", set_wins_a, set_wins_b, total_a, total_b

    # tie total points => last set winner
    last = sets[-1]
    winner = "A" if int(last["a"]) > int(last["b"]) else "B"
    return winner, set_wins_a, set_wins_b, total_a, total_b

# -----------------------------
# ELO MMR UPDATE (score-aware)
# -----------------------------
def expected_win(my_avg, opp_avg):
    return 1.0 / (1.0 + (10 ** ((opp_avg - my_avg) / 400.0)))

def apply_mmr(db, team_a_ids, team_b_ids, winner, point_diff, target_points, bo):
    players = db["players"]

    a_avg = sum(int(players[x].get("mmr", 1000)) for x in team_a_ids) / len(team_a_ids)
    b_avg = sum(int(players[x].get("mmr", 1000)) for x in team_b_ids) / len(team_b_ids)

    if winner == "A":
        win_avg, lose_avg = a_avg, b_avg
        win_ids, lose_ids = team_a_ids, team_b_ids
    else:
        win_avg, lose_avg = b_avg, a_avg
        win_ids, lose_ids = team_b_ids, team_a_ids

    e_win = expected_win(win_avg, lose_avg)
    e_lose = 1.0 - e_win

    # margin factor from score (tie-breaker)
    denom = max(1, target_points * bo)
    margin = abs(point_diff) / denom
    margin_factor = 1.0 + min(0.5, margin / 2.0)  # up to +0.25

    snapshot = {}

    for uid in win_ids:
        p = players[uid]
        k = K_BASE * margin_factor * (K_CALIB_MULT if is_unrank(p) else 1.0)
        delta = int(round(k * (1.0 - e_win)))
        p["mmr"] = int(p.get("mmr", 1000)) + delta
        snapshot[uid] = {"delta": delta}

    for uid in lose_ids:
        p = players[uid]
        k = K_BASE * margin_factor * (K_CALIB_MULT if is_unrank(p) else 1.0)
        delta = int(round(k * (0.0 - e_lose)))
        p["mmr"] = int(p.get("mmr", 1000)) + delta
        snapshot[uid] = {"delta": delta}

    return snapshot

def update_calibration(db, team_a_ids, team_b_ids, winner):
    players = db["players"]

    # update win streak for calibrate bump
    all_ids = team_a_ids + team_b_ids
    for uid in all_ids:
        p = players[uid]
        if is_unrank(p):
            p["calib_games"] = int(p.get("calib_games", 0)) + 1

            is_win = (winner == "A" and uid in team_a_ids) or (winner == "B" and uid in team_b_ids)
            if is_win:
                p["calib_wins"] = int(p.get("calib_wins", 0)) + 1
                p["calib_win_streak"] = int(p.get("calib_win_streak", 0)) + 1
            else:
                p["calib_win_streak"] = 0

            # clamp
            if p["calib_games"] > 10:
                p["calib_games"] = 10

def update_stats(db, team_a_ids, team_b_ids, sets, set_wins_a, set_wins_b, total_a, total_b):
    players = db["players"]

    for uid in team_a_ids:
        p = players[uid]
        p["sets_won"] = int(p.get("sets_won", 0)) + set_wins_a
        p["sets_lost"] = int(p.get("sets_lost", 0)) + set_wins_b
        p["points_for"] = int(p.get("points_for", 0)) + total_a
        p["points_against"] = int(p.get("points_against", 0)) + total_b

    for uid in team_b_ids:
        p = players[uid]
        p["sets_won"] = int(p.get("sets_won", 0)) + set_wins_b
        p["sets_lost"] = int(p.get("sets_lost", 0)) + set_wins_a
        p["points_for"] = int(p.get("points_for", 0)) + total_b
        p["points_against"] = int(p.get("points_against", 0)) + total_a

def auto_rest_after_match(db, participant_ids):
    cd = suggested_cooldown_min(db)
    if cd <= 0:
        return
    t = now_ts()
    for uid in participant_ids:
        p = db["players"][uid]
        if p.get("auto_rest"):
            p["resting"] = True
            p["rest_until"] = t + (cd * 60)

# -----------------------------
# ROUTES
# -----------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/login", methods=["POST"])
def api_login():
    with db_lock():
        db = load_db()
        housekeeping(db)

        d = request.json or {}
        uid = d.get("userId")
        if not uid:
            return jsonify({"error": "Missing userId"}), 400

        if uid not in db["players"]:
            db["players"][uid] = {
                "id": uid,
                "nickname": d.get("displayName") or "User",
                "pictureUrl": d.get("pictureUrl") or "",
                "mmr": 1000,
                "status": "offline",
                "queue_since": None,
                "last_active": now_ts(),
                "resting": False,
                "rest_until": None,
                "auto_rest": False,

                "paired_with": None,
                "pair_outgoing": None,
                "pair_incoming": [],

                "current_court": None,
                "current_match_id": None,

                # calibration
                "calib_games": 0,
                "calib_wins": 0,
                "calib_win_streak": 0,

                # stats
                "sets_won": 0,
                "sets_lost": 0,
                "points_for": 0,
                "points_against": 0
            }
        else:
            # update latest identity
            db["players"][uid]["pictureUrl"] = d.get("pictureUrl") or db["players"][uid].get("pictureUrl", "")
            if d.get("displayName"):
                db["players"][uid]["nickname"] = d.get("displayName")

        db["players"][uid]["last_active"] = now_ts()

        role = player_role(uid, db)
        include_hidden = (role in ["super", "mod"])
        me = player_public_view(db["players"][uid], include_hidden_mmr=include_hidden)
        me["role"] = role

        # stats summary
        sw = int(db["players"][uid].get("sets_won", 0))
        sl = int(db["players"][uid].get("sets_lost", 0))
        total_sets = sw + sl
        wr = int(round((sw / total_sets) * 100)) if total_sets > 0 else 0

        me["stats"] = {
            "set_wins": sw,
            "set_losses": sl,
            "win_rate": wr,
            "points_for": int(db["players"][uid].get("points_for", 0)),
            "points_against": int(db["players"][uid].get("points_against", 0)),
        }

        save_db(db)
        return jsonify(me)

@app.route("/api/get_dashboard")
def get_dashboard():
    with db_lock():
        db = load_db()
        housekeeping(db)

        # build player lists
        t = now_ts()

        # expire called -> playing status on player too
        for cid, m in db["courts"].items():
            if m and m.get("status") == "playing":
                for uid in m["team_a_ids"] + m["team_b_ids"]:
                    p = db["players"].get(uid)
                    if p:
                        p["status"] = "playing"

        # queue: active & not resting
        queue = []
        for p in db["players"].values():
            if p.get("status") == "active" and not p.get("resting"):
                q0 = float(p.get("queue_since") or p.get("last_active") or t)
                wait_min = int((t - q0) // 60)
                pv = player_public_view(p, include_hidden_mmr=False)
                pv["wait_min"] = wait_min
                queue.append(pv)
        queue.sort(key=lambda x: float(db["players"][x["id"]].get("queue_since") or db["players"][x["id"]].get("last_active") or t))

        # all players minimal
        all_players = []
        for p in db["players"].values():
            pv = player_public_view(p, include_hidden_mmr=False)
            all_players.append(pv)

        # leaderboards
        def calc_wr(p):
            sw = int(p.get("sets_won", 0))
            sl = int(p.get("sets_lost", 0))
            total = sw + sl
            return int(round((sw / total) * 100)) if total > 0 else 0

        players_sorted_mmr = sorted(db["players"].values(), key=lambda p: int(p.get("mmr", 1000)), reverse=True)
        players_sorted_points = sorted(db["players"].values(), key=lambda p: int(p.get("points_for", 0)), reverse=True)
        players_sorted_wr = sorted(db["players"].values(), key=lambda p: calc_wr(p), reverse=True)

        def build_lb(src):
            ranked = []
            unranked = []
            for p in src:
                item = player_public_view(p, include_hidden_mmr=False)
                item["points_for"] = int(p.get("points_for", 0))
                item["points_against"] = int(p.get("points_against", 0))
                item["sets_won"] = int(p.get("sets_won", 0))
                item["sets_lost"] = int(p.get("sets_lost", 0))
                item["win_rate"] = calc_wr(p)
                if is_unrank(p):
                    unranked.append(item)
                else:
                    ranked.append(item)
            return ranked + unranked  # unranked always bottom

        leaderboard = {
            "mmr": build_lb(players_sorted_mmr),
            "points": build_lb(players_sorted_points),
            "winrate": build_lb(players_sorted_wr),
        }

        # courts view
        courts_view = {}
        for cid, m in db["courts"].items():
            if not m:
                courts_view[cid] = None
                continue

            # enrich
            start_at = float(m.get("start_at", 0))
            status = m.get("status", "called")
            actual_start = m.get("actual_start") or (start_at if status == "playing" else None)
            elapsed = int(max(0, t - float(actual_start))) if actual_start else 0
            countdown = int(max(0, start_at - t)) if status == "called" else 0

            def team_data(ids):
                out = []
                for uid in ids:
                    if uid in db["players"]:
                        out.append(player_public_view(db["players"][uid], include_hidden_mmr=False))
                return out

            courts_view[cid] = {
                "id": m.get("id"),
                "court_id": cid,
                "status": status,             # called / playing
                "countdown_sec": countdown,   # called stage
                "elapsed_sec": elapsed,       # playing stage
                "team_a_ids": m.get("team_a_ids", []),
                "team_b_ids": m.get("team_b_ids", []),
                "team_a": team_data(m.get("team_a_ids", [])),
                "team_b": team_data(m.get("team_b_ids", [])),
                "event_id": m.get("event_id"),
            }

        # match history (enriched snapshot already stored)
        history = db.get("match_history", [])[:30]

        # events list (latest 10)
        events = []
        for eid, e in db.get("events", {}).items():
            ev = dict(e)
            ev["id"] = eid
            events.append(ev)
        events.sort(key=lambda x: float(x.get("created_at", 0)), reverse=True)
        events = events[:10]

        # system
        sys = db["system_settings"]
        payload = {
            "system": {
                "total_courts": int(sys.get("total_courts", 2)),
                "is_session_active": bool(sys.get("is_session_active", False)),
                "current_event_id": sys.get("current_event_id"),
                "match_points": int(sys.get("match_points", 21)),
                "match_bo": int(sys.get("match_bo", 1)),
                "notify_enabled": bool(sys.get("notify_enabled", False)),
                "automatch": sys.get("automatch", {}),
                "suggested_cooldown_min": suggested_cooldown_min(db),
                "avg_match_min": compute_global_avg_match_minutes(db),
            },
            "courts": courts_view,
            "queue": queue,
            "queue_count": len(queue),
            "all_players": all_players,
            "leaderboard": leaderboard,
            "match_history": history,
            "events": events
        }

        save_db(db)
        return jsonify(payload)

# -----------------------------
# CHECK-IN / REST / AUTO REST
# -----------------------------
@app.route("/api/toggle_status", methods=["POST"])
def toggle_status():
    with db_lock():
        db = load_db()
        housekeeping(db)

        d = request.json or {}
        uid = d.get("userId")
        if uid not in db["players"]:
            return jsonify({"error": "User not found"}), 404

        if not db["system_settings"].get("is_session_active"):
            return jsonify({"error": "ยังไม่เริ่มก๊วน"}), 400

        p = db["players"][uid]
        cur = p.get("status", "offline")

        if cur in ["active", "resting"]:
            # checkout => reset queue time (ตามที่เตือน)
            p["status"] = "offline"
            p["resting"] = False
            p["rest_until"] = None
            p["queue_since"] = None
            p["pair_outgoing"] = None
            # pairing ไม่บังคับล้าง (แต่คุณจะยกเลิกเองได้)
        else:
            # checkin
            p["status"] = "active"
            if not p.get("queue_since"):
                p["queue_since"] = now_ts()
            p["last_active"] = now_ts()

            # add to current event roster
            eid = db["system_settings"].get("current_event_id")
            if eid and eid in db["events"]:
                roster = db["events"][eid].get("players", [])
                if uid not in roster:
                    roster.append(uid)
                db["events"][eid]["players"] = roster

        save_db(db)
        return jsonify({"success": True})

@app.route("/api/toggle_rest", methods=["POST"])
def toggle_rest():
    with db_lock():
        db = load_db()
        housekeeping(db)

        d = request.json or {}
        uid = d.get("userId")
        if uid not in db["players"]:
            return jsonify({"error": "User not found"}), 404

        p = db["players"][uid]
        if p.get("status") != "active":
            return jsonify({"error": "ต้อง Check-in ก่อน"}), 400

        p["resting"] = not bool(p.get("resting", False))
        if p["resting"]:
            p["rest_until"] = None  # manual rest (no timer)
        save_db(db)
        return jsonify({"success": True, "resting": p["resting"]})

@app.route("/api/toggle_auto_rest", methods=["POST"])
def toggle_auto_rest():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        if uid not in db["players"]:
            return jsonify({"error": "User not found"}), 404
        p = db["players"][uid]
        p["auto_rest"] = not bool(p.get("auto_rest", False))
        save_db(db)
        return jsonify({"success": True, "auto_rest": p["auto_rest"]})

# -----------------------------
# PAIR REQUEST API
# -----------------------------
@app.route("/api/pair/request", methods=["POST"])
def api_pair_request():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        tid = d.get("targetId")
        ok, err = pair_request_send(db, uid, tid)
        save_db(db)
        return jsonify({"success": ok, "error": err})

@app.route("/api/pair/cancel_outgoing", methods=["POST"])
def api_pair_cancel_outgoing():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        ok, err = pair_request_cancel_outgoing(db, uid)
        save_db(db)
        return jsonify({"success": ok, "error": err})

@app.route("/api/pair/accept", methods=["POST"])
def api_pair_accept():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        rid = d.get("requesterId")
        ok, err = pair_accept(db, uid, rid)
        save_db(db)
        return jsonify({"success": ok, "error": err})

@app.route("/api/pair/decline", methods=["POST"])
def api_pair_decline():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        rid = d.get("requesterId")
        ok, err = pair_decline(db, uid, rid)
        save_db(db)
        return jsonify({"success": ok, "error": err})

@app.route("/api/pair/cancel_pair", methods=["POST"])
def api_pair_cancel_pair():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        ok, err = pair_cancel_pair(db, uid)
        save_db(db)
        return jsonify({"success": ok, "error": err})

# -----------------------------
# SESSION / COURTS ADMIN
# -----------------------------
def is_staff(uid, db):
    return uid == SUPER_ADMIN_ID or uid in db.get("mod_ids", [])

@app.route("/api/admin/toggle_session", methods=["POST"])
def toggle_session():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}

        uid = d.get("userId")
        action = d.get("action")
        if not is_staff(uid, db):
            return jsonify({"error": "Unauthorized"}), 403

        if action == "start":
            # settings
            pts = int(d.get("match_points", db["system_settings"].get("match_points", 21)))
            bo = int(d.get("match_bo", db["system_settings"].get("match_bo", 1)))
            notify = bool(d.get("notify_enabled", False))
            name = d.get("event_name")

            if pts not in [11, 21]:
                pts = 21
            if bo not in [1, 2, 3]:
                bo = 1

            db["system_settings"]["is_session_active"] = True
            db["system_settings"]["match_points"] = pts
            db["system_settings"]["match_bo"] = bo
            db["system_settings"]["notify_enabled"] = notify

            # create new event/session
            eid = str(uuid.uuid4())[:8]
            today = datetime.now().strftime("%d/%m/%Y")
            db["events"][eid] = {
                "id": eid,
                "name": name or f"ก๊วน {today}",
                "created_at": now_ts(),
                "ended_at": None,
                "status": "active",
                "players": [],
                "settings": {
                    "match_points": pts,
                    "match_bo": bo,
                    "notify_enabled": notify
                }
            }
            db["system_settings"]["current_event_id"] = eid

        else:
            db["system_settings"]["is_session_active"] = False
            eid = db["system_settings"].get("current_event_id")
            db["system_settings"]["current_event_id"] = None

            # close event
            if eid and eid in db["events"]:
                db["events"][eid]["status"] = "closed"
                db["events"][eid]["ended_at"] = now_ts()

            # clear courts + players state
            for cid in list(db["courts"].keys()):
                db["courts"][cid] = None

            for p in db["players"].values():
                p["status"] = "offline"
                p["resting"] = False
                p["rest_until"] = None
                p["queue_since"] = None
                p["current_court"] = None
                p["current_match_id"] = None
                # pairing ไม่ล้างอัตโนมัติ (แต่จะยกเลิกเองได้)

        save_db(db)
        return jsonify({"success": True})

@app.route("/api/admin/update_courts", methods=["POST"])
def update_courts():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        if not is_staff(uid, db):
            return jsonify({"error": "Unauthorized"}), 403

        c = int(d.get("count", 2))
        c = max(1, min(10, c))
        db["system_settings"]["total_courts"] = c
        ensure_courts(db)
        save_db(db)
        return jsonify({"success": True})

@app.route("/api/court/automatch_toggle", methods=["POST"])
def automatch_toggle():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        if not is_staff(uid, db):
            return jsonify({"error": "Unauthorized"}), 403

        cid = str(d.get("courtId"))
        val = bool(d.get("enabled", False))
        ensure_courts(db)
        if cid not in db["system_settings"]["automatch"]:
            return jsonify({"error": "Court not found"}), 404

        db["system_settings"]["automatch"][cid] = val
        save_db(db)
        return jsonify({"success": True})

@app.route("/api/admin/manage_mod", methods=["POST"])
def manage_mod():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        if d.get("requesterId") != SUPER_ADMIN_ID:
            return jsonify({"error": "Super Admin Only"}), 403
        tid = d.get("targetUserId")
        action = d.get("action")
        if not tid:
            return jsonify({"error": "Missing target"}), 400
        if action == "promote":
            if tid not in db["mod_ids"]:
                db["mod_ids"].append(tid)
        else:
            if tid in db["mod_ids"]:
                db["mod_ids"].remove(tid)
        save_db(db)
        return jsonify({"success": True})

@app.route("/api/admin/set_mmr", methods=["POST"])
def set_mmr():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        if not is_staff(uid, db):
            return jsonify({"error": "Unauthorized"}), 403

        target = d.get("targetUserId")
        new_mmr = d.get("newMmr")
        if target not in db["players"]:
            return jsonify({"error": "Not found"}), 404
        try:
            db["players"][target]["mmr"] = int(new_mmr)
        except:
            return jsonify({"error": "Invalid mmr"}), 400

        save_db(db)
        return jsonify({"success": True})

# -----------------------------
# MANUAL & AUTO MATCHMAKE REQUESTS
# -----------------------------
@app.route("/api/matchmake", methods=["POST"])
def matchmake_request():
    """
    ปุ่ม "จับคู่อัตโนมัติ" (manual request):
    - จะพยายามเติมคอร์ทที่ automatch=off ที่ว่างก่อน
    - ถ้า automatch ปิดทุกสนาม ก็ทำหน้าที่ request ให้ลงสนามว่าง
    """
    with db_lock():
        db = load_db()
        housekeeping(db)

        d = request.json or {}
        uid = d.get("userId")
        if not is_staff(uid, db):
            return jsonify({"error": "Unauthorized"}), 403

        ensure_courts(db)
        # pick idle courts with automatch off first
        candidates = []
        for cid in sorted(db["courts"].keys(), key=lambda x: int(x)):
            if db["courts"][cid] is None and not db["system_settings"]["automatch"].get(cid, False):
                candidates.append(cid)
        if not candidates:
            # any idle court
            for cid in sorted(db["courts"].keys(), key=lambda x: int(x)):
                if db["courts"][cid] is None:
                    candidates.append(cid)

        if not candidates:
            save_db(db)
            return jsonify({"status": "full"})

        ok, res = create_match_on_court(db, candidates[0], initiated_by="manual")
        save_db(db)
        if not ok:
            return jsonify({"status": "waiting", "error": res})
        return jsonify({"status": "matched", "match": res})

@app.route("/api/matchmake/manual", methods=["POST"])
def manual_matchmake():
    with db_lock():
        db = load_db()
        housekeeping(db)
        d = request.json or {}
        uid = d.get("userId")
        if not is_staff(uid, db):
            return jsonify({"error": "Unauthorized"}), 403

        cid = str(d.get("courtId"))
        p_ids = d.get("playerIds") or []
        if len(p_ids) != 4:
            return jsonify({"error": "Need 4 players"}), 400

        ensure_courts(db)
        if db["courts"].get(cid) is not None:
            return jsonify({"error": "Court busy"}), 400

        # validate all players eligible-ish (allow resting? no)
        for pid in p_ids:
            if pid not in db["players"]:
                return jsonify({"error": "Player not found"}), 404
            if db["players"][pid].get("status") != "active":
                return jsonify({"error": "Players must be active"}), 400
            if db["players"][pid].get("resting"):
                return jsonify({"error": "Player is resting"}), 400
            if db["players"][pid].get("current_match_id"):
                return jsonify({"error": "Player busy"}), 400

        # form teams: first 2 A, last 2 B
        team_a = [p_ids[0], p_ids[1]]
        team_b = [p_ids[2], p_ids[3]]

        mid = str(uuid.uuid4())[:8]
        t = now_ts()
        m = {
            "id": mid,
            "event_id": db["system_settings"].get("current_event_id"),
            "court_id": cid,
            "team_a_ids": team_a,
            "team_b_ids": team_b,
            "created_at": t,
            "start_at": t + 60,
            "actual_start": None,
            "status": "called",
            "initiated_by": "manual_admin",
        }
        db["courts"][cid] = m

        for pid in team_a + team_b:
            p = db["players"][pid]
            p["current_court"] = cid
            p["current_match_id"] = mid
            p["status"] = "called"

        # roster
        eid = db["system_settings"].get("current_event_id")
        if eid and eid in db["events"]:
            roster = db["events"][eid].get("players", [])
            for pid in team_a + team_b:
                if pid not in roster:
                    roster.append(pid)
            db["events"][eid]["players"] = roster

        save_db(db)
        return jsonify({"success": True})

# -----------------------------
# CANCEL MATCH (no mmr, no history)
# -----------------------------
@app.route("/api/match/cancel", methods=["POST"])
def cancel_match():
    with db_lock():
        db = load_db()
        housekeeping(db)

        d = request.json or {}
        uid = d.get("userId")
        cid = str(d.get("courtId"))

        if not is_staff(uid, db):
            return jsonify({"error": "Unauthorized"}), 403

        m = db["courts"].get(cid)
        if not m:
            return jsonify({"error": "No match"}), 400

        # record signature to avoid same match again
        sigA = tuple(sorted(m.get("team_a_ids", [])))
        sigB = tuple(sorted(m.get("team_b_ids", [])))
        sig = tuple(sorted([sigA, sigB]))
        recent = db.get("recent_match_signatures", []) or []
        recent.append({"sig": sig, "ts": now_ts()})
        db["recent_match_signatures"] = recent

        # free players
        for pid in m.get("team_a_ids", []) + m.get("team_b_ids", []):
            p = db["players"].get(pid)
            if p:
                p["current_court"] = None
                p["current_match_id"] = None
                # back to active (keep queue_since)
                if p.get("status") in ["called", "playing"]:
                    p["status"] = "active"

        db["courts"][cid] = None

        save_db(db)
        return jsonify({"success": True})

# -----------------------------
# SUBMIT RESULT (score-aware, set-based stats)
# -----------------------------
@app.route("/api/submit_result", methods=["POST"])
def submit_result():
    with db_lock():
        db = load_db()
        housekeeping(db)

        d = request.json or {}
        uid = d.get("userId")
        cid = str(d.get("courtId"))

        m = db["courts"].get(cid)
        if not m:
            return jsonify({"error": "No match"}), 400

        team_a_ids = m.get("team_a_ids", [])
        team_b_ids = m.get("team_b_ids", [])

        is_mod = is_staff(uid, db)
        is_player = (uid in team_a_ids) or (uid in team_b_ids)
        if not (is_mod or is_player):
            return jsonify({"error": "Unauthorized"}), 403

        # parse sets
        sets = d.get("sets") or []
        bo = int(db["system_settings"].get("match_bo", 1))
        target = int(db["system_settings"].get("match_points", 21))

        if len(sets) != bo:
            return jsonify({"error": f"ต้องกรอกคะแนน {bo} เซต"}), 400

        # validate each set
        norm_sets = []
        for s in sets:
            a = s.get("a")
            b = s.get("b")
            ok, err = validate_set_score(a, b, target)
            if not ok:
                return jsonify({"error": err}), 400
            norm_sets.append({"a": int(a), "b": int(b)})

        winner, set_wins_a, set_wins_b, total_a, total_b = decide_winner_from_sets(norm_sets, bo, target)
        point_diff = (total_a - total_b) if winner == "A" else (total_b - total_a)

        # duration
        t = now_ts()
        actual_start = m.get("actual_start") or m.get("start_at") or m.get("created_at") or t
        duration_min = int(math.ceil(max(0.0, t - float(actual_start)) / 60.0))

        # apply mmr changes
        mmr_snapshot = apply_mmr(
            db=db,
            team_a_ids=team_a_ids,
            team_b_ids=team_b_ids,
            winner=winner,
            point_diff=point_diff,
            target_points=target,
            bo=bo
        )

        # update stats (set-based + points)
        update_stats(db, team_a_ids, team_b_ids, norm_sets, set_wins_a, set_wins_b, total_a, total_b)

        # calibration increments
        update_calibration(db, team_a_ids, team_b_ids, winner)

        # history snapshot (store player info at that moment)
        def snap(uid_):
            p = db["players"].get(uid_)
            return {
                "id": uid_,
                "nickname": p.get("nickname", "User") if p else "User",
                "pictureUrl": p.get("pictureUrl", "") if p else "",
                "mmr": int(p.get("mmr", 1000)) if p else 1000,
                "calib_games": int(p.get("calib_games", 0)) if p else 0,
                "unrank": is_unrank(p) if p else True,
                "rank_title": (f"Unrank ({int(p.get('calib_games',0))}/10)" if p and is_unrank(p) else get_rank_title(int(p.get("mmr", 1000))) if p else "Unrank (0/10)")
            }

        hist = {
            "id": str(uuid.uuid4())[:8],
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "event_id": m.get("event_id"),
            "court_id": cid,
            "winner_team": winner,  # "A" or "B"
            "sets": norm_sets,
            "set_wins_a": set_wins_a,
            "set_wins_b": set_wins_b,
            "total_points_a": total_a,
            "total_points_b": total_b,
            "duration_min": duration_min,
            "mmr_snapshot": mmr_snapshot,
            "team_a": [snap(x) for x in team_a_ids],
            "team_b": [snap(x) for x in team_b_ids],
            "score_summary": f"Sets {set_wins_a}-{set_wins_b} | Points {total_a}-{total_b}"
        }
        db["match_history"].insert(0, hist)

        # recent signature to avoid immediate repeat
        sigA = tuple(sorted(team_a_ids))
        sigB = tuple(sorted(team_b_ids))
        sig = tuple(sorted([sigA, sigB]))
        recent = db.get("recent_match_signatures", []) or []
        recent.append({"sig": sig, "ts": now_ts()})
        db["recent_match_signatures"] = recent

        # free court
        db["courts"][cid] = None

        # restore players status -> active, optional auto-rest
        participants = team_a_ids + team_b_ids
        for pid in participants:
            p = db["players"].get(pid)
            if not p:
                continue
            p["current_court"] = None
            p["current_match_id"] = None
            # back to active
            p["status"] = "active"
            p["last_active"] = now_ts()

        auto_rest_after_match(db, participants)

        save_db(db)
        return jsonify({"success": True, "winner": winner})

# -----------------------------
# PROFILE VIEW
# -----------------------------
@app.route("/api/player/profile/<uid>")
def player_profile(uid):
    with db_lock():
        db = load_db()
        housekeeping(db)

        if uid not in db["players"]:
            return jsonify({"error": "Not found"}), 404

        p = db["players"][uid]
        # last 10 matches include this user
        last = []
        for m in db.get("match_history", []):
            a_ids = [x["id"] for x in m.get("team_a", [])]
            b_ids = [x["id"] for x in m.get("team_b", [])]
            if uid in a_ids or uid in b_ids:
                last.append(m)
            if len(last) >= 10:
                break

        sw = int(p.get("sets_won", 0))
        sl = int(p.get("sets_lost", 0))
        total_sets = sw + sl
        wr = int(round((sw / total_sets) * 100)) if total_sets > 0 else 0

        out = player_public_view(p, include_hidden_mmr=False)
        out["stats"] = {
            "set_wins": sw,
            "set_losses": sl,
            "win_rate": wr,
            "points_for": int(p.get("points_for", 0)),
            "points_against": int(p.get("points_against", 0)),
        }
        out["last_10"] = last
        save_db(db)
        return jsonify(out)

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    # Render/Gunicorn จะไม่ใช้ส่วนนี้ แต่รัน local ได้
    app.run(host="0.0.0.0", port=5000, debug=True)
