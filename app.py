# app.py
import json
import os
import time
import uuid
import math
import sys
from datetime import datetime
from flask import Flask, request, jsonify, render_template
import fcntl

app = Flask(__name__)

# --- CONFIG ---
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"
DATA_FILE = "/var/data/izesquad_data.json"

# --- DEFAULT DB ---
default_db = {
    "system_settings": {
        "total_courts": 2,
        "is_session_active": False,
        "current_event_id": None,
        "session_config": {
            "target_points": 21,       # 11 or 21
            "bo": 1,                   # 1,2,3 (bo2 plays 2 sets always and winner can be decided by total points if 1-1)
            "enable_notifications": False
        },
        "auto_match_courts": {},       # {"1": true, "2": false, ...}
        "suggested_cooldown_min": 0
    },
    "mod_ids": [],
    "players": {},
    "events": {},
    "match_history": [],
    "recent_avoids": [],              # [{"pair":[id1,id2], "until":ts}, ...] avoids teammate pairing for a while
    "courts_state": {}                # {"1": match_obj_or_null, "2": null, ...}
}

def now_ts() -> float:
    return time.time()

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stdout)
    sys.stdout.flush()

# --- FILE LOCKED IO ---
def _ensure_file():
    directory = os.path.dirname(DATA_FILE)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(default_db, f, ensure_ascii=False, indent=4)

def get_db():
    _ensure_file()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
    except Exception as e:
        log(f"DB read error: {e}")
        data = json.loads(json.dumps(default_db))

    # Backfill top-level keys
    for k, v in default_db.items():
        if k not in data:
            data[k] = json.loads(json.dumps(v))

    # Backfill system_settings keys
    ss = data.get("system_settings", {})
    for k, v in default_db["system_settings"].items():
        if k not in ss:
            ss[k] = json.loads(json.dumps(v))
    if "session_config" not in ss:
        ss["session_config"] = json.loads(json.dumps(default_db["system_settings"]["session_config"]))
    for k, v in default_db["system_settings"]["session_config"].items():
        if k not in ss["session_config"]:
            ss["session_config"][k] = v
    if "auto_match_courts" not in ss or not isinstance(ss["auto_match_courts"], dict):
        ss["auto_match_courts"] = {}
    if "suggested_cooldown_min" not in ss:
        ss["suggested_cooldown_min"] = 0
    data["system_settings"] = ss

    # Refresh courts containers to ensure keys exist
    _refresh_courts_storage(data)

    # --- MIGRATION / BACKFILL PLAYERS (fix "rank disappeared" for old data) ---
    players = data.get("players", {})
    for uid, p in players.items():
        p.setdefault("id", uid)
        p.setdefault("nickname", "User")
        p.setdefault("pictureUrl", "")
        p.setdefault("mmr", 1000)
        # If old players had no calibrate, assume already ranked
        if "calibrate_games" not in p:
            p["calibrate_games"] = 10
        p.setdefault("calibrate_wins", 0)
        p.setdefault("calibrate_losses", 0)

        p.setdefault("status", "offline")               # offline/active/playing
        p.setdefault("last_active", now_ts())
        p.setdefault("queue_join_time", None)           # when they checked-in (used for waiting time)
        p.setdefault("resting", False)
        p.setdefault("rest_until", None)                # if set, resting=True until this time
        p.setdefault("auto_rest", False)                # player opt-in
        # pairing request system
        p.setdefault("outgoing_request_to", None)        # requester -> target
        p.setdefault("incoming_requests", [])            # list of requester ids
        p.setdefault("pair_lock", None)                  # accepted pair partner id

        # stats (set-based)
        p.setdefault("sets_w", 0)
        p.setdefault("sets_l", 0)
        p.setdefault("pts_for", 0)
        p.setdefault("pts_against", 0)
    data["players"] = players

    # Backfill mod_ids
    if "mod_ids" not in data or not isinstance(data["mod_ids"], list):
        data["mod_ids"] = []

    # Backfill lists
    if "match_history" not in data or not isinstance(data["match_history"], list):
        data["match_history"] = []
    if "recent_avoids" not in data or not isinstance(data["recent_avoids"], list):
        data["recent_avoids"] = []

    return data

def save_db(data):
    _ensure_file()
    try:
        with open(DATA_FILE, "r+", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.seek(0)
            f.truncate()
            json.dump(data, f, ensure_ascii=False, indent=4)
            f.flush()
            os.fsync(f.fileno())
            fcntl.flock(f, fcntl.LOCK_UN)
    except Exception as e:
        log(f"CRITICAL DB save error: {e}")

def _refresh_courts_storage(db):
    total = int(db["system_settings"].get("total_courts", 2) or 2)
    # courts_state
    cs = db.get("courts_state", {})
    if not isinstance(cs, dict):
        cs = {}
    for i in range(1, total + 1):
        cs.setdefault(str(i), None)
    for k in list(cs.keys()):
        try:
            if int(k) > total:
                del cs[k]
        except:
            pass
    db["courts_state"] = cs
    # auto_match_courts
    am = db["system_settings"].get("auto_match_courts", {})
    if not isinstance(am, dict):
        am = {}
    for i in range(1, total + 1):
        am.setdefault(str(i), False)
    for k in list(am.keys()):
        try:
            if int(k) > total:
                del am[k]
        except:
            pass
    db["system_settings"]["auto_match_courts"] = am

# --- RANK / BADGE (Set A + T1–T9 with T8 secondary, T9 error) ---
def _is_unranked(p) -> bool:
    return int(p.get("calibrate_games", 0) or 0) < 10

def rank_title_set_a(mmr: int) -> str:
    if mmr <= 899: return "ลูกเจี๊ยบหลุดคอร์ท 🐣"
    if mmr <= 999: return "มือใหม่ใจเกิน 💫"
    if mmr <= 1099: return "ตีได้…แต่มั่วอยู่ 😵‍💫"
    if mmr <= 1199: return "พอรู้เรื่องละนะ 😏"
    if mmr <= 1299: return "สายวางลูก 🎯"
    if mmr <= 1399: return "ตบดังแต่ไม่ลง 🔨"
    if mmr <= 1499: return "คุมเกมนิ่ง ๆ 🧠"
    if mmr <= 1599: return "ขาไว ไม้ไว ⚡"
    if mmr <= 1699: return "นักแก้ทาง 🧩"
    if mmr <= 1799: return "ตัวเปิดเกมของก๊วน 🚀"
    if mmr <= 1899: return "แบกได้ (นิดนึง) 🫠"
    if mmr <= 1999: return "โปรก๊วนประจำวัน 👑"
    if mmr <= 2149: return "จอมยุทธสายตบ 🐉"
    if mmr <= 2299: return "บอสประจำสนาม 🧨"
    if mmr <= 2499: return "ตำนานเล่าขาน 📜"
    return "เทพเจ้าก๊วนแบด ⚜️"

def get_rank_badges(p):
    """
    Returns:
      - primary badge: {text, cls}
      - optional secondary badge (for UNRANK): {text, cls}
    """
    cg = int(p.get("calibrate_games", 0) or 0)
    if cg < 10:
        return (
            {"text": "UNRANK", "cls": "badge-ghost"},
            {"text": f"{cg}/10", "cls": "badge-outline"}
        )

    mmr = int(p.get("mmr", 1000) or 1000)

    # Tiers T1–T9 (T8: 1600–1799 secondary, T9: 1800+ error)
    if mmr <= 899:
        return ({"text": "T1 🐣", "cls": "badge-neutral"}, None)
    if mmr <= 999:
        return ({"text": "T2 💫", "cls": "badge-info"}, None)
    if mmr <= 1099:
        return ({"text": "T3 😵‍💫", "cls": "badge-ghost"}, None)
    if mmr <= 1199:
        return ({"text": "T4 😏", "cls": "badge-success"}, None)
    if mmr <= 1299:
        return ({"text": "T5 🎯", "cls": "badge-primary"}, None)
    if mmr <= 1399:
        return ({"text": "T6 🔨", "cls": "badge-warning"}, None)
    if mmr <= 1599:
        # T7 (accent), name changes are handled by title
        return ({"text": "T7 🧠", "cls": "badge-accent"}, None)
    if mmr <= 1799:
        # T8 secondary
        return ({"text": "T8 🚀", "cls": "badge-secondary"}, None)
    # T9 error
    return ({"text": "T9 👑", "cls": "badge-error"}, None)

def mmr_display(p):
    # Hide mmr everywhere for unranked
    cg = int(p.get("calibrate_games", 0) or 0)
    if cg < 10:
        return f"UNRANK ({cg}/10)"
    return str(int(p.get("mmr", 1000) or 1000))

def progress_info(p):
    cg = int(p.get("calibrate_games", 0) or 0)
    if cg < 10:
        pct = int(round((cg / 10) * 100))
        return {
            "mode": "calibrate",
            "min": 0, "max": 10,
            "value": cg,
            "pct": pct,
            "hint": "Calibrating"
        }
    mmr = int(p.get("mmr", 1000) or 1000)
    base = (mmr // 100) * 100
    seg_min = base
    seg_max = base + 99
    pct = int(round(((mmr - seg_min) / 99) * 100))
    hint = ""
    if pct >= 90:
        hint = "ใกล้อัป ⬆️"
    elif pct <= 10:
        hint = "เสี่ยงตก ⬇️"
    return {
        "mode": "mmr100",
        "min": seg_min, "max": seg_max,
        "value": mmr,
        "pct": max(0, min(100, pct)),
        "hint": hint
    }

def winrate_percent(p):
    w = int(p.get("sets_w", 0) or 0)
    l = int(p.get("sets_l", 0) or 0)
    total = w + l
    if total <= 0:
        return None
    return int(round((w / total) * 100))

def get_wr_badge(p):
    wr = winrate_percent(p)
    if wr is None:
        return {"text": "WR - 🙂", "cls": "badge-ghost"}
    if wr < 40:
        return {"text": f"WR {wr}% 😵‍💫", "cls": "badge-error"}
    if wr < 60:
        return {"text": f"WR {wr}% 🙂", "cls": "badge-neutral"}
    return {"text": f"WR {wr}% 🔥", "cls": "badge-success"}

# --- SESSION HELPERS ---
def is_staff(uid, db):
    return uid == SUPER_ADMIN_ID or uid in db.get("mod_ids", [])

def _current_event(db):
    eid = db["system_settings"].get("current_event_id")
    if eid and eid in db["events"]:
        return db["events"][eid]
    return None

def _ensure_participant(db, uid):
    evt = _current_event(db)
    if not evt:
        return
    evt.setdefault("participants", [])
    if uid not in evt["participants"]:
        evt["participants"].append(uid)

def _cleanup_resting(db):
    now = now_ts()
    for p in db["players"].values():
        if p.get("resting") and p.get("rest_until"):
            try:
                if now >= float(p["rest_until"]):
                    p["resting"] = False
                    p["rest_until"] = None
            except:
                p["resting"] = False
                p["rest_until"] = None

def _compute_suggested_cooldown_min(db):
    # avg duration of last 10 matches (minutes)
    hist = [m for m in db.get("match_history", []) if m.get("duration_sec") is not None and not m.get("cancelled")]
    hist = hist[:10]
    if len(hist) == 0:
        avg_min = 12
    else:
        avg_min = max(5, int(round(sum([h.get("duration_sec", 0) for h in hist]) / len(hist) / 60)))
    total_courts = int(db["system_settings"].get("total_courts", 2) or 2)
    active_count = len([p for p in db["players"].values() if p.get("status") == "active"])
    ratio = active_count / max(1, total_courts * 4)
    # fewer players -> more likely to repeat -> larger cooldown suggestion
    cooldown = int(round(avg_min * max(0.0, 1.2 - ratio)))
    cooldown = max(0, min(avg_min, cooldown))
    db["system_settings"]["suggested_cooldown_min"] = cooldown

# --- PAIR REQUEST RULES ---
def _cancel_outgoing(db, uid):
    p = db["players"].get(uid)
    if not p:
        return
    target = p.get("outgoing_request_to")
    if target and target in db["players"]:
        tr = db["players"][target]
        if uid in tr.get("incoming_requests", []):
            tr["incoming_requests"].remove(uid)
    p["outgoing_request_to"] = None

def _break_pair(db, uid):
    p = db["players"].get(uid)
    if not p:
        return
    partner = p.get("pair_lock")
    if partner and partner in db["players"]:
        db["players"][partner]["pair_lock"] = None
    p["pair_lock"] = None

def _clear_pairing_state_on_offline(db, uid):
    p = db["players"].get(uid)
    if not p:
        return
    # break pair + cancel outgoing
    _break_pair(db, uid)
    _cancel_outgoing(db, uid)
    # remove this uid from others' incoming lists (optional: keep requests even if offline? better remove)
    for op in db["players"].values():
        if uid in op.get("incoming_requests", []):
            op["incoming_requests"].remove(uid)

# --- MATCHMAKING ---
def _eligible_queue(db):
    _cleanup_resting(db)
    now = now_ts()
    players = []
    for p in db["players"].values():
        if p.get("status") != "active":
            continue
        # rest exclude
        if p.get("resting"):
            continue
        qjt = p.get("queue_join_time")
        if not qjt:
            p["queue_join_time"] = now
            qjt = now
        players.append(p)
    players.sort(key=lambda x: float(x.get("queue_join_time", now)))
    return players

def _effective_mmr_for_matchmaking(p):
    # During calibrate, push effective mmr up/down faster based on results to match harder/easier opponents quickly
    mmr = int(p.get("mmr", 1000) or 1000)
    cg = int(p.get("calibrate_games", 0) or 0)
    if cg >= 10:
        return mmr
    w = int(p.get("calibrate_wins", 0) or 0)
    l = int(p.get("calibrate_losses", 0) or 0)
    # small but meaningful spread
    bump = (w - l) * 80
    return mmr + bump

def _avoid_pairs(db):
    now = now_ts()
    cleaned = []
    blocked = set()
    for item in db.get("recent_avoids", []):
        try:
            until = float(item.get("until", 0))
            pair = item.get("pair", [])
            if until > now and isinstance(pair, list) and len(pair) == 2:
                a, b = pair[0], pair[1]
                key = tuple(sorted([a, b]))
                blocked.add(key)
                cleaned.append(item)
        except:
            continue
    db["recent_avoids"] = cleaned
    return blocked

def _pair_constraints_ok(group):
    # If someone is pair_locked, partner must be included
    ids = set([p["id"] for p in group])
    for p in group:
        pl = p.get("pair_lock")
        if pl and pl not in ids:
            return False
    return True

def _team_splits_for_four(players4):
    # returns list of (teamA, teamB) where each is list of 2 players
    a,b,c,d = players4
    return [
        ([a,b],[c,d]),
        ([a,c],[b,d]),
        ([a,d],[b,c]),
    ]

def _respects_pair_lock(teamA, teamB):
    # If any player has pair_lock, they must be on same team as partner
    idsA = set([p["id"] for p in teamA])
    idsB = set([p["id"] for p in teamB])
    for p in teamA + teamB:
        partner = p.get("pair_lock")
        if partner:
            if (p["id"] in idsA and partner not in idsA) or (p["id"] in idsB and partner not in idsB):
                return False
    return True

def _score_split(teamA, teamB, waits, avoid_blocked):
    # objective: waiting time first (prefer larger total waiting), then fairness:
    # - team sum diff small
    # - within-team diff small (avoid carry)
    # - large within-team diff gets extra penalty unless unavoidable
    mmrA = sum([_effective_mmr_for_matchmaking(p) for p in teamA])
    mmrB = sum([_effective_mmr_for_matchmaking(p) for p in teamB])
    sum_diff = abs(mmrA - mmrB)

    within = abs(_effective_mmr_for_matchmaking(teamA[0]) - _effective_mmr_for_matchmaking(teamA[1])) + \
             abs(_effective_mmr_for_matchmaking(teamB[0]) - _effective_mmr_for_matchmaking(teamB[1]))
    max_within = max(
        abs(_effective_mmr_for_matchmaking(teamA[0]) - _effective_mmr_for_matchmaking(teamA[1])),
        abs(_effective_mmr_for_matchmaking(teamB[0]) - _effective_mmr_for_matchmaking(teamB[1]))
    )

    # avoid repeating same teammate pair (after cancel)
    penalty_avoid = 0
    pairA = tuple(sorted([teamA[0]["id"], teamA[1]["id"]]))
    pairB = tuple(sorted([teamB[0]["id"], teamB[1]["id"]]))
    if pairA in avoid_blocked:
        penalty_avoid += 5000
    if pairB in avoid_blocked:
        penalty_avoid += 5000

    # special unfairness: 2000+200 vs 1100+1100 -> within too large
    carry_penalty = 0
    if max_within >= 700:   # soft rule
        carry_penalty += (max_within - 700) * 3

    # waiting priority: bigger waits => lower score
    wait_sum = sum(waits.values())

    score = (-1.0 * wait_sum) + (2.0 * sum_diff) + (1.2 * within) + (1.5 * max_within) + penalty_avoid + carry_penalty
    return score, {"sum_diff": sum_diff, "within": within, "max_within": max_within}

def _pick_best_match(db, pool, must_include_id=None):
    avoid_blocked = _avoid_pairs(db)
    now = now_ts()

    # ensure waiting values
    waits = {}
    for p in pool:
        qjt = float(p.get("queue_join_time", now))
        waits[p["id"]] = max(0.0, now - qjt)

    # generate candidate groups of 4
    best = None
    best_meta = None

    ids = [p["id"] for p in pool]
    # brute force combos of 4 (pool small: <= 10)
    n = len(pool)
    for i in range(n):
        for j in range(i+1, n):
            for k in range(j+1, n):
                for l in range(k+1, n):
                    group = [pool[i], pool[j], pool[k], pool[l]]
                    gids = set([p["id"] for p in group])
                    if must_include_id and must_include_id not in gids:
                        continue
                    if not _pair_constraints_ok(group):
                        continue

                    # Evaluate best split respecting pair locks
                    for teamA, teamB in _team_splits_for_four(group):
                        if not _respects_pair_lock(teamA, teamB):
                            continue
                        score, meta = _score_split(teamA, teamB, {pid: waits[pid] for pid in gids}, avoid_blocked)
                        if best is None or score < best:
                            best = score
                            best_meta = {"teamA": teamA, "teamB": teamB, "score": score, "meta": meta}
    return best_meta

def _create_match_on_court(db, court_id: str, source: str = "auto"):
    if not db["system_settings"].get("is_session_active"):
        return {"ok": False, "reason": "session_inactive"}

    cs = db["courts_state"]
    if cs.get(court_id):
        return {"ok": False, "reason": "court_full"}

    queue = _eligible_queue(db)
    if len(queue) < 4:
        return {"ok": False, "reason": "not_enough_players"}

    # waiting priority: must include oldest
    oldest = queue[0]["id"]
    pool = queue[:min(10, len(queue))]

    best = _pick_best_match(db, pool, must_include_id=oldest)
    if not best:
        # fallback: just take oldest 4 with simple split
        group = pool[:4]
        teamA, teamB = group[:2], group[2:]
    else:
        teamA, teamB = best["teamA"], best["teamB"]

    match_id = str(uuid.uuid4())[:8]
    countdown_start = now_ts()
    scheduled_start = countdown_start + 60  # 1 minute to "ลงสนาม"

    match = {
        "match_id": match_id,
        "state": "countdown",         # countdown -> playing
        "source": source,
        "created_at": countdown_start,
        "countdown_start": countdown_start,
        "scheduled_start": scheduled_start,
        "start_time": None,
        "team_a_ids": [p["id"] for p in teamA],
        "team_b_ids": [p["id"] for p in teamB],
        "team_a": [p.get("nickname", "") for p in teamA],
        "team_b": [p.get("nickname", "") for p in teamB],
    }

    # set players to playing (but timer starts after countdown)
    for p in teamA + teamB:
        uid = p["id"]
        db["players"][uid]["status"] = "playing"
        # ensure participant list
        _ensure_participant(db, uid)

    cs[court_id] = match
    db["courts_state"] = cs
    return {"ok": True, "match_id": match_id, "court_id": court_id}

def _tick_courts(db):
    """
    - advance countdown -> playing
    - auto-match free courts if enabled
    """
    _refresh_courts_storage(db)
    _cleanup_resting(db)
    _compute_suggested_cooldown_min(db)

    now = now_ts()
    changed = False

    # advance countdown
    for cid, m in db["courts_state"].items():
        if not m:
            continue
        if m.get("state") == "countdown":
            if now >= float(m.get("scheduled_start", now)):
                m["state"] = "playing"
                m["start_time"] = float(m.get("scheduled_start", now))
                db["courts_state"][cid] = m
                changed = True

    # auto-match on free courts
    if db["system_settings"].get("is_session_active"):
        for cid in sorted(db["courts_state"].keys(), key=lambda x: int(x)):
            if db["courts_state"].get(cid) is None and db["system_settings"]["auto_match_courts"].get(cid):
                # try to create match
                res = _create_match_on_court(db, cid, source="auto")
                if res.get("ok"):
                    changed = True

    return changed

# --- RESULT / MMR ---
def _elo_expected(rA, rB):
    return 1.0 / (1.0 + 10 ** ((rB - rA) / 400.0))

def _score_factor_from_sets(cfg, sets, winner_team):
    # incorporates set diff + point diff as tie-breaker influence, but kept moderate
    target = int(cfg.get("target_points", 21) or 21)
    a_sets = 0
    b_sets = 0
    a_pts = 0
    b_pts = 0
    for s in sets:
        a = int(s.get("a", 0) or 0)
        b = int(s.get("b", 0) or 0)
        a_pts += a
        b_pts += b
        if a > b:
            a_sets += 1
        else:
            b_sets += 1
    set_diff = abs(a_sets - b_sets)
    pt_diff = abs(a_pts - b_pts)

    # base factor
    g = 1.0 + 0.12 * set_diff + 0.08 * (pt_diff / max(1, target))
    # keep sane
    g = max(0.80, min(1.60, g))
    return g, {"a_sets": a_sets, "b_sets": b_sets, "a_pts": a_pts, "b_pts": b_pts}

def _validate_set_score(cfg, a, b):
    """
    Valid badminton:
    - winner must reach target_points (11 or 21)
    - win by 2 unless reaches 30 cap
    - cap at 30 (30-x) allowed
    """
    target = int(cfg.get("target_points", 21) or 21)
    cap = 30
    a = int(a); b = int(b)
    if a < 0 or b < 0:
        return False, "คะแนนติดลบไม่ได้"
    if a == b:
        return False, "คะแนนห้ามเท่ากัน"
    hi = max(a, b)
    lo = min(a, b)
    if hi > cap:
        return False, "แต้มเกิน 30 ไม่ได้"
    if hi < target:
        return False, f"ผู้ชนะต้องถึง {target} แต้ม"
    if hi == cap:
        # cap reached -> always allowed if winner reached 30
        return True, None
    # hi >= target and hi < cap: must lead by 2
    if (hi - lo) < 2:
        return False, "ต้องชนะห่าง 2 แต้ม (ยกเว้นถึง 30)"
    return True, None

def _winner_from_sets(cfg, sets):
    bo = int(cfg.get("bo", 1) or 1)
    a_sets = 0
    b_sets = 0
    a_pts = 0
    b_pts = 0
    for s in sets:
        a = int(s.get("a", 0) or 0)
        b = int(s.get("b", 0) or 0)
        a_pts += a
        b_pts += b
        if a > b:
            a_sets += 1
        else:
            b_sets += 1

    if bo == 1:
        return ("A" if a_sets > b_sets else "B"), {"a_sets": a_sets, "b_sets": b_sets, "a_pts": a_pts, "b_pts": b_pts}
    if bo == 3:
        return ("A" if a_sets >= 2 else "B"), {"a_sets": a_sets, "b_sets": b_sets, "a_pts": a_pts, "b_pts": b_pts}

    # bo2: play 2 sets always. If tie 1-1, decide by total points
    if a_sets != b_sets:
        return ("A" if a_sets > b_sets else "B"), {"a_sets": a_sets, "b_sets": b_sets, "a_pts": a_pts, "b_pts": b_pts}
    # tie sets -> total points
    if a_pts != b_pts:
        return ("A" if a_pts > b_pts else "B"), {"a_sets": a_sets, "b_sets": b_sets, "a_pts": a_pts, "b_pts": b_pts}
    # rare tie -> last set winner
    last = sets[-1]
    return ("A" if int(last.get("a", 0)) > int(last.get("b", 0)) else "B"), {"a_sets": a_sets, "b_sets": b_sets, "a_pts": a_pts, "b_pts": b_pts}

def _apply_mmr_and_stats(db, match, cfg, sets, winner_team, duration_sec):
    # team ratings use average mmr
    a_ids = match["team_a_ids"]
    b_ids = match["team_b_ids"]

    teamA = [db["players"][uid] for uid in a_ids if uid in db["players"]]
    teamB = [db["players"][uid] for uid in b_ids if uid in db["players"]]

    if len(teamA) != 2 or len(teamB) != 2:
        return None, "ผู้เล่นในแมตช์ไม่ครบ"

    rA = sum([int(p.get("mmr", 1000) or 1000) for p in teamA]) / 2.0
    rB = sum([int(p.get("mmr", 1000) or 1000) for p in teamB]) / 2.0
    expA = _elo_expected(rA, rB)
    expB = 1.0 - expA

    g, meta = _score_factor_from_sets(cfg, sets, winner_team)
    baseK_ranked = 25.0

    # compute per-player K (unranked calibrate more volatile)
    def player_k(p):
        cg = int(p.get("calibrate_games", 0) or 0)
        if cg >= 10:
            return baseK_ranked
        # bigger early, smaller later
        return 40.0 + 4.0 * (10 - cg)  # game1=76 -> ... -> game9=44

    # actual results
    actA = 1.0 if winner_team == "A" else 0.0
    actB = 1.0 - actA

    # team deltas (for logging only)
    team_deltaA = (baseK_ranked * g) * (actA - expA)
    team_deltaB = (baseK_ranked * g) * (actB - expB)

    # apply per player
    snapshot = {}
    for p in teamA:
        k = player_k(p) * g
        delta = k * (actA - expA)
        delta_i = int(round(delta))
        p["mmr"] = int(p.get("mmr", 1000) or 1000) + delta_i
        snapshot[p["id"]] = {"delta": delta_i}
    for p in teamB:
        k = player_k(p) * g
        delta = k * (actB - expB)
        delta_i = int(round(delta))
        p["mmr"] = int(p.get("mmr", 1000) or 1000) + delta_i
        snapshot[p["id"]] = {"delta": delta_i}

    # set-based stats + points
    a_sets = meta["a_sets"]
    b_sets = meta["b_sets"]
    a_pts = meta["a_pts"]
    b_pts = meta["b_pts"]

    # each set is a win/loss for stats (as requested)
    for uid in a_ids:
        if uid in db["players"]:
            db["players"][uid]["sets_w"] += a_sets
            db["players"][uid]["sets_l"] += b_sets
            db["players"][uid]["pts_for"] += a_pts
            db["players"][uid]["pts_against"] += b_pts
    for uid in b_ids:
        if uid in db["players"]:
            db["players"][uid]["sets_w"] += b_sets
            db["players"][uid]["sets_l"] += a_sets
            db["players"][uid]["pts_for"] += b_pts
            db["players"][uid]["pts_against"] += a_pts

    # calibrate progress is per match
    for uid in a_ids + b_ids:
        if uid in db["players"]:
            p = db["players"][uid]
            cg = int(p.get("calibrate_games", 0) or 0)
            if cg < 10:
                p["calibrate_games"] = cg + 1
                # win/loss during calibrate (match-based)
                if (winner_team == "A" and uid in a_ids) or (winner_team == "B" and uid in b_ids):
                    p["calibrate_wins"] = int(p.get("calibrate_wins", 0) or 0) + 1
                else:
                    p["calibrate_losses"] = int(p.get("calibrate_losses", 0) or 0) + 1

    return {"snapshot": snapshot, "meta": meta, "team_deltaA": team_deltaA, "team_deltaB": team_deltaB}, None

# --- ROUTES ---
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/login", methods=["POST"])
def api_login():
    try:
        db = get_db()
        d = request.json or {}
        uid = d.get("userId")
        if not uid:
            return jsonify({"error": "Missing userId"}), 400

        if uid not in db["players"]:
            db["players"][uid] = {
                "id": uid,
                "nickname": d.get("displayName", "User"),
                "pictureUrl": d.get("pictureUrl", ""),
                "mmr": 1000,
                "calibrate_games": 0,
                "calibrate_wins": 0,
                "calibrate_losses": 0,
                "status": "offline",
                "last_active": now_ts(),
                "queue_join_time": None,
                "resting": False,
                "rest_until": None,
                "auto_rest": False,
                "outgoing_request_to": None,
                "incoming_requests": [],
                "pair_lock": None,
                "sets_w": 0, "sets_l": 0, "pts_for": 0, "pts_against": 0
            }
            log(f"New user: {db['players'][uid]['nickname']}")
        else:
            db["players"][uid]["nickname"] = d.get("displayName", db["players"][uid].get("nickname", "User"))
            db["players"][uid]["pictureUrl"] = d.get("pictureUrl", db["players"][uid].get("pictureUrl", ""))

        db["players"][uid]["last_active"] = now_ts()
        save_db(db)

        p = db["players"][uid]
        role = "super" if uid == SUPER_ADMIN_ID else ("mod" if uid in db["mod_ids"] else "user")
        rb1, rb2 = get_rank_badges(p)
        resp = {
            "id": uid,
            "nickname": p.get("nickname", ""),
            "pictureUrl": p.get("pictureUrl", ""),
            "role": role,
            "status": p.get("status", "offline"),
            "resting": bool(p.get("resting")),
            "auto_rest": bool(p.get("auto_rest")),
            "pair_lock": p.get("pair_lock"),
            "outgoing_request_to": p.get("outgoing_request_to"),
            "incoming_requests": p.get("incoming_requests", []),
            "mmr_display": mmr_display(p),
            "rank_title": rank_title_set_a(int(p.get("mmr", 1000) or 1000)),
            "rank_badge": rb1,
            "rank_badge2": rb2,
            "wr_badge": get_wr_badge(p),
            "progress": progress_info(p),
            "stats": {
                "sets_w": int(p.get("sets_w", 0) or 0),
                "sets_l": int(p.get("sets_l", 0) or 0),
                "pts_for": int(p.get("pts_for", 0) or 0),
                "pts_against": int(p.get("pts_against", 0) or 0),
                "wr": winrate_percent(p)  # None ok
            }
        }
        return jsonify(resp)
    except Exception as e:
        log(f"login error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/get_dashboard", methods=["GET"])
def api_dashboard():
    try:
        db = get_db()
        changed = _tick_courts(db)
        if changed:
            save_db(db)

        ss = db["system_settings"]
        cfg = ss.get("session_config", {})
        now = now_ts()

        # courts payload
        courts = {}
        for cid, m in db["courts_state"].items():
            if not m:
                courts[cid] = None
                continue

            # build team data (name, pic, rank badges)
            team_a_data = []
            for uid in m.get("team_a_ids", []):
                if uid in db["players"]:
                    pl = db["players"][uid]
                    rb1, rb2 = get_rank_badges(pl)
                    team_a_data.append({
                        "id": uid,
                        "name": pl.get("nickname", ""),
                        "pic": pl.get("pictureUrl", ""),
                        "rank_badge": rb1,
                        "rank_badge2": rb2
                    })
            team_b_data = []
            for uid in m.get("team_b_ids", []):
                if uid in db["players"]:
                    pl = db["players"][uid]
                    rb1, rb2 = get_rank_badges(pl)
                    team_b_data.append({
                        "id": uid,
                        "name": pl.get("nickname", ""),
                        "pic": pl.get("pictureUrl", ""),
                        "rank_badge": rb1,
                        "rank_badge2": rb2
                    })

            elapsed_sec = 0
            starts_in_sec = 0
            if m.get("state") == "countdown":
                starts_in_sec = max(0, int(round(float(m.get("scheduled_start", now)) - now)))
            else:
                st = m.get("start_time")
                if st:
                    elapsed_sec = max(0, int(round(now - float(st))))

            courts[cid] = {
                "match_id": m.get("match_id"),
                "state": m.get("state"),
                "team_a": m.get("team_a", []),
                "team_b": m.get("team_b", []),
                "team_a_ids": m.get("team_a_ids", []),
                "team_b_ids": m.get("team_b_ids", []),
                "team_a_data": team_a_data,
                "team_b_data": team_b_data,
                "elapsed_sec": elapsed_sec,
                "starts_in_sec": starts_in_sec,
                "auto_match": bool(ss.get("auto_match_courts", {}).get(cid, False))
            }

        # queue list with waiting minutes
        queue = []
        for p in db["players"].values():
            if p.get("status") not in ["active", "playing"]:
                continue
            qjt = p.get("queue_join_time")
            wait_min = 0
            if qjt:
                wait_min = int((now - float(qjt)) // 60)
            rb1, rb2 = get_rank_badges(p)
            queue.append({
                "id": p["id"],
                "nickname": p.get("nickname", ""),
                "pictureUrl": p.get("pictureUrl", ""),
                "status": p.get("status", "offline"),
                "waiting_min": wait_min,
                "resting": bool(p.get("resting")),
                "pair_lock": p.get("pair_lock"),
                "rank_badge": rb1,
                "rank_badge2": rb2,
                "rank_title": rank_title_set_a(int(p.get("mmr", 1000) or 1000)),
                "mmr_display": mmr_display(p)  # will show UNRANK(...) if needed
            })
        # sort queue: active first by join time; playing after
        def _qkey(x):
            # active first
            st = 0 if x["status"] == "active" else 1
            return (st, -x["waiting_min"])
        queue.sort(key=_qkey)

        # all players minimal (for mod panels, profile list)
        all_players = []
        for p in db["players"].values():
            rb1, rb2 = get_rank_badges(p)
            all_players.append({
                "id": p["id"],
                "nickname": p.get("nickname", ""),
                "pictureUrl": p.get("pictureUrl", ""),
                "status": p.get("status", "offline"),
                "resting": bool(p.get("resting")),
                "auto_rest": bool(p.get("auto_rest")),
                "pair_lock": p.get("pair_lock"),
                "mmr_display": mmr_display(p),
                "rank_title": rank_title_set_a(int(p.get("mmr", 1000) or 1000)),
                "rank_badge": rb1,
                "rank_badge2": rb2,
                "wr_badge": get_wr_badge(p),
                "sets_w": int(p.get("sets_w", 0) or 0),
                "sets_l": int(p.get("sets_l", 0) or 0),
                "pts_for": int(p.get("pts_for", 0) or 0),
                "pts_against": int(p.get("pts_against", 0) or 0),
                "is_mod": p["id"] in db["mod_ids"]
            })

        # leaderboards (mmr / points / winrate) with unranked always at bottom
        ranked = []
        unranked = []
        for p in db["players"].values():
            (unranked if _is_unranked(p) else ranked).append(p)

        lb_mmr = sorted(ranked, key=lambda x: int(x.get("mmr", 1000) or 1000), reverse=True) + \
                 sorted(unranked, key=lambda x: int(x.get("mmr", 1000) or 1000), reverse=True)

        lb_points = sorted(ranked, key=lambda x: int(x.get("pts_for", 0) or 0), reverse=True) + \
                    sorted(unranked, key=lambda x: int(x.get("pts_for", 0) or 0), reverse=True)

        def wr_val(p):
            wr = winrate_percent(p)
            return -1 if wr is None else wr

        lb_wr = sorted(ranked, key=lambda x: wr_val(x), reverse=True) + \
                sorted(unranked, key=lambda x: wr_val(x), reverse=True)

        def _pack_lb(p):
            rb1, rb2 = get_rank_badges(p)
            return {
                "id": p["id"],
                "nickname": p.get("nickname", ""),
                "pictureUrl": p.get("pictureUrl", ""),
                "mmr_display": mmr_display(p),
                "rank_title": rank_title_set_a(int(p.get("mmr", 1000) or 1000)),
                "rank_badge": rb1,
                "rank_badge2": rb2,
                "wr_badge": get_wr_badge(p),
                "pts_for": int(p.get("pts_for", 0) or 0),
                "wr": winrate_percent(p),
                "sets_w": int(p.get("sets_w", 0) or 0),
                "sets_l": int(p.get("sets_l", 0) or 0)
            }

        # history (global)
        history = []
        for m in db.get("match_history", [])[:30]:
            history.append(m)

        # events (for list/billing)
        event_list = []
        for eid, e in db.get("events", {}).items():
            event_list.append(e)
        # sort by datetime desc
        event_list.sort(key=lambda x: float(x.get("datetime", 0) or 0), reverse=True)

        return jsonify({
            "system": {
                "total_courts": ss.get("total_courts", 2),
                "is_session_active": bool(ss.get("is_session_active")),
                "current_event_id": ss.get("current_event_id"),
                "session_config": cfg,
                "auto_match_courts": ss.get("auto_match_courts", {}),
                "suggested_cooldown_min": ss.get("suggested_cooldown_min", 0)
            },
            "courts": courts,
            "queue": [q for q in queue if q["status"] == "active"],  # waiting list
            "queue_count": len([q for q in queue if q["status"] == "active"]),
            "playing_count": len([q for q in queue if q["status"] == "playing"]),
            "all_players": all_players,
            "leaderboards": {
                "mmr": [_pack_lb(p) for p in lb_mmr],
                "points": [_pack_lb(p) for p in lb_points],
                "wr": [_pack_lb(p) for p in lb_wr]
            },
            "history": history,
            "events": event_list
        })
    except Exception as e:
        log(f"dashboard error: {e}")
        return jsonify({"error": str(e)}), 500

# --- STATUS / REST ---
@app.route("/api/toggle_status", methods=["POST"])
def api_toggle_status():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404

    p = db["players"][uid]
    if not db["system_settings"].get("is_session_active"):
        return jsonify({"error": "Session not active"}), 400

    curr = p.get("status", "offline")
    if curr in ["active", "playing"]:
        # going offline resets queue time (as requested)
        p["status"] = "offline"
        p["queue_join_time"] = None
        p["resting"] = False
        p["rest_until"] = None
        # clear pairing state
        _clear_pairing_state_on_offline(db, uid)
    else:
        p["status"] = "active"
        p["queue_join_time"] = now_ts()
        p["last_active"] = now_ts()
        # keep resting false
        p["resting"] = False
        p["rest_until"] = None
        _ensure_participant(db, uid)

    save_db(db)
    return jsonify({"success": True, "status": p["status"]})

@app.route("/api/rest_toggle", methods=["POST"])
def api_rest_toggle():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    p = db["players"][uid]
    if p.get("status") != "active":
        return jsonify({"error": "ต้อง Check-in ก่อน"}), 400

    # toggle resting (waiting time still continues; we do not change queue_join_time)
    if p.get("resting"):
        p["resting"] = False
        p["rest_until"] = None
    else:
        p["resting"] = True
        p["rest_until"] = None
    save_db(db)
    return jsonify({"success": True, "resting": bool(p.get("resting"))})

@app.route("/api/auto_rest_toggle", methods=["POST"])
def api_auto_rest_toggle():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    p = db["players"][uid]
    p["auto_rest"] = not bool(p.get("auto_rest"))
    save_db(db)
    return jsonify({"success": True, "auto_rest": bool(p.get("auto_rest"))})

# --- PAIR REQUESTS ---
@app.route("/api/pair/request", methods=["POST"])
def api_pair_request():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    target = d.get("targetId")
    if not uid or not target:
        return jsonify({"error": "Missing"}), 400
    if uid not in db["players"] or target not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if uid == target:
        return jsonify({"error": "ขอตัวเองไม่ได้"}), 400
    if not db["system_settings"].get("is_session_active"):
        return jsonify({"error": "Session not active"}), 400

    sender = db["players"][uid]
    recv = db["players"][target]

    if sender.get("status") != "active":
        return jsonify({"error": "ต้อง Check-in ก่อน"}), 400

    if sender.get("pair_lock"):
        return jsonify({"error": "คุณจับคู่แล้ว (ยกเลิกก่อน)"}), 400

    if sender.get("outgoing_request_to") and sender.get("outgoing_request_to") != target:
        return jsonify({"error": "คุณส่งคำขออยู่แล้ว (ยกเลิกก่อน)"}), 400

    # requester can request only one person
    if sender.get("outgoing_request_to") == target:
        return jsonify({"success": True})

    sender["outgoing_request_to"] = target
    recv.setdefault("incoming_requests", [])
    if uid not in recv["incoming_requests"]:
        recv["incoming_requests"].append(uid)

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/pair/cancel_outgoing", methods=["POST"])
def api_pair_cancel_outgoing():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    _cancel_outgoing(db, uid)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/pair/accept", methods=["POST"])
def api_pair_accept():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")     # receiver
    from_id = d.get("fromId") # requester
    if not uid or not from_id:
        return jsonify({"error": "Missing"}), 400
    if uid not in db["players"] or from_id not in db["players"]:
        return jsonify({"error": "User not found"}), 404

    receiver = db["players"][uid]
    sender = db["players"][from_id]

    if receiver.get("pair_lock"):
        return jsonify({"error": "คุณจับคู่แล้ว (ยกเลิกก่อน)"}), 400
    if sender.get("pair_lock"):
        return jsonify({"error": "อีกฝ่ายจับคู่แล้ว"}), 400

    # receiver can receive many requests but accept only one
    if from_id not in receiver.get("incoming_requests", []):
        return jsonify({"error": "คำขอไม่พบแล้ว"}), 400

    # if sender had outgoing request to someone else, cancel it (rule)
    if sender.get("outgoing_request_to") and sender.get("outgoing_request_to") != uid:
        _cancel_outgoing(db, from_id)

    # if receiver has outgoing request to someone else and now accepts, cancel receiver outgoing too
    if receiver.get("outgoing_request_to"):
        _cancel_outgoing(db, uid)

    # If sender requested receiver, accepting should also clear sender outgoing
    _cancel_outgoing(db, from_id)

    receiver["pair_lock"] = from_id
    sender["pair_lock"] = uid

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/pair/cancel_pair", methods=["POST"])
def api_pair_cancel_pair():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    _break_pair(db, uid)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/pair/inbox", methods=["POST"])
def api_pair_inbox():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    p = db["players"][uid]
    inbox = []
    for rid in p.get("incoming_requests", []):
        if rid in db["players"]:
            rp = db["players"][rid]
            rb1, rb2 = get_rank_badges(rp)
            inbox.append({
                "id": rid,
                "nickname": rp.get("nickname", ""),
                "pictureUrl": rp.get("pictureUrl", ""),
                "status": rp.get("status", "offline"),
                "pair_lock": rp.get("pair_lock"),
                "rank_badge": rb1,
                "rank_badge2": rb2
            })
    # sort: active first
    inbox.sort(key=lambda x: 0 if x["status"] == "active" else 1)
    return jsonify({
        "outgoing_request_to": p.get("outgoing_request_to"),
        "pair_lock": p.get("pair_lock"),
        "inbox": inbox
    })

# --- ADMIN / SESSION ---
@app.route("/api/admin/toggle_session", methods=["POST"])
def api_toggle_session():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    action = d.get("action")
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    if action == "start":
        # session config
        target_points = int(d.get("target_points", 21) or 21)
        bo = int(d.get("bo", 1) or 1)
        enable_notifications = bool(d.get("enable_notifications", False))

        if target_points not in [11, 21]:
            target_points = 21
        if bo not in [1, 2, 3]:
            bo = 1

        db["system_settings"]["is_session_active"] = True
        db["system_settings"]["session_config"] = {
            "target_points": target_points,
            "bo": bo,
            "enable_notifications": enable_notifications
        }

        # create new session event
        eid = str(uuid.uuid4())[:8]
        now = now_ts()
        today = datetime.now().strftime("%d/%m/%Y %H:%M")
        db["events"][eid] = {
            "id": eid,
            "name": f"ก๊วน {today}",
            "datetime": now,
            "status": "active",
            "participants": []
        }
        db["system_settings"]["current_event_id"] = eid

        # reset courts state
        _refresh_courts_storage(db)
        for cid in db["courts_state"].keys():
            db["courts_state"][cid] = None

        save_db(db)
        return jsonify({"success": True})

    # end session
    db["system_settings"]["is_session_active"] = False
    db["system_settings"]["current_event_id"] = None
    # clear courts
    for cid in db["courts_state"].keys():
        db["courts_state"][cid] = None

    # set everyone offline and clear queue/pairing
    for p in db["players"].values():
        if p.get("status") != "offline":
            p["status"] = "offline"
        p["queue_join_time"] = None
        p["resting"] = False
        p["rest_until"] = None
        _clear_pairing_state_on_offline(db, p["id"])

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/update_courts", methods=["POST"])
def api_update_courts():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    count = int(d.get("count", 2) or 2)
    count = max(1, min(12, count))
    db["system_settings"]["total_courts"] = count
    _refresh_courts_storage(db)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/toggle_auto_match", methods=["POST"])
def api_toggle_auto_match():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    cid = str(d.get("courtId"))
    enabled = bool(d.get("enabled"))
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    _refresh_courts_storage(db)
    if cid not in db["system_settings"]["auto_match_courts"]:
        return jsonify({"error": "Court not found"}), 404
    db["system_settings"]["auto_match_courts"][cid] = enabled
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/manage_mod", methods=["POST"])
def api_manage_mod():
    db = get_db()
    d = request.json or {}
    uid = d.get("requesterId")
    if uid != SUPER_ADMIN_ID:
        return jsonify({"error": "Super Admin Only"}), 403
    target = d.get("targetUserId")
    action = d.get("action")
    if not target or target not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if action == "promote":
        if target not in db["mod_ids"]:
            db["mod_ids"].append(target)
    else:
        if target in db["mod_ids"]:
            db["mod_ids"].remove(target)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/set_mmr", methods=["POST"])
def api_set_mmr():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    target = d.get("targetUserId")
    new_mmr = d.get("newMmr")
    if not target or target not in db["players"]:
        return jsonify({"error": "Target not found"}), 404
    try:
        db["players"][target]["mmr"] = int(new_mmr)
        # If mod edits mmr, we consider ranked (optional) -> keep as-is; do NOT force calibrate finish.
        save_db(db)
        return jsonify({"success": True})
    except:
        return jsonify({"error": "Invalid MMR"}), 400

# --- MATCHMAKE BUTTONS ---
@app.route("/api/matchmake/run", methods=["POST"])
def api_matchmake_run():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    if not db["system_settings"].get("is_session_active"):
        return jsonify({"error": "Session not active"}), 400

    _refresh_courts_storage(db)
    results = []
    # fill free courts where auto_match is OFF
    for cid in sorted(db["courts_state"].keys(), key=lambda x: int(x)):
        if db["courts_state"].get(cid) is None and not db["system_settings"]["auto_match_courts"].get(cid, False):
            res = _create_match_on_court(db, cid, source="manual_button")
            results.append(res)
    save_db(db)
    return jsonify({"success": True, "results": results})

@app.route("/api/matchmake/court", methods=["POST"])
def api_matchmake_court():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    cid = str(d.get("courtId"))
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    if cid not in db["courts_state"]:
        return jsonify({"error": "Court not found"}), 404
    res = _create_match_on_court(db, cid, source="manual_court")
    save_db(db)
    return jsonify(res)

@app.route("/api/match/cancel", methods=["POST"])
def api_cancel_match():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    cid = str(d.get("courtId"))
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    if cid not in db["courts_state"] or not db["courts_state"][cid]:
        return jsonify({"error": "No match"}), 400

    m = db["courts_state"][cid]
    a_ids = m.get("team_a_ids", [])
    b_ids = m.get("team_b_ids", [])

    # add avoid teammate pairs to prevent immediate rematch (valid for 10 minutes)
    until = now_ts() + 600
    if len(a_ids) == 2:
        db["recent_avoids"].append({"pair": [a_ids[0], a_ids[1]], "until": until})
    if len(b_ids) == 2:
        db["recent_avoids"].append({"pair": [b_ids[0], b_ids[1]], "until": until})

    # reset court
    db["courts_state"][cid] = None

    # return players to active (keep queue_join_time; waiting continues)
    for pid in a_ids + b_ids:
        if pid in db["players"]:
            db["players"][pid]["status"] = "active"
            # keep queue_join_time as-is (so their waiting continues)
            # keep pair_lock as-is
    save_db(db)
    return jsonify({"success": True})

# --- SUBMIT RESULT ---
@app.route("/api/submit_result", methods=["POST"])
def api_submit_result():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    cid = str(d.get("courtId"))
    sets = d.get("sets", [])

    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if cid not in db["courts_state"] or not db["courts_state"][cid]:
        return jsonify({"error": "No match"}), 400

    m = db["courts_state"][cid]
    a_ids = m.get("team_a_ids", [])
    b_ids = m.get("team_b_ids", [])

    # permission: player in match or staff
    if not (is_staff(uid, db) or uid in a_ids or uid in b_ids):
        return jsonify({"error": "Unauthorized"}), 403

    cfg = db["system_settings"].get("session_config", {"target_points": 21, "bo": 1})
    bo = int(cfg.get("bo", 1) or 1)

    # validate sets count
    if not isinstance(sets, list) or len(sets) != bo:
        return jsonify({"error": f"ต้องกรอก {bo} เซ็ต"}), 400

    # validate each set score
    for idx, s in enumerate(sets):
        try:
            a = int(s.get("a"))
            b = int(s.get("b"))
        except:
            return jsonify({"error": f"เซ็ต {idx+1} คะแนนไม่ถูกต้อง"}), 400
        ok, err = _validate_set_score(cfg, a, b)
        if not ok:
            return jsonify({"error": f"เซ็ต {idx+1}: {err}"}), 400

    # Determine winner
    winner_team, meta = _winner_from_sets(cfg, sets)

    # duration: if playing -> now-start_time, else 0 (countdown ended not started)
    end_ts = now_ts()
    duration_sec = 0
    if m.get("start_time"):
        duration_sec = max(0, int(round(end_ts - float(m["start_time"]))))

    # apply mmr and stats
    res, err = _apply_mmr_and_stats(db, m, cfg, sets, winner_team, duration_sec)
    if err:
        return jsonify({"error": err}), 400

    # after match: set players back to active, reset queue_join_time to now (fair rotation)
    cooldown_min = int(db["system_settings"].get("suggested_cooldown_min", 0) or 0)
    for pid in a_ids + b_ids:
        if pid in db["players"]:
            pl = db["players"][pid]
            pl["status"] = "active"
            pl["queue_join_time"] = now_ts()  # goes to back of queue after playing
            pl["last_active"] = now_ts()
            # clear outgoing request while playing ended
            _cancel_outgoing(db, pid)
            # optional auto-rest (player opt-in)
            if bool(pl.get("auto_rest")) and cooldown_min > 0:
                pl["resting"] = True
                pl["rest_until"] = now_ts() + cooldown_min * 60
            else:
                # do not force rest
                if pl.get("rest_until") and pl.get("resting"):
                    # keep if already resting set manually
                    pass

    # clear court
    db["courts_state"][cid] = None

    # store history (with rank badge snapshots + W/L context computed in frontend)
    match_rec_id = str(uuid.uuid4())[:10]
    rank_snapshot = {}
    for pid in a_ids + b_ids:
        if pid in db["players"]:
            rb1, rb2 = get_rank_badges(db["players"][pid])
            rank_snapshot[pid] = {"rank_badge": rb1, "rank_badge2": rb2, "mmr_display": mmr_display(db["players"][pid])}

    hist = {
        "id": match_rec_id,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "court_id": int(cid),
        "winner_team": winner_team,
        "sets": sets,
        "meta": meta,
        "duration_sec": duration_sec,
        "team_a_ids": a_ids,
        "team_b_ids": b_ids,
        "team_a": m.get("team_a", []),
        "team_b": m.get("team_b", []),
        "pics": {
            "A": [db["players"][pid].get("pictureUrl","") for pid in a_ids if pid in db["players"]],
            "B": [db["players"][pid].get("pictureUrl","") for pid in b_ids if pid in db["players"]],
        },
        "mmr_snapshot": res.get("snapshot", {}),
        "rank_snapshot": rank_snapshot,
        "cancelled": False
    }
    db["match_history"].insert(0, hist)

    save_db(db)
    return jsonify({"success": True, "winner": winner_team})

# --- MANUAL MATCHMAKE (admin picks 4 players) ---
@app.route("/api/matchmake/manual", methods=["POST"])
def api_manual_matchmake():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    cid = str(d.get("courtId"))
    p_ids = d.get("playerIds", [])

    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    if cid not in db["courts_state"]:
        return jsonify({"error": "Court not found"}), 404
    if db["courts_state"].get(cid):
        return jsonify({"error": "Court Full"}), 400

    players = []
    for pid in p_ids:
        if pid in db["players"]:
            players.append(db["players"][pid])
    if len(players) != 4:
        return jsonify({"error": "Need 4 players"}), 400
    if len(set([p["id"] for p in players])) != 4:
        return jsonify({"error": "ผู้เล่นซ้ำกัน"}), 400

    # simple split 2/2; manual overrides fairness
    teamA = players[:2]
    teamB = players[2:]

    match_id = str(uuid.uuid4())[:8]
    countdown_start = now_ts()
    scheduled_start = countdown_start + 60

    db["courts_state"][cid] = {
        "match_id": match_id,
        "state": "countdown",
        "source": "manual_pick",
        "created_at": countdown_start,
        "countdown_start": countdown_start,
        "scheduled_start": scheduled_start,
        "start_time": None,
        "team_a_ids": [p["id"] for p in teamA],
        "team_b_ids": [p["id"] for p in teamB],
        "team_a": [p.get("nickname","") for p in teamA],
        "team_b": [p.get("nickname","") for p in teamB],
    }

    for p in teamA + teamB:
        db["players"][p["id"]]["status"] = "playing"
        _ensure_participant(db, p["id"])

    save_db(db)
    return jsonify({"success": True, "match_id": match_id})

# --- PROFILE API ---
@app.route("/api/player/profile", methods=["GET"])
def api_player_profile():
    db = get_db()
    target = request.args.get("targetId")
    if not target or target not in db["players"]:
        return jsonify({"error": "Not found"}), 404
    p = db["players"][target]
    rb1, rb2 = get_rank_badges(p)
    prof = {
        "id": p["id"],
        "nickname": p.get("nickname",""),
        "pictureUrl": p.get("pictureUrl",""),
        "mmr_display": mmr_display(p),
        "rank_title": rank_title_set_a(int(p.get("mmr", 1000) or 1000)),
        "rank_badge": rb1,
        "rank_badge2": rb2,
        "wr_badge": get_wr_badge(p),
        "progress": progress_info(p),
        "stats": {
            "sets_w": int(p.get("sets_w", 0) or 0),
            "sets_l": int(p.get("sets_l", 0) or 0),
            "pts_for": int(p.get("pts_for", 0) or 0),
            "pts_against": int(p.get("pts_against", 0) or 0),
            "wr": winrate_percent(p)
        }
    }
    # last 10 matches (global history filter)
    last = []
    for m in db.get("match_history", []):
        if target in m.get("team_a_ids", []) or target in m.get("team_b_ids", []):
            last.append(m)
        if len(last) >= 10:
            break
    prof["last_matches"] = last
    return jsonify(prof)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
