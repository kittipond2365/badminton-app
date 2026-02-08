import json
import os
import time
import uuid
import math
import random
import threading
import hashlib
import gzip
import atexit
import signal
from copy import deepcopy
from itertools import combinations
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify, render_template, make_response

app = Flask(**name**)

# Thailand timezone (UTC+7)

TH_TZ = timezone(timedelta(hours=7))

# =========================

# Render-friendly settings

# =========================

SUPER_ADMIN_ID = “U1cf933e3a1559608c50c0456f6583dc9”

# Use Render Disk via env var (recommended)

DATA_FILE = os.environ.get(“IZESQUAD_DATA_FILE”, “/var/data/izesquad_data.json”)

DB_LOCK = threading.Lock()

# =========================

# Optimization: In-memory DB cache

# =========================

# IMPORTANT: Must run with 1 worker only!

# Render: set env WEB_CONCURRENCY=1

# Or use: gunicorn app:app –workers 1

MATCH_HISTORY_MAX = 2000         # #3: cap history
SAVE_INTERVAL_SEC = 5            # flush to disk every 5s
_DB_CACHE = None                 # in-memory DB (the single source of truth)
_DB_DIRTY = False                # flag: needs disk flush
_DB_VERSION = 0                  # incremented on every mutation → used for ETag
_DASHBOARD_CACHE = None          # cached dashboard JSON bytes
_DASHBOARD_VERSION = -1          # version when cache was built

# =========================

# Defaults + DB helpers

# =========================

DEFAULT_DB = {
“schema_version”: 3,
“system_settings”: {
“total_courts”: 2,
“is_session_active”: False,
“current_event_id”: None,
“scoring”: {  # mod sets on start session
“points”: 21,   # 11 or 21
“bo”: 1,        # 1,2,3
“cap”: 30       # allow deuce to 30
},
“notify_enabled”: False,     # mod option on start session
“automatch”: {},             # {“1”: false, “2”: true, …}
“avoid_4”: [],               # recent canceled 4-signatures [{“sig”:“a,b,c,d”,“ts”:…}]
“recent_teammates”: {},      # “u|v” -> {“ts”: float, “count”: int}
“recent_opponents”: {},      # “u|v” -> {“ts”: float, “count”: int}
“avg_match_minutes”: 12
},
“mod_ids”: [],
“players”: {},                  # uid -> player
“events”: {},                   # event_id -> event
“courts”: {},                   # court_id(str)->match_state or None
“match_history”: [],            # list of match records (non-canceled)
}

def _now():
return time.time()

def _deep_merge(dst, src):
“”“fill missing keys in dst from src (recursive) without overwriting existing values”””
for k, v in src.items():
if k not in dst:
dst[k] = deepcopy(v)
else:
if isinstance(v, dict) and isinstance(dst.get(k), dict):
_deep_merge(dst[k], v)

def _atomic_write_json(path, data):
tmp = f”{path}.tmp”
with open(tmp, “w”, encoding=“utf-8”) as f:
json.dump(data, f, ensure_ascii=False, indent=2)
os.replace(tmp, path)

def _init_db_file():
directory = os.path.dirname(DATA_FILE)
if directory and not os.path.exists(directory):
os.makedirs(directory, exist_ok=True)
if not os.path.exists(DATA_FILE):
_atomic_write_json(DATA_FILE, DEFAULT_DB)

def _load_db_from_disk():
“”“Load DB from disk into memory (called once at startup).”””
global _DB_CACHE, _DB_VERSION
_init_db_file()
try:
with open(DATA_FILE, “r”, encoding=“utf-8”) as f:
data = json.load(f)
_deep_merge(data, DEFAULT_DB)
_refresh_courts(data)
_normalize_players(data)
except Exception:
data = deepcopy(DEFAULT_DB)
_refresh_courts(data)
_DB_CACHE = data
_DB_VERSION = 0

def get_db():
“”“Return in-memory DB (no disk I/O).”””
global _DB_CACHE
if _DB_CACHE is None:
_load_db_from_disk()
return _DB_CACHE

def save_db(data=None):
“”“Mark DB as dirty for background flush. Trims history. No immediate disk write.”””
global _DB_DIRTY, _DB_VERSION, _DB_CACHE
if data is not None:
_DB_CACHE = data
db = _DB_CACHE
# #3: Cap match_history
if len(db.get(“match_history”, [])) > MATCH_HISTORY_MAX:
db[“match_history”] = db[“match_history”][:MATCH_HISTORY_MAX]
_DB_VERSION += 1
_DB_DIRTY = True

def save_db_now(data=None):
“”“Critical save: mark dirty + immediate flush to disk.
Use for: match submit/cancel, MMR changes, session toggle, admin actions.”””
save_db(data)
_flush_to_disk()

def _flush_to_disk():
“”“Actually write to disk (called by background thread).”””
global _DB_DIRTY
if not _DB_DIRTY or _DB_CACHE is None:
return
with DB_LOCK:
try:
_atomic_write_json(DATA_FILE, _DB_CACHE)
_DB_DIRTY = False
except Exception as e:
print(f”[FLUSH ERROR] {e}”)

def _background_save_loop():
“”“Background thread: flush dirty DB to disk every SAVE_INTERVAL_SEC.”””
while True:
time.sleep(SAVE_INTERVAL_SEC)
try:
_flush_to_disk()
except Exception as e:
print(f”[BG SAVE ERROR] {e}”)

# Start background save thread

_save_thread = threading.Thread(target=_background_save_loop, daemon=True)
_save_thread.start()

# Graceful shutdown: flush to disk before exit

def _shutdown_flush(*args):
print(”[SHUTDOWN] Flushing DB to disk…”)
_flush_to_disk()
atexit.register(_shutdown_flush)
signal.signal(signal.SIGTERM, lambda *a: (_shutdown_flush(), exit(0)))

# =========================

# #4: Gzip middleware

# =========================

@app.after_request
def gzip_response(response):
“”“Gzip JSON responses > 500 bytes if client accepts it.”””
if (response.status_code < 200 or response.status_code >= 300
or response.direct_passthrough
or ‘Content-Encoding’ in response.headers
or ‘gzip’ not in request.headers.get(‘Accept-Encoding’, ‘’)):
return response
ct = response.content_type or “”
if ‘application/json’ not in ct and ‘text/html’ not in ct:
return response
data = response.get_data()
if len(data) < 500:
return response
compressed = gzip.compress(data, compresslevel=6)
response.set_data(compressed)
response.headers[‘Content-Encoding’] = ‘gzip’
response.headers[‘Content-Length’] = len(compressed)
response.headers[‘Vary’] = ‘Accept-Encoding’
return response

def _refresh_courts(db):
total = int(db[“system_settings”].get(“total_courts”, 2))
# courts dict uses string keys for stable json
for i in range(1, total + 1):
k = str(i)
if k not in db[“courts”]:
db[“courts”][k] = None
if k not in db[“system_settings”][“automatch”]:
db[“system_settings”][“automatch”][k] = False
# remove extra courts
for k in list(db[“courts”].keys()):
if int(k) > total:
db[“courts”].pop(k, None)
for k in list(db[“system_settings”][“automatch”].keys()):
if int(k) > total:
db[“system_settings”][“automatch”].pop(k, None)

def _ensure_player(p, uid):
p.setdefault(“id”, uid)
p.setdefault(“nickname”, “User”)
p.setdefault(“pictureUrl”, “”)
p.setdefault(“mmr”, 1000)
p.setdefault(“status”, “offline”)    # offline | queue | resting | playing
p.setdefault(“queue_join_ts”, 0.0)
p.setdefault(“cooldown_until”, 0.0)
p.setdefault(“auto_rest”, False)
p.setdefault(“priority_match”, False)  # mod can set to skip queue
p.setdefault(“bio”, “”)               # self-intro text (max 150 chars)
p.setdefault(“racket”, “”)            # racket model name

```
# calibration
p.setdefault("calib_played", 0)
p.setdefault("calib_wins", 0)
p.setdefault("calib_losses", 0)
p.setdefault("calib_streak", 0)

# partner request system
p.setdefault("outgoing_req", None)       # uid
p.setdefault("incoming_reqs", [])        # list[uids]
p.setdefault("paired_with", None)        # uid (accepted)

# stats (sets-based)
p.setdefault("sets_w", 0)
p.setdefault("sets_l", 0)
p.setdefault("points_for", 0)
p.setdefault("points_against", 0)
p.setdefault("match_w", 0)
p.setdefault("match_l", 0)
p.setdefault("best_streak", 0)
p.setdefault("cur_streak", 0)
```

def _normalize_players(db):
for uid, p in db[“players”].items():
_ensure_player(p, uid)
# sanitize
try:
p[“mmr”] = int(p.get(“mmr”, 1000))
except Exception:
p[“mmr”] = 1000
for key in [“sets_w”,“sets_l”,“points_for”,“points_against”,“match_w”,“match_l”,“calib_played”,“calib_wins”,“calib_losses”,“calib_streak”,“best_streak”,“cur_streak”]:
try:
p[key] = int(p.get(key, 0))
except Exception:
p[key] = 0
for key in [“queue_join_ts”,“cooldown_until”]:
try:
p[key] = float(p.get(key, 0.0))
except Exception:
p[key] = 0.0
if not isinstance(p.get(“incoming_reqs”, []), list):
p[“incoming_reqs”] = []

# =========================

# Rank / display helpers

# =========================

def is_unranked(p):
return int(p.get(“calib_played”, 0)) < 10

def mmr_display(p):
if is_unranked(p):
return f”UNRANK ({int(p.get(‘calib_played’,0))}/10)”
return str(int(p.get(“mmr”, 1000)))

def rank_title(mmr):
# Thai title only, no emoji
v = int(mmr)
if v < 1000: return “มือใหม่หัดตี”
if v < 1200: return “ตีเรื่อยๆ”
if v < 1400: return “เริ่มเข้าที่”
if v < 1600: return “ตัวจริงก๊วน”
if v < 1700: return “ตัวแบก”
if v < 1800: return “หัวหน้าก๊วน”
if v < 2000: return “เทพท้องถิ่น”
if v < 2300: return “เทพเจ้าก๊วนแบด”
return “บอสสนาม”

def rank_color(mmr):
v = int(mmr)
# 1600-1799 secondary, 1800+ error
if v >= 1800:
return “badge-error”
if v >= 1600:
return “badge-secondary”
return “badge-primary”

def wl_badge_class(p):
sw = int(p.get(“sets_w”, 0))
sl = int(p.get(“sets_l”, 0))
total = sw + sl
if total == 0:
return “badge-ghost”, 0
wr = (sw / total) * 100.0
if wr < 40:
return “badge-error”, int(round(wr))
if wr < 60:
return “badge-neutral”, int(round(wr))
return “badge-success”, int(round(wr))

def progression_bar(p):
“”“Type B: 100 MMR interval progress; hide mmr if unranked”””
if is_unranked(p):
n = int(p.get(“calib_played”, 0))
return {“type”:“calib”, “label”: f”UNRANK ({n}/10)”, “pct”: int(round((n/10)*100))}
mmr = int(p.get(“mmr”, 1000))
lo = (mmr // 100) * 100
hi = lo + 99
pct = int(round(((mmr - lo) / 99) * 100)) if hi > lo else 0
# promotion / demotion indicator
to_up = max(0, (hi + 1) - mmr)  # points to next bracket
to_down = max(0, mmr - lo)      # how far into bracket
return {
“type”:“mmr”,
“label”: f”{lo}-{hi}”,
“pct”: max(0, min(100, pct)),
“to_up”: to_up,
“to_down”: to_down,
“mid”: lo + 50
}

def effective_mmr_for_matchmaking(p):
“”“During calibration, push winners up faster to find true level”””
base = int(p.get(“mmr”, 1000))
if not is_unranked(p):
return base
w = int(p.get(“calib_wins”, 0))
l = int(p.get(“calib_losses”, 0))
streak = int(p.get(“calib_streak”, 0))
adj = (w - l) * 60 + streak * 40
return base + adj

# =========================

# Session / Event helpers

# =========================

def _current_event(db):
eid = db[“system_settings”].get(“current_event_id”)
if not eid:
return None
return db[“events”].get(eid)

def _touch_participant(db, uid):
evt = _current_event(db)
if not evt:
return
parts = evt.setdefault(“participants”, [])
if uid not in parts:
parts.append(uid)

def _create_event(db, name, dt_ts=None, status=“active”, scoring=None, location=””, notify=False, end_datetime=None):
eid = str(uuid.uuid4())[:8]
if dt_ts is None:
dt_ts = _now()
if scoring is None:
scoring = {“points”: 21, “bo”: 1, “cap”: 30}
db[“events”][eid] = {
“id”: eid,
“name”: name,
“datetime”: float(dt_ts),
“end_datetime”: float(end_datetime) if end_datetime else None,
“created_ts”: float(_now()),
“status”: status,              # open|active|ended
“participants”: [],            # played in session
“pre_registered”: [],          # signed up beforehand (open events)
“matches”: [],
“scoring”: scoring,
“location”: location,
“notify”: notify
}
return eid

# =========================

# Matchmaking core (v2: Wait Window + Diversity + Priority)

# =========================

# — TUNING CONSTANTS —

WAIT_WINDOW_SEC = 180        # 3 minutes base window
WAIT_EXPAND_STEPS = [300, 480, 720]  # 5, 8, 12 min progressive expansion
W_WAIT = 35.0                # weight for wait-time bonus (more wait = preferred)
ALPHA_DIFF = 2.5             # weight for team MMR diff
BETA_WITHIN = 1.8            # weight for within-team disparity (anti extreme-carry)
HARD_SKILL_THRESHOLD = 500   # discard combo if diff > this when alternatives exist
DIVERSITY_WINDOW_SEC = 3600  # 60 min for diversity tracking cleanup
TEAMMATE_PENALTIES = [1200, 700, 400, 200, 100]  # last 1–5 matches
OPPONENT_PENALTIES = [600, 300, 150, 80]          # last 1–4 matches
GROUP4_HARD_BAN_SEC = 600    # 10 min
GROUP4_SOFT_PENALTY = 2000   # within soft window
GROUP4_SOFT_SEC = 3600       # 60 min
PRIORITY_WAIT_BOOST = 9999   # massive boost for priority players
NOISE_SCALE = 5.0            # random jitter

def _eligible_players(db):
now = _now()
players = []
for p in db[“players”].values():
if p.get(“status”) != “queue”:
continue
if float(p.get(“cooldown_until”, 0)) > now:
continue
players.append(p)
# sort by queue time (oldest first)
players.sort(key=lambda x: float(x.get(“queue_join_ts”, now)))
return players

def _pair_units(db, players_sorted):
“”“Build units: either a paired_with group or solo. Preserve queue priority.”””
seen = set()
units = []
for p in players_sorted:
uid = p[“id”]
if uid in seen:
continue
paired = p.get(“paired_with”)
if paired and paired in db[“players”]:
q = db[“players”][paired]
if q.get(“status”) == “queue” and float(q.get(“cooldown_until”, 0)) <= _now():
ts = min(float(p.get(“queue_join_ts”, _now())), float(q.get(“queue_join_ts”, _now())))
units.append({“members”: [p, q], “ts”: ts, “size”: 2})
seen.add(uid); seen.add(paired)
continue
units.append({“members”: [p], “ts”: float(p.get(“queue_join_ts”, _now())), “size”: 1})
seen.add(uid)
units.sort(key=lambda u: u[“ts”])
return units

def _player_wait(p, now):
“”“Wait in seconds, boosted by priority flag.”””
base = max(0.0, now - float(p.get(“queue_join_ts”, now)))
if p.get(“priority_match”):
base += PRIORITY_WAIT_BOOST
return base

def _build_candidate_pool(db, units, now):
“”“Build candidate pool using wait window.”””
if not units:
return [], []

```
# Compute wait for each unit
for u in units:
    u["wait"] = max(_player_wait(m, now) for m in u["members"])

oldest_wait = max(u["wait"] for u in units)

# Try progressively wider windows
windows = [WAIT_WINDOW_SEC] + WAIT_EXPAND_STEPS + [999999]
for window in windows:
    threshold = oldest_wait - window
    pool = [u for u in units if u["wait"] >= threshold]
    # Count total player slots
    total_slots = sum(u["size"] for u in pool)
    if total_slots >= 4:
        return pool, [uid for u in pool for uid in [m["id"] for m in u["members"]]]
return [], []
```

def _pair_key(a, b):
return “|”.join(sorted([a, b]))

def _group4_sig(four_ids):
return “,”.join(sorted(four_ids))

def _cleanup_diversity(db, now):
“”“Clean old diversity entries.”””
for store_key in [“recent_teammates”, “recent_opponents”]:
store = db[“system_settings”].setdefault(store_key, {})
to_del = [k for k, v in store.items() if now - float(v.get(“ts”, 0)) > DIVERSITY_WINDOW_SEC]
for k in to_del:
del store[k]

```
avoid = db["system_settings"].setdefault("avoid_4", [])
db["system_settings"]["avoid_4"] = [
    item for item in avoid
    if now - float(item.get("ts", 0)) <= GROUP4_SOFT_SEC
]
```

def _score_group4_diversity(db, four_ids, now):
“”“Check group-of-4 ban/penalty.”””
sig = _group4_sig(four_ids)
for item in db[“system_settings”].get(“avoid_4”, []):
if item.get(“sig”) != sig:
continue
age = now - float(item.get(“ts”, 0))
if age <= GROUP4_HARD_BAN_SEC:
return None  # hard ban
if age <= GROUP4_SOFT_SEC:
return GROUP4_SOFT_PENALTY * (1.0 - age / GROUP4_SOFT_SEC)
return 0

def _score_pair_diversity(db, team_ids, opponent_ids, partner_pair_set):
“”“Score teammate + opponent repetition penalties.”””
pen = 0
teammates_store = db[“system_settings”].get(“recent_teammates”, {})
opponents_store = db[“system_settings”].get(“recent_opponents”, {})

```
# Teammate penalty (exclude partner pair - they chose to be together)
for i in range(len(team_ids)):
    for j in range(i + 1, len(team_ids)):
        pair = (team_ids[i], team_ids[j])
        if tuple(sorted(pair)) in partner_pair_set:
            continue  # partner pair exemption
        key = _pair_key(team_ids[i], team_ids[j])
        entry = teammates_store.get(key)
        if entry:
            count = int(entry.get("count", 0))
            if count > 0 and count <= len(TEAMMATE_PENALTIES):
                pen += TEAMMATE_PENALTIES[count - 1]
            elif count > len(TEAMMATE_PENALTIES):
                pen += TEAMMATE_PENALTIES[-1]

# Opponent penalty (between team_ids and opponent_ids)
for u in team_ids:
    for v in opponent_ids:
        key = _pair_key(u, v)
        entry = opponents_store.get(key)
        if entry:
            count = int(entry.get("count", 0))
            if count > 0 and count <= len(OPPONENT_PENALTIES):
                pen += OPPONENT_PENALTIES[count - 1]
            elif count > len(OPPONENT_PENALTIES):
                pen += OPPONENT_PENALTIES[-1]

return pen
```

def _skill_score(db, teamA, teamB):
“”“Compute skill fairness score: team diff + anti-carry.”””
mmrA = [effective_mmr_for_matchmaking(db[“players”][i]) for i in teamA]
mmrB = [effective_mmr_for_matchmaking(db[“players”][i]) for i in teamB]

```
diff_sum = abs(sum(mmrA) - sum(mmrB))
dispA = max(mmrA) - min(mmrA)
dispB = max(mmrB) - min(mmrB)
within = dispA + dispB

return ALPHA_DIFF * diff_sum + BETA_WITHIN * within
```

def _get_partner_pairs(db, four_ids):
“”“Get set of partner pairs in these 4 players.”””
pairs = set()
for uid in four_ids:
p = db[“players”].get(uid)
if not p:
continue
pw = p.get(“paired_with”)
if pw and pw in four_ids:
pairs.add(tuple(sorted([uid, pw])))
return pairs

def _best_split_for_four(db, four_ids, now):
“”“Return best (teamA_ids, teamB_ids, total_score) respecting pairs. None if no valid split.”””
a = four_ids
splits = [
([a[0], a[1]], [a[2], a[3]]),
([a[0], a[2]], [a[1], a[3]]),
([a[0], a[3]], [a[1], a[2]]),
]

```
partner_pairs = _get_partner_pairs(db, four_ids)

# Group-of-4 diversity
g4_pen = _score_group4_diversity(db, four_ids, now)
if g4_pen is None:
    return None  # hard banned

# Wait score (higher total wait = better = lower total score)
total_wait_min = sum(_player_wait(db["players"][uid], now) for uid in four_ids) / 60.0
s_wait = -W_WAIT * total_wait_min

best = None
for tA, tB in splits:
    # Enforce partner pair must be same team
    ok = True
    for u, v in partner_pairs:
        if (u in tA and v in tB) or (u in tB and v in tA):
            ok = False
            break
    if not ok:
        continue

    s_skill = _skill_score(db, tA, tB)

    # Diversity score for this split
    s_div_a = _score_pair_diversity(db, tA, tB, partner_pairs)
    s_div_b = _score_pair_diversity(db, tB, tA, partner_pairs)
    s_div = s_div_a + s_div_b + g4_pen

    noise = random.uniform(0, NOISE_SCALE)

    total = s_wait + s_skill + s_div + noise

    if best is None or total < best[2]:
        best = (tA, tB, total)

return best
```

def _choose_four_for_court(db):
“”“Main matchmaking: Wait Window + Skill + Diversity + Priority.”””
eligible = _eligible_players(db)
if len(eligible) < 4:
return None

```
now = _now()
units = _pair_units(db, eligible)
_cleanup_diversity(db, now)

pool_units, pool_ids = _build_candidate_pool(db, units, now)
if len(pool_ids) < 4:
    return None

# Limit candidates for performance (first ~14 by queue priority)
cand = pool_ids[:14]
if len(cand) < 4:
    return None

best_pick = None
has_alternative = len(cand) > 4

for combo in combinations(cand, 4):
    combo = list(combo)

    # Paired rule: if someone is paired, partner must be in combo
    valid = True
    for uid in combo:
        p = db["players"][uid]
        pw = p.get("paired_with")
        if pw and pw in db["players"] and db["players"][pw].get("status") == "queue":
            if pw not in combo:
                valid = False
                break
    if not valid:
        continue

    split = _best_split_for_four(db, combo, now)
    if not split:
        continue
    teamA, teamB, score = split

    # Hard skill cap: discard extreme unfairness if alternatives exist
    if has_alternative:
        mmrA = [effective_mmr_for_matchmaking(db["players"][i]) for i in teamA]
        mmrB = [effective_mmr_for_matchmaking(db["players"][i]) for i in teamB]
        if abs(sum(mmrA) - sum(mmrB)) > HARD_SKILL_THRESHOLD:
            continue

    if best_pick is None or score < best_pick["score"]:
        best_pick = {"combo": combo, "teamA": teamA, "teamB": teamB, "score": score}

# Clear priority_match flag for matched players
if best_pick:
    for uid in best_pick["combo"]:
        db["players"][uid]["priority_match"] = False

return best_pick
```

def _update_diversity_after_match(db, team_a_ids, team_b_ids):
“”“Update diversity tracking after a match finishes or starts.”””
now = _now()
tm_store = db[“system_settings”].setdefault(“recent_teammates”, {})
op_store = db[“system_settings”].setdefault(“recent_opponents”, {})

```
# Update teammates
for team in [team_a_ids, team_b_ids]:
    for i in range(len(team)):
        for j in range(i + 1, len(team)):
            key = _pair_key(team[i], team[j])
            entry = tm_store.get(key, {"ts": 0, "count": 0})
            entry["ts"] = now
            entry["count"] = int(entry.get("count", 0)) + 1
            tm_store[key] = entry

# Update opponents
for u in team_a_ids:
    for v in team_b_ids:
        key = _pair_key(u, v)
        entry = op_store.get(key, {"ts": 0, "count": 0})
        entry["ts"] = now
        entry["count"] = int(entry.get("count", 0)) + 1
        op_store[key] = entry

# Update group4
sig = _group4_sig(team_a_ids + team_b_ids)
avoid = db["system_settings"].setdefault("avoid_4", [])
avoid.append({"sig": sig, "ts": now})
```

def _recent_avoid_penalty(db, four_ids):
“”“Legacy: check if this 4-group is hard-banned.”””
sig = _group4_sig(four_ids)
now = _now()
for item in db[“system_settings”].get(“avoid_4”, []):
try:
ts = float(item.get(“ts”, 0))
if now - ts <= GROUP4_HARD_BAN_SEC and item.get(“sig”) == sig:
return 10_000
except Exception:
continue
return 0

def _match_id():
return str(uuid.uuid4())[:10]

def _create_match_on_court(db, court_id, teamA_ids, teamB_ids, reason=“auto”):
now = _now()
mid = _match_id()

```
# 1-minute "report to court" window
start_at = now + 60.0

mmrA = sum(effective_mmr_for_matchmaking(db["players"][i]) for i in teamA_ids) / len(teamA_ids)
mmrB = sum(effective_mmr_for_matchmaking(db["players"][i]) for i in teamB_ids) / len(teamB_ids)

match_state = {
    "match_id": mid,
    "court_id": int(court_id),
    "created_at": now,
    "start_at": start_at,
    "team_a_ids": teamA_ids,
    "team_b_ids": teamB_ids,
    "team_mmr_a": mmrA,
    "team_mmr_b": mmrB,
    "status": "pending",
    "reason": reason
}

for uid in teamA_ids + teamB_ids:
    p = db["players"][uid]
    p["status"] = "playing"
    _touch_participant(db, uid)

evt = _current_event(db)
if evt:
    evt.setdefault("matches", []).append(mid)

db["courts"][str(court_id)] = match_state

# Track diversity for future matchmaking
_update_diversity_after_match(db, teamA_ids, teamB_ids)

return match_state
```

def _maybe_run_automatch(db):
“”“Run automatch for any empty court with automatch on.”””
if not db[“system_settings”].get(“is_session_active”):
return False
changed = False
for cid, state in db[“courts”].items():
if state is not None:
continue
if not db[“system_settings”][“automatch”].get(cid, False):
continue
pick = _choose_four_for_court(db)
if not pick:
continue
_create_match_on_court(db, cid, pick[“teamA”], pick[“teamB”], reason=“automatch”)
changed = True
return changed

# =========================

# Scoring / MMR

# =========================

def _validate_set_score(a, b, points, cap):
# BUG FIX: handle None/empty string gracefully
if a is None or b is None or a == “” or b == “”:
return False, “Score must not be empty”
try:
a = int(a); b = int(b)
except (ValueError, TypeError):
return False, “Score must be integer”
if a < 0 or b < 0:
return False, “Score must be >= 0”
mx = max(a, b)
mn = min(a, b)
if mx < points:
return False, f”Winner must reach at least {points}”
# must win by 2 unless reached cap
if mx < cap:
if mx - mn < 2:
return False, “Must win by 2 (unless cap)”
else:
# At cap: valid scores are cap-(cap-2) e.g. 30-28 (win by 2)
# and cap-(cap-1) e.g. 30-29 (cap rule, first to 30 wins)
if mx != cap:
return False, f”Max cap is {cap}”
if mn not in (cap - 1, cap - 2):
return False, f”At cap {cap}, score must be {cap}-{cap-1} or {cap}-{cap-2}”
# prevent impossible big scores
if mx > cap or mn > cap:
return False, f”Max cap is {cap}”
return True, “”

def _winner_from_sets(set_scores, bo, points, cap):
“””
Returns result dict or (None, error_msg).
BO2: winner by total points.
“””
clean = []
for s in set_scores:
if s is None:
continue
a = s.get(“a”); b = s.get(“b”)
# BUG FIX: skip sets where both scores are empty (BO3 set 3 not played)
if (a is None or a == “”) and (b is None or b == “”):
continue
ok, msg = _validate_set_score(a, b, points, cap)
if not ok:
return None, msg
clean.append((int(a), int(b)))

```
if len(clean) == 0:
    return None, "No valid sets submitted"

if bo == 1 and len(clean) != 1:
    return None, "BO1 must have exactly 1 set"
if bo == 2 and len(clean) != 2:
    return None, "BO2 must have exactly 2 sets"
if bo == 3:
    if len(clean) < 2 or len(clean) > 3:
        return None, "BO3 must have 2 or 3 sets"
    # BUG FIX: validate BO3 logic - if someone won 2-0, 3rd set shouldn't exist
    sets_a = sum(1 for a, b in clean if a > b)
    sets_b = sum(1 for a, b in clean if b > a)
    if len(clean) == 3:
        # after 2 sets, neither should have 2 wins already
        sets_a_2 = sum(1 for a, b in clean[:2] if a > b)
        sets_b_2 = sum(1 for a, b in clean[:2] if b > a)
        if sets_a_2 >= 2 or sets_b_2 >= 2:
            return None, "BO3: match was already decided after 2 sets, 3rd set invalid"
    if len(clean) == 2:
        # both sets must be won by same team (2-0)
        if sets_a != 2 and sets_b != 2:
            return None, "BO3: need a 3rd set (score is 1-1)"

sets_won_a = 0
sets_won_b = 0
total_a = 0
total_b = 0
for a, b in clean:
    total_a += a
    total_b += b
    if a > b:
        sets_won_a += 1
    else:
        sets_won_b += 1

if bo == 2:
    # winner by total points
    if total_a > total_b:
        winner = "A"
    elif total_b > total_a:
        winner = "B"
    else:
        if sets_won_a > sets_won_b:
            winner = "A"
        elif sets_won_b > sets_won_a:
            winner = "B"
        else:
            winner = "A" if clean[-1][0] > clean[-1][1] else "B"
else:
    winner = "A" if sets_won_a > sets_won_b else "B"

return {
    "sets_won_a": sets_won_a,
    "sets_won_b": sets_won_b,
    "total_points_a": total_a,
    "total_points_b": total_b,
    "winner": winner,
    "clean": clean
}, ""
```

def _elo_expected(ra, rb):
return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))

def _score_multiplier(total_a, total_b, sets_won_a, sets_won_b):
margin = abs(total_a - total_b)
total = max(1, total_a + total_b)
m = margin / total
sf = 1.0 + min(0.6, m * 2.0)
sd = abs(sets_won_a - sets_won_b)
sf += min(0.2, sd * 0.1)
return min(1.8, sf)

def _k_for_player(p):
return 50 if is_unranked(p) else 25

def _apply_match_results(db, match_state, set_scores):
scoring = db[“system_settings”][“scoring”]
points = int(scoring.get(“points”, 21))
bo = int(scoring.get(“bo”, 1))
cap = int(scoring.get(“cap”, 30))

```
res, msg = _winner_from_sets(set_scores, bo, points, cap)
if not res:
    return None, msg

winner = res["winner"]
teamA = match_state["team_a_ids"]
teamB = match_state["team_b_ids"]

ra = float(match_state.get("team_mmr_a", 1000))
rb = float(match_state.get("team_mmr_b", 1000))
ea = _elo_expected(ra, rb)
eb = 1.0 - ea

sA = 1.0 if winner == "A" else 0.0
sB = 1.0 - sA

sf = _score_multiplier(res["total_points_a"], res["total_points_b"], res["sets_won_a"], res["sets_won_b"])

baseK = 25.0 * sf
dA = baseK * (sA - ea)
dB = -dA

mmr_changes = {}
win_ids = teamA if winner == "A" else teamB
lose_ids = teamB if winner == "A" else teamA

for uid in teamA + teamB:
    p = db["players"][uid]
    kp = _k_for_player(p) / 25.0
    if uid in teamA:
        delta = dA * kp
    else:
        delta = dB * kp
    mmr_changes[uid] = int(round(delta))

# Apply stats per set
for (a_pts, b_pts) in res["clean"]:
    for uid in teamA:
        p = db["players"][uid]
        p["points_for"] += a_pts
        p["points_against"] += b_pts
    for uid in teamB:
        p = db["players"][uid]
        p["points_for"] += b_pts
        p["points_against"] += a_pts

    if a_pts > b_pts:
        for uid in teamA: db["players"][uid]["sets_w"] += 1
        for uid in teamB: db["players"][uid]["sets_l"] += 1
    else:
        for uid in teamB: db["players"][uid]["sets_w"] += 1
        for uid in teamA: db["players"][uid]["sets_l"] += 1

# match W/L
for uid in win_ids:
    p = db["players"][uid]
    p["match_w"] += 1
    p["cur_streak"] = int(p.get("cur_streak", 0)) + 1
    p["best_streak"] = max(int(p.get("best_streak", 0)), int(p["cur_streak"]))
for uid in lose_ids:
    p = db["players"][uid]
    p["match_l"] += 1
    p["cur_streak"] = 0

# update mmr + calibration
for uid, delta in mmr_changes.items():
    p = db["players"][uid]
    p["mmr"] = int(p.get("mmr", 1000)) + int(delta)
    if is_unranked(p):
        p["calib_played"] += 1
        if uid in win_ids:
            p["calib_wins"] += 1
            p["calib_streak"] = int(p.get("calib_streak", 0)) + 1
        else:
            p["calib_losses"] += 1
            p["calib_streak"] = 0

return {
    "winner": winner,
    "sets_won_a": res["sets_won_a"],
    "sets_won_b": res["sets_won_b"],
    "total_points_a": res["total_points_a"],
    "total_points_b": res["total_points_b"],
    "set_scores": [{"a": a, "b": b} for a, b in res["clean"]],
    "mmr_changes": mmr_changes,
    "sf": sf,
    "expected_a": ea
}, ""
```

def _recompute_avg_match_minutes(db):
items = [m for m in db.get(“match_history”, []) if isinstance(m, dict) and not m.get(“canceled”)]
items = items[:10]
if not items:
db[“system_settings”][“avg_match_minutes”] = 12
return 12
mins = []
for m in items:
try:
mins.append(float(m.get(“duration_sec”, 0)) / 60.0)
except Exception:
continue
if not mins:
db[“system_settings”][“avg_match_minutes”] = 12
return 12
avg = sum(mins) / len(mins)
avg = max(6, min(30, avg))
db[“system_settings”][“avg_match_minutes”] = int(round(avg))
return int(round(avg))

def _cooldown_minutes(db):
avg = int(db[“system_settings”].get(“avg_match_minutes”, 12))
total_courts = int(db[“system_settings”].get(“total_courts”, 2))
active = 0
for p in db[“players”].values():
if p.get(“status”) in [“queue”, “resting”, “playing”]:
active += 1
denom = max(1, total_courts * 4)
ratio = active / denom
cd = int(round(avg * min(1.8, ratio)))
cd = max(0, min(25, cd))
if active <= denom:
cd = 0
return cd

def _maybe_auto_start_scheduled_event(db):
“”“Check if any open event’s scheduled time has arrived → auto-start session.”””
if db[“system_settings”].get(“is_session_active”):
return False  # already running

```
now = _now()
for eid, evt in db["events"].items():
    if evt.get("status") != "open":
        continue
    evt_time = float(evt.get("datetime", 0))
    if evt_time <= 0 or evt_time > now:
        continue

    # Time has arrived! Auto-start session with this event's settings
    scoring = evt.get("scoring", {"points": 21, "bo": 1, "cap": 30})
    notify = evt.get("notify", False)

    db["system_settings"]["is_session_active"] = True
    db["system_settings"]["notify_enabled"] = notify
    db["system_settings"]["scoring"] = scoring
    db["system_settings"]["current_event_id"] = eid
    evt["status"] = "active"

    return True
return False
```

def _maybe_auto_end_session(db):
“”“Auto-end session 2 hours after event’s end_datetime.”””
if not db[“system_settings”].get(“is_session_active”):
return False

```
eid = db["system_settings"].get("current_event_id")
if not eid or eid not in db["events"]:
    return False

evt = db["events"][eid]
end_dt = evt.get("end_datetime")
if not end_dt:
    return False

now = _now()
auto_close_at = float(end_dt) + (2 * 3600)  # 2 hours after end time
if now < auto_close_at:
    return False

# Auto-end session
db["system_settings"]["is_session_active"] = False
evt["status"] = "ended"
db["system_settings"]["current_event_id"] = None

for cid in list(db["courts"].keys()):
    db["courts"][cid] = None

for p in db["players"].values():
    p["status"] = "offline"
    p["queue_join_ts"] = 0.0
    p["cooldown_until"] = 0.0
    p["paired_with"] = None
    p["outgoing_req"] = None
    p["incoming_reqs"] = []
    p["priority_match"] = False

return True
```

# =========================

# Public API shaping

# =========================

def _public_player_min(db, p):
cls, wr = wl_badge_class(p)
return {
“id”: p[“id”],
“nickname”: p.get(“nickname”,“User”),
“pictureUrl”: p.get(“pictureUrl”,””),
“status”: p.get(“status”,“offline”),
“mmr_display”: mmr_display(p),
“mmr”: int(p.get(“mmr”,1000)),
“unranked”: is_unranked(p),
“calib_played”: int(p.get(“calib_played”,0)),
“rank_title”: rank_title(int(p.get(“mmr”,1000))),
“rank_color”: rank_color(int(p.get(“mmr”,1000))),
“wr”: wr,
“wr_badge”: cls,
“queue_join_ts”: float(p.get(“queue_join_ts”,0)),
“cooldown_until”: float(p.get(“cooldown_until”,0)),
“auto_rest”: bool(p.get(“auto_rest”,False)),
“priority_match”: bool(p.get(“priority_match”, False)),
“paired_with”: p.get(“paired_with”),
“outgoing_req”: p.get(“outgoing_req”),
“incoming_reqs”: p.get(“incoming_reqs”, []),
# BUG FIX: include stats needed by leaderboard
“points_for”: int(p.get(“points_for”, 0)),
“points_against”: int(p.get(“points_against”, 0)),
“sets_w”: int(p.get(“sets_w”, 0)),
“sets_l”: int(p.get(“sets_l”, 0)),
“match_w”: int(p.get(“match_w”, 0)),
“match_l”: int(p.get(“match_l”, 0)),
“best_streak”: int(p.get(“best_streak”, 0)),
“cur_streak”: int(p.get(“cur_streak”, 0)),
“progress”: progression_bar(p),
}

def _public_match_state(db, state):
if not state:
return None
def pl(uid):
p = db[“players”].get(uid, {“id”: uid, “nickname”:”?”, “pictureUrl”:””, “mmr”:1000, “calib_played”:0})
cls, wr = wl_badge_class(p)
return {
“id”: uid,
“nickname”: p.get(“nickname”,”?”),
“pictureUrl”: p.get(“pictureUrl”,””),
“unranked”: is_unranked(p),
“mmr_display”: mmr_display(p),
“rank_title”: rank_title(int(p.get(“mmr”,1000))),
“rank_color”: rank_color(int(p.get(“mmr”,1000))),
“wr”: wr,
“wr_badge”: cls
}
teamA = [pl(uid) for uid in state.get(“team_a_ids”,[])]
teamB = [pl(uid) for uid in state.get(“team_b_ids”,[])]
now = _now()
start_at = float(state.get(“start_at”, now))
created_at = float(state.get(“created_at”, now))
started = now >= start_at
elapsed = int(max(0, now - start_at)) if started else 0
countdown = int(max(0, start_at - now))
return {
“match_id”: state.get(“match_id”),
“court_id”: state.get(“court_id”),
“created_at”: created_at,
“start_at”: start_at,
“started”: started,
“elapsed_sec”: elapsed,
“countdown_sec”: countdown,
“team_a”: teamA,
“team_b”: teamB,
}

# =========================

# Routes

# =========================

@app.route(”/”)
def index():
return render_template(“index.html”)

@app.route(”/tv”)
def tv_monitor():
return render_template(“tv.html”)

@app.route(”/api/health”)
def health():
db = _DB_CACHE
players = len(db[“players”]) if db else 0
history = len(db.get(“match_history”, [])) if db else 0
return jsonify({
“ok”: True, “time”: _now(), “data_file”: DATA_FILE,
“cache”: “in-memory”, “version”: _DB_VERSION, “dirty”: _DB_DIRTY,
“players”: players, “history_count”: history, “history_max”: MATCH_HISTORY_MAX,
})

@app.route(”/api/login”, methods=[“POST”])
def login():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if not uid:
return jsonify({“error”:“missing userId”}), 400

```
if uid not in db["players"]:
    db["players"][uid] = {}
p = db["players"][uid]
_ensure_player(p, uid)
p["nickname"] = d.get("displayName") or p.get("nickname","User")
p["pictureUrl"] = d.get("pictureUrl") or p.get("pictureUrl","")

role = "super" if uid == SUPER_ADMIN_ID else ("mod" if uid in db["mod_ids"] else "user")

save_db(db)

# return incoming request info
incoming = []
for rid in p.get("incoming_reqs", []):
    rp = db["players"].get(rid)
    if rp:
        incoming.append({"id": rid, "nickname": rp.get("nickname","User"), "pictureUrl": rp.get("pictureUrl","")})

paired = None
if p.get("paired_with") and p["paired_with"] in db["players"]:
    q = db["players"][p["paired_with"]]
    paired = {"id": q["id"], "nickname": q.get("nickname","User"), "pictureUrl": q.get("pictureUrl","")}

return jsonify({
    "id": uid,
    "nickname": p.get("nickname","User"),
    "pictureUrl": p.get("pictureUrl",""),
    "role": role,
    "status": p.get("status","offline"),
    "mmr_display": mmr_display(p),
    "unranked": is_unranked(p),
    "rank_title": rank_title(int(p.get("mmr",1000))),
    "rank_color": rank_color(int(p.get("mmr",1000))),
    "wr_badge": wl_badge_class(p)[0],
    "wr": wl_badge_class(p)[1],
    "progress": progression_bar(p),
    "bio": p.get("bio", ""),
    "racket": p.get("racket", ""),
    "auto_rest": bool(p.get("auto_rest", False)),
    "outgoing_req": p.get("outgoing_req"),
    "incoming_reqs": incoming,
    "paired_with": paired
})
```

@app.route(”/api/get_dashboard”)
def get_dashboard():
global _DASHBOARD_CACHE, _DASHBOARD_VERSION

```
db = get_db()

# run automatch opportunistically
changed = _maybe_run_automatch(db)

# check if any scheduled event should auto-start
auto_started = _maybe_auto_start_scheduled_event(db)

# check if active session should auto-end (2h after event end time)
auto_ended = _maybe_auto_end_session(db)

if changed or auto_started or auto_ended:
    save_db(db)

# #2: ETag — skip recompute if nothing changed
etag = f'W/"{_DB_VERSION}"'
if_none_match = request.headers.get('If-None-Match')
if if_none_match == etag and _DASHBOARD_CACHE is not None:
    return make_response('', 304)

now = _now()

# courts
courts = {}
for cid, state in db["courts"].items():
    courts[cid] = _public_match_state(db, state)

# players min list
all_players = [_public_player_min(db, p) for p in db["players"].values()]

# queue & resting lists
queue = [p for p in all_players if p["status"] == "queue"]
resting = [p for p in all_players if p["status"] == "resting"]

for p in queue + resting:
    qts = float(p.get("queue_join_ts", 0))
    p["wait_min"] = int(max(0, now - qts) // 60) if qts > 0 else 0
    # BUG FIX: also provide wait_sec for more precise display
    p["wait_sec"] = int(max(0, now - qts)) if qts > 0 else 0
    cd = float(p.get("cooldown_until", 0))
    p["cooldown_left_sec"] = int(max(0, cd - now)) if cd > now else 0

queue.sort(key=lambda x: float(x.get("queue_join_ts", now)))
resting.sort(key=lambda x: float(x.get("queue_join_ts", now)))

# events: active first, then by datetime newest
events = list(db["events"].values())
now_ts = _now()
for e in events:
    # participants (played in session)
    joined = []
    for uid in e.get("participants", []):
        if uid in db["players"]:
            joined.append(_public_player_min(db, db["players"][uid]))
    e["participants_public"] = joined

    # pre-registered (signed up beforehand)
    pre_pub = []
    for uid in e.get("pre_registered", []):
        if uid in db["players"]:
            pre_pub.append(_public_player_min(db, db["players"][uid]))
    e["pre_registered_public"] = pre_pub

    # countdown seconds for open future events
    evt_dt = float(e.get("datetime", 0))
    e["countdown_sec"] = int(max(0, evt_dt - now_ts)) if evt_dt > now_ts else 0

    # ensure scoring/location/end_datetime exist for older events
    e.setdefault("scoring", {"points": 21, "bo": 1, "cap": 30})
    e.setdefault("location", "")
    e.setdefault("end_datetime", None)

    # Auto-close countdown for active events with end_datetime
    end_dt = e.get("end_datetime")
    if end_dt and e.get("status") == "active":
        auto_close_at = float(end_dt) + (2 * 3600)
        e["auto_close_sec"] = int(max(0, auto_close_at - now_ts))
    else:
        e["auto_close_sec"] = None

# Sort: active first, then open (nearest future first), then ended (newest first)
def event_sort_key(e):
    status = e.get("status", "open")
    dt = float(e.get("datetime", 0))
    if status == "active":
        return (0, -dt)
    elif status == "open":
        return (1, dt)   # nearest future first
    else:
        return (2, -dt)  # ended newest first
events.sort(key=event_sort_key)

# leaderboards
def lb_mmr_key(p):
    return (1 if p["unranked"] else 0, -int(p.get("mmr", 1000)))
mmr_lb = sorted(all_players, key=lb_mmr_key)

# BUG FIX: use points_for from all_players (now included)
points_lb = sorted(all_players, key=lambda p: (1 if p["unranked"] else 0, -int(p.get("points_for", 0))))

def wr_key(p):
    sw = int(p.get("sets_w",0)); sl = int(p.get("sets_l",0))
    total = sw + sl
    wr = (sw/total) if total > 0 else -1
    return (1 if p["unranked"] else 0, -wr, -total)
winrate_lb = sorted(all_players, key=wr_key)

history = [m for m in db.get("match_history", [])[:50]
           if isinstance(m, dict) and "team_a_ids" in m and "team_b_ids" in m][:40]

resp = jsonify({
    "system": db["system_settings"],
    "mod_ids": db.get("mod_ids", []),
    "courts": courts,
    "automatch": db["system_settings"]["automatch"],
    "queue": queue,
    "resting": resting,
    "events": events,
    "leaderboards": {
        "mmr": mmr_lb[:200],
        "points": points_lb[:200],
        "winrate": winrate_lb[:200]
    },
    "history": history,
    "all_players": all_players
})
resp.headers['ETag'] = etag
resp.headers['Cache-Control'] = 'no-cache'
_DASHBOARD_CACHE = True
_DASHBOARD_VERSION = _DB_VERSION
return resp
```

@app.route(”/api/player/<uid>”)
def get_player(uid):
db = get_db()
if uid not in db[“players”]:
return jsonify({“error”:“not found”}), 404
p = db[“players”][uid]
_ensure_player(p, uid)

```
last = []
for m in db.get("match_history", []):
    if not m or not isinstance(m, dict):
        continue
    if "team_a_ids" not in m or "team_b_ids" not in m:
        continue
    if uid in m.get("team_a_ids", []) or uid in m.get("team_b_ids", []):
        last.append(m)
    if len(last) >= 10:
        break

cls, wr = wl_badge_class(p)
return jsonify({
    "id": uid,
    "nickname": p.get("nickname","User"),
    "pictureUrl": p.get("pictureUrl",""),
    "unranked": is_unranked(p),
    "mmr_display": mmr_display(p),
    "mmr": int(p.get("mmr",1000)),
    "rank_title": rank_title(int(p.get("mmr",1000))),
    "rank_color": rank_color(int(p.get("mmr",1000))),
    "wr": wr,
    "wr_badge": cls,
    "progress": progression_bar(p),
    "bio": p.get("bio", ""),
    "racket": p.get("racket", ""),
    "stats": {
        "sets_w": int(p.get("sets_w",0)),
        "sets_l": int(p.get("sets_l",0)),
        "match_w": int(p.get("match_w",0)),
        "match_l": int(p.get("match_l",0)),
        "points_for": int(p.get("points_for",0)),
        "points_against": int(p.get("points_against",0)),
        "best_streak": int(p.get("best_streak",0)),
    },
    "last10": last
})
```

# =========================

# Player actions

# =========================

@app.route(”/api/toggle_status”, methods=[“POST”])
def toggle_status():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if not uid or uid not in db[“players”]:
return jsonify({“error”:“user not found”}), 404

```
if not db["system_settings"].get("is_session_active"):
    return jsonify({"error":"Session not active"}), 400

p = db["players"][uid]
cur = p.get("status","offline")

if cur == "playing":
    return jsonify({"error":"Can't toggle while playing"}), 400

if cur == "offline":
    p["status"] = "queue"
    p["queue_join_ts"] = _now()
    p["cooldown_until"] = 0.0
    _touch_participant(db, uid)
else:
    # leaving -> offline
    p["status"] = "offline"
    p["queue_join_ts"] = 0.0
    p["cooldown_until"] = 0.0
    # unpair
    if p.get("paired_with"):
        other = p["paired_with"]
        if other in db["players"]:
            db["players"][other]["paired_with"] = None
        p["paired_with"] = None
    # cancel outgoing request
    if p.get("outgoing_req"):
        tgt = p["outgoing_req"]
        if tgt in db["players"]:
            db["players"][tgt]["incoming_reqs"] = [x for x in db["players"][tgt].get("incoming_reqs",[]) if x != uid]
        p["outgoing_req"] = None
    # BUG FIX: also remove self from all incoming_reqs of others
    for other_uid, other_p in db["players"].items():
        if uid in other_p.get("incoming_reqs", []):
            other_p["incoming_reqs"] = [x for x in other_p["incoming_reqs"] if x != uid]

save_db(db)
return jsonify({"success": True, "status": p["status"]})
```

@app.route(”/api/toggle_rest”, methods=[“POST”])
def toggle_rest():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if not uid or uid not in db[“players”]:
return jsonify({“error”:“user not found”}), 404
p = db[“players”][uid]
if p.get(“status”) not in [“queue”, “resting”]:
return jsonify({“error”:“Not in queue/resting”}), 400

```
if p["status"] == "queue":
    p["status"] = "resting"
else:
    p["status"] = "queue"
save_db(db)
return jsonify({"success": True, "status": p["status"]})
```

@app.route(”/api/toggle_auto_rest”, methods=[“POST”])
def toggle_auto_rest():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
val = bool(d.get(“value”, False))
if not uid or uid not in db[“players”]:
return jsonify({“error”:“user not found”}), 404
db[“players”][uid][“auto_rest”] = val
save_db(db)
return jsonify({“success”: True, “auto_rest”: val})

@app.route(”/api/update_profile”, methods=[“POST”])
def update_profile():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if not uid or uid not in db[“players”]:
return jsonify({“error”:“user not found”}), 404
p = db[“players”][uid]

```
BIO_MAX = 150
RACKET_MAX = 60

if "bio" in d:
    bio = str(d["bio"] or "").strip()[:BIO_MAX]
    p["bio"] = bio
if "racket" in d:
    racket = str(d["racket"] or "").strip()[:RACKET_MAX]
    p["racket"] = racket

save_db(db)
return jsonify({"success": True, "bio": p.get("bio",""), "racket": p.get("racket","")})
```

# =========================

# Partner request system

# =========================

@app.route(”/api/partner/request”, methods=[“POST”])
def partner_request():
db = get_db()
d = request.json or {}
uid = d.get(“userId”); target = d.get(“targetId”)
if not uid or not target:
return jsonify({“error”:“missing data”}), 400
if uid == target:
return jsonify({“error”:“Cannot request yourself”}), 400
if uid not in db[“players”] or target not in db[“players”]:
return jsonify({“error”:“user not found”}), 404

```
p = db["players"][uid]
t = db["players"][target]

if p.get("paired_with"):
    return jsonify({"error":"Already paired; unpair first"}), 400

if p.get("outgoing_req") and p["outgoing_req"] != target:
    return jsonify({"error":"You already requested someone; cancel first"}), 400
if p.get("outgoing_req") == target:
    return jsonify({"success": True})

inc = t.get("incoming_reqs", [])
if uid not in inc:
    inc.append(uid)
t["incoming_reqs"] = inc
p["outgoing_req"] = target

save_db(db)
return jsonify({"success": True})
```

@app.route(”/api/partner/cancel_outgoing”, methods=[“POST”])
def partner_cancel_outgoing():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if not uid or uid not in db[“players”]:
return jsonify({“error”:“user not found”}), 404
p = db[“players”][uid]
tgt = p.get(“outgoing_req”)
if tgt and tgt in db[“players”]:
db[“players”][tgt][“incoming_reqs”] = [x for x in db[“players”][tgt].get(“incoming_reqs”, []) if x != uid]
p[“outgoing_req”] = None
save_db(db)
return jsonify({“success”: True})

@app.route(”/api/partner/respond”, methods=[“POST”])
def partner_respond():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
from_id = d.get(“fromId”)
action = d.get(“action”)
if not uid or not from_id or action not in [“accept”,“decline”]:
return jsonify({“error”:“missing data”}), 400
if uid not in db[“players”] or from_id not in db[“players”]:
return jsonify({“error”:“user not found”}), 404

```
me = db["players"][uid]
sender = db["players"][from_id]

if action == "accept":
    if me.get("paired_with"):
        return jsonify({"error":"You already paired; unpair first"}), 400
    if sender.get("paired_with"):
        return jsonify({"error":"Sender already paired"}), 400

    # cancel my outgoing
    if me.get("outgoing_req"):
        tgt = me["outgoing_req"]
        if tgt in db["players"]:
            db["players"][tgt]["incoming_reqs"] = [x for x in db["players"][tgt].get("incoming_reqs",[]) if x != uid]
        me["outgoing_req"] = None

    # cancel sender's outgoing to someone else
    if sender.get("outgoing_req") and sender["outgoing_req"] != uid:
        tgt = sender["outgoing_req"]
        if tgt in db["players"]:
            db["players"][tgt]["incoming_reqs"] = [x for x in db["players"][tgt].get("incoming_reqs",[]) if x != from_id]
    sender["outgoing_req"] = None

    # pair them
    me["paired_with"] = from_id
    sender["paired_with"] = uid

    # BUG FIX: remove accepted request from incoming list
    me["incoming_reqs"] = [x for x in me.get("incoming_reqs", []) if x != from_id]

    save_db(db)
    return jsonify({"success": True, "paired_with": from_id})

# decline
me["incoming_reqs"] = [x for x in me.get("incoming_reqs", []) if x != from_id]
if sender.get("outgoing_req") == uid:
    sender["outgoing_req"] = None
save_db(db)
return jsonify({"success": True})
```

@app.route(”/api/partner/unpair”, methods=[“POST”])
def partner_unpair():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if not uid or uid not in db[“players”]:
return jsonify({“error”:“user not found”}), 404
me = db[“players”][uid]
other = me.get(“paired_with”)
if other and other in db[“players”]:
db[“players”][other][“paired_with”] = None
me[“paired_with”] = None
save_db(db)
return jsonify({“success”: True})

# =========================

# Matchmaking endpoints

# =========================

@app.route(”/api/matchmake”, methods=[“POST”])
def matchmake():
db = get_db()
d = request.json or {}
court_id = d.get(“courtId”)

```
if not db["system_settings"].get("is_session_active"):
    return jsonify({"error":"Session not active"}), 400

changed = False

def try_fill(cid):
    nonlocal changed
    if db["courts"].get(str(cid)) is not None:
        return
    pick = _choose_four_for_court(db)
    if not pick:
        return
    _create_match_on_court(db, str(cid), pick["teamA"], pick["teamB"], reason="manual_button")
    changed = True

if court_id:
    try_fill(str(court_id))
else:
    for cid, state in db["courts"].items():
        if state is None and not db["system_settings"]["automatch"].get(cid, False):
            try_fill(cid)

if changed:
    save_db_now(db)
    return jsonify({"success": True})
return jsonify({"success": False, "status": "waiting_or_full"})
```

@app.route(”/api/matchmake/manual”, methods=[“POST”])
def manual_matchmake():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if not uid:
return jsonify({“error”:“missing userId”}), 400
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403

```
cid = str(d.get("courtId"))
pids = d.get("playerIds", [])
if cid not in db["courts"]:
    return jsonify({"error":"invalid court"}), 400
if db["courts"][cid] is not None:
    return jsonify({"error":"Court full"}), 400
if len(pids) != 4 or len(set(pids)) != 4:
    return jsonify({"error":"Need 4 unique players"}), 400
for x in pids:
    if x not in db["players"]:
        return jsonify({"error":"Player not found"}), 400

teamA = [pids[0], pids[1]]
teamB = [pids[2], pids[3]]
_create_match_on_court(db, cid, teamA, teamB, reason="manual_admin")
save_db_now(db)
return jsonify({"success": True})
```

@app.route(”/api/match/cancel”, methods=[“POST”])
def cancel_match():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
cid = str(d.get(“courtId”))
reason = d.get(“reason”,””)
if not uid or cid not in db[“courts”]:
return jsonify({“error”:“bad request”}), 400
state = db[“courts”].get(cid)
if not state:
return jsonify({“error”:“No match”}), 400

```
is_staff = uid == SUPER_ADMIN_ID or uid in db["mod_ids"]
in_match = uid in state.get("team_a_ids", []) or uid in state.get("team_b_ids", [])
if not (is_staff or in_match):
    return jsonify({"error":"Unauthorized"}), 403

sig = ",".join(sorted(state.get("team_a_ids", []) + state.get("team_b_ids", [])))
db["system_settings"].setdefault("avoid_4", []).append({"sig": sig, "ts": _now(), "reason": reason})

now = _now()
for pid in state.get("team_a_ids", []) + state.get("team_b_ids", []):
    p = db["players"].get(pid)
    if not p:
        continue
    p["status"] = "queue"
    p["queue_join_ts"] = now
    p["cooldown_until"] = 0.0

db["courts"][cid] = None

changed = _maybe_run_automatch(db)
save_db_now(db)
return jsonify({"success": True, "automatch_triggered": changed})
```

@app.route(”/api/match/submit”, methods=[“POST”])
def submit_match():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
cid = str(d.get(“courtId”))
set_scores = d.get(“set_scores”, [])
if not uid or cid not in db[“courts”]:
return jsonify({“error”:“bad request”}), 400

```
state = db["courts"].get(cid)
if not state:
    return jsonify({"error":"No match"}), 400

is_staff = uid == SUPER_ADMIN_ID or uid in db["mod_ids"]
in_match = uid in state.get("team_a_ids", []) or uid in state.get("team_b_ids", [])
if not (is_staff or in_match):
    return jsonify({"error":"Unauthorized"}), 403

result, msg = _apply_match_results(db, state, set_scores)
if not result:
    return jsonify({"error": msg}), 400

now = _now()
start_at = float(state.get("start_at", now))
duration = int(max(0, now - start_at))

match_record = {
    "match_id": state.get("match_id"),
    "event_id": db["system_settings"].get("current_event_id"),
    "court_id": int(cid),
    "created_at": float(state.get("created_at", now)),
    "start_at": start_at,
    "end_at": now,
    "duration_sec": duration,
    "team_a_ids": state.get("team_a_ids", []),
    "team_b_ids": state.get("team_b_ids", []),
    "winner": result["winner"],
    "sets_won_a": result["sets_won_a"],
    "sets_won_b": result["sets_won_b"],
    "total_points_a": result["total_points_a"],
    "total_points_b": result["total_points_b"],
    "set_scores": result["set_scores"],
    "mmr_changes": result["mmr_changes"],
    "meta": {
        "score_factor": result["sf"],
        "expected_a": result["expected_a"],
        "scoring": db["system_settings"]["scoring"]
    }
}
db["match_history"].insert(0, match_record)

_recompute_avg_match_minutes(db)

cd_min = _cooldown_minutes(db)
cd_sec = cd_min * 60
for pid in state.get("team_a_ids", []) + state.get("team_b_ids", []):
    p = db["players"].get(pid)
    if not p:
        continue
    p["queue_join_ts"] = now
    if p.get("auto_rest") and cd_sec > 0:
        p["status"] = "resting"
        p["cooldown_until"] = now + cd_sec
    else:
        p["status"] = "queue"
        p["cooldown_until"] = 0.0

db["courts"][cid] = None

changed = _maybe_run_automatch(db)

save_db_now(db)
return jsonify({"success": True, "winner": result["winner"], "cooldown_min": cd_min, "automatch_triggered": changed})
```

# =========================

# Admin endpoints

# =========================

@app.route(”/api/admin/toggle_session”, methods=[“POST”])
def admin_toggle_session():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
action = d.get(“action”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403
if action not in [“start”,“end”]:
return jsonify({“error”:“bad action”}), 400

```
if action == "start":
    points = int(d.get("points", 21))
    bo = int(d.get("bo", 1))
    notify = bool(d.get("notify", False))
    event_id = d.get("eventId")  # if starting from a scheduled event

    if points not in [11, 21]:
        points = 21
    if bo not in [1,2,3]:
        bo = 1

    scoring = {"points": points, "bo": bo, "cap": 30}
    db["system_settings"]["is_session_active"] = True
    db["system_settings"]["notify_enabled"] = notify
    db["system_settings"]["scoring"] = scoring

    if event_id and event_id in db["events"]:
        # Activate existing scheduled event
        evt = db["events"][event_id]
        evt["status"] = "active"
        db["system_settings"]["current_event_id"] = event_id
    else:
        # Create new event with Thailand timezone name
        today_th = datetime.now(TH_TZ).strftime("%d/%m/%Y")
        eid = _create_event(db, name=f"ก๊วน {today_th}", dt_ts=_now(), status="active", scoring=scoring, notify=notify)
        db["system_settings"]["current_event_id"] = eid

else:
    db["system_settings"]["is_session_active"] = False
    eid = db["system_settings"].get("current_event_id")
    if eid and eid in db["events"]:
        db["events"][eid]["status"] = "ended"
    db["system_settings"]["current_event_id"] = None

    for cid in list(db["courts"].keys()):
        db["courts"][cid] = None

    for p in db["players"].values():
        p["status"] = "offline"
        p["queue_join_ts"] = 0.0
        p["cooldown_until"] = 0.0
        p["paired_with"] = None
        p["outgoing_req"] = None
        p["incoming_reqs"] = []
        p["priority_match"] = False

save_db_now(db)
return jsonify({"success": True})
```

@app.route(”/api/admin/update_courts”, methods=[“POST”])
def admin_update_courts():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403
try:
c = int(d.get(“count”, 2))
c = max(1, min(10, c))
except Exception:
c = 2
db[“system_settings”][“total_courts”] = c
_refresh_courts(db)
save_db(db)
return jsonify({“success”: True, “total_courts”: c})

@app.route(”/api/admin/set_automatch”, methods=[“POST”])
def admin_set_automatch():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403
cid = str(d.get(“courtId”))
val = bool(d.get(“value”, False))
if cid not in db[“system_settings”][“automatch”]:
return jsonify({“error”:“invalid court”}), 400
db[“system_settings”][“automatch”][cid] = val
if val and db[“courts”].get(cid) is None:
_maybe_run_automatch(db)
save_db(db)
return jsonify({“success”: True, “courtId”: cid, “value”: val})

@app.route(”/api/admin/manage_mod”, methods=[“POST”])
def admin_manage_mod():
db = get_db()
d = request.json or {}
if d.get(“requesterId”) != SUPER_ADMIN_ID:
return jsonify({“error”:“Super Admin Only”}), 403
tid = d.get(“targetUserId”)
action = d.get(“action”)
if not tid or tid not in db[“players”]:
return jsonify({“error”:“Target not found”}), 404
if action == “promote”:
if tid not in db[“mod_ids”]:
db[“mod_ids”].append(tid)
elif action == “demote”:
if tid in db[“mod_ids”]:
db[“mod_ids”].remove(tid)
else:
return jsonify({“error”:“bad action”}), 400
save_db_now(db)
return jsonify({“success”: True, “mod_ids”: db[“mod_ids”]})

@app.route(”/api/admin/set_mmr”, methods=[“POST”])
def admin_set_mmr():
db = get_db()
d = request.json or {}
uid = d.get(“requesterId”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403
tid = d.get(“targetUserId”)
new = d.get(“newMmr”)
if not tid or tid not in db[“players”]:
return jsonify({“error”:“Target not found”}), 404
try:
nv = int(new)
except Exception:
return jsonify({“error”:“Invalid mmr”}), 400
db[“players”][tid][“mmr”] = nv
save_db_now(db)
return jsonify({“success”: True})

@app.route(”/api/admin/skip_queue”, methods=[“POST”])
def admin_skip_queue():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403

```
target = d.get("targetId")
if not target or target not in db["players"]:
    return jsonify({"error":"ไม่พบผู้เล่น"}), 404

p = db["players"][target]
if p.get("status") != "queue":
    return jsonify({"error":"ผู้เล่นยังไม่ได้อยู่ในคิว"}), 400

p["priority_match"] = True

# Also set priority for partner if paired
pw = p.get("paired_with")
if pw and pw in db["players"] and db["players"][pw].get("status") == "queue":
    db["players"][pw]["priority_match"] = True

# Try to run automatch immediately
changed = _maybe_run_automatch(db)

save_db(db)
return jsonify({"success": True, "auto_matched": changed})
```

@app.route(”/api/admin/cancel_skip_queue”, methods=[“POST”])
def admin_cancel_skip_queue():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403

```
target = d.get("targetId")
if not target or target not in db["players"]:
    return jsonify({"error":"ไม่พบผู้เล่น"}), 404

p = db["players"][target]
p["priority_match"] = False

# Also cancel for partner if paired
pw = p.get("paired_with")
if pw and pw in db["players"]:
    db["players"][pw]["priority_match"] = False

save_db(db)
return jsonify({"success": True})
```

@app.route(”/api/admin/hard_reset”, methods=[“POST”])
def admin_hard_reset():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if uid != SUPER_ADMIN_ID:
return jsonify({“error”:“Super Admin เท่านั้น”}), 403

```
mode = d.get("mode", "stats")

if mode == "all":
    # Complete wipe — back to empty DB
    global _DB_CACHE
    new_db = deepcopy(DEFAULT_DB)
    _refresh_courts(new_db)
    _DB_CACHE = new_db
    save_db_now(new_db)
    return jsonify({"success": True, "mode": "all"})

elif mode == "stats":
    # Keep players (name, pic, role) but reset all stats
    stat_keys = [
        "mmr", "calib_played", "calib_wins", "calib_losses", "calib_streak",
        "sets_w", "sets_l", "points_for", "points_against",
        "match_w", "match_l", "best_streak", "cur_streak"
    ]
    for p in db["players"].values():
        for k in stat_keys:
            p[k] = 1000 if k == "mmr" else 0
        p["status"] = "offline"
        p["queue_join_ts"] = 0.0
        p["cooldown_until"] = 0.0
        p["priority_match"] = False
        p["paired_with"] = None
        p["outgoing_req"] = None
        p["incoming_reqs"] = []
        p["auto_rest"] = False

    # Clear match history, events, courts, diversity
    db["match_history"] = []
    db["events"] = {}
    db["courts"] = {}
    db["system_settings"]["is_session_active"] = False
    db["system_settings"]["current_event_id"] = None
    db["system_settings"]["avoid_4"] = []
    db["system_settings"]["recent_teammates"] = {}
    db["system_settings"]["recent_opponents"] = {}
    db["system_settings"]["automatch"] = {}

    save_db_now(db)
    return jsonify({"success": True, "mode": "stats"})

return jsonify({"error": "Invalid mode"}), 400
```

@app.route(”/api/event/create”, methods=[“POST”])
def event_create():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403

```
dt = d.get("datetime")
if dt is None:
    return jsonify({"error":"กรุณาระบุเวลาเริ่ม"}), 400
try:
    dt = float(dt)
except Exception:
    dt = _now()

# Reject past start time (allow 2 min tolerance)
if dt < _now() - 120:
    return jsonify({"error":"ไม่สามารถตั้งเวลาเริ่มย้อนหลังได้"}), 400

# Name: optional — auto-generate from event date if empty
name = (d.get("name") or "").strip()
if not name:
    evt_dt_th = datetime.fromtimestamp(dt, tz=TH_TZ)
    name = f"ก๊วน {evt_dt_th.strftime('%d/%m/%Y')}"

# End datetime: default to start + 4 hours if not provided
end_dt = d.get("end_datetime")
if end_dt is not None:
    try:
        end_dt = float(end_dt)
        if end_dt <= dt:
            return jsonify({"error":"เวลาสิ้นสุดต้องหลังเวลาเริ่ม"}), 400
    except Exception:
        end_dt = dt + (4 * 3600)
else:
    end_dt = dt + (4 * 3600)  # default +4h

# Scoring settings
points = int(d.get("points", 21))
bo = int(d.get("bo", 1))
notify = bool(d.get("notify", False))
location = d.get("location", "")
if points not in [11, 21]:
    points = 21
if bo not in [1, 2, 3]:
    bo = 1
scoring = {"points": points, "bo": bo, "cap": 30}

eid = _create_event(db, name=name, dt_ts=dt, status="open", scoring=scoring, location=location, notify=notify, end_datetime=end_dt)
save_db_now(db)
return jsonify({"success": True, "eventId": eid})
```

@app.route(”/api/event/delete”, methods=[“POST”])
def event_delete():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
if uid != SUPER_ADMIN_ID and uid not in db[“mod_ids”]:
return jsonify({“error”:“Unauthorized”}), 403
eid = d.get(“eventId”)
if not eid or eid not in db[“events”]:
return jsonify({“error”:“Not found”}), 404
if db[“system_settings”].get(“current_event_id”) == eid and db[“system_settings”].get(“is_session_active”):
return jsonify({“error”:“Can’t delete active session event”}), 400
db[“events”].pop(eid, None)
save_db_now(db)
return jsonify({“success”: True})

@app.route(”/api/event/join”, methods=[“POST”])
def event_join():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
eid = d.get(“eventId”)
if not uid or not eid:
return jsonify({“error”: “missing data”}), 400
if uid not in db[“players”]:
return jsonify({“error”: “user not found”}), 404
if eid not in db[“events”]:
return jsonify({“error”: “event not found”}), 404

```
evt = db["events"][eid]
# Only allow pre-registration for open (scheduled future) events
if evt.get("status") != "open":
    return jsonify({"error": "สามารถลงชื่อได้เฉพาะ Event ที่ยังไม่เริ่มเท่านั้น"}), 400

pre = evt.setdefault("pre_registered", [])
if uid not in pre:
    pre.append(uid)
save_db(db)
return jsonify({"success": True})
```

@app.route(”/api/event/leave”, methods=[“POST”])
def event_leave():
db = get_db()
d = request.json or {}
uid = d.get(“userId”)
eid = d.get(“eventId”)
if not uid or not eid:
return jsonify({“error”: “missing data”}), 400
if eid not in db[“events”]:
return jsonify({“error”: “event not found”}), 404

```
evt = db["events"][eid]
if evt.get("status") != "open":
    return jsonify({"error": "ไม่สามารถยกเลิกได้ (event เริ่มแล้ว)"}), 400

pre = evt.get("pre_registered", [])
if uid in pre:
    pre.remove(uid)
save_db(db)
return jsonify({"success": True})
```

# Load DB into memory at import time

_load_db_from_disk()

if **name** == “**main**”:
app.run(host=“0.0.0.0”, port=5000, debug=True)
