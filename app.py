import json
import os
import time
import uuid
import sys
from datetime import datetime
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# ---------------- CONFIG ----------------
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"
DATA_FILE = "/var/data/izesquad_data.json"

# ---------------- DEFAULT DB ----------------
default_db = {
    "system_settings": {
        "total_courts": 2,
        "is_session_active": False,
        "current_event_id": None,
        "session_config": {
            "target_points": 21,     # 11 or 21
            "bo": 1,                 # 1/2/3
            "ready_check": False,    # default off (not used in this version)
            "notify_enabled": False  # mod can toggle; frontend uses sound/vibrate anyway
        },
        "court_automatch": {},       # courtId(str) -> bool
        "avoid_teammate_pairs": []   # list of {"a":uid,"b":uid,"until":ts}
    },
    "mod_ids": [],
    "players": {},
    "events": {},                   # session history
    "match_history": [],            # global completed matches (latest first)
    "billing_history": [],
    "courts_state": {}              # courtId(str) -> match dict or None
}

# ---------------- LOGGER ----------------
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stdout)
    sys.stdout.flush()

# ---------------- FILE OPS ----------------
def init_db():
    directory = os.path.dirname(DATA_FILE)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(default_db, f, ensure_ascii=False, indent=4)

def get_db():
    init_db()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # fill missing keys
        for k in default_db:
            if k not in data:
                data[k] = default_db[k]
        # deep fill for system_settings
        for k in default_db["system_settings"]:
            if k not in data["system_settings"]:
                data["system_settings"][k] = default_db["system_settings"][k]
        # ensure dicts exist
        if "court_automatch" not in data["system_settings"]:
            data["system_settings"]["court_automatch"] = {}
        if "avoid_teammate_pairs" not in data["system_settings"]:
            data["system_settings"]["avoid_teammate_pairs"] = []
        if "courts_state" not in data:
            data["courts_state"] = {}
        return data
    except Exception as e:
        log(f"Error reading DB: {e}")
        return json.loads(json.dumps(default_db))

def save_db(data):
    # atomic save
    try:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        os.replace(tmp, DATA_FILE)
    except Exception as e:
        log(f"CRITICAL ERROR SAVING DB: {e}")

def refresh_courts(db):
    target = int(db["system_settings"].get("total_courts", 2))
    # courts_state
    for i in range(1, target + 1):
        k = str(i)
        if k not in db["courts_state"]:
            db["courts_state"][k] = None
    for k in list(db["courts_state"].keys()):
        if int(k) > target:
            del db["courts_state"][k]

    # automatch map
    cam = db["system_settings"].get("court_automatch", {})
    for i in range(1, target + 1):
        k = str(i)
        if k not in cam:
            cam[k] = False
    for k in list(cam.keys()):
        if int(k) > target:
            del cam[k]
    db["system_settings"]["court_automatch"] = cam

# ---------------- AUTH ----------------
def is_super(uid):
    return uid == SUPER_ADMIN_ID

def is_mod(db, uid):
    return uid in db.get("mod_ids", []) or is_super(uid)

# ---------------- RANK / DISPLAY ----------------
def get_rank_title(mmr):
    try:
        val = int(mmr)
    except:
        val = 1000
    if val <= 800:
        return "NOOB DOG 🐶"
    elif val <= 1200:
        return "NOOB 🐣"
    elif val <= 1400:
        return "เด็กกระโปก 👶"
    elif val <= 1600:
        return "ชนะจนเบื่อ 🥱"
    else:
        return "โปรเพรเย๋อ 👽"

def is_unranked(p):
    return int(p.get("calibrate_games", 0)) < 10

def display_rank_text(p):
    cg = int(p.get("calibrate_games", 0))
    if cg < 10:
        return f"UNRANKED ({cg}/10)"
    return get_rank_title(p.get("mmr", 1000))

# ---------------- MATCH TIME AVERAGE (last 10 completed) ----------------
def compute_avg_match_minutes(db, n=10, default=18, min_cap=5, max_cap=60):
    durations = []
    for m in db.get("match_history", []):
        if m.get("status") != "completed":
            continue
        dur_sec = m.get("duration_sec")
        if isinstance(dur_sec, (int, float)):
            dm = dur_sec / 60.0
        else:
            dm = m.get("duration_min")
            if not isinstance(dm, (int, float)):
                continue
            dm = float(dm)
        dm = max(min_cap, min(max_cap, dm))
        durations.append(dm)
        if len(durations) >= n:
            break
    if not durations:
        return default
    return sum(durations) / len(durations)

# ---------------- SCORE VALIDATION (deuce to 30) ----------------
def validate_set_score(a, b, target):
    # returns (ok, msg)
    if not isinstance(a, int) or not isinstance(b, int):
        return (False, "คะแนนต้องเป็นตัวเลข")
    if a < 0 or b < 0:
        return (False, "คะแนนต้องไม่ติดลบ")
    if a > 30 or b > 30:
        return (False, "คะแนนสูงสุดคือ 30")
    if a == b:
        return (False, "ห้ามเสมอ")

    win = max(a, b)
    lose = min(a, b)

    if win < target:
        return (False, f"แต้มต้องถึง {target} หรือดิวส์")
    if win == 30:
        if lose == 29:
            return (True, "")
        return (False, "ถ้าแต้มถึง 30 ต้องเป็น 30-29 เท่านั้น")
    # win < 30
    if (win - lose) >= 2:
        return (True, "")
    return (False, "ดิวส์ต้องชนะห่าง 2 แต้ม (จนกว่าจะถึง 30-29)")

def determine_winner_from_scores(set_scores, config):
    """
    set_scores: list of {"a":int,"b":int}
    config: {"target_points": 11/21, "bo":1/2/3}
    """
    target = int(config.get("target_points", 21))
    bo = int(config.get("bo", 1))

    if bo == 1:
        if len(set_scores) != 1:
            return (None, "BO1 ต้องมี 1 เซต")
    elif bo == 2:
        if len(set_scores) != 2:
            return (None, "BO2 ต้องมี 2 เซต")
    elif bo == 3:
        if len(set_scores) < 2 or len(set_scores) > 3:
            return (None, "BO3 ต้องมี 2-3 เซต")
    else:
        return (None, "ค่า BO ไม่ถูกต้อง")

    # validate each set
    for s in set_scores:
        ok, msg = validate_set_score(int(s["a"]), int(s["b"]), target)
        if not ok:
            return (None, msg)

    pointsA = sum(int(s["a"]) for s in set_scores)
    pointsB = sum(int(s["b"]) for s in set_scores)

    sets_won_a = 0
    sets_won_b = 0
    for s in set_scores:
        if int(s["a"]) > int(s["b"]):
            sets_won_a += 1
        else:
            sets_won_b += 1

    # winner rule
    if bo == 1:
        winner = "A" if sets_won_a == 1 else "B"
    elif bo == 2:
        if pointsA == pointsB:
            return (None, "BO2 นับแต้มรวม ห้ามแต้มรวมเสมอ")
        winner = "A" if pointsA > pointsB else "B"
    else:  # bo3
        if sets_won_a >= 2:
            winner = "A"
        elif sets_won_b >= 2:
            winner = "B"
        else:
            return (None, "BO3 ต้องมีฝ่ายใดฝ่ายหนึ่งชนะถึง 2 เซต")

    return ({
        "winner": winner,
        "sets_won_a": sets_won_a,
        "sets_won_b": sets_won_b,
        "points_a_total": pointsA,
        "points_b_total": pointsB
    }, "")

# ---------------- MMR CALC (Elo + score margin + format factor) ----------------
def k_for_player(p):
    cg = int(p.get("calibrate_games", 0))
    # K = 90 - 4*games_played, not lower than 50
    return max(50.0, 90.0 - 4.0 * cg) if cg < 10 else 50.0

def calc_expected(teamA_mmr, teamB_mmr):
    # Elo 400 scale
    return 1.0 / (1.0 + 10 ** ((teamB_mmr - teamA_mmr) / 400.0))

def margin_multiplier(pointsA, pointsB):
    diff = abs(pointsA - pointsB)
    total = max(1, pointsA + pointsB)
    margin = diff / total
    m = 1.0 + margin
    # clamp 0.85 .. 1.15
    return max(0.85, min(1.15, m))

def format_factor(bo):
    bo = int(bo)
    if bo == 2:
        return 1.05
    if bo == 3:
        return 1.10
    return 1.00

def compute_team_mmr(db, team_ids):
    vals = []
    for uid in team_ids:
        p = db["players"].get(uid)
        if not p:
            continue
        try:
            vals.append(int(p.get("mmr", 1000)))
        except:
            vals.append(1000)
    if not vals:
        return 1000.0
    return sum(vals) / len(vals)

def compute_mmr_delta(db, team_a_ids, team_b_ids, winner, pointsA, pointsB, bo):
    teamA = compute_team_mmr(db, team_a_ids)
    teamB = compute_team_mmr(db, team_b_ids)
    EA = calc_expected(teamA, teamB)
    SA = 1.0 if winner == "A" else 0.0

    # K base = average K of all 4 players (keeps near zero-sum)
    ks = []
    for uid in (team_a_ids + team_b_ids):
        p = db["players"].get(uid)
        if p:
            ks.append(k_for_player(p))
    K = sum(ks) / len(ks) if ks else 50.0

    M = margin_multiplier(pointsA, pointsB)
    F = format_factor(bo)

    delta_team = K * (SA - EA) * M * F
    # winners get +round, losers get -round
    d = int(round(delta_team))
    if d == 0:
        # keep minimum movement if extremely tiny? user didn't ask; keep 0 possible
        pass

    delta_by_player = {}
    if winner == "A":
        for uid in team_a_ids:
            delta_by_player[uid] = abs(d)
        for uid in team_b_ids:
            delta_by_player[uid] = -abs(d)
    else:
        for uid in team_a_ids:
            delta_by_player[uid] = -abs(d)
        for uid in team_b_ids:
            delta_by_player[uid] = abs(d)

    return delta_by_player

# ---------------- STATS FROM HISTORY ----------------
def compute_player_stats(db, uid, limit=None):
    """
    stats by SET (as requested) + points for/against totals
    """
    sets_won = 0
    sets_lost = 0
    points_for = 0
    points_against = 0
    matches = 0

    hist = db.get("match_history", [])
    for m in hist:
        if m.get("status") != "completed":
            continue
        a_ids = m.get("team_a_ids", [])
        b_ids = m.get("team_b_ids", [])
        if uid not in a_ids and uid not in b_ids:
            continue

        matches += 1
        sw_a = int(m.get("sets_won_a", 0))
        sw_b = int(m.get("sets_won_b", 0))
        pa = int(m.get("points_a_total", 0))
        pb = int(m.get("points_b_total", 0))

        if uid in a_ids:
            sets_won += sw_a
            sets_lost += sw_b
            points_for += pa
            points_against += pb
        else:
            sets_won += sw_b
            sets_lost += sw_a
            points_for += pb
            points_against += pa

        if limit and matches >= limit:
            break

    total_sets = sets_won + sets_lost
    win_rate = int(round((sets_won / total_sets) * 100)) if total_sets > 0 else 0
    diff = points_for - points_against

    return {
        "set_winrate": win_rate,
        "sets_won": sets_won,
        "sets_lost": sets_lost,
        "points_for_total": points_for,
        "points_against_total": points_against,
        "point_diff_total": diff
    }

# ---------------- PARTNER REQUEST LOGIC ----------------
def remove_from_incoming(db, target_id, requester_id):
    t = db["players"].get(target_id)
    if not t:
        return
    inc = t.get("incoming_requests", [])
    if requester_id in inc:
        inc.remove(requester_id)
    t["incoming_requests"] = inc

def cancel_outgoing(db, requester_id):
    p = db["players"].get(requester_id)
    if not p:
        return
    tgt = p.get("outgoing_request_to")
    if tgt:
        remove_from_incoming(db, tgt, requester_id)
    p["outgoing_request_to"] = None

def unpair(db, uid):
    p = db["players"].get(uid)
    if not p:
        return
    partner = p.get("pair_lock")
    p["pair_lock"] = None
    if partner and partner in db["players"]:
        db["players"][partner]["pair_lock"] = None

# ---------------- MATCHMAKING ----------------
def now_ts():
    return time.time()

def clean_expired_avoid_pairs(db):
    ap = db["system_settings"].get("avoid_teammate_pairs", [])
    t = now_ts()
    ap = [x for x in ap if x.get("until", 0) > t]
    db["system_settings"]["avoid_teammate_pairs"] = ap

def is_avoid_pair(db, u1, u2):
    a = str(min(u1, u2))
    b = str(max(u1, u2))
    for x in db["system_settings"].get("avoid_teammate_pairs", []):
        if x.get("a") == a and x.get("b") == b and x.get("until", 0) > now_ts():
            return True
    return False

def eligible_players(db):
    """
    Eligible for matchmaking:
    - session active
    - status == active
    - not playing
    - not resting (resting True or rest_until in future)
    - if pair_lock exists, partner must also be eligible (so they can play together)
    """
    if not db["system_settings"].get("is_session_active"):
        return []

    t = now_ts()
    players = []
    for p in db["players"].values():
        if p.get("status") != "active":
            continue
        if p.get("resting"):
            # resting but keep queue time; not eligible
            ru = p.get("rest_until")
            if ru and isinstance(ru, (int, float)) and ru <= t:
                # auto rest expired
                p["resting"] = False
                p["rest_until"] = None
            else:
                continue

        # if locked pair, ensure partner eligible too
        partner = p.get("pair_lock")
        if partner:
            pp = db["players"].get(partner)
            if not pp:
                continue
            if pp.get("status") != "active":
                continue
            if pp.get("resting"):
                ru2 = pp.get("rest_until")
                if ru2 and isinstance(ru2, (int, float)) and ru2 <= t:
                    pp["resting"] = False
                    pp["rest_until"] = None
                else:
                    continue
        # must have queue_join_time
        if not isinstance(p.get("queue_join_time"), (int, float)):
            p["queue_join_time"] = t
        players.append(p)
    return players

def build_groups_for_queue(db, players):
    """
    Groups = locked pairs (size 2) or singles (size 1)
    group_wait = max(queue_join_time of members) to avoid pair jumping ahead unfairly
    """
    seen = set()
    groups = []
    for p in players:
        uid = p["id"]
        if uid in seen:
            continue
        partner = p.get("pair_lock")
        if partner and partner in db["players"]:
            pp = db["players"][partner]
            if pp.get("status") == "active" and (not pp.get("resting")):
                qt = max(float(p.get("queue_join_time", now_ts())), float(pp.get("queue_join_time", now_ts())))
                groups.append({"type": "pair", "members": [p, pp], "qt": qt})
                seen.add(uid)
                seen.add(partner)
                continue
        groups.append({"type": "single", "members": [p], "qt": float(p.get("queue_join_time", now_ts()))})
        seen.add(uid)
    groups.sort(key=lambda g: g["qt"])
    return groups

def fairness_score(db, teamA_ids, teamB_ids):
    # team avg diff + penalty for internal diff to avoid "carry" pairs
    teamA = compute_team_mmr(db, teamA_ids)
    teamB = compute_team_mmr(db, teamB_ids)
    diff_team = abs(teamA - teamB)

    def internal_gap(ids):
        if len(ids) != 2:
            return 0.0
        a = int(db["players"][ids[0]].get("mmr", 1000))
        b = int(db["players"][ids[1]].get("mmr", 1000))
        return abs(a - b)

    gapA = internal_gap(teamA_ids)
    gapB = internal_gap(teamB_ids)
    internal_max = max(gapA, gapB)

    # avoid same teammate pair right after cancel (hard penalty)
    avoid_pen = 0.0
    if len(teamA_ids) == 2 and is_avoid_pair(db, teamA_ids[0], teamA_ids[1]):
        avoid_pen += 99999.0
    if len(teamB_ids) == 2 and is_avoid_pair(db, teamB_ids[0], teamB_ids[1]):
        avoid_pen += 99999.0

    alpha = 0.6  # penalty weight for carry pairing
    return diff_team + alpha * internal_max + avoid_pen

def best_team_split(db, ids, locked_pairs):
    """
    Try all partitions into 2 teams of 2, respecting locked pairs
    """
    if len(ids) != 4:
        return None

    candidates = [
        ([ids[0], ids[1]], [ids[2], ids[3]]),
        ([ids[0], ids[2]], [ids[1], ids[3]]),
        ([ids[0], ids[3]], [ids[1], ids[2]]),
    ]

    def respects_locked(teamA, teamB):
        for a, b in locked_pairs:
            # must be in same team
            inA = (a in teamA and b in teamA)
            inB = (a in teamB and b in teamB)
            if not (inA or inB):
                return False
        return True

    best = None
    best_score = None
    for ta, tb in candidates:
        if not respects_locked(ta, tb):
            continue
        sc = fairness_score(db, ta, tb)
        if best is None or sc < best_score:
            best = (ta, tb)
            best_score = sc
    return best

def choose_best_four(db, max_candidates=10):
    """
    Priority waiting time first, then fairness.
    We evaluate combinations of 4 among first K candidates (by queue time),
    but we strongly weight waiting rank to keep oldest-first.
    """
    clean_expired_avoid_pairs(db)

    players = eligible_players(db)
    if len(players) < 4:
        return None

    # sort by queue time
    players.sort(key=lambda p: float(p.get("queue_join_time", now_ts())))
    # build groups (pairs/singles)
    groups = build_groups_for_queue(db, players)

    # flatten to unique player list (keep order by queue)
    ordered = []
    seen = set()
    for g in groups:
        for m in g["members"]:
            if m["id"] not in seen:
                ordered.append(m)
                seen.add(m["id"])

    K = min(max_candidates, len(ordered))
    cand = ordered[:K]

    # locked pairs inside cand
    locked_pairs = set()
    for p in cand:
        partner = p.get("pair_lock")
        if partner and partner in db["players"]:
            a = p["id"]
            b = partner
            if a in [x["id"] for x in cand] and b in [x["id"] for x in cand]:
                locked_pairs.add(tuple(sorted([a, b])))

    cand_ids = [p["id"] for p in cand]
    rank_index = {uid: i for i, uid in enumerate(cand_ids)}

    # helper to validate combination respects locked pairs (must include both or neither)
    def valid_combo(combo_ids):
        s = set(combo_ids)
        for a, b in locked_pairs:
            if (a in s) ^ (b in s):
                return False
        return True

    # enumerate combinations
    import itertools
    best_choice = None
    best_score = None

    W = 10000.0  # huge to ensure waiting dominates
    for combo in itertools.combinations(cand_ids, 4):
        if not valid_combo(combo):
            continue

        # waiting penalty
        wait_pen = sum(rank_index[u] for u in combo) * W

        # locked pairs within combo
        lp = []
        for a, b in locked_pairs:
            if a in combo and b in combo:
                lp.append((a, b))

        split = best_team_split(db, list(combo), lp)
        if not split:
            continue
        ta, tb = split
        fair = fairness_score(db, ta, tb)

        score = wait_pen + fair
        if best_choice is None or score < best_score:
            best_choice = (ta, tb)
            best_score = score

    return best_choice

def start_match_on_court(db, court_id, teamA_ids, teamB_ids, source="auto"):
    """
    Create match with 60s countdown; store queue snapshot for cancel restore.
    """
    t = now_ts()
    match_id = str(uuid.uuid4())[:8]
    teamA_names = [db["players"][u]["nickname"] for u in teamA_ids]
    teamB_names = [db["players"][u]["nickname"] for u in teamB_ids]

    queue_snapshot = {}
    for uid in teamA_ids + teamB_ids:
        p = db["players"].get(uid)
        if not p:
            continue
        queue_snapshot[uid] = float(p.get("queue_join_time", t))

    # set players status to playing
    for uid in teamA_ids + teamB_ids:
        p = db["players"].get(uid)
        if p:
            p["status"] = "playing"
            p["last_active"] = t

    m = {
        "match_id": match_id,
        "court_id": str(court_id),
        "created_at": t,
        "start_at": t + 60.0,  # start timer after 1 minute
        "team_a_ids": teamA_ids,
        "team_b_ids": teamB_ids,
        "team_a": teamA_names,
        "team_b": teamB_names,
        "queue_snapshot": queue_snapshot,
        "source": source
    }
    db["courts_state"][str(court_id)] = m
    return m

def try_automatch_for_court(db, court_id):
    refresh_courts(db)
    if not db["system_settings"].get("is_session_active"):
        return False
    cam = db["system_settings"].get("court_automatch", {})
    if not cam.get(str(court_id), False):
        return False
    if db["courts_state"].get(str(court_id)) is not None:
        return False

    choice = choose_best_four(db)
    if not choice:
        return False
    teamA_ids, teamB_ids = choice
    start_match_on_court(db, court_id, teamA_ids, teamB_ids, source="automatch")
    return True

def try_automatch_all(db):
    refresh_courts(db)
    changed = False
    for cid in sorted(db["courts_state"].keys(), key=lambda x: int(x)):
        if try_automatch_for_court(db, cid):
            changed = True
    return changed

def first_empty_non_auto_court(db):
    refresh_courts(db)
    cam = db["system_settings"].get("court_automatch", {})
    # prefer courts with automatch OFF
    for cid in sorted(db["courts_state"].keys(), key=lambda x: int(x)):
        if db["courts_state"][cid] is None and (not cam.get(str(cid), False)):
            return cid
    # fallback any empty
    for cid in sorted(db["courts_state"].keys(), key=lambda x: int(x)):
        if db["courts_state"][cid] is None:
            return cid
    return None

# ---------------- ROUTES ----------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/login", methods=["POST"])
def api_login():
    try:
        db = get_db()
        refresh_courts(db)
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
                "last_active": now_ts(),
                "queue_join_time": None,
                "resting": False,
                "rest_until": None,
                "auto_rest": False,
                "outgoing_request_to": None,
                "incoming_requests": [],
                "pair_lock": None,
                "calibrate_games": 0
            }
            log(f"New user: {db['players'][uid]['nickname']}")
        else:
            db["players"][uid]["pictureUrl"] = d.get("pictureUrl") or db["players"][uid].get("pictureUrl", "")
            if d.get("displayName"):
                db["players"][uid]["nickname"] = d.get("displayName")

        save_db(db)
        p = db["players"][uid]
        role = "super" if is_super(uid) else ("mod" if uid in db.get("mod_ids", []) else "user")

        # minimal response
        return jsonify({
            "id": p["id"],
            "nickname": p["nickname"],
            "pictureUrl": p.get("pictureUrl", ""),
            "role": role
        })
    except Exception as e:
        log(f"Login error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/get_dashboard")
def api_get_dashboard():
    """
    Optional query: uid=... to enrich "me" data and my history
    """
    try:
        db = get_db()
        refresh_courts(db)
        clean_expired_avoid_pairs(db)

        uid = request.args.get("uid")

        # opportunistic automatch (safe): if any court automatch ON and empty, try
        # (helps when no one triggers explicit call)
        if db["system_settings"].get("is_session_active"):
            # only run if at least one court automatch is on
            if any(db["system_settings"].get("court_automatch", {}).values()):
                changed = try_automatch_all(db)
                if changed:
                    save_db(db)

        t = now_ts()

        # enrich courts with elapsed / countdown
        courts = {}
        for cid, m in db["courts_state"].items():
            if m:
                start_at = float(m.get("start_at", m.get("created_at", t)))
                if t < start_at:
                    m_view = dict(m)
                    m_view["state"] = "countdown"
                    m_view["starts_in_sec"] = int(max(0, start_at - t))
                    m_view["elapsed_sec"] = 0
                else:
                    m_view = dict(m)
                    m_view["state"] = "live"
                    m_view["starts_in_sec"] = 0
                    m_view["elapsed_sec"] = int(t - start_at)

                # attach player data
                def pack_team(ids):
                    out = []
                    for pid in ids:
                        pl = db["players"].get(pid)
                        if pl:
                            out.append({"id": pid, "nickname": pl["nickname"], "pictureUrl": pl.get("pictureUrl", "")})
                    return out

                m_view["team_a_data"] = pack_team(m.get("team_a_ids", []))
                m_view["team_b_data"] = pack_team(m.get("team_b_ids", []))
                courts[cid] = m_view
            else:
                courts[cid] = None

        # players minimal + queue list (active + playing)
        all_players = []
        for p in db["players"].values():
            status = p.get("status", "offline")
            qjt = p.get("queue_join_time")
            wait_min = int((t - float(qjt)) / 60) if isinstance(qjt, (int, float)) and status == "active" else 0

            all_players.append({
                "id": p["id"],
                "nickname": p["nickname"],
                "pictureUrl": p.get("pictureUrl", ""),
                "status": status,
                "wait_min": wait_min,
                "resting": bool(p.get("resting")),
                "auto_rest": bool(p.get("auto_rest")),
                "outgoing_request_to": p.get("outgoing_request_to"),
                "incoming_requests": p.get("incoming_requests", []),
                "pair_lock": p.get("pair_lock"),
                "is_mod": (p["id"] in db.get("mod_ids", [])) or is_super(p["id"]),
                "is_unranked": is_unranked(p),
                "rank_text": display_rank_text(p),
                # do not expose mmr if unranked (frontend should not show)
                "mmr": int(p.get("mmr", 1000)) if not is_unranked(p) else None
            })

        # queue = active only (exclude playing/offline). resting stay in list but flagged
        queue = [x for x in all_players if x["status"] == "active"]
        queue.sort(key=lambda x: x["wait_min"], reverse=True)  # longest wait first display
        queue_count = len([x for x in queue if not x["resting"]])

        # histories
        def enrich_match(m):
            def pack_ids(ids):
                arr = []
                for pid in ids:
                    pl = db["players"].get(pid)
                    if pl:
                        arr.append({"id": pid, "nickname": pl["nickname"], "pictureUrl": pl.get("pictureUrl", "")})
                return arr
            mm = dict(m)
            mm["team_a_data"] = pack_ids(m.get("team_a_ids", []))
            mm["team_b_data"] = pack_ids(m.get("team_b_ids", []))
            return mm

        global_hist = [enrich_match(m) for m in db.get("match_history", [])[:50]]

        my_hist = []
        if uid and uid in db["players"]:
            for m in db.get("match_history", []):
                if m.get("status") != "completed":
                    continue
                if uid in m.get("team_a_ids", []) or uid in m.get("team_b_ids", []):
                    my_hist.append(enrich_match(m))
                if len(my_hist) >= 50:
                    break

        # leaderboards (3 tabs) with unranked always at bottom
        def leaderboard_rows():
            rows = []
            for p in db["players"].values():
                st = compute_player_stats(db, p["id"])
                rows.append({
                    "id": p["id"],
                    "nickname": p["nickname"],
                    "pictureUrl": p.get("pictureUrl", ""),
                    "is_unranked": is_unranked(p),
                    "rank_text": display_rank_text(p),
                    "mmr": int(p.get("mmr", 1000)),
                    "points_for_total": int(st["points_for_total"]),
                    "points_against_total": int(st["points_against_total"]),
                    "point_diff_total": int(st["point_diff_total"]),
                    "set_winrate": int(st["set_winrate"]),
                    "sets_won": int(st["sets_won"]),
                    "sets_lost": int(st["sets_lost"])
                })
            return rows

        rows = leaderboard_rows()

        def bucket_key(r):
            return 1 if r["is_unranked"] else 0  # ranked first

        # MMR board: mmr desc, tie diff, points_for, winrate
        lb_mmr = sorted(rows, key=lambda r: (
            bucket_key(r),
            -r["mmr"],
            -r["point_diff_total"],
            -r["points_for_total"],
            -r["set_winrate"],
            -(r["sets_won"])
        ))

        # POINTS board: points_for desc, tie diff, winrate, mmr
        lb_points = sorted(rows, key=lambda r: (
            bucket_key(r),
            -r["points_for_total"],
            -r["point_diff_total"],
            -r["set_winrate"],
            -r["mmr"]
        ))

        # WINRATE board: winrate desc, tie sets_won, diff, mmr
        lb_winrate = sorted(rows, key=lambda r: (
            bucket_key(r),
            -r["set_winrate"],
            -(r["sets_won"]),
            -r["point_diff_total"],
            -r["mmr"]
        ))

        # events (sessions) with participants (for billing)
        events = []
        for eid, e in db.get("events", {}).items():
            ev = dict(e)
            # participants data
            participants = []
            for pid in e.get("participants", []):
                pl = db["players"].get(pid)
                if pl:
                    participants.append({
                        "id": pid,
                        "nickname": pl["nickname"],
                        "pictureUrl": pl.get("pictureUrl", ""),
                        "is_unranked": is_unranked(pl),
                        "rank_text": display_rank_text(pl)
                    })
            ev["participants_data"] = participants
            events.append(ev)
        # sort newest first by start_time
        events.sort(key=lambda x: float(x.get("start_time", 0)), reverse=True)

        me = None
        if uid and uid in db["players"]:
            p = db["players"][uid]
            st = compute_player_stats(db, uid)
            me = {
                "id": uid,
                "nickname": p["nickname"],
                "pictureUrl": p.get("pictureUrl", ""),
                "role": "super" if is_super(uid) else ("mod" if uid in db.get("mod_ids", []) else "user"),
                "status": p.get("status", "offline"),
                "resting": bool(p.get("resting")),
                "auto_rest": bool(p.get("auto_rest")),
                "outgoing_request_to": p.get("outgoing_request_to"),
                "incoming_requests": p.get("incoming_requests", []),
                "pair_lock": p.get("pair_lock"),
                "rank_text": display_rank_text(p),
                "is_unranked": is_unranked(p),
                "mmr": int(p.get("mmr", 1000)) if not is_unranked(p) else None,
                "calibrate_games": int(p.get("calibrate_games", 0)),
                "stats": st
            }

        return jsonify({
            "system": db["system_settings"],
            "courts": courts,
            "queue": queue,
            "queue_count": queue_count,
            "all_players": all_players,
            "leaderboards": {
                "mmr": lb_mmr,
                "points": lb_points,
                "winrate": lb_winrate
            },
            "history": {
                "global": global_hist,
                "my": my_hist
            },
            "events": events[:30],
            "me": me,
            "avg_match_minutes_10": compute_avg_match_minutes(db)
        })
    except Exception as e:
        log(f"Dashboard error: {e}")
        return jsonify({"error": str(e)}), 500

# ---------------- SESSION CONTROL ----------------
@app.route("/api/admin/toggle_session", methods=["POST"])
def api_toggle_session():
    db = get_db()
    refresh_courts(db)
    d = request.json or {}
    uid = d.get("userId")
    action = d.get("action")

    if not is_mod(db, uid):
        return jsonify({"error": "Unauthorized"}), 403

    if action == "start":
        # config
        cfg = db["system_settings"].get("session_config", {})
        cfg["target_points"] = int(d.get("target_points", cfg.get("target_points", 21)))
        cfg["bo"] = int(d.get("bo", cfg.get("bo", 1)))
        cfg["ready_check"] = bool(d.get("ready_check", cfg.get("ready_check", False)))
        cfg["notify_enabled"] = bool(d.get("notify_enabled", cfg.get("notify_enabled", False)))
        db["system_settings"]["session_config"] = cfg

        db["system_settings"]["is_session_active"] = True
        eid = str(uuid.uuid4())[:8]
        today = datetime.now().strftime("%d/%m/%Y")
        db["events"][eid] = {
            "id": eid,
            "name": f"ก๊วน {today}",
            "status": "active",
            "start_time": now_ts(),
            "end_time": None,
            "settings": cfg,
            "participants": []  # IMPORTANT for billing
        }
        db["system_settings"]["current_event_id"] = eid

        # clear courts
        for cid in db["courts_state"]:
            db["courts_state"][cid] = None

        # reset player states for new session
        for p in db["players"].values():
            if p.get("status") != "offline":
                p["status"] = "offline"
            p["queue_join_time"] = None
            p["resting"] = False
            p["rest_until"] = None
            # keep calibrate and mmr
            # keep incoming requests
            p["outgoing_request_to"] = None
            p["pair_lock"] = None

        save_db(db)
        return jsonify({"success": True, "event_id": eid})

    # end session
    if action == "end":
        db["system_settings"]["is_session_active"] = False
        eid = db["system_settings"].get("current_event_id")
        db["system_settings"]["current_event_id"] = None

        if eid and eid in db["events"]:
            db["events"][eid]["status"] = "closed"
            db["events"][eid]["end_time"] = now_ts()

        # clear courts and set everyone offline
        for cid in db["courts_state"]:
            db["courts_state"][cid] = None

        for p in db["players"].values():
            p["status"] = "offline"
            p["queue_join_time"] = None
            p["resting"] = False
            p["rest_until"] = None
            p["outgoing_request_to"] = None
            p["pair_lock"] = None

        save_db(db)
        return jsonify({"success": True})

    return jsonify({"error": "Invalid action"}), 400

@app.route("/api/admin/update_courts", methods=["POST"])
def api_update_courts():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not is_mod(db, uid):
        return jsonify({"error": "Unauthorized"}), 403

    count = int(d.get("count", 2))
    count = max(1, min(8, count))
    db["system_settings"]["total_courts"] = count
    refresh_courts(db)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/toggle_automatch", methods=["POST"])
def api_toggle_automatch():
    db = get_db()
    refresh_courts(db)
    d = request.json or {}
    uid = d.get("userId")
    if not is_mod(db, uid):
        return jsonify({"error": "Unauthorized"}), 403
    cid = str(d.get("courtId"))
    enabled = bool(d.get("enabled"))
    if cid not in db["system_settings"]["court_automatch"]:
        return jsonify({"error": "Court not found"}), 404
    db["system_settings"]["court_automatch"][cid] = enabled
    save_db(db)
    return jsonify({"success": True})

# ---------------- MOD MANAGEMENT ----------------
@app.route("/api/admin/manage_mod", methods=["POST"])
def api_manage_mod():
    db = get_db()
    d = request.json or {}
    if d.get("requesterId") != SUPER_ADMIN_ID:
        return jsonify({"error": "Super Admin Only"}), 403
    tid = d.get("targetUserId")
    action = d.get("action")
    if not tid or tid not in db["players"]:
        return jsonify({"error": "Target not found"}), 404

    if action == "promote":
        if tid not in db["mod_ids"]:
            db["mod_ids"].append(tid)
    else:
        if tid in db["mod_ids"]:
            db["mod_ids"].remove(tid)

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/set_mmr", methods=["POST"])
def api_set_mmr():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not is_mod(db, uid):
        return jsonify({"error": "Unauthorized"}), 403

    tid = d.get("targetUserId")
    new_mmr = d.get("newMmr")
    force_ranked = bool(d.get("force_ranked", False))

    if tid not in db["players"]:
        return jsonify({"error": "Not found"}), 404
    try:
        nm = int(new_mmr)
    except:
        return jsonify({"error": "Invalid MMR"}), 400

    db["players"][tid]["mmr"] = nm
    if force_ranked:
        db["players"][tid]["calibrate_games"] = 10

    save_db(db)
    return jsonify({"success": True})

# ---------------- PLAYER STATE ----------------
@app.route("/api/toggle_status", methods=["POST"])
def api_toggle_status():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404

    if not db["system_settings"].get("is_session_active"):
        return jsonify({"error": "Session not active"}), 400

    p = db["players"][uid]
    curr = p.get("status", "offline")
    t = now_ts()

    if curr == "offline":
        # check-in
        p["status"] = "active"
        p["queue_join_time"] = t
        p["last_active"] = t
        p["resting"] = False
        p["rest_until"] = None

        # add to current event participants
        eid = db["system_settings"].get("current_event_id")
        if eid and eid in db["events"]:
            parts = db["events"][eid].get("participants", [])
            if uid not in parts:
                parts.append(uid)
            db["events"][eid]["participants"] = parts

    elif curr == "active":
        # check-out
        # warn handled on frontend; here we reset wait time and clear pairing/outgoing
        p["status"] = "offline"
        p["queue_join_time"] = None
        p["resting"] = False
        p["rest_until"] = None

        # cancel outgoing request
        cancel_outgoing(db, uid)
        # unpair if paired
        unpair(db, uid)

    elif curr == "playing":
        return jsonify({"error": "กำลังแข่งอยู่"}), 400

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/toggle_rest", methods=["POST"])
def api_toggle_rest():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    p = db["players"][uid]
    if p.get("status") != "active":
        return jsonify({"error": "ต้องอยู่สถานะรอคิว"}), 400

    # toggle resting (manual rest)
    if p.get("resting"):
        p["resting"] = False
        p["rest_until"] = None
    else:
        p["resting"] = True
        p["rest_until"] = None

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/toggle_auto_rest", methods=["POST"])
def api_toggle_auto_rest():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    p = db["players"][uid]
    if p.get("status") == "offline":
        return jsonify({"error": "ต้อง check-in ก่อน"}), 400
    p["auto_rest"] = not bool(p.get("auto_rest"))
    save_db(db)
    return jsonify({"success": True, "auto_rest": bool(p["auto_rest"])})

# ---------------- PARTNER REQUEST ----------------
@app.route("/api/partner/request", methods=["POST"])
def api_partner_request():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    target = d.get("targetId")

    if uid not in db["players"] or target not in db["players"]:
        return jsonify({"error": "User not found"}), 404

    p = db["players"][uid]
    if p.get("status") == "offline":
        return jsonify({"error": "ต้อง Check-in ก่อน"}), 400

    # if already paired, cannot send new request
    if p.get("pair_lock"):
        return jsonify({"error": "คุณจับคู่แล้ว (ยกเลิกคู่ก่อน)"}), 400

    # can request only one target at a time
    out = p.get("outgoing_request_to")
    if out and out != target:
        return jsonify({"error": "คุณขอจับคู่ได้ทีละคน (ยกเลิกก่อน)"}), 400

    if uid == target:
        return jsonify({"error": "เลือกตัวเองไม่ได้"}), 400

    # add to target incoming list
    tp = db["players"][target]
    inc = tp.get("incoming_requests", [])
    if uid not in inc:
        inc.append(uid)
    tp["incoming_requests"] = inc
    p["outgoing_request_to"] = target

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/partner/cancel_outgoing", methods=["POST"])
def api_partner_cancel_outgoing():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    cancel_outgoing(db, uid)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/partner/accept", methods=["POST"])
def api_partner_accept():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")            # acceptor
    requester = d.get("requesterId") # who requested me

    if uid not in db["players"] or requester not in db["players"]:
        return jsonify({"error": "User not found"}), 404

    acc = db["players"][uid]
    req = db["players"][requester]

    if acc.get("pair_lock"):
        return jsonify({"error": "คุณจับคู่แล้ว (ยกเลิกคู่ก่อนถึงจะรับได้)"}), 400
    if req.get("pair_lock"):
        return jsonify({"error": "อีกฝ่ายจับคู่แล้ว"}), 400

    # must have request relationship
    if requester not in acc.get("incoming_requests", []):
        return jsonify({"error": "ไม่พบคำขอ"}), 400

    # If acceptor had outgoing request to someone else -> cancel it (as requested)
    cancel_outgoing(db, uid)

    # requester might have outgoing to acceptor; but even if not, allow accept based on inbox
    # lock pair
    acc["pair_lock"] = requester
    req["pair_lock"] = uid

    # requester cannot keep outgoing after paired
    cancel_outgoing(db, requester)

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/partner/unpair", methods=["POST"])
def api_partner_unpair():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    unpair(db, uid)
    save_db(db)
    return jsonify({"success": True})

# ---------------- MATCHMAKE ----------------
@app.route("/api/matchmake", methods=["POST"])
def api_matchmake():
    db = get_db()
    refresh_courts(db)

    if not db["system_settings"].get("is_session_active"):
        return jsonify({"error": "Session not active"}), 400

    # pick court (prefer automatch off)
    cid = first_empty_non_auto_court(db)
    if not cid:
        return jsonify({"status": "full"})

    choice = choose_best_four(db)
    if not choice:
        return jsonify({"status": "waiting"})

    teamA_ids, teamB_ids = choice
    m = start_match_on_court(db, cid, teamA_ids, teamB_ids, source="manual_request")
    save_db(db)
    return jsonify({"status": "matched", "courtId": cid, "match_id": m["match_id"]})

@app.route("/api/matchmake/manual", methods=["POST"])
def api_matchmake_manual():
    db = get_db()
    refresh_courts(db)
    d = request.json or {}
    uid = d.get("userId")
    if not is_mod(db, uid):
        return jsonify({"error": "Unauthorized"}), 403

    cid = str(d.get("courtId"))
    p_ids = d.get("playerIds", [])
    if cid not in db["courts_state"]:
        return jsonify({"error": "Court not found"}), 404
    if db["courts_state"][cid] is not None:
        return jsonify({"error": "Court Full"}), 400
    if len(p_ids) != 4 or len(set(p_ids)) != 4:
        return jsonify({"error": "Need 4 unique players"}), 400
    for pid in p_ids:
        if pid not in db["players"]:
            return jsonify({"error": "Player not found"}), 404
        if db["players"][pid].get("status") != "active":
            return jsonify({"error": "Player must be checked-in and active"}), 400
        if db["players"][pid].get("resting"):
            return jsonify({"error": "Player is resting"}), 400

    # keep requested locked pair together? admin can override; we allow any
    teamA_ids = [p_ids[0], p_ids[1]]
    teamB_ids = [p_ids[2], p_ids[3]]
    m = start_match_on_court(db, cid, teamA_ids, teamB_ids, source="manual_admin")
    save_db(db)
    return jsonify({"success": True, "match_id": m["match_id"]})

# ---------------- CANCEL MATCH ----------------
@app.route("/api/match/cancel", methods=["POST"])
def api_cancel_match():
    db = get_db()
    refresh_courts(db)
    d = request.json or {}
    uid = d.get("userId")
    cid = str(d.get("courtId"))

    if cid not in db["courts_state"]:
        return jsonify({"error": "Court not found"}), 404
    m = db["courts_state"].get(cid)
    if not m:
        return jsonify({"error": "No match"}), 400

    # auth: mod or one of 4 players
    players4 = m.get("team_a_ids", []) + m.get("team_b_ids", [])
    if not (is_mod(db, uid) or uid in players4):
        return jsonify({"error": "Unauthorized"}), 403

    # restore player states to active and restore queue time snapshot
    snap = m.get("queue_snapshot", {})
    for pid in players4:
        p = db["players"].get(pid)
        if not p:
            continue
        p["status"] = "active"
        # restore queue time so they don't lose waiting time
        if pid in snap and isinstance(snap[pid], (int, float)):
            p["queue_join_time"] = float(snap[pid])
        else:
            if not isinstance(p.get("queue_join_time"), (int, float)):
                p["queue_join_time"] = now_ts()

    # avoid teammate pairs next match (store all teammate pairs from this match)
    # we store both teams pairs
    avoid = db["system_settings"].get("avoid_teammate_pairs", [])
    until = now_ts() + 600  # 10 minutes
    def add_pair(a, b):
        a2 = str(min(a, b)); b2 = str(max(a, b))
        avoid.append({"a": a2, "b": b2, "until": until})
    ta = m.get("team_a_ids", [])
    tb = m.get("team_b_ids", [])
    if len(ta) == 2:
        add_pair(ta[0], ta[1])
    if len(tb) == 2:
        add_pair(tb[0], tb[1])
    db["system_settings"]["avoid_teammate_pairs"] = avoid

    # clear court
    db["courts_state"][cid] = None

    # if automatch enabled for this court, try rematch immediately (new pairing)
    try_automatch_for_court(db, cid)

    save_db(db)
    return jsonify({"success": True})

# ---------------- SUBMIT RESULT (score-based) ----------------
@app.route("/api/submit_result", methods=["POST"])
def api_submit_result():
    db = get_db()
    refresh_courts(db)
    d = request.json or {}
    uid = d.get("userId")
    cid = str(d.get("courtId"))
    set_scores = d.get("set_scores", [])

    if cid not in db["courts_state"]:
        return jsonify({"error": "Court not found"}), 404
    m = db["courts_state"].get(cid)
    if not m:
        return jsonify({"error": "No match"}), 400

    players4 = m.get("team_a_ids", []) + m.get("team_b_ids", [])
    if not (is_mod(db, uid) or uid in players4):
        return jsonify({"error": "Unauthorized"}), 403

    cfg = db["system_settings"].get("session_config", {"target_points": 21, "bo": 1})
    # parse set scores
    parsed = []
    try:
        for s in set_scores:
            if s is None:
                continue
            a = int(s.get("a"))
            b = int(s.get("b"))
            parsed.append({"a": a, "b": b})
    except:
        return jsonify({"error": "Invalid set_scores"}), 400

    info, msg = determine_winner_from_scores(parsed, cfg)
    if not info:
        return jsonify({"error": msg}), 400

    winner = info["winner"]
    sets_won_a = info["sets_won_a"]
    sets_won_b = info["sets_won_b"]
    pointsA = info["points_a_total"]
    pointsB = info["points_b_total"]

    # duration
    t = now_ts()
    start_at = float(m.get("start_at", m.get("created_at", t)))
    duration_sec = max(0, int(t - start_at))

    # MMR delta
    delta_by_player = compute_mmr_delta(
        db,
        m.get("team_a_ids", []),
        m.get("team_b_ids", []),
        winner,
        pointsA,
        pointsB,
        int(cfg.get("bo", 1))
    )

    # apply MMR, calibration + reset queue join time (they played)
    # cooldown based on avg last 10
    D = compute_avg_match_minutes(db)
    C = int(db["system_settings"].get("total_courts", 2))
    # N = eligible count (non-resting active)
    elig_now = len([p for p in db["players"].values() if p.get("status") == "active" and not p.get("resting")])
    load = (elig_now / max(1, (4 * C)))
    cooldown_min = max(0.0, (load - 1.0) * D)
    cooldown_sec = int(cooldown_min * 60)

    for pid in players4:
        p = db["players"].get(pid)
        if not p:
            continue
        # mmr apply (even if unranked, stored)
        old = int(p.get("mmr", 1000))
        change = int(delta_by_player.get(pid, 0))
        p["mmr"] = old + change

        # calibrate games +1 per completed match (not per set)
        cg = int(p.get("calibrate_games", 0))
        if cg < 10:
            p["calibrate_games"] = cg + 1

        # back to active, reset waiting time to now (so everyone plays equally)
        p["status"] = "active"
        p["queue_join_time"] = t
        p["last_active"] = t

        # auto rest if enabled
        if bool(p.get("auto_rest")) and cooldown_sec > 0:
            p["resting"] = True
            p["rest_until"] = t + cooldown_sec
        else:
            p["resting"] = False
            p["rest_until"] = None

    # write history
    hist = {
        "status": "completed",
        "match_id": m.get("match_id"),
        "event_id": db["system_settings"].get("current_event_id"),
        "court_id": cid,
        "created_at": m.get("created_at"),
        "start_at": m.get("start_at"),
        "ended_at": t,
        "duration_sec": duration_sec,
        "duration_min": round(duration_sec / 60.0, 1),
        "settings": cfg,
        "bo": int(cfg.get("bo", 1)),
        "target_points": int(cfg.get("target_points", 21)),
        "set_scores": parsed,
        "points_a_total": pointsA,
        "points_b_total": pointsB,
        "sets_won_a": sets_won_a,
        "sets_won_b": sets_won_b,
        "winner_team": winner,
        "team_a": m.get("team_a", []),
        "team_a_ids": m.get("team_a_ids", []),
        "team_b": m.get("team_b", []),
        "team_b_ids": m.get("team_b_ids", []),
        "mmr_delta_by_player": delta_by_player
    }
    db["match_history"].insert(0, hist)

    # clear court
    db["courts_state"][cid] = None

    # automatch next if enabled for this court
    try_automatch_for_court(db, cid)

    save_db(db)
    return jsonify({"success": True})

# ---------------- PLAYER PROFILE ----------------
@app.route("/api/player/profile/<uid>")
def api_player_profile(uid):
    db = get_db()
    if uid not in db["players"]:
        return jsonify({"error": "Not found"}), 404
    p = db["players"][uid]
    st_all = compute_player_stats(db, uid)
    # last 10 matches
    last10 = []
    for m in db.get("match_history", []):
        if m.get("status") != "completed":
            continue
        if uid in m.get("team_a_ids", []) or uid in m.get("team_b_ids", []):
            # enrich teams
            def pack(ids):
                arr = []
                for pid in ids:
                    pl = db["players"].get(pid)
                    if pl:
                        arr.append({"id": pid, "nickname": pl["nickname"], "pictureUrl": pl.get("pictureUrl", "")})
                return arr
            mm = dict(m)
            mm["team_a_data"] = pack(m.get("team_a_ids", []))
            mm["team_b_data"] = pack(m.get("team_b_ids", []))
            last10.append(mm)
        if len(last10) >= 10:
            break

    return jsonify({
        "id": uid,
        "nickname": p["nickname"],
        "pictureUrl": p.get("pictureUrl", ""),
        "rank_text": display_rank_text(p),
        "is_unranked": is_unranked(p),
        "mmr": int(p.get("mmr", 1000)) if not is_unranked(p) else None,
        "calibrate_games": int(p.get("calibrate_games", 0)),
        "stats": st_all,
        "last10": last10
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
