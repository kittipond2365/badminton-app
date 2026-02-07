import json
import os
import time
import uuid
import sys
from datetime import datetime
from itertools import combinations
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# ---------------- CONFIG ----------------
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"
DATA_FILE = os.environ.get("DATA_FILE", "/var/data/izesquad_data.json")

DEFAULT_DB = {
    "system_settings": {
        "total_courts": 2,
        "is_session_active": False,
        "current_event_id": None,
        "auto_match": False
    },
    "mod_ids": [],
    "players": {},
    "events": {},
    "match_history": [],
    "billing_history": [],
    "courts_state": {},            # { "1": None or matchObj }
    "recent_avoids": [],           # history-based constraints (finish/cancel)
    "notifications": {}            # { uid: [ {id,ts,text,payload,read} ] }
}

# ---------------- UTIL ----------------
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stdout)
    sys.stdout.flush()

def now_ts() -> float:
    return time.time()

def safe_int(x, default=0):
    try:
        return int(x)
    except:
        return default

def safe_float(x, default=0.0):
    try:
        return float(x)
    except:
        return default

def ensure_dir_and_file():
    directory = os.path.dirname(DATA_FILE)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_DB, f, ensure_ascii=False, indent=2)

def deep_merge_defaults(d: dict, defaults: dict) -> dict:
    for k, v in defaults.items():
        if k not in d:
            d[k] = v
        else:
            if isinstance(v, dict) and isinstance(d[k], dict):
                deep_merge_defaults(d[k], v)
    return d

def get_db() -> dict:
    ensure_dir_and_file()
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # merge defaults (safe)
        data = deep_merge_defaults(data, json.loads(json.dumps(DEFAULT_DB)))
        return data
    except Exception as e:
        log(f"DB read error: {e}")
        return json.loads(json.dumps(DEFAULT_DB))

def save_db(db: dict):
    try:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)
        os.replace(tmp, DATA_FILE)
    except Exception as e:
        log(f"CRITICAL DB save error: {e}")

def is_staff(uid: str, db: dict) -> bool:
    return uid == SUPER_ADMIN_ID or uid in db.get("mod_ids", [])

def refresh_courts(db: dict):
    total = max(1, min(6, safe_int(db["system_settings"].get("total_courts", 2), 2)))
    if "courts_state" not in db or not isinstance(db["courts_state"], dict):
        db["courts_state"] = {}

    for i in range(1, total + 1):
        k = str(i)
        if k not in db["courts_state"]:
            db["courts_state"][k] = None

    for k in list(db["courts_state"].keys()):
        try:
            if int(k) > total:
                del db["courts_state"][k]
        except:
            pass

def get_rank_title(mmr):
    mmr = safe_int(mmr, 1000)
    if mmr <= 800:
        return "NOOB DOG 🐶"
    elif mmr <= 1200:
        return "NOOB 🐣"
    elif mmr <= 1400:
        return "เด็กกระโปก 👶"
    elif mmr <= 1600:
        return "ชนะจนเบื่อ 🥱"
    else:
        return "โปรเพรเย๋อ 👽"

# ---------------- SESSION STATS ----------------
def ensure_event_stats(event: dict):
    if "stats" not in event or not isinstance(event["stats"], dict):
        event["stats"] = {}
    event["stats"].setdefault("matches_played", {})
    event["stats"].setdefault("seconds_played", {})

def add_participant(event: dict, uid: str):
    event.setdefault("players", [])
    if uid not in event["players"]:
        event["players"].append(uid)
    ensure_event_stats(event)
    event["stats"]["matches_played"].setdefault(uid, 0)
    event["stats"]["seconds_played"].setdefault(uid, 0)

def inc_match_played(event: dict, uid: str):
    ensure_event_stats(event)
    event["stats"]["matches_played"][uid] = int(event["stats"]["matches_played"].get(uid, 0)) + 1

def add_seconds_played(event: dict, uid: str, sec: int):
    ensure_event_stats(event)
    event["stats"]["seconds_played"][uid] = int(event["stats"]["seconds_played"].get(uid, 0)) + int(sec)

def matches_played_in_event(event: dict, uid: str) -> int:
    try:
        ensure_event_stats(event)
        return int(event["stats"]["matches_played"].get(uid, 0))
    except:
        return 0

def seconds_played_in_event(event: dict, uid: str) -> int:
    try:
        ensure_event_stats(event)
        return int(event["stats"]["seconds_played"].get(uid, 0))
    except:
        return 0

# ---------------- NOTIFICATIONS (in-app) ----------------
def push_notification(db: dict, uid: str, text: str, payload=None):
    db.setdefault("notifications", {})
    db["notifications"].setdefault(uid, [])
    nid = str(uuid.uuid4())[:10]
    db["notifications"][uid].insert(0, {
        "id": nid,
        "ts": now_ts(),
        "text": text,
        "payload": payload or {},
        "read": False
    })
    # keep last 30
    db["notifications"][uid] = db["notifications"][uid][:30]
    return nid

def get_unread_notifications(db: dict, uid: str, limit: int = 5):
    arr = db.get("notifications", {}).get(uid, [])
    out = [n for n in arr if not n.get("read")]
    return out[:limit]

def ack_notifications(db: dict, uid: str, ids: list):
    arr = db.get("notifications", {}).get(uid, [])
    s = set(ids)
    for n in arr:
        if n.get("id") in s:
            n["read"] = True

# ---------------- AVOIDS (repeat/cancel constraints) ----------------
def canonical_teams(team_a_ids, team_b_ids):
    ta = tuple(sorted(team_a_ids))
    tb = tuple(sorted(team_b_ids))
    return (ta, tb) if ta <= tb else (tb, ta)

def teammate_pairs(team_ids):
    ids = list(team_ids)
    if len(ids) < 2:
        return []
    a, b = ids[0], ids[1]
    return [tuple(sorted((a, b)))]

def prune_avoids(db: dict):
    now = now_ts()
    kept = []
    for a in db.get("recent_avoids", []):
        ttl = safe_int(a.get("ttl", 600), 600)
        ts = safe_float(a.get("ts", now), now)
        if now - ts <= ttl:
            kept.append(a)
    db["recent_avoids"] = kept[-80:]

def add_avoid(db: dict, kind: str, team_a_ids, team_b_ids, ttl: int):
    prune_avoids(db)
    can = canonical_teams(team_a_ids, team_b_ids)
    pairs = set(teammate_pairs(team_a_ids) + teammate_pairs(team_b_ids))
    db["recent_avoids"].append({
        "id": str(uuid.uuid4())[:8],
        "kind": kind,  # "finish" | "cancel"
        "ts": now_ts(),
        "ttl": int(ttl),
        "canonical": [list(can[0]), list(can[1])],
        "teammate_pairs": [list(p) for p in sorted(list(pairs))]
    })
    db["recent_avoids"] = db["recent_avoids"][-80:]

def get_avoid_info(db: dict, team_a_ids, team_b_ids):
    prune_avoids(db)
    cand = canonical_teams(team_a_ids, team_b_ids)
    cand_list = [list(cand[0]), list(cand[1])]
    for a in db.get("recent_avoids", []):
        if a.get("canonical") == cand_list:
            return True, a.get("kind", "")
    return False, ""

def teammate_pair_avoided(db: dict, pair_tuple):
    # pair_tuple is (uid1, uid2) sorted
    prune_avoids(db)
    pair_list = list(pair_tuple)
    for a in db.get("recent_avoids", []):
        if pair_list in a.get("teammate_pairs", []):
            return True, a.get("kind", "")
    return False, ""

# ---------------- MMR (Elo team) ----------------
def expected_score(rA: float, rB: float) -> float:
    return 1.0 / (1.0 + (10 ** ((rB - rA) / 400.0)))

def mmr_k_factor(avg_rating: int) -> int:
    if avg_rating < 1200:
        return 44
    elif avg_rating < 1500:
        return 40
    elif avg_rating < 1800:
        return 36
    else:
        return 32

def compute_team_deltas(team_a_ids, team_b_ids, winner_team, players_map):
    ra = sum(safe_int(players_map[pid].get("mmr", 1000), 1000) for pid in team_a_ids) / max(1, len(team_a_ids))
    rb = sum(safe_int(players_map[pid].get("mmr", 1000), 1000) for pid in team_b_ids) / max(1, len(team_b_ids))

    exp_a = expected_score(ra, rb)
    score_a = 1.0 if winner_team == "A" else 0.0

    avg_all = int((ra + rb) / 2)
    k = mmr_k_factor(avg_all)

    delta_a = int(round(k * (score_a - exp_a)))
    if delta_a == 0:
        delta_a = 1 if winner_team == "A" else -1
    delta_b = -delta_a

    deltas = {}
    for pid in team_a_ids:
        deltas[pid] = delta_a
    for pid in team_b_ids:
        deltas[pid] = delta_b

    meta = {
        "team_a_avg": ra,
        "team_b_avg": rb,
        "k": k,
        "exp_a": exp_a,
        "delta_a": delta_a
    }
    return deltas, meta

# ---------------- MATCHMAKING ----------------
def player_queue_since(p):
    qs = p.get("queue_since")
    if isinstance(qs, (int, float)):
        return float(qs)
    # fallback
    return safe_float(p.get("last_active", 0), 0.0)

def build_groups(active_players: list, players_map: dict, current_event: dict):
    """
    group singles + requested pairs (pair must stay same team)
    sort priority: waiting (oldest), matches_played (less first), seconds_played (less first)
    """
    active_ids = {p["id"] for p in active_players}
    processed = set()
    groups = []

    active_sorted = sorted(
        active_players,
        key=lambda p: (
            player_queue_since(p),
            matches_played_in_event(current_event, p["id"]),
            seconds_played_in_event(current_event, p["id"])
        )
    )

    for p in active_sorted:
        pid = p["id"]
        if pid in processed:
            continue

        partner_id = p.get("partner_req")
        if partner_id and partner_id in active_ids and partner_id not in processed:
            w = min(player_queue_since(players_map[pid]), player_queue_since(players_map[partner_id]))
            mp = matches_played_in_event(current_event, pid) + matches_played_in_event(current_event, partner_id)
            sp = seconds_played_in_event(current_event, pid) + seconds_played_in_event(current_event, partner_id)
            groups.append({"members": [pid, partner_id], "wait_since": w, "matches_played": mp, "seconds_played": sp})
            processed.add(pid)
            processed.add(partner_id)
        else:
            w = player_queue_since(players_map[pid])
            mp = matches_played_in_event(current_event, pid)
            sp = seconds_played_in_event(current_event, pid)
            groups.append({"members": [pid], "wait_since": w, "matches_played": mp, "seconds_played": sp})
            processed.add(pid)

    groups.sort(key=lambda g: (g["wait_since"], g["matches_played"], g["seconds_played"]))
    return groups

def sum_mmr(players_map, ids):
    return sum(safe_int(players_map[i].get("mmr", 1000), 1000) for i in ids)

def split_metrics(players_map, team_a_ids, team_b_ids):
    sa = sum_mmr(players_map, team_a_ids)
    sb = sum_mmr(players_map, team_b_ids)
    total_diff = abs(sa - sb)

    # pair-skill fairness: avoid huge carry (intra-team mmr gap)
    def intra_gap(team):
        if len(team) != 2:
            return 0
        a, b = team[0], team[1]
        return abs(safe_int(players_map[a].get("mmr", 1000), 1000) - safe_int(players_map[b].get("mmr", 1000), 1000))

    gap_a = intra_gap(team_a_ids)
    gap_b = intra_gap(team_b_ids)
    max_gap = max(gap_a, gap_b)
    gap_sum = gap_a + gap_b

    return {
        "sum_a": sa,
        "sum_b": sb,
        "total_diff": total_diff,
        "gap_a": gap_a,
        "gap_b": gap_b,
        "max_gap": max_gap,
        "gap_sum": gap_sum
    }

def requested_pair_set(groups_in_selected):
    s = set()
    for g in groups_in_selected:
        if len(g["members"]) == 2:
            a, b = g["members"][0], g["members"][1]
            s.add(tuple(sorted((a, b))))
    return s

def compute_repeat_penalty(db, team_a_ids, team_b_ids, requested_pairs: set, strict_cancel_rules: bool):
    """
    repeat_penalty counts teammate pairs that appeared recently (finish/cancel).
    strict_cancel_rules:
      - if match was canceled recently, strongly avoid SAME teammate pairs again (unless requested)
      - also avoid EXACT same match (teams) if possible
    """
    penalty = 0

    # exact match avoid
    exact, kind = get_avoid_info(db, team_a_ids, team_b_ids)
    if exact and strict_cancel_rules and kind == "cancel":
        # hard reject handled outside (return special)
        pass
    elif exact:
        penalty += 3

    # teammate pair avoid
    for pair in teammate_pairs(team_a_ids) + teammate_pairs(team_b_ids):
        avoided, ak = teammate_pair_avoided(db, pair)
        if avoided:
            if strict_cancel_rules and ak == "cancel" and pair not in requested_pairs:
                # hard reject handled outside (return special)
                pass
            penalty += 2 if ak == "cancel" else 1

    return penalty

def best_team_split_for_four(players_map, ids4, groups_in_selected, db, strict_mode: bool):
    """
    Choose best split with constraints:
    - requested pairs must stay together
    - prefer: (max_gap, total_diff) rather than sum-only
    - avoid canceled teammate pairs if possible
    """
    ids = list(ids4)
    req_pairs = requested_pair_set(groups_in_selected)

    best = None
    best_score = None

    # two-pass: strict -> relaxed (allow canceled repeats)
    for pass_idx in [0, 1]:
        strict_cancel = (pass_idx == 0) if strict_mode else False

        for comb in combinations(ids, 2):
            team_a = set(comb)
            team_b = set(ids) - team_a

            # keep requested pairs same team
            ok = True
            for a, b in req_pairs:
                in_a = (a in team_a and b in team_a)
                in_b = (a in team_b and b in team_b)
                if not (in_a or in_b):
                    ok = False
                    break
            if not ok:
                continue

            team_a_list = list(team_a)
            team_b_list = list(team_b)

            # normalize size 2-2
            if len(team_a_list) != 2 or len(team_b_list) != 2:
                continue

            # hard reject canceled exact/teammate pairs in strict_cancel pass (unless requested)
            if strict_cancel:
                exact, kind = get_avoid_info(db, team_a_list, team_b_list)
                if exact and kind == "cancel":
                    continue

                # teammate pairs
                for pair in teammate_pairs(team_a_list) + teammate_pairs(team_b_list):
                    avoided, ak = teammate_pair_avoided(db, pair)
                    if avoided and ak == "cancel" and pair not in req_pairs:
                        ok = False
                        break
                if not ok:
                    continue

            m = split_metrics(players_map, team_a_list, team_b_list)
            repeat_pen = compute_repeat_penalty(db, team_a_list, team_b_list, req_pairs, strict_cancel)

            # IMPORTANT: for badminton, prioritize pair-balance (max_gap) BEFORE team sum diff
            # score order: repeat_pen -> max_gap -> total_diff -> gap_sum
            score = (repeat_pen, m["max_gap"], m["total_diff"], m["gap_sum"])

            if best is None or score < best_score:
                best = (team_a_list, team_b_list)
                best_score = score

        if best is not None:
            return best[0], best[1], best_score

    # fallback (shouldn't happen)
    ids_sorted = sorted(ids, key=lambda x: safe_int(players_map[x].get("mmr", 1000), 1000), reverse=True)
    team_a = [ids_sorted[0], ids_sorted[-1]]
    team_b = [ids_sorted[1], ids_sorted[2]]
    m = split_metrics(players_map, team_a, team_b)
    return team_a, team_b, (999, m["max_gap"], m["total_diff"], m["gap_sum"])

def choose_match(groups, players_map, current_event, db):
    """
    Priority 1: waiting time (must include oldest group)
    Priority 2: fairness:
      - avoid canceled teammate pairs when possible
      - minimize carry-heavy pairing (max_gap) first, then sum diff
    """
    if not groups:
        return None

    # focus first N groups for search
    N = min(12, len(groups))
    cand_groups = groups[:N]
    must_idx = 0

    best_pick = None
    best_score = None

    # choose subset of groups indices whose total members = 4, includes must_idx
    indices = list(range(len(cand_groups)))
    for r in range(1, len(indices) + 1):
        for idxs in combinations(indices, r):
            if must_idx not in idxs:
                continue

            members = []
            for i in idxs:
                members.extend(cand_groups[i]["members"])
            if len(members) != 4:
                continue

            max_idx = max(idxs)  # how deep we had to go beyond oldest (smaller is better)
            groups_in_selected = [cand_groups[i] for i in idxs]

            team_a_ids, team_b_ids, split_score = best_team_split_for_four(
                players_map, members, groups_in_selected, db, strict_mode=True
            )

            # overall score: (max_idx, split_score...)
            score = (max_idx,) + split_score

            if best_pick is None or score < best_score:
                best_pick = (team_a_ids, team_b_ids)
                best_score = score

    if best_pick:
        return best_pick[0], best_pick[1]

    # fallback: take first 4 by waiting and best split (relaxed)
    members = []
    used_groups = []
    for g in groups:
        used_groups.append(g)
        members.extend(g["members"])
        if len(members) >= 4:
            members = members[:4]
            break

    if len(members) < 4:
        return None

    team_a_ids, team_b_ids, _ = best_team_split_for_four(players_map, members, used_groups, db, strict_mode=False)
    return team_a_ids, team_b_ids

def find_free_court(db, preferred=None):
    refresh_courts(db)
    if preferred is not None:
        cid = str(preferred)
        if cid in db["courts_state"] and db["courts_state"][cid] is None:
            return cid
    for cid, m in db["courts_state"].items():
        if m is None:
            return cid
    return None

def matchmake_internal(db, preferred_court=None, manual=False, manual_players=None, created_by=None):
    """
    Returns dict:
      {status: matched|waiting|full|session_off|no_session_event, courtId, matchId}
    This modifies db but DOES NOT save_db.
    """
    refresh_courts(db)

    if not db["system_settings"].get("is_session_active"):
        return {"status": "session_off"}

    free_court = find_free_court(db, preferred_court)
    if not free_court:
        return {"status": "full"}

    ceid = db["system_settings"].get("current_event_id")
    current_event = db["events"].get(ceid) if ceid else None
    if not current_event:
        return {"status": "no_session_event"}

    if manual:
        if not manual_players or len(manual_players) != 4:
            return {"status": "invalid_manual"}
        pids = [pid for pid in manual_players if pid in db["players"]]
        if len(pids) != 4 or len(set(pids)) != 4:
            return {"status": "invalid_manual"}
        team_a_ids = [pids[0], pids[1]]
        team_b_ids = [pids[2], pids[3]]
    else:
        active_players = [p for p in db["players"].values() if p.get("status") == "active"]
        if len(active_players) < 4:
            return {"status": "waiting"}

        groups = build_groups(active_players, db["players"], current_event)
        pick = choose_match(groups, db["players"], current_event, db)
        if not pick:
            return {"status": "waiting"}
        team_a_ids, team_b_ids = pick

    # Snapshot queue + partner to restore on cancel
    queue_snapshot = {pid: db["players"][pid].get("queue_since") for pid in (team_a_ids + team_b_ids)}
    partner_snapshot = {pid: db["players"][pid].get("partner_req") for pid in (team_a_ids + team_b_ids)}

    match_id = str(uuid.uuid4())[:8]
    match_obj = {
        "match_id": match_id,
        "event_id": ceid,
        "team_a_ids": team_a_ids,
        "team_b_ids": team_b_ids,
        "start_time": now_ts(),
        "manual": bool(manual),
        "created_by": created_by,
        "queue_snapshot": queue_snapshot,
        "partner_snapshot": partner_snapshot
    }
    db["courts_state"][free_court] = match_obj

    # Set players playing + clear partner_req (one-match request)
    for pid in team_a_ids + team_b_ids:
        p = db["players"][pid]
        p["status"] = "playing"
        p["last_active"] = now_ts()
        p["partner_req"] = None
        add_participant(current_event, pid)

    # In-app notification
    def name(pid):
        return db["players"].get(pid, {}).get("nickname", pid)

    msg = (
        f"🏸 จับคู่แล้ว! Court {free_court}\n"
        f"ทีม A: {name(team_a_ids[0])}, {name(team_a_ids[1])}\n"
        f"ทีม B: {name(team_b_ids[0])}, {name(team_b_ids[1])}"
    )
    payload = {"courtId": free_court, "matchId": match_id, "teamA": team_a_ids, "teamB": team_b_ids}
    for pid in team_a_ids + team_b_ids:
        push_notification(db, pid, msg, payload)

    return {"status": "matched", "courtId": free_court, "matchId": match_id}

# ---------------- ROUTES ----------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/login", methods=["POST"])
def login():
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
                "status": "offline",        # offline | active | playing
                "queue_since": None,        # when checked-in
                "last_active": now_ts(),
                "last_seen": now_ts(),
                "partner_req": None
            }
        else:
            db["players"][uid]["nickname"] = d.get("displayName", db["players"][uid].get("nickname", "User"))
            db["players"][uid]["pictureUrl"] = d.get("pictureUrl", db["players"][uid].get("pictureUrl", ""))
            db["players"][uid]["last_seen"] = now_ts()

        p = db["players"][uid]
        p["role"] = "super" if uid == SUPER_ADMIN_ID else ("mod" if uid in db["mod_ids"] else "user")
        p["rank_title"] = get_rank_title(p.get("mmr", 1000))

        # my history
        my_hist = []
        for m in db.get("match_history", []):
            if uid in m.get("team_a_ids", []) or uid in m.get("team_b_ids", []):
                my_hist.append(m)
        p["my_history"] = my_hist[:50]

        save_db(db)
        return jsonify(p)
    except Exception as e:
        log(f"Login error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/get_dashboard")
def get_dashboard():
    try:
        db = get_db()
        refresh_courts(db)

        system = db.get("system_settings", {})
        current_eid = system.get("current_event_id")
        current_event = db["events"].get(current_eid) if current_eid else None

        # courts output (attach player info)
        courts_out = {}
        for cid, match in db["courts_state"].items():
            if match:
                match_out = dict(match)
                match_out["elapsed"] = int(now_ts() - safe_float(match.get("start_time", now_ts()), now_ts()))
                match_out["team_a_data"] = []
                match_out["team_b_data"] = []
                for uid in match.get("team_a_ids", []):
                    if uid in db["players"]:
                        pl = db["players"][uid]
                        match_out["team_a_data"].append({"id": uid, "name": pl.get("nickname",""), "pic": pl.get("pictureUrl","")})
                for uid in match.get("team_b_ids", []):
                    if uid in db["players"]:
                        pl = db["players"][uid]
                        match_out["team_b_data"].append({"id": uid, "name": pl.get("nickname",""), "pic": pl.get("pictureUrl","")})
                courts_out[cid] = match_out
            else:
                courts_out[cid] = None

        # players list
        players_list = []
        for p in db["players"].values():
            p["mmr"] = safe_int(p.get("mmr", 1000), 1000)
            p["rank_title"] = get_rank_title(p["mmr"])
            players_list.append(p)

        # queue (active + playing)
        queue = [p for p in players_list if p.get("status") in ("active", "playing")]
        queue.sort(key=lambda x: (player_queue_since(x), safe_int(x.get("mmr", 1000), 1000)))

        leaderboard = sorted(players_list, key=lambda x: x.get("mmr", 1000), reverse=True)

        # events list
        event_list = []
        for eid, e in db["events"].items():
            joined = []
            for pid in e.get("players", []):
                if pid in db["players"]:
                    pl = db["players"][pid]
                    joined.append({"id": pid, "nickname": pl.get("nickname",""), "pictureUrl": pl.get("pictureUrl","")})

            e_out = dict(e)
            e_out["joined_users"] = joined

            raw_dt = e_out.get("datetime", 0)
            if isinstance(raw_dt, (int, float)):
                e_out["sort_key"] = float(raw_dt)
            elif isinstance(raw_dt, str):
                try:
                    e_out["sort_key"] = datetime.fromisoformat(raw_dt).timestamp()
                except:
                    e_out["sort_key"] = 0
            else:
                e_out["sort_key"] = 0

            event_list.append(e_out)

        event_list.sort(key=lambda x: x["sort_key"], reverse=True)

        # minimal players for frontend
        all_players = []
        for p in players_list:
            all_players.append({
                "id": p["id"],
                "nickname": p.get("nickname",""),
                "pictureUrl": p.get("pictureUrl",""),
                "status": p.get("status","offline"),
                "queue_since": p.get("queue_since"),
                "mmr": p.get("mmr", 1000),
                "rank_title": p.get("rank_title",""),
                "is_mod": p["id"] in db["mod_ids"],
                "partner_req": p.get("partner_req")
            })

        # incoming partner req + notifications for uid query
        uid = request.args.get("uid")
        incoming = []
        notis = []
        if uid and uid in db["players"]:
            for p in db["players"].values():
                if p.get("partner_req") == uid and p["id"] != uid:
                    incoming.append({"id": p["id"], "nickname": p.get("nickname",""), "pictureUrl": p.get("pictureUrl","")})
            notis = get_unread_notifications(db, uid, limit=5)

        save_db(db)  # mark prune/merge safe
        return jsonify({
            "system": system,
            "courts": courts_out,
            "queue": queue,
            "queue_count": len([p for p in queue if p.get("status") == "active"]),
            "events": event_list,
            "leaderboard": leaderboard,
            "match_history": db.get("match_history", [])[:20],
            "all_players": all_players,
            "incoming_partner_requests": incoming,
            "notifications_unread": notis
        })
    except Exception as e:
        log(f"Dashboard error: {e}")
        return jsonify({
            "system": {}, "courts": {}, "queue": [], "queue_count": 0,
            "events": [], "leaderboard": [], "match_history": [], "all_players": [],
            "incoming_partner_requests": [], "notifications_unread": []
        })

# ---------------- Notifications ACK ----------------
@app.route("/api/notifications/ack", methods=["POST"])
def notifications_ack():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    ids = d.get("ids", [])
    if not uid or not isinstance(ids, list):
        return jsonify({"error": "Missing data"}), 400
    ack_notifications(db, uid, ids)
    save_db(db)
    return jsonify({"success": True})

# ---------------- Partner ----------------
@app.route("/api/request_partner", methods=["POST"])
def request_partner():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    target = d.get("targetId")
    if not uid or not target:
        return jsonify({"error": "Missing data"}), 400
    if uid not in db["players"] or target not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    if db["players"][uid].get("status") != "active":
        return jsonify({"error": "ต้อง Check-in ก่อนนะครับ"}), 400

    db["players"][uid]["partner_req"] = target
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/cancel_request", methods=["POST"])
def cancel_request():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404
    db["players"][uid]["partner_req"] = None
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/partner/respond", methods=["POST"])
def respond_partner():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    rid = d.get("requesterId")
    action = d.get("action")
    if not uid or not rid or uid not in db["players"] or rid not in db["players"]:
        return jsonify({"error": "Missing/invalid data"}), 400

    if action == "accept":
        db["players"][uid]["partner_req"] = rid
        db["players"][rid]["partner_req"] = uid
    else:
        if db["players"][rid].get("partner_req") == uid:
            db["players"][rid]["partner_req"] = None

    save_db(db)
    return jsonify({"success": True})

# ---------------- Toggle Status (Check-in) ----------------
@app.route("/api/toggle_status", methods=["POST"])
def toggle_status():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or uid not in db["players"]:
        return jsonify({"error": "User not found"}), 404

    if not db["system_settings"].get("is_session_active"):
        return jsonify({"error": "Session not active"}), 400

    p = db["players"][uid]
    curr = p.get("status", "offline")

    if curr in ("active", "playing"):
        p["status"] = "offline"
        p["partner_req"] = None
        p["queue_since"] = None
    else:
        p["status"] = "active"
        p["queue_since"] = now_ts()
        p["last_active"] = now_ts()
        # add to session participants
        ceid = db["system_settings"].get("current_event_id")
        if ceid and ceid in db["events"]:
            add_participant(db["events"][ceid], uid)

    save_db(db)
    return jsonify({"success": True})

# ---------------- Admin / Session ----------------
@app.route("/api/admin/toggle_session", methods=["POST"])
def toggle_session():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    action = d.get("action")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    refresh_courts(db)

    if action == "start":
        db["system_settings"]["is_session_active"] = True

        # create new session event
        eid = str(uuid.uuid4())[:8]
        today = datetime.now().strftime("%d/%m/%Y")
        db["events"][eid] = {
            "id": eid,
            "name": f"ก๊วน {today}",
            "datetime": now_ts(),
            "players": [],
            "status": "active",
            "type": "session",
            "created_at": now_ts(),
            "ended_at": None,
            "stats": {"matches_played": {}, "seconds_played": {}}
        }
        db["system_settings"]["current_event_id"] = eid

        # reset courts
        for k in db["courts_state"].keys():
            db["courts_state"][k] = None

        # reset everyone
        for p in db["players"].values():
            p["status"] = "offline"
            p["partner_req"] = None
            p["queue_since"] = None

    else:
        db["system_settings"]["is_session_active"] = False

        curr = db["system_settings"].get("current_event_id")
        if curr and curr in db["events"]:
            db["events"][curr]["status"] = "ended"
            db["events"][curr]["ended_at"] = now_ts()

        db["system_settings"]["current_event_id"] = None

        for k in db["courts_state"].keys():
            db["courts_state"][k] = None

        for p in db["players"].values():
            p["status"] = "offline"
            p["partner_req"] = None
            p["queue_since"] = None

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/set_auto_match", methods=["POST"])
def set_auto_match():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    val = d.get("enabled")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    db["system_settings"]["auto_match"] = bool(val)
    save_db(db)
    return jsonify({"success": True, "auto_match": db["system_settings"]["auto_match"]})

@app.route("/api/admin/update_courts", methods=["POST"])
def update_courts():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    c = safe_int(d.get("count", 2), 2)
    c = max(1, min(6, c))
    db["system_settings"]["total_courts"] = c
    refresh_courts(db)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/manage_mod", methods=["POST"])
def manage_mod():
    db = get_db()
    d = request.json or {}
    if d.get("requesterId") != SUPER_ADMIN_ID:
        return jsonify({"error": "Super Admin Only"}), 403

    tid = d.get("targetUserId")
    action = d.get("action")
    if not tid:
        return jsonify({"error": "Missing targetUserId"}), 400

    if action == "promote":
        if tid not in db["mod_ids"]:
            db["mod_ids"].append(tid)
    else:
        if tid in db["mod_ids"]:
            db["mod_ids"].remove(tid)

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/reset_system", methods=["POST"])
def reset_system():
    db = get_db()
    d = request.json or {}
    if d.get("userId") != SUPER_ADMIN_ID:
        return jsonify({"error": "Super Admin Only"}), 403

    db["match_history"] = []
    db["billing_history"] = []
    db["events"] = {}
    db["recent_avoids"] = []
    db["notifications"] = {}
    db["system_settings"]["is_session_active"] = False
    db["system_settings"]["current_event_id"] = None

    for p in db["players"].values():
        p["mmr"] = 1000
        p["status"] = "offline"
        p["partner_req"] = None
        p["queue_since"] = None

    refresh_courts(db)
    for k in db["courts_state"].keys():
        db["courts_state"][k] = None

    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/set_mmr", methods=["POST"])
def set_mmr():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    tid = d.get("targetUserId")
    new_mmr = d.get("newMmr")
    if not tid or new_mmr is None:
        return jsonify({"error": "Missing data"}), 400

    if tid in db["players"]:
        db["players"][tid]["mmr"] = max(0, safe_int(new_mmr, 1000))
        save_db(db)
        return jsonify({"success": True})
    return jsonify({"error": "Not found"}), 404

# ---------------- Matchmake ----------------
@app.route("/api/matchmake", methods=["POST"])
def matchmake():
    db = get_db()
    res = matchmake_internal(db)
    save_db(db)
    return jsonify(res)

@app.route("/api/matchmake/manual", methods=["POST"])
def manual_matchmake():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    court_id = d.get("courtId")
    p_ids = d.get("playerIds", [])
    res = matchmake_internal(db, preferred_court=court_id, manual=True, manual_players=p_ids, created_by=uid)
    save_db(db)
    if res.get("status") != "matched":
        return jsonify({"error": res.get("status")}), 400
    return jsonify({"success": True, "matchId": res.get("matchId")})

# ---------------- Cancel Match ----------------
@app.route("/api/cancel_match", methods=["POST"])
def cancel_match():
    """
    Cancel an ongoing match:
    - no mmr change
    - no match history
    - rematch immediately (try)
    - avoid same teammate pairs as canceled match when possible
    Auth: players in that match OR staff
    """
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    court_id = str(d.get("courtId"))

    if not uid or not court_id:
        return jsonify({"error": "Missing data"}), 400

    refresh_courts(db)
    match = db["courts_state"].get(court_id)
    if not match:
        return jsonify({"error": "No match"}), 400

    team_a_ids = match.get("team_a_ids", [])
    team_b_ids = match.get("team_b_ids", [])

    is_player = uid in team_a_ids or uid in team_b_ids
    if not (is_player or is_staff(uid, db)):
        return jsonify({"error": "Unauthorized"}), 403

    # add avoid constraint (cancel) - stronger TTL
    add_avoid(db, "cancel", team_a_ids, team_b_ids, ttl=1800)

    # restore players to active + restore queue/partner snapshots
    qsnap = match.get("queue_snapshot", {})
    psnap = match.get("partner_snapshot", {})
    for pid in team_a_ids + team_b_ids:
        if pid in db["players"]:
            p = db["players"][pid]
            p["status"] = "active" if db["system_settings"].get("is_session_active") else "offline"
            p["queue_since"] = qsnap.get(pid, p.get("queue_since", now_ts())) if db["system_settings"].get("is_session_active") else None
            p["partner_req"] = psnap.get(pid, None)

    # clear court
    db["courts_state"][court_id] = None

    # rematch immediately on same court
    rematch = matchmake_internal(db, preferred_court=court_id)
    save_db(db)
    return jsonify({"success": True, "rematch": rematch})

# ---------------- Submit Result ----------------
@app.route("/api/submit_result", methods=["POST"])
def submit_result():
    """
    End match with winner:
    Auth: players in match OR staff (mod/super)
    """
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    court_id = str(d.get("courtId"))
    winner = d.get("winner")  # "A" or "B"

    if not uid or not court_id or winner not in ("A", "B"):
        return jsonify({"error": "Missing data"}), 400

    refresh_courts(db)
    match = db["courts_state"].get(court_id)
    if not match:
        return jsonify({"error": "No match"}), 400

    team_a_ids = match.get("team_a_ids", [])
    team_b_ids = match.get("team_b_ids", [])

    is_player = uid in team_a_ids or uid in team_b_ids
    if not (is_player or is_staff(uid, db)):
        return jsonify({"error": "Unauthorized"}), 403

    end_time = now_ts()
    start_time = safe_float(match.get("start_time", end_time), end_time)
    duration_sec = max(0, int(end_time - start_time))

    # MMR update (elo)
    deltas, mmr_meta = compute_team_deltas(team_a_ids, team_b_ids, winner, db["players"])
    snapshot = {}

    for pid, delta in deltas.items():
        if pid not in db["players"]:
            continue
        old = safe_int(db["players"][pid].get("mmr", 1000), 1000)
        new = max(0, old + int(delta))
        db["players"][pid]["mmr"] = new
        db["players"][pid]["status"] = "active" if db["system_settings"].get("is_session_active") else "offline"
        # after play: reset queue_since to now so others get priority
        db["players"][pid]["queue_since"] = now_ts() if db["system_settings"].get("is_session_active") else None
        db["players"][pid]["partner_req"] = None
        snapshot[pid] = {"change": f"{int(delta):+d}", "old": old, "new": new}

    # event stats
    ceid = match.get("event_id")
    if ceid and ceid in db["events"]:
        evt = db["events"][ceid]
        for pid in team_a_ids + team_b_ids:
            add_participant(evt, pid)
            inc_match_played(evt, pid)
            add_seconds_played(evt, pid, duration_sec)

    hist = {
        "event_id": ceid,
        "match_id": match.get("match_id"),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "duration_sec": duration_sec,
        "winner_team": winner,
        "mmr_snapshot": snapshot,
        "mmr_meta": mmr_meta,
        "team_a_ids": team_a_ids,
        "team_b_ids": team_b_ids,
        "team_a": [db["players"][pid].get("nickname","") for pid in team_a_ids if pid in db["players"]],
        "team_b": [db["players"][pid].get("nickname","") for pid in team_b_ids if pid in db["players"]],
    }
    db["match_history"].insert(0, hist)

    # avoid immediate same teams (finish) for a short time
    add_avoid(db, "finish", team_a_ids, team_b_ids, ttl=600)

    # clear court
    db["courts_state"][court_id] = None

    # auto match next
    auto_on = bool(db["system_settings"].get("auto_match", False))
    auto_res = None
    if auto_on:
        auto_res = matchmake_internal(db, preferred_court=court_id)

    save_db(db)
    return jsonify({"success": True, "mmr_meta": mmr_meta, "auto": auto_res})

# ---------------- Events (optional: non-session) ----------------
@app.route("/api/event/create", methods=["POST"])
def create_event():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    name = d.get("name")
    dt = d.get("datetime")
    if not name or dt is None:
        return jsonify({"error": "Missing data"}), 400

    eid = str(uuid.uuid4())[:8]
    db["events"][eid] = {
        "id": eid,
        "name": name,
        "datetime": dt,
        "players": [],
        "status": "open",
        "type": "event",
        "created_at": now_ts(),
        "ended_at": None,
        "stats": {"matches_played": {}, "seconds_played": {}}
    }
    save_db(db)
    return jsonify({"success": True, "eventId": eid})

@app.route("/api/event/delete", methods=["POST"])
def delete_event():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403
    eid = d.get("eventId")
    if eid in db["events"]:
        del db["events"][eid]
        save_db(db)
        return jsonify({"success": True})
    return jsonify({"error": "Not found"}), 404

@app.route("/api/event/join_toggle", methods=["POST"])
def join_event_toggle():
    db = get_db()
    d = request.json or {}
    eid = d.get("eventId")
    uid = d.get("userId")
    if not eid or not uid:
        return jsonify({"error": "Missing data"}), 400
    if eid not in db["events"]:
        return jsonify({"error": "Not found"}), 404

    evt = db["events"][eid]
    if evt.get("type") == "session":
        # session participants are via check-in only
        return jsonify({"error": "Session เข้าร่วมผ่านการ Check-in เท่านั้น"}), 400

    evt.setdefault("players", [])
    if uid in evt["players"]:
        evt["players"].remove(uid)
    else:
        evt["players"].append(uid)
        add_participant(evt, uid)

    save_db(db)
    return jsonify({"success": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
