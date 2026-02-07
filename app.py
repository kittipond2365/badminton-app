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
        "auto_match_courts": {}
    },
    "mod_ids": [],
    "players": {},
    "events": {},
    "match_history": [],
    "billing_history": [],
    "courts_state": {},
    "recent_avoids": [],
    "notifications": {}
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
    total = max(1, min(10, safe_int(db["system_settings"].get("total_courts", 2), 2)))

    if "courts_state" not in db or not isinstance(db["courts_state"], dict):
        db["courts_state"] = {}
    if "auto_match_courts" not in db["system_settings"] or not isinstance(db["system_settings"]["auto_match_courts"], dict):
        db["system_settings"]["auto_match_courts"] = {}

    for i in range(1, total + 1):
        k = str(i)
        if k not in db["courts_state"]:
            db["courts_state"][k] = None
        if k not in db["system_settings"]["auto_match_courts"]:
            db["system_settings"]["auto_match_courts"][k] = True

    for k in list(db["courts_state"].keys()):
        try:
            if int(k) > total:
                del db["courts_state"][k]
        except:
            pass

    for k in list(db["system_settings"]["auto_match_courts"].keys()):
        try:
            if int(k) > total:
                del db["system_settings"]["auto_match_courts"][k]
        except:
            pass

# ---------------- RANK / CALIBRATION ----------------
def is_calibrating(p: dict) -> bool:
    return safe_int(p.get("calib_played", 0), 0) < 10

def rank_title_from_mmr(p: dict):
    mmr = safe_int(p.get("mmr", 1000), 1000)
    cp = safe_int(p.get("calib_played", 0), 0)
    if cp < 10:
        return f"UNRANKED 🧪 {cp}/10"

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

def mmr_display(p: dict) -> str:
    cp = safe_int(p.get("calib_played", 0), 0)
    if cp < 10:
        return f"UNRANKED ({cp}/10)"
    return str(safe_int(p.get("mmr", 1000), 1000))

def effective_mmr_for_matchmaking(pid: str, players_map: dict) -> int:
    p = players_map.get(pid, {})
    mmr = safe_int(p.get("mmr", 1000), 1000)

    cp = safe_int(p.get("calib_played", 0), 0)
    if cp >= 10:
        return mmr

    wins = safe_int(p.get("calib_wins", 0), 0)
    streak = safe_int(p.get("calib_streak", 0), 0)

    games = max(1, cp)
    wr = wins / games if games > 0 else 0.5

    perf_boost = int((wr - 0.5) * 600)
    streak_boost = min(700, 160 * streak)
    uncertainty = (10 - cp) * 15

    return mmr + perf_boost + streak_boost + uncertainty

def calib_multiplier(p: dict) -> float:
    cp = safe_int(p.get("calib_played", 0), 0)
    if cp >= 10:
        return 1.0
    streak = safe_int(p.get("calib_streak", 0), 0)
    m = 1.6 + 0.1 * min(5, streak)
    return min(2.2, m)

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

# ---------------- NOTIFICATIONS ----------------
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

# ---------------- AVOIDS ----------------
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
    db["recent_avoids"] = kept[-120:]

def add_avoid(db: dict, kind: str, team_a_ids, team_b_ids, ttl: int):
    prune_avoids(db)
    can = canonical_teams(team_a_ids, team_b_ids)
    pairs = set(teammate_pairs(team_a_ids) + teammate_pairs(team_b_ids))
    db["recent_avoids"].append({
        "id": str(uuid.uuid4())[:8],
        "kind": kind,
        "ts": now_ts(),
        "ttl": int(ttl),
        "canonical": [list(can[0]), list(can[1])],
        "teammate_pairs": [list(p) for p in sorted(list(pairs))]
    })
    db["recent_avoids"] = db["recent_avoids"][-120:]

def get_avoid_info(db: dict, team_a_ids, team_b_ids):
    prune_avoids(db)
    cand = canonical_teams(team_a_ids, team_b_ids)
    cand_list = [list(cand[0]), list(cand[1])]
    for a in db.get("recent_avoids", []):
        if a.get("canonical") == cand_list:
            return True, a.get("kind", "")
    return False, ""

def teammate_pair_avoided(db: dict, pair_tuple):
    prune_avoids(db)
    pair_list = list(pair_tuple)
    for a in db.get("recent_avoids", []):
        if pair_list in a.get("teammate_pairs", []):
            return True, a.get("kind", "")
    return False, ""

# ---------------- MMR (Team Elo) ----------------
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
    k_base = mmr_k_factor(avg_all)

    delta_a_base = k_base * (score_a - exp_a)
    if abs(delta_a_base) < 1:
        delta_a_base = 1.0 if winner_team == "A" else -1.0
    delta_b_base = -delta_a_base

    wa = [calib_multiplier(players_map.get(pid, {})) for pid in team_a_ids]
    wb = [calib_multiplier(players_map.get(pid, {})) for pid in team_b_ids]
    avg_wa = sum(wa) / max(1, len(wa))
    avg_wb = sum(wb) / max(1, len(wb))

    deltas = {}
    for i, pid in enumerate(team_a_ids):
        w = wa[i] / avg_wa if avg_wa > 0 else 1.0
        deltas[pid] = int(round(delta_a_base * w))

    for i, pid in enumerate(team_b_ids):
        w = wb[i] / avg_wb if avg_wb > 0 else 1.0
        deltas[pid] = int(round(delta_b_base * w))

    for pid in list(deltas.keys()):
        if deltas[pid] == 0:
            deltas[pid] = 1 if (winner_team == "A" and pid in team_a_ids) or (winner_team == "B" and pid in team_b_ids) else -1

    meta = {
        "team_a_avg": ra,
        "team_b_avg": rb,
        "k_base": k_base,
        "exp_a": exp_a,
        "delta_a_base": float(delta_a_base),
        "calibration_weights_a": wa,
        "calibration_weights_b": wb
    }
    return deltas, meta

# ---------------- SMART STATS ----------------
def calculate_smart_stats(uid: str, history: list):
    my_matches = [m for m in history if uid in m.get("team_a_ids", []) or uid in m.get("team_b_ids", [])]
    total = len(my_matches)
    if total == 0:
        return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}

    wins = 0
    streak = 0
    best_streak = 0

    partner_win = {}
    nemesis_loss = {}

    for m in reversed(my_matches):
        is_team_a = uid in m.get("team_a_ids", [])
        my_team = "A" if is_team_a else "B"
        is_win = (m.get("winner_team") == my_team)

        if is_win:
            wins += 1
            streak += 1
        else:
            streak = 0
        best_streak = max(best_streak, streak)

        if is_team_a:
            mate = [x for x in m.get("team_a_ids", []) if x != uid]
            opp = m.get("team_b_ids", [])
        else:
            mate = [x for x in m.get("team_b_ids", []) if x != uid]
            opp = m.get("team_a_ids", [])

        if mate:
            mate_id = mate[0]
            if is_win:
                partner_win[mate_id] = partner_win.get(mate_id, 0) + 1

        if not is_win:
            for o in opp:
                nemesis_loss[o] = nemesis_loss.get(o, 0) + 1

    best_partner_id = max(partner_win.items(), key=lambda x: x[1])[0] if partner_win else None
    nemesis_id = max(nemesis_loss.items(), key=lambda x: x[1])[0] if nemesis_loss else None

    return {
        "win_rate": int((wins / total) * 100),
        "total": total,
        "streak": best_streak,
        "best_partner": best_partner_id or "-",
        "nemesis": nemesis_id or "-"
    }

# ---------------- MATCHMAKING ----------------
def player_queue_since(p):
    qs = p.get("queue_since")
    if isinstance(qs, (int, float)):
        return float(qs)
    return safe_float(p.get("last_active", 0), 0.0)

def build_groups(active_players: list, players_map: dict, current_event: dict):
    active_ids = {p["id"] for p in active_players}
    processed = set()
    groups = []

    def sort_key(p):
        return (
            player_queue_since(p),
            safe_int(p.get("calib_played", 0), 0),
            matches_played_in_event(current_event, p["id"]),
            seconds_played_in_event(current_event, p["id"])
        )

    active_sorted = sorted(active_players, key=sort_key)

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

def sum_effective(players_map, ids):
    return sum(effective_mmr_for_matchmaking(i, players_map) for i in ids)

def split_metrics(players_map, team_a_ids, team_b_ids):
    sa = sum_effective(players_map, team_a_ids)
    sb = sum_effective(players_map, team_b_ids)
    total_diff = abs(sa - sb)

    def intra_gap(team):
        if len(team) != 2:
            return 0
        a, b = team[0], team[1]
        return abs(effective_mmr_for_matchmaking(a, players_map) - effective_mmr_for_matchmaking(b, players_map))

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
    penalty = 0

    exact, kind = get_avoid_info(db, team_a_ids, team_b_ids)
    if exact and strict_cancel_rules and kind == "cancel":
        pass
    elif exact:
        penalty += 3

    for pair in teammate_pairs(team_a_ids) + teammate_pairs(team_b_ids):
        avoided, ak = teammate_pair_avoided(db, pair)
        if avoided:
            if strict_cancel_rules and ak == "cancel" and pair not in requested_pairs:
                pass
            penalty += 2 if ak == "cancel" else 1

    return penalty

def best_team_split_for_four(players_map, ids4, groups_in_selected, db, strict_mode: bool):
    ids = list(ids4)
    req_pairs = requested_pair_set(groups_in_selected)

    best = None
    best_score = None

    for pass_idx in [0, 1]:
        strict_cancel = (pass_idx == 0) if strict_mode else False

        for comb in combinations(ids, 2):
            team_a = set(comb)
            team_b = set(ids) - team_a

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
            if len(team_a_list) != 2 or len(team_b_list) != 2:
                continue

            if strict_cancel:
                exact, kind = get_avoid_info(db, team_a_list, team_b_list)
                if exact and kind == "cancel":
                    continue

                for pair in teammate_pairs(team_a_list) + teammate_pairs(team_b_list):
                    avoided, ak = teammate_pair_avoided(db, pair)
                    if avoided and ak == "cancel" and pair not in req_pairs:
                        ok = False
                        break
                if not ok:
                    continue

            m = split_metrics(players_map, team_a_list, team_b_list)
            repeat_pen = compute_repeat_penalty(db, team_a_list, team_b_list, req_pairs, strict_cancel)

            score = (repeat_pen, m["max_gap"], m["total_diff"], m["gap_sum"])

            if best is None or score < best_score:
                best = (team_a_list, team_b_list)
                best_score = score

        if best is not None:
            return best[0], best[1], best_score

    ids_sorted = sorted(ids, key=lambda x: effective_mmr_for_matchmaking(x, players_map), reverse=True)
    team_a = [ids_sorted[0], ids_sorted[-1]]
    team_b = [ids_sorted[1], ids_sorted[2]]
    m = split_metrics(players_map, team_a, team_b)
    return team_a, team_b, (999, m["max_gap"], m["total_diff"], m["gap_sum"])

def choose_match(groups, players_map, current_event, db):
    if not groups:
        return None

    N = min(12, len(groups))
    cand_groups = groups[:N]
    must_idx = 0

    best_pick = None
    best_score = None

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

            max_idx = max(idxs)
            groups_in_selected = [cand_groups[i] for i in idxs]

            team_a_ids, team_b_ids, split_score = best_team_split_for_four(
                players_map, members, groups_in_selected, db, strict_mode=True
            )

            score = (max_idx,) + split_score

            if best_pick is None or score < best_score:
                best_pick = (team_a_ids, team_b_ids)
                best_score = score

    if best_pick:
        return best_pick[0], best_pick[1]

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

def enabled_courts(db: dict):
    refresh_courts(db)
    am = db["system_settings"].get("auto_match_courts", {})
    return [cid for cid in db["courts_state"].keys() if am.get(cid, False)]

def find_free_court(db, preferred=None, only_enabled=False):
    refresh_courts(db)
    allowed = set(enabled_courts(db)) if only_enabled else set(db["courts_state"].keys())

    if preferred is not None:
        cid = str(preferred)
        if cid in allowed and cid in db["courts_state"] and db["courts_state"][cid] is None:
            return cid

    for cid in sorted(list(allowed), key=lambda x: safe_int(x, 999)):
        if db["courts_state"].get(cid) is None:
            return cid
    return None

def matchmake_internal(db, preferred_court=None, manual=False, manual_players=None, created_by=None, only_enabled=False):
    refresh_courts(db)

    if not db["system_settings"].get("is_session_active"):
        return {"status": "session_off"}

    free_court = find_free_court(db, preferred_court, only_enabled=only_enabled)
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

    for pid in team_a_ids + team_b_ids:
        p = db["players"][pid]
        p["status"] = "playing"
        p["last_active"] = now_ts()
        p["partner_req"] = None
        add_participant(current_event, pid)

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

def fill_enabled_courts(db, prefer_court=None):
    results = []
    refresh_courts(db)

    enabled = set(enabled_courts(db))

    if prefer_court is not None:
        pc = str(prefer_court)
        if pc in enabled and db["courts_state"].get(pc) is None:
            r = matchmake_internal(db, preferred_court=pc, only_enabled=True)
            if r.get("status") == "matched":
                results.append(r)

    max_iter = len(enabled) * 3
    for _ in range(max_iter):
        free = find_free_court(db, preferred=None, only_enabled=True)
        if not free:
            break
        r = matchmake_internal(db, preferred_court=free, only_enabled=True)
        if r.get("status") == "matched":
            results.append(r)
            continue
        else:
            break

    return results

# ---------------- ROUTES ----------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/login", methods=["POST"])
def login():
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
                "nickname": d.get("displayName", "User"),
                "pictureUrl": d.get("pictureUrl", ""),
                "mmr": 1000,
                "status": "offline",
                "queue_since": None,
                "last_active": now_ts(),
                "last_seen": now_ts(),
                "partner_req": None,
                "games_played": 0,
                "calib_played": 0,
                "calib_wins": 0,
                "calib_streak": 0
            }
        else:
            p = db["players"][uid]
            p["nickname"] = d.get("displayName", p.get("nickname", "User"))
            p["pictureUrl"] = d.get("pictureUrl", p.get("pictureUrl", ""))
            p["last_seen"] = now_ts()
            p.setdefault("games_played", 0)
            p.setdefault("calib_played", 0)
            p.setdefault("calib_wins", 0)
            p.setdefault("calib_streak", 0)

        p = db["players"][uid]
        p["role"] = "super" if uid == SUPER_ADMIN_ID else ("mod" if uid in db["mod_ids"] else "user")
        p["rank_title"] = rank_title_from_mmr(p)
        p["mmr_display"] = mmr_display(p)

        stats = calculate_smart_stats(uid, db.get("match_history", []))
        if stats["best_partner"] != "-" and stats["best_partner"] in db["players"]:
            stats["best_partner"] = db["players"][stats["best_partner"]].get("nickname", stats["best_partner"])
        if stats["nemesis"] != "-" and stats["nemesis"] in db["players"]:
            stats["nemesis"] = db["players"][stats["nemesis"]].get("nickname", stats["nemesis"])
        p["stats"] = stats

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

        players_list = []
        for p in db["players"].values():
            p["mmr"] = safe_int(p.get("mmr", 1000), 1000)
            p.setdefault("games_played", 0)
            p.setdefault("calib_played", 0)
            p.setdefault("calib_wins", 0)
            p.setdefault("calib_streak", 0)
            p["rank_title"] = rank_title_from_mmr(p)
            p["mmr_display"] = mmr_display(p)
            players_list.append(p)

        queue = [p for p in players_list if p.get("status") in ("active", "playing")]
        queue.sort(key=lambda x: (player_queue_since(x), safe_int(x.get("calib_played", 0), 0), safe_int(x.get("mmr", 1000), 1000)))

        leaderboard = sorted(players_list, key=lambda x: x.get("mmr", 1000), reverse=True)

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

        all_players = []
        for p in players_list:
            all_players.append({
                "id": p["id"],
                "nickname": p.get("nickname",""),
                "pictureUrl": p.get("pictureUrl",""),
                "status": p.get("status","offline"),
                "queue_since": p.get("queue_since"),
                "mmr": p.get("mmr", 1000),
                "mmr_display": p.get("mmr_display", str(p.get("mmr", 1000))),
                "rank_title": p.get("rank_title",""),
                "is_mod": p["id"] in db["mod_ids"],
                "partner_req": p.get("partner_req"),
                "calib_played": safe_int(p.get("calib_played", 0), 0),
                "calib_wins": safe_int(p.get("calib_wins", 0), 0),
                "calib_streak": safe_int(p.get("calib_streak", 0), 0)
            })

        uid = request.args.get("uid")
        incoming = []
        notis = []
        if uid and uid in db["players"]:
            for p in db["players"].values():
                if p.get("partner_req") == uid and p["id"] != uid:
                    incoming.append({"id": p["id"], "nickname": p.get("nickname",""), "pictureUrl": p.get("pictureUrl","")})
            notis = get_unread_notifications(db, uid, limit=5)

        save_db(db)
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

# ---------------- Toggle Status ----------------
@app.route("/api/toggle_status", methods=["POST"])
def toggle_status():
    db = get_db()
    refresh_courts(db)

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
        ceid = db["system_settings"].get("current_event_id")
        if ceid and ceid in db["events"]:
            add_participant(db["events"][ceid], uid)

    save_db(db)
    return jsonify({"success": True})

# ---------------- Admin / Session ----------------
@app.route("/api/admin/toggle_session", methods=["POST"])
def toggle_session():
    db = get_db()
    refresh_courts(db)

    d = request.json or {}
    uid = d.get("userId")
    action = d.get("action")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    if action == "start":
        db["system_settings"]["is_session_active"] = True

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

        for k in db["courts_state"].keys():
            db["courts_state"][k] = None

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

@app.route("/api/admin/update_courts", methods=["POST"])
def update_courts():
    db = get_db()
    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    c = safe_int(d.get("count", 2), 2)
    c = max(1, min(10, c))
    db["system_settings"]["total_courts"] = c
    refresh_courts(db)
    save_db(db)
    return jsonify({"success": True})

@app.route("/api/admin/set_auto_match_court", methods=["POST"])
def set_auto_match_court():
    db = get_db()
    refresh_courts(db)

    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    court_id = str(d.get("courtId"))
    enabled = bool(d.get("enabled"))

    if court_id not in db["courts_state"]:
        return jsonify({"error": "Invalid court"}), 400

    db["system_settings"]["auto_match_courts"][court_id] = enabled
    save_db(db)
    return jsonify({"success": True, "courtId": court_id, "enabled": enabled})

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
        p["games_played"] = 0
        p["calib_played"] = 0
        p["calib_wins"] = 0
        p["calib_streak"] = 0

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
        # ไม่แตะ calibration (ยัง unrank อยู่จนกว่าจะครบ 10 เกม)
        save_db(db)
        return jsonify({"success": True})
    return jsonify({"error": "Not found"}), 404

# ---------------- Matchmake (STAFF ONLY) ----------------
@app.route("/api/matchmake", methods=["POST"])
def matchmake():
    db = get_db()
    refresh_courts(db)

    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    matches = fill_enabled_courts(db)
    save_db(db)
    return jsonify({"status": "matched" if matches else "waiting", "matched_count": len(matches), "matches": matches})

@app.route("/api/matchmake/manual", methods=["POST"])
def manual_matchmake():
    db = get_db()
    refresh_courts(db)

    d = request.json or {}
    uid = d.get("userId")
    if not uid or not is_staff(uid, db):
        return jsonify({"error": "Unauthorized"}), 403

    court_id = d.get("courtId")
    p_ids = d.get("playerIds", [])
    res = matchmake_internal(db, preferred_court=court_id, manual=True, manual_players=p_ids, created_by=uid, only_enabled=False)
    save_db(db)
    if res.get("status") != "matched":
        return jsonify({"error": res.get("status")}), 400
    return jsonify({"success": True, "matchId": res.get("matchId")})

# ---------------- Cancel Match ----------------
@app.route("/api/cancel_match", methods=["POST"])
def cancel_match():
    db = get_db()
    refresh_courts(db)

    d = request.json or {}
    uid = d.get("userId")
    court_id = str(d.get("courtId"))

    if not uid or not court_id:
        return jsonify({"error": "Missing data"}), 400

    match = db["courts_state"].get(court_id)
    if not match:
        return jsonify({"error": "No match"}), 400

    team_a_ids = match.get("team_a_ids", [])
    team_b_ids = match.get("team_b_ids", [])

    is_player = uid in team_a_ids or uid in team_b_ids
    if not (is_player or is_staff(uid, db)):
        return jsonify({"error": "Unauthorized"}), 403

    add_avoid(db, "cancel", team_a_ids, team_b_ids, ttl=1800)

    qsnap = match.get("queue_snapshot", {})
    psnap = match.get("partner_snapshot", {})
    for pid in team_a_ids + team_b_ids:
        if pid in db["players"]:
            p = db["players"][pid]
            p["status"] = "active" if db["system_settings"].get("is_session_active") else "offline"
            p["queue_since"] = qsnap.get(pid, p.get("queue_since", now_ts())) if db["system_settings"].get("is_session_active") else None
            p["partner_req"] = psnap.get(pid, None)

    db["courts_state"][court_id] = None

    prefer = court_id if db["system_settings"]["auto_match_courts"].get(court_id, False) else None
    matches = fill_enabled_courts(db, prefer_court=prefer)

    save_db(db)
    return jsonify({"success": True, "auto_matches": matches, "matched_count": len(matches)})

# ---------------- Submit Result ----------------
@app.route("/api/submit_result", methods=["POST"])
def submit_result():
    db = get_db()
    refresh_courts(db)

    d = request.json or {}
    uid = d.get("userId")
    court_id = str(d.get("courtId"))
    winner = d.get("winner")

    if not uid or not court_id or winner not in ("A", "B"):
        return jsonify({"error": "Missing data"}), 400

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

    deltas, mmr_meta = compute_team_deltas(team_a_ids, team_b_ids, winner, db["players"])
    snapshot = {}

    def did_win(pid: str) -> bool:
        return (winner == "A" and pid in team_a_ids) or (winner == "B" and pid in team_b_ids)

    for pid in team_a_ids + team_b_ids:
        if pid not in db["players"]:
            continue
        p = db["players"][pid]
        p.setdefault("games_played", 0)
        p.setdefault("calib_played", 0)
        p.setdefault("calib_wins", 0)
        p.setdefault("calib_streak", 0)

        p["games_played"] = safe_int(p["games_played"], 0) + 1

        if safe_int(p["calib_played"], 0) < 10:
            p["calib_played"] = safe_int(p["calib_played"], 0) + 1
            if did_win(pid):
                p["calib_wins"] = safe_int(p["calib_wins"], 0) + 1
                p["calib_streak"] = safe_int(p["calib_streak"], 0) + 1
            else:
                p["calib_streak"] = 0

            if p["calib_played"] == 10:
                push_notification(db, pid, "✅ Calibration ครบ 10 เกมแล้ว! Rank ถูกเปิดใช้งาน 🎖️", {"type":"calib_done"})

    for pid, delta in deltas.items():
        if pid not in db["players"]:
            continue
        p = db["players"][pid]
        old = safe_int(p.get("mmr", 1000), 1000)
        new = max(0, old + int(delta))
        p["mmr"] = new
        p["status"] = "active" if db["system_settings"].get("is_session_active") else "offline"
        p["queue_since"] = now_ts() if db["system_settings"].get("is_session_active") else None
        p["partner_req"] = None
        snapshot[pid] = {"change": f"{int(delta):+d}", "old": old, "new": new}

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

    add_avoid(db, "finish", team_a_ids, team_b_ids, ttl=600)
    db["courts_state"][court_id] = None

    prefer = court_id if db["system_settings"]["auto_match_courts"].get(court_id, False) else None
    matches = fill_enabled_courts(db, prefer_court=prefer)

    save_db(db)
    return jsonify({"success": True, "mmr_meta": mmr_meta, "auto_matches": matches, "matched_count": len(matches)})

# ---------------- Events (optional) ----------------
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
