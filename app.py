import json
import os
import time
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# --- CONFIG ---
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"
DATA_FILE = "/var/data/izesquad_data.json"

# --- DATABASE ---
default_db = {
    "system_settings": {"total_courts": 2, "is_session_active": False, "current_event_id": None},
    "mod_ids": [], "players": {}, "events": {}, "match_history": [], "billing_history": []
}
active_courts = {} 

def load_data():
    global db, active_courts
    if not os.path.exists(DATA_FILE):
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        db = default_db.copy()
        save_data()
    else:
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                db = json.load(f)
                for k in default_db:
                    if k not in db: db[k] = default_db[k]
                if "system_settings" not in db: db["system_settings"] = default_db["system_settings"]
        except: db = default_db.copy(); save_data()
    refresh_courts()

def save_data():
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, ensure_ascii=False, indent=4)
    except: pass

def refresh_courts():
    target = db['system_settings'].get('total_courts', 2)
    for i in range(1, target + 1):
        if i not in active_courts: active_courts[i] = None
    current_keys = list(active_courts.keys())
    for k in current_keys:
        if k > target: del active_courts[k]

load_data()

# --- HELPERS ---
def get_rank_title(mmr):
    try: mmr = int(mmr)
    except: mmr = 1000
    if mmr <= 800: return "NOOB DOG 🐶"
    elif mmr <= 1200: return "NOOB 🐣"
    elif mmr <= 1400: return "เด็กกระโปก 👶"
    elif mmr <= 1600: return "ชนะจนเบื่อ 🥱"
    else: return "โปรเพรเย๋อ 👽"

def calculate_smart_stats(uid):
    try:
        my_matches = [m for m in db['match_history'] if uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', [])]
        total = len(my_matches)
        if total == 0: return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}
        wins = 0; current_streak = 0; max_streak = 0; partners = {}; opponents = {}
        for m in reversed(my_matches):
            team_a_ids = m.get('team_a_ids', []); team_b_ids = m.get('team_b_ids', [])
            is_team_a = uid in team_a_ids; my_team = 'A' if is_team_a else 'B'
            is_winner = (m.get('winner_team') == my_team)
            if is_winner: wins += 1; current_streak += 1
            else: current_streak = 0
            if current_streak > max_streak: max_streak = current_streak
            my_ids = team_a_ids if is_team_a else team_b_ids
            my_names = m.get('team_a', []) if is_team_a else m.get('team_b', [])
            for pid, pname in zip(my_ids, my_names):
                if pid != uid:
                    if pid not in partners: partners[pid] = {'played': 0, 'won': 0, 'name': pname}
                    partners[pid]['played'] += 1
                    if is_winner: partners[pid]['won'] += 1
            opp_ids = team_b_ids if is_team_a else team_a_ids
            opp_names = m.get('team_b', []) if is_team_a else m.get('team_a', [])
            for pid, pname in zip(opp_ids, opp_names):
                if pid not in opponents: opponents[pid] = {'played': 0, 'lost': 0, 'name': pname}
                opponents[pid]['played'] += 1
                if not is_winner: opponents[pid]['lost'] += 1
        best_partner, best_wr = "-", -1
        for pid, d in partners.items():
            if d['played'] >= 2:
                wr = (d['won'] / d['played']) * 100
                if wr > best_wr: best_wr = wr; best_partner = f"{d['name']} ({int(wr)}%)"
        nemesis, worst_wr = "-", -1
        for pid, d in opponents.items():
            if d['played'] >= 2:
                lr = (d['lost'] / d['played']) * 100
                if lr > worst_wr: worst_wr = lr; nemesis = f"{d['name']} (แพ้ {int(lr)}%)"
        return {"win_rate": int((wins / total) * 100), "total": total, "streak": max_streak, "best_partner": best_partner, "nemesis": nemesis}
    except: return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}

# --- ROUTES ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    d = request.json; uid = d.get('userId')
    if uid not in db['players']:
        db['players'][uid] = {"id": uid, "nickname": d.get('displayName'), "pictureUrl": d.get('pictureUrl'), "mmr": 1000, "status": "offline", "last_active": time.time(), "partner_req": None}
        save_data()
    p = db['players'][uid]; p['pictureUrl'] = d.get('pictureUrl')
    p['role'] = 'super' if uid == SUPER_ADMIN_ID else ('mod' if uid in db['mod_ids'] else 'user')
    p['rank_title'] = get_rank_title(p.get('mmr', 1000))
    p['stats'] = calculate_smart_stats(uid)
    my_history = [m for m in db['match_history'] if uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', [])]
    p['my_history'] = my_history[:50]
    return jsonify(p)

@app.route('/api/get_dashboard')
def get_dashboard():
    try:
        c_data = {}
        for cid, m in active_courts.items():
            if m:
                m['elapsed'] = int(time.time() - m['start_time'])
                m['team_a_data'] = []
                for uid in m.get('team_a_ids', []):
                    if uid in db['players']: m['team_a_data'].append({"name": db['players'][uid]['nickname'], "pic": db['players'][uid].get('pictureUrl', '')})
                m['team_b_data'] = []
                for uid in m.get('team_b_ids', []):
                    if uid in db['players']: m['team_b_data'].append({"name": db['players'][uid]['nickname'], "pic": db['players'][uid].get('pictureUrl', '')})
            c_data[cid] = m

        players_list = []
        for p in db['players'].values():
            try: mmr = int(p.get('mmr', 1000))
            except: mmr = 1000
            p['mmr'] = mmr
            try: la = float(p.get('last_active', 0))
            except: la = 0.0
            p['last_active'] = la
            p['rank_title'] = get_rank_title(mmr)
            players_list.append(p)

        active = [p for p in players_list if p.get('status') in ['active', 'playing']]
        active.sort(key=lambda x: x['last_active'])
        lb = sorted(players_list, key=lambda x: x['mmr'], reverse=True)

        event_list = []
        for eid, e in db['events'].items():
            joined_users = []
            for pid in e.get('players', []):
                if pid in db['players']:
                    joined_users.append({"id": pid, "nickname": db['players'][pid]['nickname'], "pictureUrl": db['players'][pid].get('pictureUrl', '')})
            e['joined_users'] = joined_users
            
            raw_dt = e.get('datetime', 0)
            if raw_dt is None: 
                e['sort_key'] = 0 # ✅ FIX: Handle None
            elif isinstance(raw_dt, str):
                try: e['sort_key'] = datetime.fromisoformat(raw_dt).timestamp()
                except: e['sort_key'] = 0
            else:
                e['sort_key'] = float(raw_dt) # ✅ FIX: Handle numbers/floats correctly
            event_list.append(e)
        event_list.sort(key=lambda x: x['sort_key'])

        all_players_data = []
        for p in players_list:
            all_players_data.append({"id": p['id'], "nickname": p['nickname'], "pictureUrl": p.get('pictureUrl', ''), "status": p.get('status', 'offline'), "last_active": p['last_active'], "is_mod": p['id'] in db['mod_ids'], "partner_req": p.get('partner_req')})

        return jsonify({
            "system": db['system_settings'], "courts": c_data, "queue": active, "queue_count": len(active),
            "events": event_list, "leaderboard": lb, "match_history": db['match_history'][:20], "all_players": all_players_data
        })
    except Exception as e:
        print(f"DASHBOARD ERROR: {e}")
        return jsonify({"system": {}, "courts": {}, "queue": [], "queue_count": 0, "events": [], "leaderboard": [], "match_history": [], "all_players": []})

@app.route('/api/toggle_status', methods=['POST'])
def toggle_status():
    uid = request.json.get('userId')
    if uid in db['players']:
        curr = db['players'][uid].get('status', 'offline')
        if curr in ['active', 'playing']: db['players'][uid]['status'] = 'offline'; db['players'][uid]['partner_req'] = None
        else: db['players'][uid]['status'] = 'active'; db['players'][uid]['last_active'] = time.time()
        save_data()
    return jsonify({"success": True})

@app.route('/api/request_partner', methods=['POST'])
def request_partner():
    d = request.json; uid = d['userId']; target = d['targetId']
    if uid in db['players']:
        if db['players'][uid].get('status') != 'active': return jsonify({"error": "ต้อง Check-in ก่อนนะครับ"})
        if target not in db['players'] or db['players'][target].get('status') != 'active': return jsonify({"error": "เพื่อนยังไม่ Check-in"})
        db['players'][uid]['partner_req'] = target; save_data()
        return jsonify({"success": True})
    return jsonify({"error": "User not found"})

@app.route('/api/cancel_request', methods=['POST'])
def cancel_request():
    uid = request.json['userId']
    if uid in db['players']: db['players'][uid]['partner_req'] = None; save_data(); return jsonify({"success": True})
    return jsonify({"error": "User not found"})

@app.route('/api/matchmake', methods=['POST'])
def matchmake():
    free = next((k for k, v in active_courts.items() if v is None), None)
    if not free: return jsonify({"status": "full"})
    active_players = [p for p in db['players'].values() if p.get('status') == 'active']
    
    groups = []; processed_ids = set()
    for p in active_players:
        if p['id'] in processed_ids: continue
        partner_id = p.get('partner_req')
        partner = db['players'].get(partner_id) if partner_id else None
        if partner and partner.get('status') == 'active' and partner['id'] not in processed_ids:
            try: queue_time = max(float(p.get('last_active', 0)), float(partner.get('last_active', 0)))
            except: queue_time = time.time()
            groups.append({"type": "pair", "members": [p, partner], "queue_time": queue_time})
            processed_ids.add(p['id']); processed_ids.add(partner['id'])
        else:
            try: queue_time = float(p.get('last_active', 0))
            except: queue_time = time.time()
            groups.append({"type": "single", "members": [p], "queue_time": queue_time})
            processed_ids.add(p['id'])
            
    groups.sort(key=lambda x: x['queue_time'])
    selected_players = []
    for g in groups:
        if len(selected_players) + len(g['members']) <= 4: selected_players.extend(g['members'])
        if len(selected_players) == 4: break
    if len(selected_players) < 4: return jsonify({"status": "waiting"})
    
    team_a = []; team_b = []; remaining = selected_players.copy()
    def are_pair(u1, u2): return u1.get('partner_req') == u2['id'] or u2.get('partner_req') == u1['id']
    pair_found = False
    for i in range(len(remaining)):
        for j in range(i + 1, len(remaining)):
            if are_pair(remaining[i], remaining[j]):
                team_a = [remaining[i], remaining[j]]; remaining.pop(j); remaining.pop(i); pair_found = True; break
        if pair_found: break
    if not team_a:
        remaining.sort(key=lambda x: int(x.get('mmr', 1000)))
        team_a = [remaining.pop(0), remaining.pop(0)]
    team_b = remaining
    
    match = {"team_a": [p['nickname'] for p in team_a], "team_a_ids": [p['id'] for p in team_a], "team_b": [p['nickname'] for p in team_b], "team_b_ids": [p['id'] for p in team_b], "start_time": time.time()}
    active_courts[free] = match
    for p in selected_players: db['players'][p['id']]['status'] = 'playing'
    save_data()
    return jsonify({"status": "matched"})

@app.route('/api/submit_result', methods=['POST'])
def submit_result():
    d=request.json; cid=int(d['courtId']); winner=d['winner']; req_uid=d['userId']
    m = active_courts.get(cid)
    if not m: return jsonify({"error": "No match"})
    is_mod = req_uid in db['mod_ids']; is_super = req_uid == SUPER_ADMIN_ID
    is_player = (req_uid in m.get('team_a_ids', [])) or (req_uid in m.get('team_b_ids', []))
    if not (is_super or is_mod or is_player): return jsonify({"error": "Unauthorized"}), 403
    
    mmr_snapshot = {}; win_ids = m['team_a_ids'] if winner == 'A' else m['team_b_ids']; lose_ids = m['team_b_ids'] if winner == 'A' else m['team_a_ids']
    for uid in win_ids:
        if uid in db['players']:
            old = int(db['players'][uid].get('mmr', 1000)); new = old + 25
            db['players'][uid]['mmr'] = new; db['players'][uid]['status'] = 'active'; db['players'][uid]['last_active'] = time.time(); db['players'][uid]['partner_req'] = None
            mmr_snapshot[uid] = {"old": old, "new": new, "change": "+25"}
    for uid in lose_ids:
        if uid in db['players']:
            old = int(db['players'][uid].get('mmr', 1000)); new = old - 20
            db['players'][uid]['mmr'] = new; db['players'][uid]['status'] = 'active'; db['players'][uid]['last_active'] = time.time(); db['players'][uid]['partner_req'] = None
            mmr_snapshot[uid] = {"old": old, "new": new, "change": "-20"}

    hist = {"date": datetime.now().strftime("%Y-%m-%d %H:%M"), "team_a": m['team_a'], "team_a_ids": m['team_a_ids'], "team_b": m['team_b'], "team_b_ids": m['team_b_ids'], "winner_team": winner, "mmr_snapshot": mmr_snapshot}
    db['match_history'].insert(0, hist); active_courts[cid] = None; save_data()
    return jsonify({"success": True})

@app.route('/api/matchmake/manual', methods=['POST'])
def manual_matchmake():
    d=request.json; req_uid=d['userId']; is_mod = req_uid in db['mod_ids']
    if req_uid != SUPER_ADMIN_ID and not is_mod: return jsonify({"error": "Unauthorized"}), 403
    court_id = int(d['courtId']); p_ids = d['playerIds']
    if active_courts.get(court_id): return jsonify({"error": "Court not empty"})
    players = [db['players'][pid] for pid in p_ids if pid in db['players']]
    if len(players) != 4: return jsonify({"error": "Need 4 valid players"})
    match = {"team_a": [players[0]['nickname'], players[1]['nickname']], "team_a_ids": [players[0]['id'], players[1]['id']], "team_b": [players[2]['nickname'], players[3]['nickname']], "team_b_ids": [players[2]['id'], players[3]['id']], "start_time": time.time()}
    active_courts[court_id] = match
    for p in players: db['players'][p['id']]['status'] = 'playing'; db['players'][p['id']]['partner_req'] = None
    save_data()
    return jsonify({"success": True})

@app.route('/api/admin/toggle_session', methods=['POST'])
def toggle_session():
    d=request.json; uid=d.get('userId'); action=d.get('action'); is_mod = uid in db['mod_ids']
    if uid != SUPER_ADMIN_ID and not is_mod: return jsonify({"error": "Unauthorized"}), 403
    if action == 'start':
        db['system_settings']['is_session_active'] = True
        if not db['system_settings'].get('current_event_id'):
            eid = str(uuid.uuid4())[:8]; today = datetime.now().strftime("%d/%m/%Y")
            db['events'][eid] = {"id": eid, "name": f"ก๊วน {today}", "datetime": time.time(), "players": [], "status": "active"}
            db['system_settings']['current_event_id'] = eid
    else:
        db['system_settings']['is_session_active'] = False; db['system_settings']['current_event_id'] = None
        for p in db['players'].values(): 
            if p.get('status') != 'offline': p['status'] = 'offline'; p['partner_req'] = None
    save_data(); return jsonify({"success": True})

@app.route('/api/admin/update_courts', methods=['POST'])
def update_courts(): count = int(request.json.get('count', 2)); db['system_settings']['total_courts'] = count; save_data(); refresh_courts(); return jsonify({"success": True})
@app.route('/api/admin/manage_mod', methods=['POST'])
def manage_mod():
    d=request.json; requester=d.get('requesterId'); target_id=d.get('targetUserId'); action=d.get('action')
    if requester != SUPER_ADMIN_ID: return jsonify({"error": "Super Admin Only"}), 403
    if action == 'promote': 
        if target_id not in db['mod_ids']: db['mod_ids'].append(target_id)
    else: 
        if target_id in db['mod_ids']: db['mod_ids'].remove(target_id)
    save_data(); return jsonify({"success": True})
@app.route('/api/admin/reset_system', methods=['POST'])
def reset_system():
    d=request.json; uid=d.get('userId')
    if uid != SUPER_ADMIN_ID: return jsonify({"error": "Super Admin Only"}), 403
    db['match_history'] = []; db['billing_history'] = []; db['events'] = {}
    for pid in db['players']: db['players'][pid]['mmr'] = 1000; db['players'][pid]['status'] = 'offline'; db['players'][pid]['partner_req'] = None
    global active_courts; 
    for cid in active_courts: active_courts[cid] = None
    db['system_settings']['is_session_active'] = False; db['system_settings']['current_event_id'] = None
    save_data(); return jsonify({"success": True})
@app.route('/api/event/create', methods=['POST'])
def create_event(): d=request.json; eid=str(uuid.uuid4())[:8]; db['events'][eid] = {"id": eid, "name": d['name'], "datetime": d['datetime'], "players": [], "status": "open"}; save_data(); return jsonify({"success": True})
@app.route('/api/event/delete', methods=['POST'])
def delete_event(): eid=request.json.get('eventId'); 
    if eid in db['events']: del db['events'][eid]
    save_data(); return jsonify({"success": True})
@app.route('/api/event/join_toggle', methods=['POST'])
def join_event_toggle():
    d=request.json; eid=d['eventId']; uid=d['userId']
    if eid in db['events']:
        if uid in db['events'][eid]['players']: db['events'][eid]['players'].remove(uid)
        else: db['events'][eid]['players'].append(uid)
        save_data()
        return jsonify({"success": True})
    return jsonify({"error": "Not found"})
@app.route('/api/admin/set_mmr', methods=['POST'])
def set_mmr(): d=request.json; uid=d['targetUserId']; new_mmr=int(d['newMmr']); 
    if uid in db['players']: db['players'][uid]['mmr'] = new_mmr; save_data(); return jsonify({"success": True})
    return jsonify({"error": "User not found"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)