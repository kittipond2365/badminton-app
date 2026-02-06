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
    "system_settings": {
        "total_courts": 2,
        "is_session_active": False,
        "current_event_id": None
    },
    "mod_ids": [],
    "players": {},
    "events": {},
    "match_history": [],
    "billing_history": []
}

# Load data fresh from disk every time to ensure sync across workers
def get_db():
    if not os.path.exists(DATA_FILE):
        if not os.path.exists(os.path.dirname(DATA_FILE)):
            os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(default_db, f, ensure_ascii=False, indent=4)
        return default_db.copy()
    else:
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Ensure structure
                for k in default_db:
                    if k not in data: data[k] = default_db[k]
                return data
        except:
            return default_db.copy()

def save_db(data):
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Save Error: {e}")

# Global cache for courts (reset on restart is fine, or could save to db)
active_courts = {} 

def refresh_courts(db_data):
    target = db_data['system_settings'].get('total_courts', 2)
    # Ensure keys exist
    for i in range(1, target + 1):
        if i not in active_courts:
            active_courts[i] = None
    # Remove excess
    current = list(active_courts.keys())
    for k in current:
        if k > target:
            del active_courts[k]

# --- HELPERS ---
def get_rank_title(mmr):
    try: mmr = int(mmr)
    except: mmr = 1000
    if mmr <= 800: return "NOOB DOG 🐶"
    elif mmr <= 1200: return "NOOB 🐣"
    elif mmr <= 1400: return "เด็กกระโปก 👶"
    elif mmr <= 1600: return "ชนะจนเบื่อ 🥱"
    else: return "โปรเพรเย๋อ 👽"

def calculate_stats(uid, history):
    if not history: return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}
    wins = 0; current_streak = 0; max_streak = 0; partners = {}; opponents = {}
    
    # Filter my matches
    my_matches = [m for m in history if uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', [])]
    total = len(my_matches)
    if total == 0: return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}

    for m in reversed(my_matches):
        team_a = m.get('team_a_ids', [])
        team_b = m.get('team_b_ids', [])
        my_team = 'A' if uid in team_a else 'B'
        winner = m.get('winner_team')
        
        is_win = (winner == my_team)
        if is_win: 
            wins += 1
            current_streak += 1
        else: 
            current_streak = 0
        if current_streak > max_streak: max_streak = current_streak
        
        # Partner & Nemesis Logic
        my_side = team_a if my_team == 'A' else team_b
        op_side = team_b if my_team == 'A' else team_a
        my_names = m.get('team_a' if my_team == 'A' else 'team_b', [])
        op_names = m.get('team_b' if my_team == 'A' else 'team_a', [])
        
        for pid, pname in zip(my_side, my_names):
            if pid != uid:
                if pid not in partners: partners[pid] = {'p': 0, 'w': 0, 'n': pname}
                partners[pid]['p'] += 1
                if is_win: partners[pid]['w'] += 1
                
        for pid, pname in zip(op_side, op_names):
            if pid not in opponents: opponents[pid] = {'p': 0, 'l': 0, 'n': pname}
            opponents[pid]['p'] += 1
            if not is_win: opponents[pid]['l'] += 1

    best_p = "-"
    best_rate = -1
    for pid, v in partners.items():
        if v['p'] >= 2:
            rate = (v['w']/v['p'])*100
            if rate > best_rate: best_rate=rate; best_p=f"{v['n']} ({int(rate)}%)"
            
    nemesis = "-"
    worst_rate = -1
    for pid, v in opponents.items():
        if v['p'] >= 2:
            rate = (v['l']/v['p'])*100
            if rate > worst_rate: worst_rate=rate; nemesis=f"{v['n']} (แพ้ {int(rate)}%)"

    return {"win_rate": int((wins/total)*100), "total": total, "streak": max_streak, "best_partner": best_p, "nemesis": nemesis}

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    db = get_db() # Reload DB
    d = request.json
    uid = d.get('userId')
    
    if uid not in db['players']:
        db['players'][uid] = {
            "id": uid, "nickname": d.get('displayName'), "pictureUrl": d.get('pictureUrl'),
            "mmr": 1000, "status": "offline", "last_active": time.time(), "partner_req": None
        }
        save_db(db)
    else:
        # Update info
        db['players'][uid]['pictureUrl'] = d.get('pictureUrl')
        # Keep nickname update optional or mandatory? Let's update it.
        # db['players'][uid]['nickname'] = d.get('displayName') 
        save_db(db)
    
    p = db['players'][uid]
    p['role'] = 'super' if uid == SUPER_ADMIN_ID else ('mod' if uid in db['mod_ids'] else 'user')
    p['rank_title'] = get_rank_title(p.get('mmr', 1000))
    p['stats'] = calculate_stats(uid, db['match_history'])
    
    # History
    my_hist = [m for m in db['match_history'] if uid in m.get('team_a_ids',[]) or uid in m.get('team_b_ids',[])]
    p['my_history'] = my_hist[:50]
    
    return jsonify(p)

@app.route('/api/get_dashboard')
def get_dashboard():
    db = get_db() # Reload DB fresh
    refresh_courts(db)
    
    # Process Courts
    c_data = {}
    for cid, m in active_courts.items():
        if m:
            m['elapsed'] = int(time.time() - m['start_time'])
            # Populate names/pics
            m['team_a_data'] = []
            for uid in m.get('team_a_ids', []):
                pl = db['players'].get(uid)
                if pl: m['team_a_data'].append({"name": pl['nickname'], "pic": pl.get('pictureUrl','')})
            m['team_b_data'] = []
            for uid in m.get('team_b_ids', []):
                pl = db['players'].get(uid)
                if pl: m['team_b_data'].append({"name": pl['nickname'], "pic": pl.get('pictureUrl','')})
        c_data[cid] = m

    # Process Players
    players_list = []
    for p in db['players'].values():
        try: p['mmr'] = int(p.get('mmr', 1000))
        except: p['mmr'] = 1000
        try: p['last_active'] = float(p.get('last_active', 0))
        except: p['last_active'] = 0.0
        p['rank_title'] = get_rank_title(p['mmr'])
        players_list.append(p)

    # Active Queue
    active = [p for p in players_list if p.get('status') in ['active','playing']]
    active.sort(key=lambda x: x['last_active'])

    # Leaderboard
    lb = sorted(players_list, key=lambda x: x['mmr'], reverse=True)

    # Events
    event_list = []
    for eid, e in db['events'].items():
        joined = []
        for pid in e.get('players', []):
            pl = db['players'].get(pid)
            if pl: joined.append({"id": pid, "nickname": pl['nickname'], "pictureUrl": pl.get('pictureUrl','')})
        e['joined_users'] = joined
        
        # Sort Key Safe
        raw_dt = e.get('datetime')
        if isinstance(raw_dt, (int, float)):
            e['sort_key'] = raw_dt
        elif isinstance(raw_dt, str):
            try: e['sort_key'] = datetime.fromisoformat(raw_dt).timestamp()
            except: e['sort_key'] = 0
        else:
            e['sort_key'] = 0
        event_list.append(e)
    event_list.sort(key=lambda x: x['sort_key'])

    # All Players (Lightweight)
    all_players = []
    for p in players_list:
        all_players.append({
            "id": p['id'], "nickname": p['nickname'], "pictureUrl": p.get('pictureUrl',''),
            "status": p.get('status','offline'), "last_active": p['last_active'],
            "is_mod": p['id'] in db['mod_ids'], "partner_req": p.get('partner_req')
        })

    return jsonify({
        "system": db['system_settings'],
        "courts": c_data,
        "queue": active,
        "queue_count": len(active),
        "events": event_list,
        "leaderboard": lb,
        "match_history": db['match_history'][:20],
        "all_players": all_players
    })

@app.route('/api/toggle_status', methods=['POST'])
def toggle_status():
    db = get_db()
    uid = request.json.get('userId')
    if uid in db['players']:
        curr = db['players'][uid].get('status', 'offline')
        if curr in ['active','playing']:
            db['players'][uid]['status'] = 'offline'
            db['players'][uid]['partner_req'] = None
        else:
            db['players'][uid]['status'] = 'active'
            db['players'][uid]['last_active'] = time.time()
        save_db(db)
    return jsonify({"success":True})

@app.route('/api/request_partner', methods=['POST'])
def request_partner():
    db = get_db()
    d = request.json
    uid = d['userId']; target = d['targetId']
    if uid in db['players']:
        if db['players'][uid].get('status') != 'active':
            return jsonify({"error": "ต้อง Check-in ก่อนนะครับ"})
        if target not in db['players'] or db['players'][target].get('status') != 'active':
            return jsonify({"error": "เพื่อนยังไม่ Check-in"})
        
        db['players'][uid]['partner_req'] = target
        save_db(db)
        return jsonify({"success":True})
    return jsonify({"error":"User not found"})

@app.route('/api/cancel_request', methods=['POST'])
def cancel_request():
    db = get_db()
    uid = request.json['userId']
    if uid in db['players']:
        db['players'][uid]['partner_req'] = None
        save_db(db)
        return jsonify({"success":True})
    return jsonify({"error":"User not found"})

# --- GAME LOGIC ---

@app.route('/api/matchmake', methods=['POST'])
def matchmake():
    db = get_db()
    refresh_courts(db)
    free = next((k for k,v in active_courts.items() if v is None), None)
    if not free: return jsonify({"status":"full"})
    
    active_players = [p for p in db['players'].values() if p.get('status')=='active']
    groups = []; processed = set()
    
    for p in active_players:
        if p['id'] in processed: continue
        partner_id = p.get('partner_req')
        partner = db['players'].get(partner_id) if partner_id else None
        
        if partner and partner.get('status') == 'active' and partner['id'] not in processed:
            # Pair
            qt = max(float(p.get('last_active',0)), float(partner.get('last_active',0)))
            groups.append({"type":"pair", "members":[p, partner], "qt": qt})
            processed.add(p['id']); processed.add(partner['id'])
        else:
            # Single
            qt = float(p.get('last_active',0))
            groups.append({"type":"single", "members":[p], "qt": qt})
            processed.add(p['id'])
            
    groups.sort(key=lambda x: x['qt'])
    
    selected = []
    for g in groups:
        if len(selected) + len(g['members']) <= 4:
            selected.extend(g['members'])
        if len(selected) == 4: break
        
    if len(selected) < 4: return jsonify({"status":"waiting"})
    
    # Team formation
    team_a = []; team_b = []
    remaining = selected.copy()
    
    def are_pair(u1, u2): return u1.get('partner_req') == u2['id'] or u2.get('partner_req') == u1['id']
    
    pair_found = False
    for i in range(len(remaining)):
        for j in range(i+1, len(remaining)):
            if are_pair(remaining[i], remaining[j]):
                team_a = [remaining[i], remaining[j]]
                remaining.pop(j); remaining.pop(i)
                pair_found = True; break
        if pair_found: break
        
    if not team_a:
        remaining.sort(key=lambda x: int(x.get('mmr', 1000)))
        team_a = [remaining.pop(0), remaining.pop(0)]
    
    team_b = remaining
    
    match = {
        "team_a": [p['nickname'] for p in team_a], "team_a_ids": [p['id'] for p in team_a],
        "team_b": [p['nickname'] for p in team_b], "team_b_ids": [p['id'] for p in team_b],
        "start_time": time.time()
    }
    active_courts[free] = match
    
    for p in selected:
        db['players'][p['id']]['status'] = 'playing'
        
    save_db(db)
    return jsonify({"status":"matched"})

@app.route('/api/matchmake/manual', methods=['POST'])
def manual_matchmake():
    db = get_db()
    d = request.json
    uid = d['userId']
    
    if uid != SUPER_ADMIN_ID and uid not in db['mod_ids']:
        return jsonify({"error":"Unauthorized"}), 403
        
    cid = int(d['courtId'])
    if active_courts.get(cid): return jsonify({"error":"Court full"})
    
    p_ids = d['playerIds']
    players = [db['players'][pid] for pid in p_ids if pid in db['players']]
    if len(players) != 4: return jsonify({"error":"Need 4 players"})
    
    match = {
        "team_a": [players[0]['nickname'], players[1]['nickname']], "team_a_ids": [players[0]['id'], players[1]['id']],
        "team_b": [players[2]['nickname'], players[3]['nickname']], "team_b_ids": [players[2]['id'], players[3]['id']],
        "start_time": time.time()
    }
    active_courts[cid] = match
    for p in players:
        db['players'][p['id']]['status'] = 'playing'
        db['players'][p['id']]['partner_req'] = None
        
    save_db(db)
    return jsonify({"success":True})

@app.route('/api/submit_result', methods=['POST'])
def submit_result():
    db = get_db()
    d = request.json
    cid = int(d['courtId'])
    winner = d['winner']
    uid = d['userId']
    
    m = active_courts.get(cid)
    if not m: return jsonify({"error":"No match"})
    
    is_mod = uid in db['mod_ids']
    is_super = uid == SUPER_ADMIN_ID
    is_player = (uid in m.get('team_a_ids',[]) or uid in m.get('team_b_ids',[]))
    
    if not (is_super or is_mod or is_player): return jsonify({"error":"Unauthorized"}), 403
    
    snapshot = {}
    win_ids = m['team_a_ids'] if winner=='A' else m['team_b_ids']
    lose_ids = m['team_b_ids'] if winner=='A' else m['team_a_ids']
    
    for pid in win_ids:
        if pid in db['players']:
            old = int(db['players'][pid].get('mmr', 1000))
            new = old + 25
            db['players'][pid]['mmr'] = new
            db['players'][pid]['status'] = 'active'
            db['players'][pid]['last_active'] = time.time()
            db['players'][pid]['partner_req'] = None
            snapshot[pid] = {"old":old, "new":new, "change":"+25"}
            
    for pid in lose_ids:
        if pid in db['players']:
            old = int(db['players'][pid].get('mmr', 1000))
            new = old - 20
            db['players'][pid]['mmr'] = new
            db['players'][pid]['status'] = 'active'
            db['players'][pid]['last_active'] = time.time()
            db['players'][pid]['partner_req'] = None
            snapshot[pid] = {"old":old, "new":new, "change":"-20"}
            
    hist = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "team_a": m['team_a'], "team_a_ids": m['team_a_ids'],
        "team_b": m['team_b'], "team_b_ids": m['team_b_ids'],
        "winner_team": winner, "mmr_snapshot": snapshot
    }
    db['match_history'].insert(0, hist)
    active_courts[cid] = None
    save_db(db)
    return jsonify({"success":True})

# --- ADMIN / EVENTS ---

@app.route('/api/admin/toggle_session', methods=['POST'])
def toggle_session():
    db = get_db()
    d = request.json
    uid = d['userId']
    action = d['action']
    
    if uid != SUPER_ADMIN_ID and uid not in db['mod_ids']: return jsonify({"error":"Unauthorized"}), 403
    
    if action == 'start':
        db['system_settings']['is_session_active'] = True
        if not db['system_settings'].get('current_event_id'):
            eid = str(uuid.uuid4())[:8]
            today = datetime.now().strftime("%d/%m/%Y")
            db['events'][eid] = {"id":eid, "name": f"ก๊วน {today}", "datetime": time.time(), "players":[], "status":"active"}
            db['system_settings']['current_event_id'] = eid
    else:
        db['system_settings']['is_session_active'] = False
        db['system_settings']['current_event_id'] = None
        for p in db['players'].values():
            if p.get('status')!='offline': p['status']='offline'; p['partner_req']=None
            
    save_db(db)
    return jsonify({"success":True})

@app.route('/api/admin/update_courts', methods=['POST'])
def update_courts():
    db = get_db()
    c = int(request.json.get('count', 2))
    db['system_settings']['total_courts'] = c
    save_db(db)
    return jsonify({"success":True})

@app.route('/api/admin/manage_mod', methods=['POST'])
def manage_mod():
    db = get_db()
    d = request.json
    if d['requesterId'] != SUPER_ADMIN_ID: return jsonify({"error":"Super Admin Only"}), 403
    tid = d['targetUserId']
    if d['action'] == 'promote':
        if tid not in db['mod_ids']: db['mod_ids'].append(tid)
    else:
        if tid in db['mod_ids']: db['mod_ids'].remove(tid)
    save_db(db)
    return jsonify({"success":True})

@app.route('/api/admin/reset_system', methods=['POST'])
def reset_system():
    db = get_db()
    d = request.json
    if d['userId'] != SUPER_ADMIN_ID: return jsonify({"error":"Super Admin Only"}), 403
    
    db['match_history'] = []
    db['billing_history'] = []
    db['events'] = {}
    db['system_settings']['is_session_active'] = False
    db['system_settings']['current_event_id'] = None
    
    for p in db['players'].values():
        p['mmr'] = 1000
        p['status'] = 'offline'
        p['partner_req'] = None
        
    global active_courts
    for cid in active_courts: active_courts[cid] = None
    
    save_db(db)
    return jsonify({"success":True})

@app.route('/api/event/create', methods=['POST'])
def create_event():
    db = get_db()
    d = request.json
    eid = str(uuid.uuid4())[:8]
    db['events'][eid] = {
        "id": eid, "name": d['name'], "datetime": d['datetime'],
        "players": [], "status": "open"
    }
    save_db(db)
    return jsonify({"success":True})

@app.route('/api/event/delete', methods=['POST'])
def delete_event():
    db = get_db()
    eid = request.json.get('eventId')
    if eid in db['events']:
        del db['events'][eid]
        save_db(db)
        return jsonify({"success":True})
    return jsonify({"error":"Not found"})

@app.route('/api/event/join_toggle', methods=['POST'])
def join_event_toggle():
    db = get_db()
    d = request.json
    eid = d['eventId']; uid = d['userId']
    if eid in db['events']:
        if uid in db['events'][eid]['players']: db['events'][eid]['players'].remove(uid)
        else: db['events'][eid]['players'].append(uid)
        save_db(db)
        return jsonify({"success":True})
    return jsonify({"error":"Not found"})

@app.route('/api/admin/set_mmr', methods=['POST'])
def set_mmr():
    db = get_db()
    d = request.json
    uid = d['targetUserId']
    if uid in db['players']:
        db['players'][uid]['mmr'] = int(d['newMmr'])
        save_db(db)
        return jsonify({"success":True})
    return jsonify({"error":"Not found"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)