import json
import os
import time
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# --- CONFIG ---
# ⚠️ ID ของ Super Admin (คุณ)
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"

# ✅ Path สำหรับ Render Disk (ห้ามแก้ถ้าใช้ Disk)
DATA_FILE = "/var/data/izesquad_data.json"

# --- DATABASE SETUP ---
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
                # Migration: เติม key ที่ขาด
                for k in default_db:
                    if k not in db: db[k] = default_db[k]
                if "system_settings" not in db: db["system_settings"] = default_db["system_settings"]
        except: 
            db = default_db.copy()
            save_data()
    
    refresh_courts()

def save_data():
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, ensure_ascii=False, indent=4)
    except: pass

def refresh_courts():
    # ปรับจำนวนคอร์ทตาม database
    target = db['system_settings'].get('total_courts', 2)
    # เพิ่ม
    for i in range(1, target + 1):
        if i not in active_courts: active_courts[i] = None
    # ลด
    current_keys = list(active_courts.keys())
    for k in current_keys:
        if k > target: del active_courts[k]

load_data()

# --- HELPER FUNCTIONS ---
def get_rank_title(mmr):
    if mmr <= 800: return "NOOB DOG 🐶"
    elif mmr <= 1200: return "NOOB 🐣"
    elif mmr <= 1400: return "เด็กกระโปก 👶"
    elif mmr <= 1600: return "ชนะจนเบื่อ 🥱"
    else: return "โปรเพรเย๋อ 👽"

def calculate_smart_stats(uid):
    my_matches = [m for m in db['match_history'] if uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', [])]
    total = len(my_matches)
    if total == 0: return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}

    wins = 0
    current_streak = 0
    max_streak = 0
    partners = {}
    opponents = {}

    for m in reversed(my_matches):
        is_team_a = uid in m['team_a_ids']
        my_team = 'A' if is_team_a else 'B'
        is_winner = (m['winner_team'] == my_team)
        
        if is_winner:
            wins += 1; current_streak += 1
        else:
            current_streak = 0
        if current_streak > max_streak: max_streak = current_streak

        my_ids = m['team_a_ids'] if is_team_a else m['team_b_ids']
        my_names = m['team_a'] if is_team_a else m['team_b']
        for pid, pname in zip(my_ids, my_names):
            if pid != uid:
                if pid not in partners: partners[pid] = {'played':0, 'won':0, 'name':pname}
                partners[pid]['played'] += 1
                if is_winner: partners[pid]['won'] += 1

        opp_ids = m['team_b_ids'] if is_team_a else m['team_a_ids']
        opp_names = m['team_b'] if is_team_a else m['team_a']
        for pid, pname in zip(opp_ids, opp_names):
            if pid not in opponents: opponents[pid] = {'played':0, 'lost':0, 'name':pname}
            opponents[pid]['played'] += 1
            if not is_winner: opponents[pid]['lost'] += 1

    best_partner, best_wr = "-", -1
    for pid, d in partners.items():
        if d['played'] >= 2:
            wr = (d['won']/d['played'])*100
            if wr > best_wr: best_wr=wr; best_partner=f"{d['name']} ({int(wr)}%)"

    nemesis, worst_wr = "-", -1
    for pid, d in opponents.items():
        if d['played'] >= 2:
            lr = (d['lost']/d['played'])*100
            if lr > worst_wr: worst_wr=lr; nemesis=f"{d['name']} (แพ้ {int(lr)}%)"

    return {"win_rate": int((wins/total)*100), "total": total, "streak": max_streak, "best_partner": best_partner, "nemesis": nemesis}

# --- API ROUTES ---

@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    d = request.json
    uid = d.get('userId')
    if uid not in db['players']:
        db['players'][uid] = {
            "id": uid, "nickname": d.get('displayName'), "pictureUrl": d.get('pictureUrl'),
            "mmr": 1000, "status": "offline", "last_active": time.time()
        }
        save_data()
    
    p = db['players'][uid]
    p['pictureUrl'] = d.get('pictureUrl')
    p['role'] = 'super' if uid == SUPER_ADMIN_ID else ('mod' if uid in db['mod_ids'] else 'user')
    p['rank_title'] = get_rank_title(p['mmr'])
    p['stats'] = calculate_smart_stats(uid)
    return jsonify(p)

@app.route('/api/get_dashboard')
def get_dashboard():
    # Courts
    c_data = {}
    for cid, m in active_courts.items():
        if m: m['elapsed'] = int(time.time() - m['start_time'])
        c_data[cid] = m

    # Queue
    active = [p for p in db['players'].values() if p['status'] in ['active','playing']]
    active.sort(key=lambda x: x.get('last_active', 0))

    # Leaderboard
    lb = sorted(db['players'].values(), key=lambda x: x['mmr'], reverse=True)
    for p in lb: p['rank_title'] = get_rank_title(p['mmr'])
    
    # Events
    event_list = []
    for eid, e in db['events'].items():
        joined_users = []
        for pid in e.get('players', []):
            if pid in db['players']:
                joined_users.append({"id":pid, "nickname": db['players'][pid]['nickname'], "pictureUrl": db['players'][pid]['pictureUrl']})
        e['joined_users'] = joined_users
        event_list.append(e)
    event_list.sort(key=lambda x: x['datetime'])

    return jsonify({
        "system": db['system_settings'],
        "courts": c_data,
        "queue": active,
        "queue_count": len(active),
        "events": event_list,
        "leaderboard": lb,
        "match_history": db['match_history'][:20],
        "billing_history": db.get('billing_history', [])[:5],
        # ส่งรายชื่อทั้งหมดไปให้ Super Admin ใช้แต่งตั้ง Mod
        "all_players": [{"id": p['id'], "nickname": p['nickname'], "is_mod": p['id'] in db['mod_ids']} for p in db['players'].values()]
    })

@app.route('/api/toggle_status', methods=['POST'])
def toggle_status():
    uid = request.json.get('userId')
    if uid in db['players']:
        curr = db['players'][uid]['status']
        if curr in ['active','playing']: db['players'][uid]['status'] = 'offline'
        else: 
            db['players'][uid]['status'] = 'active'
            db['players'][uid]['last_active'] = time.time()
        save_data()
    return jsonify({"success":True})

# --- ADMIN / MOD SYSTEM ---

@app.route('/api/admin/toggle_session', methods=['POST'])
def toggle_session():
    d = request.json
    uid = d.get('userId')
    action = d.get('action')
    
    # ✅ Check สิทธิ์: ต้องเป็น Super Admin หรือ Mod
    is_super = uid == SUPER_ADMIN_ID
    is_mod = uid in db['mod_ids']
    
    if not (is_super or is_mod):
        return jsonify({"error": "Unauthorized"}), 403

    if action == 'start':
        db['system_settings']['is_session_active'] = True
        # สร้าง Event อัตโนมัติ ถ้ายังไม่มี
        if not db['system_settings'].get('current_event_id'):
            eid = str(uuid.uuid4())[:8]
            today = datetime.now().strftime("%d/%m/%Y")
            db['events'][eid] = {"id":eid, "name": f"ก๊วน {today}", "datetime": datetime.now().isoformat(), "players":[], "status":"active"}
            db['system_settings']['current_event_id'] = eid
    else:
        db['system_settings']['is_session_active'] = False
        db['system_settings']['current_event_id'] = None
        # Reset Status
        for p in db['players'].values():
            if p['status'] != 'offline': p['status'] = 'offline'
            
    save_data()
    return jsonify({"success":True})

@app.route('/api/admin/update_courts', methods=['POST'])
def update_courts():
    # Mod เพิ่มลดสนามได้
    count = int(request.json.get('count', 2))
    if count < 1: count = 1
    db['system_settings']['total_courts'] = count
    save_data()
    refresh_courts()
    return jsonify({"success":True})

@app.route('/api/admin/manage_mod', methods=['POST'])
def manage_mod():
    # เฉพาะ Super Admin เท่านั้น
    d = request.json
    requester = d.get('requesterId') # คนกด
    target_id = d.get('targetUserId')
    action = d.get('action')
    
    if requester != SUPER_ADMIN_ID:
        return jsonify({"error": "Super Admin Only"}), 403

    if action == 'promote':
        if target_id not in db['mod_ids']: db['mod_ids'].append(target_id)
    else:
        if target_id in db['mod_ids']: db['mod_ids'].remove(target_id)
    save_data()
    return jsonify({"success":True})

# --- GAME & EVENT ---

@app.route('/api/submit_result', methods=['POST'])
def submit_result():
    d=request.json; cid=int(d['courtId']); winner=d['winner']; req_uid=d['userId']
    m = active_courts.get(cid)
    if not m: return jsonify({"error":"No match"})
    
    # Permission Check: Super Admin / Mod / Player in that game
    is_staff = (req_uid == SUPER_ADMIN_ID) or (req_uid in db['mod_ids'])
    is_player = (req_uid in m['team_a_ids']) or (req_uid in m['team_b_ids'])
    
    if not (is_staff or is_player):
        return jsonify({"error": "ไม่มีสิทธิ์จบเกมนี้"}), 403
    
    # Save History
    hist = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "team_a": m['team_a'], "team_a_ids": m['team_a_ids'],
        "team_b": m['team_b'], "team_b_ids": m['team_b_ids'],
        "winner_team": winner
    }
    db['match_history'].insert(0, hist)
    
    # MMR Calc
    win_ids = m['team_a_ids'] if winner=='A' else m['team_b_ids']
    lose_ids = m['team_b_ids'] if winner=='A' else m['team_a_ids']
    for uid in win_ids: 
        db['players'][uid]['mmr'] += 25
        db['players'][uid]['status'] = 'active'; db['players'][uid]['last_active'] = time.time()
    for uid in lose_ids: 
        db['players'][uid]['mmr'] -= 20
        db['players'][uid]['status'] = 'active'; db['players'][uid]['last_active'] = time.time()
        
    active_courts[cid] = None
    save_data()
    return jsonify({"success":True})

@app.route('/api/matchmake', methods=['POST'])
def matchmake():
    free = next((k for k,v in active_courts.items() if v is None), None)
    if not free: return jsonify({"status":"full"})
    q = [p for p in db['players'].values() if p['status']=='active']
    q.sort(key=lambda x: x.get('last_active',0))
    if len(q) < 4: return jsonify({"status":"waiting"})
    
    players = q[:4]
    players.sort(key=lambda x: x['mmr'])
    match = {
        "team_a": [players[0]['nickname'], players[3]['nickname']],
        "team_a_ids": [players[0]['id'], players[3]['id']],
        "team_b": [players[1]['nickname'], players[2]['nickname']],
        "team_b_ids": [players[1]['id'], players[2]['id']],
        "start_time": time.time()
    }
    active_courts[free] = match
    for p in players: db['players'][p['id']]['status'] = 'playing'
    save_data()
    return jsonify({"status":"matched"})

@app.route('/api/event/create', methods=['POST'])
def create_event():
    d=request.json; eid=str(uuid.uuid4())[:8]
    db['events'][eid] = {"id":eid, "name":d['name'], "datetime":d['datetime'], "players":[], "status":"open"}
    save_data()
    return jsonify({"success":True})

@app.route('/api/event/delete', methods=['POST'])
def delete_event():
    eid=request.json.get('eventId')
    if eid in db['events']: del db['events'][eid]; save_data()
    return jsonify({"success":True})

@app.route('/api/event/join_toggle', methods=['POST'])
def join_event_toggle():
    d=request.json; eid=d['eventId']; uid=d['userId']
    if eid in db['events']:
        if uid in db['events'][eid]['players']: db['events'][eid]['players'].remove(uid)
        else: db['events'][eid]['players'].append(uid)
        save_data()
        return jsonify({"success":True})
    return jsonify({"error":"Not found"})

@app.route('/api/admin/set_mmr', methods=['POST'])
def set_mmr():
    d=request.json; uid=d['targetUserId']; new_mmr=int(d['newMmr'])
    if uid in db['players']: db['players'][uid]['mmr']=new_mmr; save_data(); return jsonify({"success":True})
    return jsonify({"error":"User not found"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)