import json
import os
import time
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# --- CONFIG ---
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9" # ⚠️ อย่าลืมแก้ ID นี้นะครับ
DATA_FILE = "/var/data/izesquad_data.json" # สำหรับ Render Disk

# --- DATABASE ---
default_db = {
    "mod_ids": [],
    "players": {},
    "events": {},
    "match_history": [],
    "billing_history": [] # 👈 เพิ่มตารางเก็บประวัติการคิดเงิน
}
active_courts = {1: None, 2: None} 

def load_data():
    global db
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
        except: 
            db = default_db.copy()
            save_data()

def save_data():
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(db, f, ensure_ascii=False, indent=4)
    except: pass

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
            wins += 1
            current_streak += 1
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

    best_partner = "-"
    best_wr = -1
    for pid, data in partners.items():
        if data['played'] >= 2:
            wr = (data['won'] / data['played']) * 100
            if wr > best_wr:
                best_wr = wr
                best_partner = f"{data['name']} ({int(wr)}%)"

    nemesis = "-"
    worst_wr = -1
    for pid, data in opponents.items():
        if data['played'] >= 2:
            loss_rate = (data['lost'] / data['played']) * 100
            if loss_rate > worst_wr:
                worst_wr = loss_rate
                nemesis = f"{data['name']} (แพ้ {int(loss_rate)}%)"

    return {
        "win_rate": int((wins / total) * 100),
        "total": total,
        "streak": max_streak,
        "best_partner": best_partner,
        "nemesis": nemesis
    }

# --- ROUTES ---

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
    c_data = {}
    for cid, m in active_courts.items():
        if m: m['elapsed'] = int(time.time() - m['start_time'])
        c_data[cid] = m

    active = [p for p in db['players'].values() if p['status'] in ['active','playing']]
    active.sort(key=lambda x: x.get('last_active', 0))

    lb = sorted(db['players'].values(), key=lambda x: x['mmr'], reverse=True)
    for p in lb: p['rank_title'] = get_rank_title(p['mmr'])
    
    event_list = []
    for eid, e in db['events'].items():
        joined_users = []
        for pid in e.get('players', []):
            if pid in db['players']:
                joined_users.append({"id": pid, "nickname": db['players'][pid]['nickname'], "pictureUrl": db['players'][pid]['pictureUrl']})
        e['joined_users'] = joined_users
        event_list.append(e)
    event_list.sort(key=lambda x: x['datetime'])

    return jsonify({
        "courts": c_data,
        "queue": active,
        "queue_count": len(active),
        "events": event_list,
        "leaderboard": lb,
        "match_history": db['match_history'][:20],
        "billing_history": db.get('billing_history', [])[:5]
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

# --- EVENT & BILLING ---
@app.route('/api/event/create', methods=['POST'])
def create_event():
    d=request.json
    eid = str(uuid.uuid4())[:8]
    db['events'][eid] = {"id":eid, "name":d['name'], "datetime":d['datetime'], "players":[], "status":"open"}
    save_data()
    return jsonify({"success":True})

@app.route('/api/event/delete', methods=['POST'])
def delete_event():
    eid = request.json.get('eventId')
    if eid in db['events']:
        del db['events'][eid]
        save_data()
        return jsonify({"success":True})
    return jsonify({"error":"Not found"})

@app.route('/api/event/join_toggle', methods=['POST'])
def join_event_toggle():
    d = request.json
    eid, uid = d['eventId'], d['userId']
    if eid in db['events']:
        if uid in db['events'][eid]['players']:
            db['events'][eid]['players'].remove(uid)
            action = "removed"
        else:
            db['events'][eid]['players'].append(uid)
            action = "added"
        save_data()
        return jsonify({"success":True, "action":action})
    return jsonify({"error":"Not found"})

@app.route('/api/billing/save', methods=['POST'])
def save_billing():
    # บันทึกประวัติการคิดเงิน (เผื่อดูย้อนหลัง)
    d = request.json
    bill_record = {
        "id": str(uuid.uuid4())[:8],
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "event_name": d['eventName'],
        "total_cost": d['totalCost'],
        "details": d['details'] # list of {name, cost}
    }
    if 'billing_history' not in db: db['billing_history'] = []
    db['billing_history'].insert(0, bill_record)
    save_data()
    return jsonify({"success":True})

# --- GAMEPLAY ---
@app.route('/api/admin/set_mmr', methods=['POST'])
def set_mmr():
    d = request.json
    uid, new_mmr = d['targetUserId'], int(d['newMmr'])
    if uid in db['players']:
        db['players'][uid]['mmr'] = new_mmr
        save_data()
        return jsonify({"success":True})
    return jsonify({"error":"User not found"})

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

@app.route('/api/submit_result', methods=['POST'])
def submit_result():
    d=request.json; cid=int(d['courtId']); winner=d['winner']
    m = active_courts[cid]
    if not m: return jsonify({"error":"No match"})
    
    hist = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "team_a": m['team_a'], "team_a_ids": m['team_a_ids'],
        "team_b": m['team_b'], "team_b_ids": m['team_b_ids'],
        "winner_team": winner
    }
    db['match_history'].insert(0, hist)
    
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)