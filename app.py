import json
import os
import time
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# --- CONFIG ---
# สำคัญ: เปลี่ยน ID นี้เป็นของคุณ (Admin ID)
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"
DATA_FILE = "badminton_data.json"

# --- DATABASE ---
default_db = {
    "mod_ids": [],
    "players": {},
    "events": {},
    "match_history": [],
    "billing_history": []
}
active_courts = {1: None, 2: None} 
court_settings = {"total_courts": 2}

def load_data():
    global db
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                db = json.load(f)
                for k in default_db:
                    if k not in db: db[k] = default_db[k]
        except: db = default_db.copy()
    else:
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
    if mmr < 800: return "Rookie"
    elif mmr < 1000: return "Beginner"
    elif mmr < 1200: return "Intermediate"
    elif mmr < 1400: return "Advance"
    else: return "Pro Player"

def calculate_smart_stats(uid):
    # ดึงประวัติทั้งหมดที่ user นี้มีส่วนร่วม
    my_matches = [m for m in db['match_history'] if uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', [])]
    
    total_played = len(my_matches)
    if total_played == 0:
        return {"win_rate": 0, "total": 0, "best_partner": "-", "nemesis": "-", "streak": 0}

    wins = 0
    partners = {} # {uid: {played: 0, won: 0, name: ""}}
    opponents = {} # {uid: {played: 0, lost: 0, name: ""}}
    current_streak = 0
    max_streak = 0
    
    # วนลูปจากเก่าไปใหม่เพื่อหา Streak
    # (สมมติ match_history เก็บแบบ ใหม่ -> เก่า ต้องกลับด้านก่อน)
    for m in reversed(my_matches):
        # เช็คว่าเราอยู่ทีมไหน
        is_team_a = uid in m['team_a_ids']
        my_team = 'A' if is_team_a else 'B'
        is_winner = (m['winner_team'] == my_team)
        
        # 1. Win Count
        if is_winner: 
            wins += 1
            current_streak += 1
        else:
            current_streak = 0
        if current_streak > max_streak: max_streak = current_streak

        # 2. Partner Stats (หาคู่หู)
        my_team_ids = m['team_a_ids'] if is_team_a else m['team_b_ids']
        my_team_names = m['team_a'] if is_team_a else m['team_b']
        
        for pid, pname in zip(my_team_ids, my_team_names):
            if pid != uid: # ไม่นับตัวเอง
                if pid not in partners: partners[pid] = {'played':0, 'won':0, 'name':pname}
                partners[pid]['played'] += 1
                if is_winner: partners[pid]['won'] += 1

        # 3. Opponent Stats (หาคู่ปรับ)
        opp_team_ids = m['team_b_ids'] if is_team_a else m['team_a_ids']
        opp_team_names = m['team_b'] if is_team_a else m['team_a']
        
        for pid, pname in zip(opp_team_ids, opp_team_names):
            if pid not in opponents: opponents[pid] = {'played':0, 'lost':0, 'name':pname}
            opponents[pid]['played'] += 1
            if not is_winner: opponents[pid]['lost'] += 1 # แพ้ให้คนนี้

    # หา Best Partner (คนที่คู่ด้วยแล้ว Win Rate สูงสุด และเล่นด้วยกันเกิน 2 ครั้ง)
    best_partner = "-"
    best_wr = -1
    for pid, data in partners.items():
        if data['played'] >= 2:
            wr = (data['won'] / data['played']) * 100
            if wr > best_wr:
                best_wr = wr
                best_partner = f"{data['name']} ({int(wr)}%)"
    
    # หา Nemesis (คนที่เจอแล้วแพ้บ่อยสุด)
    nemesis = "-"
    worst_wr = -1
    for pid, data in opponents.items():
        if data['played'] >= 2:
            loss_rate = (data['lost'] / data['played']) * 100
            if loss_rate > worst_wr:
                worst_wr = loss_rate
                nemesis = f"{data['name']} (แพ้ {int(loss_rate)}%)"

    return {
        "win_rate": int((wins / total_played) * 100),
        "total": total_played,
        "wins": wins,
        "losses": total_played - wins,
        "best_partner": best_partner if best_partner != "-" else "ยังไม่ชัดเจน",
        "nemesis": nemesis if nemesis != "-" else "ยังไม่ชัดเจน",
        "streak": max_streak
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
    
    # Update Info & Load Stats
    db['players'][uid]['pictureUrl'] = d.get('pictureUrl')
    p = db['players'][uid]
    p['role'] = 'super' if uid == SUPER_ADMIN_ID else ('mod' if uid in db['mod_ids'] else 'user')
    p['rank_title'] = get_rank_title(p['mmr'])
    p['stats'] = calculate_smart_stats(uid) # <--- ใส่ Stats กลับไปให้เลย
    
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
    
    return jsonify({
        "courts": c_data,
        "queue": active,
        "queue_count": len(active),
        "events": [e for e in db['events'].values() if e['status']=='open'],
        "leaderboard": lb,
        "match_history": db['match_history'][:20] # ส่งแค่ 20 อันล่าสุดพอ
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

@app.route('/api/admin/create_event', methods=['POST'])
def create_event():
    d=request.json
    eid = str(uuid.uuid4())[:8]
    db['events'][eid] = {"id":eid, "name":d['name'], "datetime":d['datetime'], "players":[], "status":"open"}
    save_data()
    return jsonify({"success":True})

@app.route('/api/matchmake', methods=['POST'])
def matchmake():
    # Logic จับคู่ (เหมือนเดิมแต่ย่อ)
    free = next((k for k,v in active_courts.items() if v is None), None)
    if not free: return jsonify({"status":"full"})
    
    q = [p for p in db['players'].values() if p['status']=='active']
    q.sort(key=lambda x: x.get('last_active',0))
    if len(q) < 4: return jsonify({"status":"waiting"})
    
    players = q[:4]
    players.sort(key=lambda x: x['mmr'])
    match = {
        "team_a": [players[0]['nickname'], players[3]['nickname']],
        "team_a_ids": [players[0]['id'], players[3]['id']], # เก็บ ID ด้วยเพื่อ Smart Stats
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
    d=request.json; cid=int(d['courtId']); winner=d['winner'] # 'A' or 'B'
    m = active_courts[cid]
    if not m: return jsonify({"error":"No match"})
    
    # Save History
    hist = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "team_a": m['team_a'], "team_a_ids": m['team_a_ids'],
        "team_b": m['team_b'], "team_b_ids": m['team_b_ids'],
        "winner_team": winner # 'A' or 'B'
    }
    db['match_history'].insert(0, hist)
    
    # Calculate MMR
    win_ids = m['team_a_ids'] if winner=='A' else m['team_b_ids']
    lose_ids = m['team_b_ids'] if winner=='A' else m['team_a_ids']
    
    for uid in win_ids: 
        db['players'][uid]['mmr'] += 25
        db['players'][uid]['status'] = 'active'; db['players'][uid]['last_active'] = time.time()
    for uid in lose_ids: 
        db['players'][uid]['mmr'] -= 20 # แพ้ลบน้อยหน่อย
        db['players'][uid]['status'] = 'active'; db['players'][uid]['last_active'] = time.time()
        
    active_courts[cid] = None
    save_data()
    return jsonify({"success":True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)