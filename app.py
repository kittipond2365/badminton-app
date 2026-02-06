from flask import Flask, request, jsonify, render_template
import time
import uuid
from datetime import datetime

app = Flask(__name__)

# --- CONFIG ---
# ใส่ ID ของคุณปอนด์ตรงนี้ (แก้บั๊ก Assign Mod ไม่ได้)
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"

# --- DATABASE ---
mod_ids = set() # เก็บ ID ของคนที่เป็น Mod
players_db = {} 
events_db = {}
active_courts = {1: None, 2: None}
court_settings = {"total_courts": 2}

# เก็บประวัติ
match_history_db = []  # [{date, time, court, team_a:[], team_b:[], winner}]
billing_history_db = [] # [{date, total, players:[{name, cost}]}]

# --- HELPER FUNCTIONS ---

def get_rank_title(mmr):
    if mmr <= 500: return "noob dog"
    elif mmr <= 1000: return "Noob"
    elif mmr <= 1100: return "เด็กกระโปก"
    elif mmr <= 1200: return "เก่งใช้ได้"
    else: return "Pro Player"

def get_role(uid):
    if uid == SUPER_ADMIN_ID: return 'super'
    if uid in mod_ids: return 'mod'
    return 'user'

def is_staff(uid):
    return uid == SUPER_ADMIN_ID or uid in mod_ids

def get_active_players():
    active = []
    curr = time.time()
    TIMEOUT = 5 * 3600
    for uid, p in players_db.items():
        if p['status'] == 'active':
            if (curr - p['last_active']) > TIMEOUT:
                p['status'] = 'offline'
            else:
                active.append(p)
    active.sort(key=lambda x: x['last_active'])
    return active

def calculate_elo(ra, rb, sa):
    ea = 1 / (1 + 10 ** ((rb - ra) / 400))
    return int(round(32 * (sa - ea)))

def calc_penalty(ta, tb, wait):
    avg_a = sum(p['mmr'] for p in ta)/2
    avg_b = sum(p['mmr'] for p in tb)/2
    diff = abs(avg_a - avg_b)
    gap = (abs(ta[0]['mmr']-ta[1]['mmr']) + abs(tb[0]['mmr']-tb[1]['mmr']))/2
    return (diff + gap*1.5) - (wait/60)*10

# --- ROUTES ---

@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    d = request.json
    uid = d.get('userId')
    if uid not in players_db:
        players_db[uid] = {
            "id": uid, 
            "name": d.get('displayName'), # ชื่อเริ่มต้นใช้จาก LINE
            "nickname": d.get('displayName'), # ชื่อเล่น (แก้ไขได้)
            "pictureUrl": d.get('pictureUrl'),
            "mmr": 1000, 
            "status": "offline", 
            "last_active": 0, 
            "total_seconds_played": 0
        }
    # อัปเดตข้อมูลพื้นฐาน (แต่ไม่ทับ Nickname ที่แก้ไว้)
    players_db[uid]['pictureUrl'] = d.get('pictureUrl')
    
    resp = players_db[uid].copy()
    resp['role'] = get_role(uid)
    resp['rank_title'] = get_rank_title(resp['mmr'])
    return jsonify(resp)

@app.route('/api/update_profile', methods=['POST'])
def update_profile():
    uid = request.json.get('userId')
    new_name = request.json.get('nickname')
    if uid in players_db:
        players_db[uid]['nickname'] = new_name
        return jsonify({"success": True})
    return jsonify({"error": "User not found"}), 404

@app.route('/api/toggle_status', methods=['POST'])
def toggle_status():
    uid = request.json.get('userId')
    if uid in players_db:
        curr = players_db[uid]['status']
        if curr in ['active','playing']: players_db[uid]['status'] = 'offline'
        else:
            players_db[uid]['status'] = 'active'
            players_db[uid]['last_active'] = time.time()
        return jsonify({"status": players_db[uid]['status']})
    return jsonify({"error":"User not found"})

@app.route('/api/get_dashboard', methods=['GET'])
def get_dashboard():
    courts = {}
    for c in sorted(active_courts.keys()):
        courts[c] = active_courts[c]
        if courts[c]: courts[c]['elapsed'] = int(time.time() - active_courts[c]['start_time'])
    
    # Leaderboard Logic
    leaderboard = sorted(players_db.values(), key=lambda x: x['mmr'], reverse=True)
    # เติม Rank Title ให้ทุกคนก่อนส่ง
    for p in leaderboard:
        p['rank_title'] = get_rank_title(p['mmr'])

    eq = []
    for eid, e in events_db.items():
        if e['status']=='open': 
            ec = e.copy(); ec['player_count']=len(e['players']); eq.append(ec)

    return jsonify({
        "total_courts": court_settings['total_courts'],
        "courts": courts,
        "queue": get_active_players(),
        "queue_count": len(get_active_players()),
        "events": eq,
        "leaderboard": leaderboard,
        "match_history": match_history_db, # ส่งประวัติทั้งหมด
        "billing_history": billing_history_db
    })

# --- SUPER ADMIN: Toggle Mod ---
@app.route('/api/super/toggle_mod', methods=['POST'])
def toggle_mod():
    # เช็คว่าเป็น Super Admin ตัวจริงไหม
    if request.json.get('adminId') != SUPER_ADMIN_ID: 
        return jsonify({"error":"Forbidden"}), 403
    
    target = request.json.get('targetId')
    if target in mod_ids:
        mod_ids.remove(target)
        msg = "ปลด Mod แล้ว"
    else:
        mod_ids.add(target)
        msg = "แต่งตั้ง Mod แล้ว"
    return jsonify({"success": True, "msg": msg})

# --- STAFF ACTIONS ---

@app.route('/api/admin/create_event', methods=['POST'])
def create_event():
    if not is_staff(request.json.get('adminId')): return jsonify({"error":"Forbidden"}), 403
    eid = str(uuid.uuid4())[:8]
    events_db[eid] = {"id":eid, "name":request.json.get('name'), "datetime":request.json.get('datetime'), "players":[], "status":"open"}
    return jsonify({"success":True})

@app.route('/api/matchmake', methods=['POST'])
def matchmake():
    if not is_staff(request.json.get('adminId')): return jsonify({"error":"Forbidden"}), 403
    
    free = None
    for c in sorted(active_courts.keys()):
        if active_courts[c] is None: free=c; break
    if free is None: return jsonify({"status":"full", "message":"สนามเต็ม"})
    
    players = get_active_players()
    if len(players)<4: return jsonify({"status":"waiting", "message":"คนไม่ครบ"})
    
    cands = players[:4]
    wait = time.time() - cands[0]['last_active']
    best = None; min_p = float('inf')
    
    for ia, ib in [([0,1],[2,3]),([0,2],[1,3]),([0,3],[1,2])]:
        ta=[cands[i] for i in ia]; tb=[cands[i] for i in ib]
        p = calc_penalty(ta, tb, wait)
        if p < min_p:
            min_p = p
            best = {"team_a":ta, "team_b":tb, "start_time":time.time()}
            
    active_courts[free] = best
    for p in cands: players_db[p['id']]['status']='playing'
    return jsonify({"status":"matched", "court":free})

@app.route('/api/submit_result', methods=['POST'])
def submit_result():
    cid = int(request.json.get('courtId'))
    winner = request.json.get('winner')
    req_id = request.json.get('requesterId')
    
    match = active_courts.get(cid)
    if not match: return jsonify({"error"}), 400
    
    # Permission Check
    players_in_match = [p['id'] for p in match['team_a']+match['team_b']]
    if not is_staff(req_id) and req_id not in players_in_match: 
        return jsonify({"error":"No permission"}), 403
    
    # 1. บันทึก History
    match_record = {
        "id": str(uuid.uuid4())[:8],
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M"),
        "court": cid,
        "team_a": [p['nickname'] for p in match['team_a']],
        "team_b": [p['nickname'] for p in match['team_b']],
        "winner": "Team A" if winner == 'A' else "Team B"
    }
    match_history_db.insert(0, match_record) # ใส่หน้าสุด

    # 2. Update Stats
    dur = time.time() - match['start_time']
    ta=match['team_a']; tb=match['team_b']
    for p in ta+tb:
        if p['id'] in players_db:
            players_db[p['id']]['total_seconds_played'] += dur
            
    # 3. MMR Calc
    ma=sum(p['mmr'] for p in ta)/2; mb=sum(p['mmr'] for p in tb)/2
    chg = calculate_elo(ma, mb, 1 if winner=='A' else 0)
    
    for p in ta: 
        players_db[p['id']]['mmr'] += chg
        players_db[p['id']]['status']='active'; players_db[p['id']]['last_active']=time.time()
    for p in tb: 
        players_db[p['id']]['mmr'] -= chg
        players_db[p['id']]['status']='active'; players_db[p['id']]['last_active']=time.time()
        
    active_courts[cid] = None
    return jsonify({"success":True})

@app.route('/api/admin/calculate_bill', methods=['POST'])
def calculate_bill():
    # อนุญาตให้ทุกคนกดคำนวณดูเล่นๆ ได้ แต่ถ้าจะ Save ต้องเป็น Staff
    # แต่ในที่นี้เราแยก API Save ไว้ต่างหาก ดังนั้นอันนี้เปิด Public ได้
    total = float(request.json.get('totalExpense'))
    p_data = request.json.get('players')
    grand_min = sum(p['minutes'] for p in p_data)
    
    if grand_min == 0: return jsonify({"error":"เวลารวมเป็น 0"})
    rate = total/grand_min
    
    bill = []
    for p in p_data:
        if p['minutes']>0:
            hr = p['minutes']//60; mn = p['minutes']%60
            bill.append({"name":p['name'], "time":f"{hr}ชม {mn}น", "cost":int(round(p['minutes']*rate))})
            
    return jsonify({"bill_list":sorted(bill, key=lambda x:x['name']), "rate":round(rate*60, 2)})

@app.route('/api/admin/save_bill', methods=['POST'])
def save_bill():
    # อันนี้ต้อง Staff เท่านั้นถึงจะบันทึกประวัติการเงินได้
    if not is_staff(request.json.get('adminId')): return jsonify({"error":"Forbidden"}), 403
    
    bill_data = request.json.get('billData') # รับก้อนที่คำนวณเสร็จแล้ว
    record = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total": request.json.get('total'),
        "players": bill_data
    }
    billing_history_db.insert(0, record)
    return jsonify({"success": True})

@app.route('/api/admin/set_rank', methods=['POST'])
def set_rank():
    if not is_staff(request.json.get('adminId')): return jsonify({"error":"Forbidden"}), 403
    players_db[request.json.get('targetId')]['mmr'] = int(request.json.get('mmr'))
    return jsonify({"success":True})

@app.route('/api/event/join', methods=['POST'])
def join_evt():
    uid=request.json.get('userId'); eid=request.json.get('eventId')
    if eid in events_db and uid in players_db:
        if uid not in events_db[eid]['players']: events_db[eid]['players'].append(uid); return jsonify({"status":"joined"})
        else: events_db[eid]['players'].remove(uid); return jsonify({"status":"left"})
    return jsonify({"error"}), 400

@app.route('/api/get_all_players')
def get_all(): return jsonify(list(players_db.values()))

@app.route('/api/admin/set_courts', methods=['POST'])
def set_courts():
    if not is_staff(request.json.get('adminId')): return jsonify({"error"}), 403
    count = int(request.json.get('count'))
    court_settings['total_courts'] = count
    curr = list(active_courts.keys())
    if count > len(curr):
        for i in range(len(curr)+1, count+1): active_courts[i]=None
    elif count < len(curr):
        for i in range(count+1, len(curr)+1): 
            if i in active_courts: del active_courts[i]
    return jsonify({"success":True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)