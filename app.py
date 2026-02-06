import json
import os
import time
import uuid
import sys
from datetime import datetime
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# --- CONFIG ---
# ⚠️ ID ของ Super Admin
SUPER_ADMIN_ID = "U1cf933e3a1559608c50c0456f6583dc9"

# ✅ Path สำหรับ Render Disk (ต้องตรงกับที่ตั้งค่าใน Render)
DATA_FILE = "/var/data/izesquad_data.json"

# --- DATABASE DEFAULTS ---
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

# --- DATA MANAGEMENT FUNCTIONS ---

def get_db():
    """
    โหลดข้อมูลล่าสุดจากไฟล์เสมอ เพื่อป้องกันปัญหาข้อมูลไม่ตรงกันระหว่าง Worker
    """
    directory = os.path.dirname(DATA_FILE)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
        except Exception as e:
            print(f"Error creating directory: {e}", flush=True)

    if not os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_db, f, ensure_ascii=False, indent=4)
            return default_db.copy()
        except Exception as e:
            print(f"Error creating DB file: {e}", flush=True)
            return default_db.copy()

    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for k in default_db:
                if k not in data:
                    data[k] = default_db[k]
            return data
    except Exception as e:
        print(f"Error reading DB: {e}", flush=True)
        return default_db.copy()

def save_db(data):
    """
    บันทึกข้อมูลลงไฟล์ทันที (✅ atomic write + คืนค่า True/False)
    """
    try:
        directory = os.path.dirname(DATA_FILE)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        tmp = DATA_FILE + ".tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp, DATA_FILE)
        return True
    except Exception as e:
        print(f"CRITICAL ERROR SAVING DB: {e}", flush=True)
        return False

# Global Cache สำหรับสนาม (ข้อมูลชั่วคราว)
active_courts = {}

def refresh_courts(db_data):
    """
    รีเซ็ตจำนวนสนามตามการตั้งค่า
    """
    target = db_data['system_settings'].get('total_courts', 2)
    for i in range(1, target + 1):
        if i not in active_courts:
            active_courts[i] = None

    current_keys = list(active_courts.keys())
    for k in current_keys:
        if k > target:
            del active_courts[k]

# --- HELPER FUNCTIONS ---

def get_rank_title(mmr):
    try:
        mmr = int(mmr)
    except:
        mmr = 1000

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

def calculate_smart_stats(uid, history):
    if not history:
        return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}

    my_matches = [m for m in history if uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', [])]
    total = len(my_matches)

    if total == 0:
        return {"win_rate": 0, "total": 0, "streak": 0, "best_partner": "-", "nemesis": "-"}

    wins = 0
    current_streak = 0
    max_streak = 0

    for m in reversed(my_matches):
        is_team_a = uid in m.get('team_a_ids', [])
        my_team = 'A' if is_team_a else 'B'
        is_winner = (m.get('winner_team') == my_team)

        if is_winner:
            wins += 1
            current_streak += 1
        else:
            current_streak = 0

        if current_streak > max_streak:
            max_streak = current_streak

    return {
        "win_rate": int((wins / total) * 100),
        "total": total,
        "streak": max_streak,
        "best_partner": "-",
        "nemesis": "-"
    }

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    try:
        db = get_db()
        d = request.get_json(silent=True) or {}
        uid = d.get('userId')

        if not uid:
            return jsonify({"error": "Missing userId"}), 400

        if uid not in db['players']:
            db['players'][uid] = {
                "id": uid,
                "nickname": d.get('displayName'),
                "pictureUrl": d.get('pictureUrl'),
                "mmr": 1000,
                "status": "offline",
                "last_active": time.time(),
                "partner_req": None
            }
            save_db(db)
        else:
            db['players'][uid]['pictureUrl'] = d.get('pictureUrl')
            save_db(db)

        p = db['players'][uid]
        p['role'] = 'super' if uid == SUPER_ADMIN_ID else ('mod' if uid in db['mod_ids'] else 'user')
        p['rank_title'] = get_rank_title(p.get('mmr', 1000))
        p['stats'] = calculate_smart_stats(uid, db['match_history'])

        my_hist = [m for m in db['match_history'] if uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', [])]
        p['my_history'] = my_hist[:50]

        return jsonify(p)
    except Exception as e:
        print(f"Login Error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/get_dashboard')
def get_dashboard():
    try:
        db = get_db()
        refresh_courts(db)

        # 1) Courts
        c_data = {}
        for cid, m in active_courts.items():
            if m:
                m['elapsed'] = int(time.time() - m['start_time'])
                m['team_a_data'] = []
                for uid in m.get('team_a_ids', []):
                    if uid in db['players']:
                        pl = db['players'][uid]
                        m['team_a_data'].append({"name": pl['nickname'], "pic": pl.get('pictureUrl', '')})

                m['team_b_data'] = []
                for uid in m.get('team_b_ids', []):
                    if uid in db['players']:
                        pl = db['players'][uid]
                        m['team_b_data'].append({"name": pl['nickname'], "pic": pl.get('pictureUrl', '')})

            c_data[cid] = m

        # 2) Players Clean
        players_clean = []
        for p in db['players'].values():
            try:
                p['mmr'] = int(p.get('mmr', 1000))
            except:
                p['mmr'] = 1000

            try:
                p['last_active'] = float(p.get('last_active', 0))
            except:
                p['last_active'] = 0.0

            p['rank_title'] = get_rank_title(p['mmr'])
            players_clean.append(p)

        # 3) Queue
        active_queue = [p for p in players_clean if p.get('status') in ['active', 'playing']]
        active_queue.sort(key=lambda x: x['last_active'])

        # 4) Leaderboard
        leaderboard = sorted(players_clean, key=lambda x: x['mmr'], reverse=True)

        # 5) Events
        event_list = []
        for eid, e in db['events'].items():
            joined_users = []
            for pid in e.get('players', []):
                if pid in db['players']:
                    pl = db['players'][pid]
                    joined_users.append({
                        "id": pid,
                        "nickname": pl['nickname'],
                        "pictureUrl": pl.get('pictureUrl', '')
                    })
            e['joined_users'] = joined_users

            raw_dt = e.get('datetime')
            if isinstance(raw_dt, (int, float)):
                e['sort_key'] = raw_dt
            elif isinstance(raw_dt, str):
                try:
                    e['sort_key'] = datetime.fromisoformat(raw_dt).timestamp()
                except:
                    e['sort_key'] = 0
            else:
                e['sort_key'] = 0

            event_list.append(e)

        event_list.sort(key=lambda x: x['sort_key'])

        # 6) All players minimal (✅ ใส่ mmr เพิ่ม เผื่อใช้ต่อ)
        all_players_minimal = []
        for p in players_clean:
            all_players_minimal.append({
                "id": p['id'],
                "nickname": p['nickname'],
                "pictureUrl": p.get('pictureUrl', ''),
                "status": p.get('status', 'offline'),
                "last_active": p['last_active'],
                "mmr": p.get('mmr', 1000),
                "is_mod": p['id'] in db['mod_ids'],
                "partner_req": p.get('partner_req')
            })

        return jsonify({
            "system": db['system_settings'],
            "courts": c_data,
            "queue": active_queue,
            "queue_count": len(active_queue),
            "events": event_list,
            "leaderboard": leaderboard,
            "match_history": db['match_history'][:20],
            "all_players": all_players_minimal
        })

    except Exception as e:
        print(f"Dashboard Error: {e}", flush=True)
        return jsonify({
            "system": {}, "courts": {}, "queue": [], "queue_count": 0,
            "events": [], "leaderboard": [], "match_history": [], "all_players": []
        })

@app.route('/api/event/create', methods=['POST'])
def create_event():
    """
    ✅ แก้ 2 อย่าง:
    - ใช้ request.get_json(silent=True) กัน payload ไม่ใช่ json
    - เช็ค save_db() ถ้าเขียนไม่ได้ต้องตอบ 500 (จะไม่ success หลอก)
    """
    try:
        db = get_db()
        d = request.get_json(silent=True) or {}

        name = (d.get('name') or '').strip()
        dt = d.get('datetime')

        if not name or dt is None:
            return jsonify({"error": "Missing data: name/datetime"}), 400

        eid = str(uuid.uuid4())[:8]
        db['events'][eid] = {
            "id": eid,
            "name": name,
            "datetime": dt,
            "players": [],
            "status": "open"
        }

        if not save_db(db):
            return jsonify({"error": f"Cannot write DB file at {DATA_FILE}"}), 500

        return jsonify({"success": True, "eventId": eid})
    except Exception as e:
        print(f"Create Event Error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/event/delete', methods=['POST'])
def delete_event():
    db = get_db()
    d = request.get_json(silent=True) or {}
    eid = d.get('eventId')
    if eid in db['events']:
        del db['events'][eid]
        if not save_db(db):
            return jsonify({"error": f"Cannot write DB file at {DATA_FILE}"}), 500
        return jsonify({"success": True})
    return jsonify({"error": "Not found"})

@app.route('/api/event/join_toggle', methods=['POST'])
def join_event_toggle():
    db = get_db()
    d = request.get_json(silent=True) or {}
    eid = d.get('eventId')
    uid = d.get('userId')

    if not eid or not uid:
        return jsonify({"error": "Missing eventId/userId"}), 400

    if eid in db['events']:
        if uid in db['events'][eid]['players']:
            db['events'][eid]['players'].remove(uid)
        else:
            db['events'][eid]['players'].append(uid)

        if not save_db(db):
            return jsonify({"error": f"Cannot write DB file at {DATA_FILE}"}), 500

        return jsonify({"success": True})

    return jsonify({"error": "Not found"})

@app.route('/api/toggle_status', methods=['POST'])
def toggle_status():
    db = get_db()
    d = request.get_json(silent=True) or {}
    uid = d.get('userId')
    if uid in db['players']:
        curr = db['players'][uid].get('status', 'offline')
        if curr in ['active', 'playing']:
            db['players'][uid]['status'] = 'offline'
            db['players'][uid]['partner_req'] = None
        else:
            db['players'][uid]['status'] = 'active'
            db['players'][uid]['last_active'] = time.time()
        save_db(db)
    return jsonify({"success": True})

@app.route('/api/request_partner', methods=['POST'])
def request_partner():
    db = get_db()
    d = request.get_json(silent=True) or {}
    uid = d.get('userId')
    target = d.get('targetId')

    if uid in db['players']:
        if db['players'][uid].get('status') != 'active':
            return jsonify({"error": "ต้อง Check-in ก่อนนะครับ"})
        db['players'][uid]['partner_req'] = target
        save_db(db)
        return jsonify({"success": True})
    return jsonify({"error": "User not found"})

@app.route('/api/cancel_request', methods=['POST'])
def cancel_request():
    db = get_db()
    d = request.get_json(silent=True) or {}
    uid = d.get('userId')
    if uid in db['players']:
        db['players'][uid]['partner_req'] = None
        save_db(db)
        return jsonify({"success": True})
    return jsonify({"error": "User not found"})

@app.route('/api/admin/toggle_session', methods=['POST'])
def toggle_session():
    db = get_db()
    d = request.get_json(silent=True) or {}
    uid = d.get('userId')
    action = d.get('action')

    if uid != SUPER_ADMIN_ID and uid not in db['mod_ids']:
        return jsonify({"error": "Unauthorized"}), 403

    if action == 'start':
        db['system_settings']['is_session_active'] = True
        if not db['system_settings'].get('current_event_id'):
            eid = str(uuid.uuid4())[:8]
            today = datetime.now().strftime("%d/%m/%Y")
            db['events'][eid] = {
                "id": eid,
                "name": f"ก๊วน {today}",
                "datetime": time.time(),
                "players": [],
                "status": "active"
            }
            db['system_settings']['current_event_id'] = eid
    else:
        db['system_settings']['is_session_active'] = False
        db['system_settings']['current_event_id'] = None
        for p in db['players'].values():
            if p.get('status') != 'offline':
                p['status'] = 'offline'
                p['partner_req'] = None

    if not save_db(db):
        return jsonify({"error": f"Cannot write DB file at {DATA_FILE}"}), 500

    return jsonify({"success": True})

@app.route('/api/admin/update_courts', methods=['POST'])
def update_courts():
    db = get_db()
    d = request.get_json(silent=True) or {}
    c = int(d.get('count', 2))
    db['system_settings']['total_courts'] = c
    save_db(db)
    refresh_courts(db)
    return jsonify({"success": True})

@app.route('/api/admin/reset_system', methods=['POST'])
def reset_system():
    db = get_db()
    d = request.get_json(silent=True) or {}
    if d.get('userId') != SUPER_ADMIN_ID:
        return jsonify({"error": "Super Admin Only"}), 403

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
    active_courts = {}
    refresh_courts(db)

    save_db(db)
    return jsonify({"success": True})

@app.route('/api/admin/manage_mod', methods=['POST'])
def manage_mod():
    db = get_db()
    d = request.get_json(silent=True) or {}
    if d.get('requesterId') != SUPER_ADMIN_ID:
        return jsonify({"error": "Super Admin Only"}), 403

    tid = d.get('targetUserId')
    action = d.get('action')
    if action == 'promote':
        if tid not in db['mod_ids']:
            db['mod_ids'].append(tid)
    else:
        if tid in db['mod_ids']:
            db['mod_ids'].remove(tid)

    save_db(db)
    return jsonify({"success": True})

@app.route('/api/admin/set_mmr', methods=['POST'])
def set_mmr():
    db = get_db()
    d = request.get_json(silent=True) or {}
    uid = d.get('targetUserId')
    if uid in db['players']:
        db['players'][uid]['mmr'] = int(d.get('newMmr', 1000))
        save_db(db)
        return jsonify({"success": True})
    return jsonify({"error": "Not found"})

@app.route('/api/matchmake', methods=['POST'])
def matchmake():
    db = get_db()
    refresh_courts(db)
    free = next((k for k, v in active_courts.items() if v is None), None)

    if not free:
        return jsonify({"status": "full"})

    active_players = [p for p in db['players'].values() if p.get('status') == 'active']
    groups = []
    processed = set()

    for p in active_players:
        if p['id'] in processed:
            continue

        partner_id = p.get('partner_req')
        partner = db['players'].get(partner_id) if partner_id else None

        if partner and partner.get('status') == 'active' and partner['id'] not in processed:
            t = max(float(p.get('last_active', 0)), float(partner.get('last_active', 0)))
            groups.append({"type": "pair", "members": [p, partner], "qt": t})
            processed.add(p['id'])
            processed.add(partner['id'])
        else:
            t = float(p.get('last_active', 0))
            groups.append({"type": "single", "members": [p], "qt": t})
            processed.add(p['id'])

    groups.sort(key=lambda x: x['qt'])

    selected = []
    for g in groups:
        if len(selected) + len(g['members']) <= 4:
            selected.extend(g['members'])
        if len(selected) == 4:
            break

    if len(selected) < 4:
        return jsonify({"status": "waiting"})

    team_a = [selected[0], selected[1]] if len(selected) >= 2 else [selected[0]]
    team_b = [selected[2], selected[3]] if len(selected) >= 4 else []

    active_courts[free] = {
        "team_a": [p['nickname'] for p in team_a],
        "team_a_ids": [p['id'] for p in team_a],
        "team_b": [p['nickname'] for p in team_b],
        "team_b_ids": [p['id'] for p in team_b],
        "start_time": time.time()
    }

    for p in selected:
        db['players'][p['id']]['status'] = 'playing'

    save_db(db)
    return jsonify({"status": "matched"})

@app.route('/api/submit_result', methods=['POST'])
def submit_result():
    db = get_db()
    d = request.get_json(silent=True) or {}
    cid = int(d.get('courtId', 0))
    winner = d.get('winner')
    uid = d.get('userId')

    m = active_courts.get(cid)
    if not m:
        return jsonify({"error": "No match"})

    is_super = uid == SUPER_ADMIN_ID
    is_mod = uid in db['mod_ids']
    is_player = (uid in m.get('team_a_ids', []) or uid in m.get('team_b_ids', []))

    if not (is_super or is_mod or is_player):
        return jsonify({"error": "Unauthorized"}), 403

    snapshot = {}
    win_ids = m['team_a_ids'] if winner == 'A' else m['team_b_ids']
    lose_ids = m['team_b_ids'] if winner == 'A' else m['team_a_ids']

    for pid in win_ids:
        if pid in db['players']:
            old = int(db['players'][pid].get('mmr', 1000))
            new = old + 25
            db['players'][pid]['mmr'] = new
            db['players'][pid]['status'] = 'active'
            db['players'][pid]['last_active'] = time.time()
            db['players'][pid]['partner_req'] = None
            snapshot[pid] = {"change": "+25"}

    for pid in lose_ids:
        if pid in db['players']:
            old = int(db['players'][pid].get('mmr', 1000))
            new = old - 20
            db['players'][pid]['mmr'] = new
            db['players'][pid]['status'] = 'active'
            db['players'][pid]['last_active'] = time.time()
            db['players'][pid]['partner_req'] = None
            snapshot[pid] = {"change": "-20"}

    hist = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "winner_team": winner,
        "mmr_snapshot": snapshot,
        "team_a": m['team_a'],
        "team_a_ids": m['team_a_ids'],
        "team_b": m['team_b'],
        "team_b_ids": m['team_b_ids']
    }

    db['match_history'].insert(0, hist)
    active_courts[cid] = None
    save_db(db)

    return jsonify({"success": True})

@app.route('/api/matchmake/manual', methods=['POST'])
def manual_matchmake():
    db = get_db()
    d = request.get_json(silent=True) or {}
    uid = d.get('userId')

    if uid != SUPER_ADMIN_ID and uid not in db['mod_ids']:
        return jsonify({"error": "Unauthorized"}), 403

    cid = int(d.get('courtId', 0))
    if active_courts.get(cid):
        return jsonify({"error": "Court Full"})

    p_ids = d.get('playerIds', [])
    players = [db['players'][pid] for pid in p_ids if pid in db['players']]

    if len(players) != 4:
        return jsonify({"error": "Need 4 players"})

    active_courts[cid] = {
        "team_a": [players[0]['nickname'], players[1]['nickname']],
        "team_a_ids": [players[0]['id'], players[1]['id']],
        "team_b": [players[2]['nickname'], players[3]['nickname']],
        "team_b_ids": [players[2]['id'], players[3]['id']],
        "start_time": time.time()
    }

    for p in players:
        db['players'][p['id']]['status'] = 'playing'
        db['players'][p['id']]['partner_req'] = None

    save_db(db)
    return jsonify({"success": True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
