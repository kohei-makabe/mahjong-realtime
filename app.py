import streamlit as st
import pandas as pd
import sqlite3
import uuid
from datetime import datetime

# ======================
# 初期設定（スマホ最適化）
# ======================
st.set_page_config(
    page_title="麻雀リーグ精算ボード",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# ======================
# DB関連
# ======================
DB_PATH = "mahjong_league.db"

def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def init_db():
    con = connect()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS rooms(
        id TEXT PRIMARY KEY,
        name TEXT,
        oka_top INTEGER,
        rate_per_1000 REAL,
        uma1 REAL, uma2 REAL, uma3 REAL, uma4 REAL,
        target_points INTEGER,
        rounding TEXT,
        created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS players(
        id TEXT PRIMARY KEY,
        room_id TEXT,
        display_name TEXT,
        joined_at TEXT,
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS seasons(
        id TEXT PRIMARY KEY,
        room_id TEXT,
        name TEXT,
        league TEXT,
        start_date TEXT,
        end_date TEXT,
        created_at TEXT,
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS meets(
        id TEXT PRIMARY KEY,
        season_id TEXT,
        name TEXT,
        created_at TEXT,
        FOREIGN KEY(season_id) REFERENCES seasons(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS results(
        id TEXT PRIMARY KEY,
        meet_id TEXT,
        player_id TEXT,
        final_points INTEGER,
        rank INTEGER,
        raw_pt REAL,
        net_yen REAL,
        yakuman INTEGER DEFAULT 0,
        yakitori INTEGER DEFAULT 0,
        created_at TEXT,
        FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE,
        FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
    );
    """)
    con.commit()
    con.close()

init_db()

# ======================
# 関数
# ======================
def apply_rounding(v, mode):
    if mode == "round":
        return round(v)
    elif mode == "floor":
        return int(v // 1)
    elif mode == "ceil":
        import math
        return math.ceil(v)
    return v

def settlement_for_room(room: dict, finals: dict):
    """麻雀上達.com互換ルール"""
    target = room["target_points"]
    rate = room["rate_per_1000"]
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    oka_points = room["oka_top"]
    rounding = room["rounding"]

    items = [(pid, apply_rounding(pts, rounding)) for pid, pts in finals.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks = {pid: i + 1 for i, (pid, _) in enumerate(items)}

    pts = {}
    for pid, score in items:
        base_pt = (score - target) / 1000.0
        uma_pt = uma[ranks[pid] - 1]
        pts[pid] = base_pt + uma_pt

    top_pid = items[0][0]
    pts[top_pid] += oka_points / 1000.0

    mean_diff = sum(pts.values()) / len(pts)
    for pid in pts:
        pts[pid] -= mean_diff

    nets = {pid: pt * rate for pid, pt in pts.items()}
    return nets, ranks, dict(items)

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id, name, created_at FROM rooms ORDER BY datetime(created_at) DESC;", con
    )

# ======================
# サイドバーUI
# ======================
with st.sidebar:
    st.markdown("## ルーム")
    action = st.radio("操作を選択", ["ルーム作成", "ルーム参加"], horizontal=True)
    con = connect()

    if action == "ルーム作成":
        name = st.text_input("ルーム名", "放射線科麻雀格闘倶楽部")
        oka = st.number_input("オカ(点)", value=25000, step=5000)
        rate = st.number_input("レート(円/千点)", value=10)
        uma1, uma2, uma3, uma4 = 10.0, 5.0, -5.0, -10.0
        target = st.number_input("返し(点)", value=25000)
        rounding = st.selectbox("丸め", ["none", "round", "floor", "ceil"])
        if st.button("ルーム作成"):
            rid = str(uuid.uuid4())
            con.execute(
                "INSERT INTO rooms VALUES (?,?,?,?,?,?,?,?,?,?)",
                (rid, name, oka, rate, uma1, uma2, uma3, uma4, target, rounding, datetime.utcnow().isoformat()),
            )
            con.commit()
            st.session_state["room_id"] = rid
            st.success("ルーム作成完了")

    else:
        rooms_df = df_rooms(con)
        if not rooms_df.empty:
            sel = st.selectbox(
                "参加するルームを選択",
                list(range(len(rooms_df))),
                format_func=lambda i: rooms_df.iloc[i]["name"],
            )
            room_id = rooms_df.iloc[sel]["id"]
            st.markdown(f"Room ID: `{room_id}`")
            name = st.text_input("あなたの表示名", "あなた")
            if st.button("参加"):
                pid = str(uuid.uuid4())
                con.execute(
                    "INSERT INTO players VALUES (?,?,?,?)",
                    (pid, room_id, name, datetime.utcnow().isoformat()),
                )
                con.commit()
                st.session_state["room_id"] = room_id
                st.session_state["player_id"] = pid
                st.success("参加しました！")
                st.rerun()

    st.divider()
    st.markdown("### 🗑️ ルーム削除")
    rooms_df = df_rooms(con)
    if not rooms_df.empty:
        sel = st.selectbox(
            "削除するルームを選択",
            list(range(len(rooms_df))),
            format_func=lambda i: rooms_df.iloc[i]["name"],
            key="delroom",
        )
        del_id = rooms_df.iloc[sel]["id"]
        confirm = st.checkbox("⚠️ 本当に削除する（すべての成績が失われます）")
        if st.button("削除実行", disabled=not confirm):
            con.execute("DELETE FROM rooms WHERE id=?", (del_id,))
            con.commit()
            st.success("削除しました")
            st.rerun()
    con.close()

# ======================
# メイン画面
# ======================
st.title("🀄 麻雀リーグ 成績ボード")

if "room_id" not in st.session_state:
    st.info("サイドバーからルームを選択または作成してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()

room = con.execute("SELECT * FROM rooms WHERE id=?", (room_id,)).fetchone()
if not room:
    st.error("ルームが存在しません。")
    st.stop()

room_keys = ["id","name","oka_top","rate_per_1000","uma1","uma2","uma3","uma4","target_points","rounding","created_at"]
room = dict(zip(room_keys, room))

# ============ タブ =============
tab_input, tab_result, tab_manage = st.tabs(["📝 入力", "📊 成績", "👥 メンバー設定"])

# ==============================
# 1️⃣ 入力タブ
# ==============================
with tab_input:
    st.subheader("半荘入力")

    # シーズン＆ミート選択
    seasons = pd.read_sql_query("SELECT * FROM seasons WHERE room_id=?", con, params=(room_id,))
    if seasons.empty:
        st.warning("シーズンがまだありません。")
        sname = st.text_input("シーズン名（例：2025後期）")
        league = st.text_input("リーグ（例：2025後期リーグ）")
        if st.button("シーズン作成"):
            sid = str(uuid.uuid4())
            con.execute("INSERT INTO seasons VALUES (?,?,?,?,?,?,?,?)",
                        (sid, room_id, sname, league, None, None, datetime.utcnow().isoformat()))
            con.commit()
            st.success("シーズン作成完了")
            st.rerun()
        st.stop()

    sel_season = st.selectbox("対象シーズン", seasons["name"])
    season_id = seasons.loc[seasons["name"]==sel_season,"id"].iloc[0]

    meets = pd.read_sql_query("SELECT * FROM meets WHERE season_id=?", con, params=(season_id,))
    if meets.empty:
        st.info("まだミートがありません。")
        if st.button("第1回ミートを作成"):
            mid = str(uuid.uuid4())
            con.execute("INSERT INTO meets VALUES (?,?,?,?)", (mid, season_id, "第1回", datetime.utcnow().isoformat()))
            con.commit()
            st.rerun()
    else:
        sel_meet = st.selectbox("対象ミート", meets["name"])
        meet_id = meets.loc[meets["name"]==sel_meet,"id"].iloc[0]

        # メンバーと点数入力
        players = pd.read_sql_query("SELECT * FROM players WHERE room_id=?", con)
        names = players["display_name"].tolist()
        cols = st.columns(4)
        finals = {}
        for i, col in enumerate(cols):
            if i < len(names):
                p = col.selectbox(f"{i+1}位", names, key=f"rank{i}")
                finals[p] = st.number_input(f"{p}の最終点", 0, 100000, 25000, step=100)
        st.write("役満・焼き鳥記録")
        yakuman = st.number_input("役満回数（合計）", 0, 10, 0)
        yakitori = st.checkbox("焼き鳥（上がり無し）")

        if st.button("精算を登録"):
            # プレイヤーID変換
            pid_map = {r["display_name"]:r["id"] for _,r in players.iterrows()}
            finals_pid = {pid_map[k]:v for k,v in finals.items() if k in pid_map}
            nets, ranks, raw = settlement_for_room(room, finals_pid)
            for pid,v in finals_pid.items():
                con.execute(
                    "INSERT INTO results VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (str(uuid.uuid4()), meet_id, pid, v, ranks[pid],
                     (v-room['target_points'])/1000, nets[pid],
                     yakuman if ranks[pid]==1 else 0, 1 if yakitori else 0,
                     datetime.utcnow().isoformat())
                )
            con.commit()
            st.success("記録しました！")
            st.rerun()

# ==============================
# 2️⃣ 成績タブ
# ==============================
with tab_result:
    st.subheader("成績 / 履歴")

    results = pd.read_sql_query("""
    SELECT s.league, s.name AS season, m.name AS meet, p.display_name, 
           r.rank, r.final_points, r.raw_pt, r.net_yen, r.yakuman, r.yakitori
    FROM results r
    JOIN meets m ON r.meet_id=m.id
    JOIN seasons s ON m.season_id=s.id
    JOIN players p ON r.player_id=p.id
    WHERE s.room_id=?;
    """, con, params=(room_id,))

    if results.empty:
        st.info("まだ記録がありません。")
    else:
        agg = results.groupby("display_name").agg(
            対局数=("rank","count"),
            一位=("rank",lambda x:(x==1).sum()),
            二位=("rank",lambda x:(x==2).sum()),
            三位=("rank",lambda x:(x==3).sum()),
            四位=("rank",lambda x:(x==4).sum()),
            平均順位=("rank","mean"),
            素点合計=("raw_pt","sum"),
            平均素点=("raw_pt","mean"),
            収支合計=("net_yen","sum"),
            平均収支=("net_yen","mean"),
            役満=("yakuman","sum"),
            焼き鳥=("yakitori","sum")
        ).reset_index()

        st.dataframe(agg, use_container_width=True)

# ==============================
# 3️⃣ メンバー管理
# ==============================
with tab_manage:
    st.subheader("メンバー一覧")
    members = pd.read_sql_query("SELECT * FROM players WHERE room_id=?", con, params=(room_id,))
    st.dataframe(members[["display_name","joined_at"]])

con.close()
