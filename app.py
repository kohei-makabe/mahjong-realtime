
import streamlit as st
import uuid
import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional

DB_PATH = Path("mahjong.db")

# ---------------- Utilities ----------------
def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def table_has_column(con, table: str, col: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def init_db():
    con = connect()
    cur = con.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            start_points INTEGER NOT NULL,
            target_points INTEGER NOT NULL,
            rate_per_1000 REAL NOT NULL,
            oka_top REAL NOT NULL,
            uma1 REAL NOT NULL,
            uma2 REAL NOT NULL,
            uma3 REAL NOT NULL,
            uma4 REAL NOT NULL,
            rounding TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS players (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            joined_at TEXT NOT NULL,
            UNIQUE(room_id, display_name),
            FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS seasons (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            name TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS meets (
            id TEXT PRIMARY KEY,
            season_id TEXT NOT NULL,
            name TEXT NOT NULL,
            meet_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(season_id) REFERENCES seasons(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS hanchan (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            memo TEXT,
            FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS results (
            id TEXT PRIMARY KEY,
            hanchan_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            final_points INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            net_cash REAL NOT NULL,
            FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
            FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
            UNIQUE(hanchan_id, player_id)
        );
        """
    )
    if not table_has_column(con, "hanchan", "meet_id"):
        try:
            con.execute("ALTER TABLE hanchan ADD COLUMN meet_id TEXT;")
        except Exception:
            pass
    con.commit()
    con.close()

def yen(x: float) -> str:
    return f"{x:,.0f}"

def apply_rounding(points: int, mode: str) -> int:
    if mode == "none":
        return int(points)
    if mode == "floor":
        return (points // 100) * 100
    elif mode == "ceil":
        return ((points + 99) // 100) * 100
    else:  # 'round'
        return int(round(points / 100.0) * 100)

def settlement_for_room(room: dict, finals: Dict[str, int]):
    target = room["target_points"]
    rate = room["rate_per_1000"]
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    oka_yen = room["oka_top"]
    rounding = room["rounding"]

    items = [(pid, apply_rounding(pts, rounding)) for pid, pts in finals.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks = {pid: i+1 for i, (pid, _) in enumerate(items)}

    nets = {pid: 0.0 for pid, _ in items}
    for pid, pts in items:
        base = (pts - target) / 1000.0 * rate
        uma_yen = uma[ranks[pid]-1] * rate
        nets[pid] = base + uma_yen
    top_pid = items[0][0]
    nets[top_pid] += oka_yen
    return nets, ranks, dict(items)

def row_to_dict(row, columns):
    return {columns[i]: row[i] for i in range(len(columns))}

def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?;", (room_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    d = row_to_dict(row, cols)
    for k in ["start_points","target_points"]:
        d[k] = int(d[k])
    for k in ["rate_per_1000","oka_top","uma1","uma2","uma3","uma4"]:
        d[k] = float(d[k])
    return d

def df_players(con, room_id):
    return pd.read_sql_query("SELECT * FROM players WHERE room_id=? ORDER BY joined_at;", con, params=(room_id,))

def df_seasons(con, room_id):
    return pd.read_sql_query("SELECT * FROM seasons WHERE room_id=? ORDER BY start_date;", con, params=(room_id,))

def df_meets(con, season_id):
    return pd.read_sql_query("SELECT * FROM meets WHERE season_id=? ORDER BY meet_date;", con, params=(season_id,))

def df_hanchan_join(con, room_id, season_id=None, meet_id=None):
    q = """
        SELECT h.id, h.room_id, h.meet_id, h.started_at, h.finished_at, h.memo,
               p.display_name, r.final_points, r.rank, r.net_cash, r.player_id,
               m.name as meet_name, m.meet_date, s.name as season_name
        FROM hanchan h
        JOIN results r ON r.hanchan_id = h.id
        JOIN players p ON p.id = r.player_id
        LEFT JOIN meets m ON m.id = h.meet_id
        LEFT JOIN seasons s ON s.id = m.season_id
        WHERE h.room_id=?
    """
    params = [room_id]
    if season_id:
        q += " AND s.id=?"
        params.append(season_id)
    if meet_id:
        q += " AND h.meet_id=?"
        params.append(meet_id)
    q += " ORDER BY h.started_at DESC, r.rank ASC;"
    return pd.read_sql_query(q, con, params=tuple(params))

# --------------- UI ---------------
st.set_page_config(page_title="麻雀・リーグ（シーズン/ミート）デモ", page_icon="🀄", layout="wide")
st.title("🀄 麻雀・リーグ（シーズン/ミート）デモ")

init_db()

with st.sidebar:
    st.header("ルーム")
    action = st.radio("操作を選択", ["ルーム作成","ルーム参加"], horizontal=True)
    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1, col2 = st.columns(2)
        with col1:
            start_points = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate_per_1000 = st.number_input("レート(円/千点)", value=100.0, step=10.0)
        with col2:
            oka_top = st.number_input("オカ(トップ/円)", value=2500.0, step=100.0)
            uma1 = st.number_input("ウマ 1位(+千点)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ 2位(+千点)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ 3位(−千点)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ 4位(−千点)", value=-10.0, step=1.0)
        rounding = st.selectbox("点数丸め", ["none","round","floor","ceil"], index=0)
        creator = st.text_input("あなたの表示名", value="あなた")
        if st.button("ルーム作成"):
            room_id = str(uuid.uuid4())
            con = connect()
            con.execute(
                """INSERT INTO rooms(id,name,created_at,start_points,target_points,rate_per_1000,oka_top,uma1,uma2,uma3,uma4,rounding)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?);""",
                (room_id, name, datetime.utcnow().isoformat(), start_points, target_points, rate_per_1000,
                 oka_top, uma1, uma2, uma3, uma4, rounding)
            )
            pid = str(uuid.uuid4())
            con.execute("INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?);",
                        (pid, room_id, creator, datetime.utcnow().isoformat()))
            con.commit(); con.close()
            st.session_state["room_id"] = room_id
            st.session_state["player_id"] = pid
            st.success(f"作成OK！ Room ID: {room_id}")
    else:
        room_id_in = st.text_input("ルームIDを入力")
        name_in = st.text_input("あなたの表示名", value="あなた")
        if st.button("参加"):
            con = connect()
            cur = con.execute("SELECT id FROM rooms WHERE id=?", (room_id_in,))
            if not cur.fetchone():
                st.error("そのルームIDは存在しません。")
            else:
                cur = con.execute("SELECT id FROM players WHERE room_id=? AND display_name=?", (room_id_in, name_in))
                row = cur.fetchone()
                if row:
                    pid = row[0]
                else:
                    pid = str(uuid.uuid4())
                    con.execute("INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?);",
                                (pid, room_id_in, name_in, datetime.utcnow().isoformat()))
                    con.commit()
                st.session_state["room_id"] = room_id_in
                st.session_state["player_id"] = pid
                st.success("参加しました！")
            con.close()

st.caption("誰でも入力OK。シーズン→ミート→半荘で管理します。")

if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。"); st.stop()

st.subheader(f"ルーム: {room['name']}")
players_df = df_players(con, room_id)
st.dataframe(players_df[["display_name","joined_at"]].rename(columns={"display_name":"プレイヤー","joined_at":"参加"}))

# Seasons
st.divider()
st.subheader("シーズン")
seasons_df = df_seasons(con, room_id)
colA, colB = st.columns([2,1])
with colA:
    st.dataframe(seasons_df.rename(columns={"name":"シーズン名","start_date":"開始日","end_date":"終了日"}))
with colB:
    with st.form("season_form"):
        s_name = st.text_input("シーズン名", value=f"{date.today().year} 前期")
        s_start = st.date_input("開始日", value=date(date.today().year,1,1))
        s_end = st.date_input("終了日", value=date(date.today().year,6,30))
        if st.form_submit_button("シーズン作成"):
            sid = str(uuid.uuid4())
            con.execute("""INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at)
                           VALUES (?,?,?,?,?,?);""",
                        (sid, room_id, s_name, s_start.isoformat(), s_end.isoformat(), datetime.utcnow().isoformat()))
            con.commit()
            st.rerun()

sel_season_id = None
if not seasons_df.empty:
    sel_season_name = st.selectbox("集計対象シーズン", seasons_df["name"].tolist())
    sel_season_id = seasons_df[seasons_df["name"]==sel_season_name]["id"].values[0]

# Meets
st.divider()
st.subheader("ミート（開催）")
if sel_season_id:
    meets_df = df_meets(con, sel_season_id)
    colM1, colM2 = st.columns([2,1])
    with colM1:
        st.dataframe(meets_df.rename(columns={"name":"ミート名","meet_date":"開催日"}))
    with colM2:
        with st.form("meet_form"):
            m_name = st.text_input("ミート名", value="第1回")
            m_date = st.date_input("開催日", value=date.today())
            if st.form_submit_button("ミート作成"):
                mid = str(uuid.uuid4())
                con.execute("""INSERT INTO meets(id,season_id,name,meet_date,created_at)
                               VALUES (?,?,?,?,?);""",
                            (mid, sel_season_id, m_name, m_date.isoformat(), datetime.utcnow().isoformat()))
                con.commit()
                st.rerun()
    sel_meet_id = None
    if not meets_df.empty:
        sel_meet_name = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist())
        sel_meet_id = meets_df[meets_df["name"]==sel_meet_name]["id"].values[0]
else:
    st.info("まずシーズンを選んでください。")
    sel_meet_id = None

# Input Hanchan
st.divider()
st.subheader("半荘入力（誰でも）")
if not sel_meet_id:
    st.info("入力するにはミートを選択してください。")
else:
    with st.form("hanchan_form"):
        finals = {}
        plist = players_df["id"].tolist()[:4]
        pmap = dict(zip(players_df["id"], players_df["display_name"]))
        cols = st.columns(max(1, min(4, len(plist))))
        for i, pid in enumerate(plist):
            with cols[i % len(cols)]:
                finals[pid] = st.number_input(f"{pmap[pid]}", value=25000, step=100, key=f"fp_{pid}")
        memo = st.text_input("メモ（任意）", value="")
        submitted = st.form_submit_button("精算を記録")
        if submitted and len(finals)==4:
            nets, ranks, rounded_finals = settlement_for_room(room, finals)
            hid = str(uuid.uuid4())
            con.execute("""INSERT INTO hanchan(id, room_id, started_at, finished_at, memo, meet_id)
                           VALUES (?,?,?,?,?,?);""",
                        (hid, room_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), memo, sel_meet_id))
            for pid in plist:
                rid = str(uuid.uuid4())
                con.execute("""INSERT INTO results(id, hanchan_id, player_id, final_points, rank, net_cash)
                               VALUES (?,?,?,?,?,?);""",
                            (rid, hid, pid, int(rounded_finals[pid]), int(ranks[pid]), float(nets[pid])))
            con.commit()
            st.success("半荘を登録しました！")

# Stats
st.divider()
st.subheader("成績 / 履歴（シーズン/ミートで絞り込み）")
hdf = df_hanchan_join(con, room_id, sel_season_id, sel_meet_id)
if hdf.empty:
    st.info("まだ成績がありません。")
else:
    # Standings
    g = hdf.groupby("display_name")
    summary = pd.DataFrame({
        "回数": g["rank"].count(),
        "1位": g["rank"].apply(lambda s: (s == 1).sum()),
        "2位": g["rank"].apply(lambda s: (s == 2).sum()),
        "3位": g["rank"].apply(lambda s: (s == 3).sum()),
        "4位": g["rank"].apply(lambda s: (s == 4).sum()),
        "収支合計": g["net_cash"].sum(),
        "平均順位": g["rank"].mean(),
    }).reset_index().sort_values("収支合計", ascending=False)
    st.write("### 個人成績（累積）")
    st.dataframe(summary)

    st.write("### 半荘履歴")
    disp = hdf.copy()
    disp["net_cash"] = disp["net_cash"].map(lambda x: f"{x:,.0f}")
    disp["final_points"] = disp["final_points"].map(lambda x: f"{x:,}")
    disp = disp.rename(columns={
        "season_name":"シーズン",
        "meet_name":"ミート",
        "display_name":"プレイヤー",
        "final_points":"最終点",
        "rank":"着順",
        "net_cash":"精算(円)",
        "started_at":"開始UTC",
        "memo": "メモ"
    })
    st.dataframe(disp[["シーズン","ミート","開始UTC","メモ","プレイヤー","最終点","着順","精算(円)"]])

    # Head-to-head within current filter
    st.write("### 対人（ヘッドトゥヘッド）")
    rows = []
    for hid, gg in hdf.groupby("id"):
        net = gg.set_index("player_id")["net_cash"]
        pids = list(net.index)
        names = gg.set_index("player_id")["display_name"].to_dict()
        for i in range(len(pids)):
            for j in range(i+1, len(pids)):
                a, b = pids[i], pids[j]
                rows.append({"A": names[a], "B": names[b], "同卓回数": 1, "A基準ネット(円)": (net[a]-net[b])/2.0})
    if rows:
        h2h = pd.DataFrame(rows).groupby(["A","B"]).agg({"同卓回数":"sum","A基準ネット(円)":"sum"}).reset_index()
        st.dataframe(h2h)

    st.download_button(
        "成績CSVをダウンロード",
        summary.to_csv(index=False).encode("utf-8-sig"),
        file_name="summary.csv",
        mime="text/csv"
    )

st.caption("式: 精算 = (最終点 - 返し)/1000 * レート + UMA(順位)×レート + OKA(トップ/円)。丸め 'none' 推奨。")
con.close()
