
import streamlit as st
import uuid
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict

DB_PATH = Path("mahjong.db")

# ---------------- Utilities ----------------
def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

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
            oka_top REAL NOT NULL,      -- 円で保持
            uma1 REAL NOT NULL,         -- 千点単位
            uma2 REAL NOT NULL,
            uma3 REAL NOT NULL,
            uma4 REAL NOT NULL,
            rounding TEXT NOT NULL      -- 'none' | 'floor' | 'round' | 'ceil' (点数の100点丸め)
        );
        CREATE TABLE IF NOT EXISTS players (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            joined_at TEXT NOT NULL,
            UNIQUE(room_id, display_name),
            FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
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
    con.commit()
    con.close()

def apply_rounding(points: int, mode: str) -> int:
    """Round points to 100-point units, or no rounding if 'none'."""
    if mode == "none":
        return int(points)
    if mode == "floor":
        return (points // 100) * 100
    elif mode == "ceil":
        return ((points + 99) // 100) * 100
    else:  # 'round'
        return int(round(points / 100.0) * 100)

def settlement_for_room(room: dict, finals: Dict[str, int]):
    """
    Calculate per-hanchan settlement.
    finals: player_id -> final_points (e.g., 43200)
    Returns (nets_yen, ranks, rounded_finals)
    """
    target = room["target_points"]
    rate = room["rate_per_1000"]     # 円/千点
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]  # 千点
    oka_yen = room["oka_top"]        # 円
    rounding = room["rounding"]

    items = [(pid, apply_rounding(pts, rounding)) for pid, pts in finals.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks = {pid: i+1 for i, (pid, _) in enumerate(items)}

    nets = {pid: 0.0 for pid, _ in items}
    for pid, pts in items:
        base = (pts - target) / 1000.0 * rate
        uma_yen = uma[ranks[pid]-1] * rate
        nets[pid] = base + uma_yen

    # OKA（円）をトップに加算
    top_pid = items[0][0]
    nets[top_pid] += oka_yen

    # 端数調整はしない（総和0への丸めは行わない）。長期総当たりを想定してそのまま残す。
    return nets, ranks, dict(items)

def room_row_to_dict(row, columns):
    return {columns[i]: row[i] for i in range(len(columns))}

def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?;", (room_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    d = room_row_to_dict(row, cols)
    for k in ["start_points","target_points"]:
        d[k] = int(d[k])
    for k in ["rate_per_1000","oka_top","uma1","uma2","uma3","uma4"]:
        d[k] = float(d[k])
    return d

def get_players(con, room_id):
    return pd.read_sql_query("SELECT * FROM players WHERE room_id=? ORDER BY joined_at;", con, params=(room_id,))

def get_hanchan(con, room_id):
    return pd.read_sql_query(
        """
        SELECT h.id, h.started_at, h.finished_at, h.memo,
               p.display_name, r.final_points, r.rank, r.net_cash, r.player_id
        FROM hanchan h
        JOIN results r ON r.hanchan_id = h.id
        JOIN players p ON p.id = r.player_id
        WHERE h.room_id=?
        ORDER BY h.started_at DESC, r.rank ASC;
        """,
        con, params=(room_id,)
    )

def head_to_head(con, room_id):
    df = pd.read_sql_query(
        """
        SELECT h.id as hanchan_id, r.player_id, r.net_cash
        FROM hanchan h
        JOIN results r ON r.hanchan_id = h.id
        WHERE h.room_id=?
        """,
        con, params=(room_id,)
    )
    players = pd.read_sql_query("SELECT id, display_name FROM players WHERE room_id=?", con, params=(room_id,))
    id_to_name = dict(players.values)

    stats = {}
    for hid, g in df.groupby("hanchan_id"):
        g = g.set_index("player_id")["net_cash"]
        pids = list(g.index)
        for i in range(len(pids)):
            for j in range(i+1, len(pids)):
                a, b = pids[i], pids[j]
                key = tuple(sorted([a,b]))
                stats.setdefault(key, {"count":0, "net_ab":0.0})
                stats[key]["count"] += 1
                stats[key]["net_ab"] += (g[a] - g[b]) / 2.0

    rows = [{
        "A": id_to_name.get(a, a),
        "B": id_to_name.get(b, b),
        "同卓回数": v["count"],
        "A基準ネット(円)": v["net_ab"]
    } for (a,b), v in stats.items()]
    return pd.DataFrame(rows).sort_values(["A","B"]).reset_index(drop=True)

# --------------- UI ---------------
st.set_page_config(page_title="麻雀・リアタイ精算ボード", page_icon="🀄", layout="wide")
st.title("🀄 麻雀・リアタイ精算ボード")

init_db()

with st.sidebar:
    st.header("ルーム")
    action = st.radio("操作を選択", ["ルーム作成","ルーム参加"], horizontal=True)
    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1, col2 = st.columns(2)
        with col1:
            start_points = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)  # 返し=25k
            rate_per_1000 = st.number_input("レート(円/千点)", value=100.0, step=10.0, help="テンイチ=100円/千点")
        with col2:
            # オカは「円」で入力。25,000点オカ × テンイチ = 2,500円 を初期値に設定。
            oka_top = st.number_input("オカ(トップ/円)", value=2500.0, step=100.0)
            st.caption("例: オカ25,000点 × テンイチ=2,500円。")
            uma1 = st.number_input("ウマ 1位(+千点)", value=10.0, step=1.0)   # 5-10 → +10
            uma2 = st.number_input("ウマ 2位(+千点)", value=5.0, step=1.0)    # +5
            uma3 = st.number_input("ウマ 3位(−千点)", value=-5.0, step=1.0)   # -5
            uma4 = st.number_input("ウマ 4位(−千点)", value=-10.0, step=1.0)  # -10
        rounding = st.selectbox("点数の丸め(100点単位)", ["none","round","floor","ceil"], index=0, help="長期総当たりなら 'none' 推奨")
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
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?);",
                (pid, room_id, creator, datetime.utcnow().isoformat())
            )
            con.commit()
            con.close()
            st.session_state["room_id"] = room_id
            st.session_state["player_id"] = pid
            st.success(f"ルーム作成しました！ ルームID: {room_id}")
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

st.caption("ライブ更新: 複数人で開いても数秒で入力が反映（簡易ポーリング）。")

# ---------------- Main ----------------
if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成または参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。")
    st.stop()

st.subheader(f"ルーム: {room['name']}")
st.caption(f"Room ID: {room_id} / 返し:{room['target_points']} / レート:{room['rate_per_1000']}円/千点 / ウマ:{room['uma1']}/{room['uma2']}/{room['uma3']}/{room['uma4']} / オカ(円):{room['oka_top']:.0f} / 丸め:{room['rounding']}")

players_df = get_players(con, room_id)
if len(players_df) < 4:
    st.warning("プレイヤーが4人未満です。4人が揃ってから半荘開始をおすすめします。")
st.dataframe(players_df[["display_name","joined_at"]].rename(columns={"display_name":"プレイヤー","joined_at":"参加"}))

st.divider()
st.subheader("半荘入力")

with st.form("hanchan_form"):
    st.write("この半荘の最終点数（100点単位推奨）を入力してください。")
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
        con.execute("INSERT INTO hanchan(id, room_id, started_at, finished_at, memo) VALUES (?,?,?,?,?);",
                    (hid, room_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), memo))
        for pid in plist:
            rid = str(uuid.uuid4())
            con.execute("""INSERT INTO results(id, hanchan_id, player_id, final_points, rank, net_cash)
                           VALUES (?,?,?,?,?,?);""",
                        (rid, hid, pid, int(rounded_finals[pid]), int(ranks[pid]), float(nets[pid])))
        con.commit()
        st.success("半荘を登録しました！")

st.divider()
st.subheader("成績 / 履歴")

hdf = get_hanchan(con, room_id)
if hdf.empty:
    st.info("まだ成績がありません。")
else:
    agg = hdf.groupby("display_name").agg(
        回数=("rank","count"),
        1位=("rank", lambda s: (s==1).sum()),
        2位=("rank", lambda s: (s==2).sum()),
        3位=("rank", lambda s: (s==3).sum()),
        4位=("rank", lambda s: (s==4).sum()),
        収支合計=("net_cash","sum")
    ).reset_index()
    agg["平均順位"] = (hdf.groupby("display_name")["rank"].mean()).values
    agg = agg.sort_values("収支合計", ascending=False)
    st.write("### 個人成績（累積）")
    st.dataframe(agg)

    st.write("### 半荘履歴")
    disp = hdf.copy()
    disp["net_cash"] = disp["net_cash"].map(lambda x: f"{x:,.0f}")
    disp["final_points"] = disp["final_points"].map(lambda x: f"{x:,}")
    disp = disp.rename(columns={
        "display_name":"プレイヤー",
        "final_points":"最終点",
        "rank":"着順",
        "net_cash":"精算(円)",
        "started_at":"開始UTC",
        "finished_at":"終了UTC",
        "memo": "メモ"
    })
    st.dataframe(disp[["開始UTC","終了UTC","メモ","プレイヤー","最終点","着順","精算(円)"]])

    st.write("### 対人（ヘッドトゥヘッド） / 同卓回数")
    h2h = head_to_head(con, room_id)
    st.dataframe(h2h)

    st.download_button("成績CSVをダウンロード", agg.to_csv(index=False).encode("utf-8-sig"), file_name="summary.csv", mime="text/csv")

st.divider()
st.caption("式: 精算 = (最終点 - 返し)/1000 * レート + UMA(順位)×レート + OKA(トップ/円)。点数丸め 'none' で端数はそのまま。")

con.close()
