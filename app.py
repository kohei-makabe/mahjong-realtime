# app.py
# 麻雀リーグ 精算ツール（スマホ最適化）
# 仕様（一般的な25k/30k返し＋ウマ方式）:
#  - 素点pt = (final_points - target_points)/1000   ※final_pointsは丸め設定後の値を使用
#  - 最終pt = 素点pt + ウマpt
#  - 収支(円) = 最終pt × rate_per_1000
#  - トップ別オカは入れない（返し点だけで表現）
# 付帯機能:
#  - ルーム作成/一覧参加/削除、メンバー候補/追加、シーズン/ミート作成・編集・削除
#  - 半荘入力（東南西北・最終点・メモ・役満回数/焼き鳥フラグ）
#  - 成績: ミート/シーズン/全リーグ切替、個人成績（回数・着順・pt合計/平均・収支合計・役満回数・焼き鳥数）
#  - 半荘履歴（シーズン/ミート/プレイヤー/点棒/素点/着順/精算円）
#  - ランキング左端は順位（pt合計でソート）

import streamlit as st
import uuid
import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional, List, Tuple

# ---------------- UI 基本 ----------------
st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# スマホ向け少し大きめ
st.markdown("""
<style>
button, .stButton>button { padding: 0.6rem 0.9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.05rem; }
[data-testid="stMetricValue"] { font-size: 1.1rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")
DEFAULT_MEMBERS = ["眞壁", "内藤", "森", "浜野", "傅田", "須崎", "中間", "高田", "内藤士"]

# ---------------- DB ----------------
def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def table_has_column(con, table: str, col: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    return col in [r[1] for r in cur.fetchall()]

def init_db():
    con = connect()
    cur = con.cursor()
    # 基本スキーマ
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS rooms (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TEXT NOT NULL,
        start_points INTEGER NOT NULL,      -- 持ち点
        target_points INTEGER NOT NULL,     -- 返し
        rate_per_1000 REAL NOT NULL,        -- レート(円/千点)
        uma1 REAL NOT NULL,                 -- 順位ウマ +千点
        uma2 REAL NOT NULL,
        uma3 REAL NOT NULL,
        uma4 REAL NOT NULL,
        rounding TEXT NOT NULL              -- none/round/floor/ceil (点棒の丸め単位=100点)
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
        meet_id TEXT,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        memo TEXT,
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE,
        FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS results (
        id TEXT PRIMARY KEY,
        hanchan_id TEXT NOT NULL,
        player_id TEXT NOT NULL,
        final_points INTEGER NOT NULL,   -- 丸め後 最終点(点棒)
        rank INTEGER NOT NULL,
        yakuman_count INTEGER NOT NULL DEFAULT 0, -- その半荘での役満回数
        yakitori INTEGER NOT NULL DEFAULT 0,      -- その半荘で焼き鳥(1/0)
        FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
        FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
        UNIQUE(hanchan_id, player_id)
    );
    """)
    # 既存DBのマイグレーション（足りない列を追加。INSERTの列数不一致の元を潰す）
    # rooms: rounding が無い古DBに追加
    if not table_has_column(con, "rooms", "rounding"):
        con.execute("ALTER TABLE rooms ADD COLUMN rounding TEXT DEFAULT 'none';")
    for col in ("uma1","uma2","uma3","uma4"):
        if not table_has_column(con, "rooms", col):
            con.execute(f"ALTER TABLE rooms ADD COLUMN {col} REAL DEFAULT 0.0;")
    if not table_has_column(con, "rooms", "rate_per_1000"):
        con.execute("ALTER TABLE rooms ADD COLUMN rate_per_1000 REAL DEFAULT 100.0;")
    if not table_has_column(con, "rooms", "start_points"):
        con.execute("ALTER TABLE rooms ADD COLUMN start_points INTEGER DEFAULT 25000;")
    if not table_has_column(con, "rooms", "target_points"):
        con.execute("ALTER TABLE rooms ADD COLUMN target_points INTEGER DEFAULT 25000;")

    # results: yakuman/yakitori が無い古DBに追加
    if not table_has_column(con, "results", "yakuman_count"):
        con.execute("ALTER TABLE results ADD COLUMN yakuman_count INTEGER DEFAULT 0;")
    if not table_has_column(con, "results", "yakitori"):
        con.execute("ALTER TABLE results ADD COLUMN yakitori INTEGER DEFAULT 0;")

    # hanchan: meet_id 無い古DBに追加
    if not table_has_column(con, "hanchan", "meet_id"):
        con.execute("ALTER TABLE hanchan ADD COLUMN meet_id TEXT;")

    con.commit()
    con.close()

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id, name, created_at FROM rooms ORDER BY datetime(created_at) DESC;",
        con
    )

def get_room(con, room_id) -> Optional[dict]:
    cur = con.execute("SELECT * FROM rooms WHERE id=?;", (room_id,))
    row = cur.fetchone()
    if not row: return None
    cols = [d[0] for d in cur.description]
    d = {cols[i]: row[i] for i in range(len(cols))}
    # 型を整える
    for k in ("start_points","target_points"):
        d[k] = int(d[k])
    for k in ("rate_per_1000","uma1","uma2","uma3","uma4"):
        d[k] = float(d[k])
    d["rounding"] = d.get("rounding","none")
    return d

def df_players(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM players WHERE room_id=? ORDER BY joined_at;",
        con, params=(room_id,)
    )

def df_seasons(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM seasons WHERE room_id=? ORDER BY start_date;",
        con, params=(room_id,)
    )

def df_meets(con, season_id):
    return pd.read_sql_query(
        "SELECT * FROM meets WHERE season_id=? ORDER BY meet_date;",
        con, params=(season_id,)
    )

def df_hanchan_join(con, room_id, season_id: Optional[str]=None, meet_id: Optional[str]=None):
    q = """
    SELECT h.id, h.room_id, h.meet_id, h.started_at, h.finished_at, h.memo,
           p.display_name, r.final_points, r.rank, r.player_id,
           r.yakuman_count, r.yakitori,
           m.name AS meet_name, m.meet_date, s.name AS season_name
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

# ---------------- ロジック ----------------
def apply_rounding(points: int, mode: str) -> int:
    if mode == "none": return int(points)
    if mode == "floor": return (points // 100) * 100
    if mode == "ceil":  return ((points + 99) // 100) * 100
    # round: 四捨五入
    return int(round(points / 100.0) * 100)

def settlement_for_room(
    room: dict,
    finals: Dict[str, int],
    ranks_by_id: Dict[str, int]
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, int]]:
    """
    戻り値:
      base_pt[player_id], uma_pt[player_id], total_pt[player_id], rounded_final_points[player_id]
    """
    tgt = room["target_points"]
    rate = room["rate_per_1000"]
    uma = {1: room["uma1"], 2: room["uma2"], 3: room["uma3"], 4: room["uma4"]}
    rnd = room["rounding"]

    rounded = {pid: apply_rounding(pts, rnd) for pid, pts in finals.items()}
    base_pt = {pid: (rounded[pid] - tgt) / 1000.0 for pid in finals}
    uma_pt  = {pid: float(uma[ranks_by_id[pid]]) for pid in finals}
    total_pt = {pid: base_pt[pid] + uma_pt[pid] for pid in finals}
    return base_pt, uma_pt, total_pt, rounded

def ensure_players(con, room_id: str, names: List[str]) -> None:
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed = False
    for name in names:
        nm = (name or "").strip()
        if nm and nm not in have:
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (str(uuid.uuid4()), room_id, nm, datetime.utcnow().isoformat())
            )
            changed = True
    if changed: con.commit()

def rank_from_points(name_ids: List[str], finals_by_id: Dict[str, int]) -> Dict[str, int]:
    # 点棒降順で 1..4 位
    order = sorted(name_ids, key=lambda pid: finals_by_id[pid], reverse=True)
    return {pid: (i+1) for i, pid in enumerate(order)}

def points_input(label: str, key: str, default: int = 25000) -> int:
    return int(st.number_input(label, value=default, step=100, key=f"{key}_num"))

# ---------------- 画面 ----------------
st.title("🀄 麻雀リーグ 精算ツール")
init_db()

with st.sidebar:
    st.header("ルーム")

    action = st.radio("操作を選択", ["ルーム作成", "ルーム参加"], horizontal=True)

    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1, col2 = st.columns(2)
        with col1:
            start_points = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate_per_1000 = st.number_input("レート(円/千点)", value=100.0, step=10.0)
        with col2:
            uma1 = st.number_input("ウマ1位(+千点)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ2位(+千点)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ3位(−千点)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ4位(−千点)", value=-10.0, step=1.0)
        rounding = st.selectbox("点数丸め", ["none", "round", "floor", "ceil"], index=0)
        creator = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            con = connect()
            rid = str(uuid.uuid4())
            con.execute(
                """INSERT INTO rooms
                   (id,name,created_at,start_points,target_points,rate_per_1000,uma1,uma2,uma3,uma4,rounding)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?);""",
                (rid, name, datetime.utcnow().isoformat(),
                 int(start_points), int(target_points), float(rate_per_1000),
                 float(uma1), float(uma2), float(uma3), float(uma4),
                 str(rounding))
            )
            pid = str(uuid.uuid4())
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (pid, rid, creator, datetime.utcnow().isoformat())
            )
            con.commit(); con.close()
            st.session_state["room_id"] = rid
            st.session_state["player_id"] = pid
            st.success("作成しました。")

    else:
        con = connect()
        rooms_df = df_rooms(con)
        if rooms_df.empty:
            st.info("まだルームがありません。『ルーム作成』からどうぞ。")
        else:
            def fmt(r):
                ts = r["created_at"].split("T")[0] + " " + r["created_at"][11:16]
                return f'{r["name"]}（{ts}）'
            labels = [fmt(r) for _, r in rooms_df.iterrows()]
            idx = st.selectbox("参加するルームを選択", options=list(range(len(labels))),
                               format_func=lambda i: labels[i])
            selected_room_id = rooms_df.iloc[idx]["id"]
            st.caption(f"Room ID: `{selected_room_id}`")
            name_in = st.text_input("あなたの表示名", value="あなた")
            if st.button("参加"):
                cur = con.execute(
                    "SELECT id FROM players WHERE room_id=? AND display_name=?",
                    (selected_room_id, name_in)
                )
                row = cur.fetchone()
                if row:
                    pid = row[0]
                else:
                    pid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                        (pid, selected_room_id, name_in, datetime.utcnow().isoformat())
                    )
                    con.commit()
                st.session_state["room_id"] = selected_room_id
                st.session_state["player_id"] = pid
                st.success("参加しました。")
                st.rerun()
        con.close()

    # ルーム削除
    st.divider()
    st.markdown("### 🗑️ ルーム削除")
    con = connect()
    rooms_df2 = df_rooms(con)
    if rooms_df2.empty:
        st.caption("ルームがありません。")
    else:
        def fmt_room(r):
            ts = r["created_at"].split("T")[0] + " " + r["created_at"][11:16]
            return f'{r["name"]}（{ts}）'
        labels_del = [fmt_room(r) for _, r in rooms_df2.iterrows()]
        idx_del = st.selectbox("削除するルームを選択", options=list(range(len(labels_del))),
                               format_func=lambda i: labels_del[i], key="del_room")
        selected_room_id_del = rooms_df2.iloc[idx_del]["id"]
        confirm = st.checkbox("⚠️ 本当に削除する（すべてのシーズン・成績が失われます）")
        if st.button("ルーム削除実行", disabled=not confirm):
            con.execute("DELETE FROM rooms WHERE id=?;", (selected_room_id_del,))
            con.commit()
            if st.session_state.get("room_id") == selected_room_id_del:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("削除しました。")
            st.rerun()
    con.close()

st.caption("計算: 素点pt=(最終点-返し)/1000, 最終pt=素点pt+ウマpt, 収支=最終pt×レート。トップ別オカなし。")

if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。")
    st.stop()

players_df = df_players(con, room_id)
st.write(f"**ルーム: {room['name']}**")
st.dataframe(players_df[["display_name","joined_at"]].rename(columns={"display_name":"プレイヤー","joined_at":"参加"}),
             use_container_width=True, height=240)

# 共通セレクタ
seasons_df = df_seasons(con, room_id)
sel_season_id = None
sel_meet_id = None
if not seasons_df.empty:
    sname = st.selectbox("集計対象シーズン", seasons_df["name"].tolist(), key="season_sel_top")
    sel_season_id = seasons_df[seasons_df["name"]==sname]["id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        mname = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist(), key="meet_sel_top")
        sel_meet_id = meets_df[meets_df["name"]==mname]["id"].values[0]

tab_in, tab_res, tab_mgmt = st.tabs(["📝 入力", "📊 成績", "👤 メンバー/設定"])

# -------- 入力 --------
with tab_in:
    st.subheader("半荘入力（誰でも）")
    if not seasons_df.empty and sel_season_id and sel_meet_id:
        names = players_df["display_name"].tolist()
        name_to_id = dict(zip(players_df["display_name"], players_df["id"]))

        colE, colS = st.columns(2)
        colW, colN = st.columns(2)
        east  = colE.selectbox("東", names, index=min(0, len(names)-1))
        south = colS.selectbox("南", names, index=min(1, len(names)-1))
        west  = colW.selectbox("西", names, index=min(2, len(names)-1))
        north = colN.selectbox("北", names, index=min(3, len(names)-1))
        picked_names = [east, south, west, north]

        if len(set(picked_names)) < 4:
            st.warning("同じ人が重複しています。4人とも別のメンバーを選んでください。")
        else:
            with st.form("hanchan_form"):
                finals_by_id = {}
                st.write("**最終点（100点単位推奨）**")
                p_e = points_input(east,  f"pt_{east}")
                p_s = points_input(south, f"pt_{south}")
                p_w = points_input(west,  f"pt_{west}")
                p_n = points_input(north, f"pt_{north}")

                finals_by_id[name_to_id[east]]  = p_e
                finals_by_id[name_to_id[south]] = p_s
                finals_by_id[name_to_id[west]]  = p_w
                finals_by_id[name_to_id[north]] = p_n

                st.write("**役満 / 焼き鳥（任意）**")
                cols = st.columns(4)
                yakuman = {}
                yakitori = {}
                for i, nm in enumerate(picked_names):
                    with cols[i]:
                        yakuman[nm]  = st.number_input(f"{nm} 役満回数", value=0, step=1, min_value=0, key=f"yaku_{nm}")
                        yakitori[nm] = st.checkbox(f"{nm} 焼き鳥", value=False, key=f"yakitori_{nm}")

                memo = st.text_input("メモ（任意）", value="")

                submitted = st.form_submit_button("精算を記録")
                if submitted:
                    # 着順は点棒降順
                    pids = [name_to_id[nm] for nm in picked_names]
                    ranks = rank_from_points(pids, finals_by_id)
                    base_pt, uma_pt, total_pt, rounded = settlement_for_room(room, finals_by_id, ranks)

                    hid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO hanchan(id, room_id, started_at, finished_at, memo, meet_id) VALUES (?,?,?,?,?,?)",
                        (hid, room_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), memo, sel_meet_id)
                    )
                    for nm in picked_names:
                        pid = name_to_id[nm]
                        con.execute(
                            """INSERT INTO results(id,hanchan_id,player_id,final_points,rank,yakuman_count,yakitori)
                               VALUES (?,?,?,?,?,?,?)""",
                            (str(uuid.uuid4()), hid, pid, int(rounded[pid]), int(ranks[pid]),
                             int(yakuman[nm]), int(1 if yakitori[nm] else 0))
                        )
                    con.commit()
                    st.success("半荘を登録しました！")
    else:
        st.info("『👤 メンバー/設定』でシーズンとミートを作成・選択してください。")

# -------- 成績 --------
with tab_res:
    st.subheader("成績 / 履歴")
    scope = st.radio("集計範囲", ["ミート（選択ミートのみ）", "シーズン（全ミート）", "全リーグ（すべて）"],
                     horizontal=True,
                     index=0 if sel_meet_id else (1 if sel_season_id else 2))
    if scope == "ミート（選択ミートのみ）" and sel_meet_id:
        hdf = df_hanchan_join(con, room_id, None, sel_meet_id)
    elif scope == "シーズン（全ミート）" and sel_season_id:
        hdf = df_hanchan_join(con, room_id, sel_season_id, None)
    else:
        hdf = df_hanchan_join(con, room_id, None, None)

    if hdf.empty:
        st.info("まだ成績がありません。")
    else:
        # 計算列（pt, 円）
        tgt = room["target_points"]
        rate = room["rate_per_1000"]
        # 素点pt
        hdf["素点pt"] = (hdf["final_points"] - tgt) / 1000.0
        # 順位ウマpt
        rank_to_uma = {1:room["uma1"], 2:room["uma2"], 3:room["uma3"], 4:room["uma4"]}
        hdf["ウマpt"] = hdf["rank"].map(rank_to_uma).astype(float)
        # 最終pt & 収支
        hdf["最終pt"] = hdf["素点pt"] + hdf["ウマpt"]
        hdf["収支(円)"] = (hdf["最終pt"] * rate).round(0)

        # 個人成績（最終pt合計でソート）＋左端に順位
        g = hdf.groupby("display_name")
        summary = pd.DataFrame({
            "回数": g["rank"].count(),
            "1位": g["rank"].apply(lambda s: (s==1).sum()),
            "2位": g["rank"].apply(lambda s: (s==2).sum()),
            "3位": g["rank"].apply(lambda s: (s==3).sum()),
            "4位": g["rank"].apply(lambda s: (s==4).sum()),
            "収支合計(円)": g["収支(円)"].sum().astype(int),
            "素点合計(千点)": g["素点pt"].sum().round(2),
            "平均素点(千点)": g["素点pt"].mean().round(2),
            "最終pt合計": g["最終pt"].sum().round(2),
            "平均最終pt": g["最終pt"].mean().round(2),
            "平均順位": g["rank"].mean().round(2),
            "役満(回)": g["yakuman_count"].sum().astype(int),
            "焼き鳥(回)": g["yakitori"].sum().astype(int),
        }).reset_index()

        # ランキングは「最終pt合計」降順で
        summary = summary.sort_values(["最終pt合計","収支合計(円)"], ascending=[False, False]).reset_index(drop=True)
        summary.insert(0, "順位", summary.index + 1)

        st.write("### 個人成績（累積）")
        st.dataframe(
            summary[["順位","display_name","回数","1位","2位","3位","4位",
                     "収支合計(円)","素点合計(千点)","平均素点(千点)",
                     "最終pt合計","平均最終pt","平均順位","役満(回)","焼き鳥(回)"]],
            use_container_width=True, height=420
        )

        st.write("### 半荘履歴（主要列）")
        disp = hdf.copy()
        disp["精算(円)"] = disp["収支(円)"].map(lambda x: f"{int(x):,}")
        disp["点棒(最終点)"] = disp["final_points"].map(lambda x: f"{int(x):,}")
        disp = disp.rename(columns={
            "season_name":"シーズン", "meet_name":"ミート",
            "display_name":"プレイヤー", "rank":"着順",
            "素点pt":"素点(千点)", "ウマpt":"ウマ(千点)"
        })
        st.dataframe(
            disp[["シーズン","ミート","プレイヤー","点棒(最終点)","素点(千点)","ウマ(千点)","着順","精算(円)"]],
            use_container_width=True, height=420
        )

        # 成績CSV
        st.download_button(
            "個人成績CSVをダウンロード",
            summary.to_csv(index=False).encode("utf-8-sig"),
            file_name="summary.csv",
            mime="text/csv"
        )

# -------- メンバー/設定 --------
with tab_mgmt:
    st.subheader("メンバー管理")
    existing = players_df["display_name"].tolist()
    candidate_pool = sorted(set(existing) | set(DEFAULT_MEMBERS))
    selected_candidates = st.multiselect(
        "候補に入れておくメンバー（未登録はボタンで一括追加）",
        options=candidate_pool,
        default=existing or DEFAULT_MEMBERS[:4]
    )
    col_add1, col_add2 = st.columns([2,1])
    with col_add1:
        new_name = st.text_input("新メンバー名（1人ずつ）", placeholder="例）Ami")
    with col_add2:
        if st.button("追加"):
            nm = (new_name or "").strip()
            if nm:
                ensure_players(con, room_id, [nm])
                st.success(f"追加しました：{nm}")
                st.rerun()
    if st.button("未登録の候補をまとめて登録"):
        ensure_players(con, room_id, selected_candidates)
        st.success("未登録メンバーを登録しました。")
        st.rerun()

    st.divider()
    st.subheader("シーズン")
    seasons_df = df_seasons(con, room_id)
    colA, colB = st.columns([2,1])
    with colA:
        st.dataframe(
            seasons_df.rename(columns={"name":"シーズン名","start_date":"開始日","end_date":"終了日"}),
            use_container_width=True, height=240
        )
    with colB:
        with st.form("season_form"):
            s_name = st.text_input("シーズン名", value=f"{date.today().year} 前期")
            s_start = st.date_input("開始日", value=date(date.today().year,1,1))
            s_end   = st.date_input("終了日", value=date(date.today().year,6,30))
            if st.form_submit_button("シーズン作成"):
                sid = str(uuid.uuid4())
                con.execute(
                    "INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at) VALUES (?,?,?,?,?,?);",
                    (sid, room_id, s_name, s_start.isoformat(), s_end.isoformat(), datetime.utcnow().isoformat())
                )
                con.commit()
                st.rerun()

    st.divider()
    st.subheader("ミート（開催）")
    if seasons_df.empty:
        st.info("先にシーズンを作成してください。")
    else:
        sel_season_name2 = st.selectbox("対象シーズン", seasons_df["name"].tolist(), key="season_sel_manage")
        sel_season_id2 = seasons_df[seasons_df["name"]==sel_season_name2]["id"].values[0]
        meets_df2 = df_meets(con, sel_season_id2)
        colM1, colM2 = st.columns([2,1])
        with colM1:
            st.dataframe(
                meets_df2.rename(columns={"name":"ミート名","meet_date":"開催日"}),
                use_container_width=True, height=240
            )
        with colM2:
            with st.form("meet_form"):
                m_name = st.text_input("ミート名", value="第1回")
                m_date = st.date_input("開催日", value=date.today())
                if st.form_submit_button("ミート作成"):
                    mid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO meets(id,season_id,name,meet_date,created_at) VALUES (?,?,?,?,?);",
                        (mid, sel_season_id2, m_name, m_date.isoformat(), datetime.utcnow().isoformat())
                    )
                    con.commit()
                    st.rerun()

            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_meet_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_meet_id = meets_df2[meets_df2["name"]==edit_meet_name]["id"].values[0]
                edit_meet_date = meets_df2[meets_df2["name"]==edit_meet_name]["meet_date"].values[0]

                with st.form("meet_edit_form"):
                    new_name = st.text_input("新しいミート名", value=edit_meet_name)
                    new_date = st.date_input("新しい開催日", value=date.fromisoformat(edit_meet_date))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?;",
                                    (new_name, new_date.isoformat(), edit_meet_id))
                        con.commit()
                        st.success("更新しました。")
                        st.rerun()

                with st.expander("⚠️ ミート削除（関連半荘・結果も削除）", expanded=False):
                    sure = st.checkbox("本当に削除する", key="meet_del_confirm")
                    if st.button("このミートを削除", disabled=not sure):
                        cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?;", (edit_meet_id,))
                        hids = [r[0] for r in cur.fetchall()]
                        if hids:
                            con.executemany("DELETE FROM results WHERE hanchan_id=?;", [(hid,) for hid in hids])
                            con.executemany("DELETE FROM hanchan WHERE id=?;", [(hid,) for hid in hids])
                        con.execute("DELETE FROM meets WHERE id=?;", (edit_meet_id,))
                        con.commit()
                        st.success("削除しました。")
                        st.rerun()

con.close()
