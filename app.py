# app.py
# 麻雀リーグ 精算ツール（スマホ最適化）
# - 期(シーズン)→開催(ミート)→半荘
# - 返しが25000でも30000でも「常にウマ適用」
# - ポイント = (最終点-返し)/1000 + ウマ + OKA_pt(トップのみ)
# - 収支(円)   = ポイント × レート
# - 役満回数、焼き鳥を半荘ごとに保存し、集計表示
# - ルーム作成/参加、ルーム削除、ミート修正/削除
# - rerun安全化（safe_rerun）

import streamlit as st
import sqlite3, uuid
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional

st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ---- 軽量モバイルCSS
st.markdown("""
<style>
button, .stButton>button { padding: 0.55rem 0.9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.05rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")

DEFAULT_MEMBERS = ["眞壁","内藤","森","浜野","傅田","須崎","中間","高田","内藤士"]

# ---------------- rerun 互換ヘルパー ----------------
def safe_rerun():
    """Streamlitのバージョンに応じて安全にrerun。決して自分を呼ばない。"""
    try:
        if getattr(st, "rerun", None):
            st.rerun()
        else:
            st.experimental_rerun()
    except RecursionError:
        # もし何かの事情で再帰が起きたら、rerunを諦めて復帰
        pass

# ---------------- DB Utils ----------------
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
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS rooms(
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TEXT NOT NULL,
        start_points INTEGER NOT NULL,
        target_points INTEGER NOT NULL,
        rate_per_1000 REAL NOT NULL,
        uma1 REAL NOT NULL,
        uma2 REAL NOT NULL,
        uma3 REAL NOT NULL,
        uma4 REAL NOT NULL,
        rounding TEXT NOT NULL,
        oka_pt REAL NOT NULL   -- OKAはポイント付与のみ（トップに加点）
    );
    CREATE TABLE IF NOT EXISTS players(
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        display_name TEXT NOT NULL,
        joined_at TEXT NOT NULL,
        UNIQUE(room_id, display_name),
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS seasons(
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        name TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS meets(
        id TEXT PRIMARY KEY,
        season_id TEXT NOT NULL,
        name TEXT NOT NULL,
        meet_date TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(season_id) REFERENCES seasons(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS hanchan(
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        meet_id TEXT,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        memo TEXT,
        yakitori_json TEXT,    -- {"player_id": true/false, ...}
        yakuman_json TEXT,     -- {"player_id": 0/1/2,... 回数}
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE,
        FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS results(
        id TEXT PRIMARY KEY,
        hanchan_id TEXT NOT NULL,
        player_id TEXT NOT NULL,
        final_points INTEGER NOT NULL,  -- 最終点(点棒)
        rank INTEGER NOT NULL,
        total_pt REAL NOT NULL,         -- (点棒-返し)/1000 + ウマ + OKA(トップのみ)
        cash_yen REAL NOT NULL,         -- total_pt × レート
        FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
        FOREIGN KEY(player_id)  REFERENCES players(id)  ON DELETE CASCADE,
        UNIQUE(hanchan_id, player_id)
    );
    """)
    con.commit()
    con.close()

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id,name,created_at FROM rooms ORDER BY datetime(created_at) DESC;", con
    )

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
    for k in ["rate_per_1000","uma1","uma2","uma3","uma4","oka_pt"]:
        d[k] = float(d[k])
    return d

def df_players(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM players WHERE room_id=? ORDER BY joined_at;", con, params=(room_id,)
    )

def df_seasons(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM seasons WHERE room_id=? ORDER BY start_date;", con, params=(room_id,)
    )

def df_meets(con, season_id):
    return pd.read_sql_query(
        "SELECT * FROM meets WHERE season_id=? ORDER BY meet_date;", con, params=(season_id,)
    )

def df_hanchan_join(con, room_id, season_id: Optional[str]=None, meet_id: Optional[str]=None):
    q = """
    SELECT  h.id, h.room_id, h.meet_id, h.started_at, h.memo,
            p.display_name, r.final_points, r.rank, r.total_pt, r.cash_yen, r.player_id,
            m.name as meet_name, m.meet_date, s.name as season_name
    FROM hanchan h
      JOIN results r ON r.hanchan_id = h.id
      JOIN players p ON p.id = r.player_id
      LEFT JOIN meets m   ON m.id = h.meet_id
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

def ensure_players(con, room_id: str, names: list[str]):
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed = False
    for name in names:
        nm = (name or "").strip()
        if nm and nm not in have:
            con.execute(
                "INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                (str(uuid.uuid4()), room_id, nm, datetime.utcnow().isoformat())
            )
            changed = True
    if changed:
        con.commit()

# ---------------- 計算ロジック ----------------
def apply_rounding(points: int, mode: str) -> int:
    if mode == "none":  return int(points)
    if mode == "floor": return (points // 100) * 100
    if mode == "ceil":  return ((points + 99) // 100) * 100
    return int(round(points / 100.0) * 100)  # 'round'

def settle_room(room: dict, finals: Dict[str, int]):
    """
    finals: {player_id: 最終点(点棒)}
    戻り: total_pt(dict), cash(dict), ranks(dict), rounded_finals(dict)
    """
    target = room["target_points"]
    rate   = room["rate_per_1000"]
    uma    = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    oka_pt = room["oka_pt"]
    rounding = room["rounding"]

    # 100点丸め後の最終点
    items = [(pid, apply_rounding(pts, rounding)) for pid, pts in finals.items()]
    # 着順（点棒の降順）
    items.sort(key=lambda x: x[1], reverse=True)
    ranks = {pid: i+1 for i, (pid, _) in enumerate(items)}

    # ポイント計算（常にウマ適用、返しはtargetで固定、OKAはトップのみに加点）
    total_pt = {}
    for pid, pts in items:
        base = (pts - target) / 1000.0
        pt = base + uma[ranks[pid]-1]
        total_pt[pid] = pt
    # OKA pt（pt加点）トップに付与
    top_pid = items[0][0]
    total_pt[top_pid] += oka_pt

    # 収支（円）
    cash = {pid: total_pt[pid] * rate for pid, _ in items}

    return total_pt, cash, ranks, dict(items)

# ---------------- 画面本体 ----------------
st.title("🀄 麻雀リーグ 精算ツール")
init_db()

# ---- Sidebar: ルーム作成/参加/削除
with st.sidebar:
    st.header("ルーム")
    action = st.radio("操作を選択", ["ルーム作成", "ルーム参加"], horizontal=True)

    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1, col2 = st.columns(2)
        with col1:
            start_points  = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate_per_1000 = st.number_input("レート(円/千点)", value=100.0, step=10.0, format="%.2f")
        with col2:
            uma1 = st.number_input("ウマ1位(+千点)", value=10.0, step=0.5, format="%.2f")
            uma2 = st.number_input("ウマ2位(+千点)", value=5.0,  step=0.5, format="%.2f")
            uma3 = st.number_input("ウマ3位(−千点)", value=-5.0, step=0.5, format="%.2f")
            uma4 = st.number_input("ウマ4位(−千点)", value=-10.0,step=0.5, format="%.2f")
        rounding = st.selectbox("点数丸め（点棒）", ["none","round","floor","ceil"], index=0)
        oka_pt   = st.number_input("OKA pt（トップ加点/pt）", value=0.0, step=0.5, format="%.2f")
        creator  = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            rid = str(uuid.uuid4())
            con = connect()
            con.execute("""
              INSERT INTO rooms(id,name,created_at,start_points,target_points,rate_per_1000,
                                uma1,uma2,uma3,uma4,rounding,oka_pt)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """, (rid, name, datetime.utcnow().isoformat(),
                  start_points, target_points, rate_per_1000,
                  uma1, uma2, uma3, uma4, rounding, oka_pt))
            pid = str(uuid.uuid4())
            con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                        (pid, rid, creator, datetime.utcnow().isoformat()))
            con.commit(); con.close()
            st.session_state["room_id"]  = rid
            st.session_state["player_id"] = pid
            st.success("ルーム作成OK！")
            safe_rerun()

    else:
        con = connect()
        rooms_df = df_rooms(con)
        if rooms_df.empty:
            st.info("まだルームがありません。『ルーム作成』から作成してください。")
        else:
            def fmt(r):
                ts = r["created_at"].split("T")[0] + " " + r["created_at"][11:16]
                return f'{r["name"]}（{ts}）'
            labels = [fmt(r) for _, r in rooms_df.iterrows()]
            idx = st.selectbox("参加するルームを選択", options=list(range(len(labels))),
                               format_func=lambda i: labels[i])
            selected_room_id = rooms_df.iloc[idx]["id"]
            st.caption(f"Room ID: `{selected_room_id}`")
            disp = st.text_input("あなたの表示名", value="あなた")
            if st.button("参加"):
                cur = con.execute(
                    "SELECT id FROM players WHERE room_id=? AND display_name=?",
                    (selected_room_id, disp)
                )
                row = cur.fetchone()
                if row: pid = row[0]
                else:
                    pid = str(uuid.uuid4())
                    con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                                (pid, selected_room_id, disp, datetime.utcnow().isoformat()))
                    con.commit()
                st.session_state["room_id"] = selected_room_id
                st.session_state["player_id"] = pid
                st.success("参加しました！")
                con.close()
                safe_rerun()
        con.close()

    st.divider()
    st.markdown("### 🗑️ ルーム削除")
    con = connect()
    rooms_df2 = df_rooms(con)
    if rooms_df2.empty:
        st.caption("まだルームは存在しません。")
    else:
        def fmt2(r):
            ts = r["created_at"].split("T")[0] + " " + r["created_at"][11:16]
            return f'{r["name"]}（{ts}）'
        labels_del = [fmt2(r) for _, r in rooms_df2.iterrows()]
        idx_del = st.selectbox("削除するルームを選択", options=list(range(len(labels_del))),
                               format_func=lambda i: labels_del[i], key="del_room")
        selected_room_id_del = rooms_df2.iloc[idx_del]["id"]
        confirm = st.checkbox("⚠️ 本当に削除する（すべてのデータが失われます）")
        if st.button("ルーム削除実行", disabled=not confirm):
            con.execute("DELETE FROM rooms WHERE id=?", (selected_room_id_del,))
            con.commit(); con.close()
            if st.session_state.get("room_id") == selected_room_id_del:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("ルームを削除しました。")
            safe_rerun()
    con.close()

st.caption("ポイント = (最終点 − 返し)/1000 + ウマ + OKA(トップのみ). 収支(円) = ポイント × レート。")

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
st.dataframe(players_df[["display_name","joined_at"]].rename(
    columns={"display_name":"プレイヤー", "joined_at":"参加"}), use_container_width=True, height=220)

# ---- シーズン/ミート選択
seasons_df = df_seasons(con, room_id)
sel_season_id = None
sel_meet_id = None

if not seasons_df.empty:
    s_name = st.selectbox("集計対象シーズン", seasons_df["name"].tolist(), key="season_top")
    sel_season_id = seasons_df[seasons_df["name"]==s_name]["id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        m_name = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist(), key="meet_top")
        sel_meet_id = meets_df[meets_df["name"]==m_name]["id"].values[0]

# ---- タブ
tab_input, tab_results, tab_manage = st.tabs(["📝 入力","📊 成績","👤 メンバー/設定"])

# ============ 入力タブ ============
with tab_input:
    st.subheader("半荘入力")
    if seasons_df.empty or not sel_season_id:
        st.info("まず『👤 メンバー/設定 → シーズン/ミート』で作成してください。")
    elif sel_meet_id is None:
        st.info("ミートを選択してください。")
    else:
        names = players_df["display_name"].tolist()
        name_to_id = dict(zip(players_df["display_name"], players_df["id"]))

        c1,c2 = st.columns(2); c3,c4 = st.columns(2)
        east  = c1.selectbox("東", names, index=0 if len(names)>0 else None)
        south = c2.selectbox("南", names, index=1 if len(names)>1 else None)
        west  = c3.selectbox("西", names, index=2 if len(names)>2 else None)
        north = c4.selectbox("北", names, index=3 if len(names)>3 else None)
        picked = [east,south,west,north]
        if len(set(picked)) < 4:
            st.warning("同じ人が重複しています。4人とも別のメンバーを選んでください。")
        else:
            with st.form("hc_form"):
                st.write("**最終点（点棒）**")
                p_e = int(st.number_input(east,  value=35000, step=100, key="pt_e"))
                p_s = int(st.number_input(south, value=26000, step=100, key="pt_s"))
                p_w = int(st.number_input(west,  value=24000, step=100, key="pt_w"))
                p_n = int(st.number_input(north, value=15000, step=100, key="pt_n"))

                memo = st.text_input("メモ（任意）", value="")

                st.write("**オプション**")
                # 焼き鳥（各人ON/OFF）
                yt_cols = st.columns(4)
                yakitori = {
                    name_to_id[east]:  yt_cols[0].checkbox(f"焼き鳥: {east}",  value=False),
                    name_to_id[south]: yt_cols[1].checkbox(f"焼き鳥: {south}", value=False),
                    name_to_id[west]:  yt_cols[2].checkbox(f"焼き鳥: {west}",  value=False),
                    name_to_id[north]: yt_cols[3].checkbox(f"焼き鳥: {north}", value=False),
                }
                # 役満回数（各人0〜）
                yk_cols = st.columns(4)
                yakuman = {
                    name_to_id[east]:  int(yk_cols[0].number_input(f"役満回数: {east}", 0, step=1)),
                    name_to_id[south]: int(yk_cols[1].number_input(f"役満回数: {south}",0, step=1)),
                    name_to_id[west]:  int(yk_cols[2].number_input(f"役満回数: {west}", 0, step=1)),
                    name_to_id[north]: int(yk_cols[3].number_input(f"役満回数: {north}",0, step=1)),
                }

                submitted = st.form_submit_button("精算を記録")
                if submitted:
                    finals = {
                        name_to_id[east]:  p_e,
                        name_to_id[south]: p_s,
                        name_to_id[west]:  p_w,
                        name_to_id[north]: p_n,
                    }
                    total_pt, cash, ranks, rounded_finals = settle_room(room, finals)
                    hid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO hanchan(id,room_id,meet_id,started_at,finished_at,memo,yakitori_json,yakuman_json) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (hid, room_id, sel_meet_id, datetime.utcnow().isoformat(),
                         datetime.utcnow().isoformat(), memo,
                         str(yakitori), str(yakuman))
                    )
                    for name in picked:
                        pid = name_to_id[name]
                        con.execute(
                            "INSERT INTO results(id,hanchan_id,player_id,final_points,rank,total_pt,cash_yen) "
                            "VALUES(?,?,?,?,?,?,?)",
                            (str(uuid.uuid4()), hid, pid,
                             int(rounded_finals[pid]), int(ranks[pid]),
                             float(total_pt[pid]), float(cash[pid]))
                        )
                    con.commit()
                    st.success("半荘を登録しました！")

# ============ 成績タブ ============
with tab_results:
    st.subheader("成績 / 履歴")

    scope = "ミート（選択のみ）"
    if sel_season_id:
        scope = st.radio("集計範囲", ["ミート（選択のみ）","シーズン（全ミート）","全リーグ（すべて）"], horizontal=True,
                         index=0 if sel_meet_id else 1)
    use_season = scope != "ミート（選択のみ）"
    use_all    = scope == "全リーグ（すべて）"

    hdf = df_hanchan_join(con, room_id,
                          None if use_all else (sel_season_id if use_season else None),
                          None if use_season or use_all else sel_meet_id)

    if hdf.empty:
        st.info("まだ成績がありません。")
    else:
        g = hdf.groupby("display_name")
        summary = pd.DataFrame({
            "回数": g["rank"].count(),
            "1位":  g["rank"].apply(lambda s: (s==1).sum()),
            "2位":  g["rank"].apply(lambda s: (s==2).sum()),
            "3位":  g["rank"].apply(lambda s: (s==3).sum()),
            "4位":  g["rank"].apply(lambda s: (s==4).sum()),
            "収支合計(円)": g["cash_yen"].sum().round(0),
            "Pt合計":       g["total_pt"].sum().round(2),
            "平均Pt":       g["total_pt"].mean().round(2),
            "平均順位":     g["rank"].mean().round(2),
        }).reset_index().sort_values(["Pt合計","収支合計(円)"], ascending=[False,False])

        # ランキング列（左端を順位表示）
        summary.insert(0, "順位", range(1, len(summary)+1))
        st.write("### 個人成績（累積）")
        st.dataframe(summary, use_container_width=True, height=380)

        st.write("### 半荘履歴（主要列）")
        disp = hdf.copy().rename(columns={
            "season_name":"シーズン", "meet_name":"ミート",
            "display_name":"プレイヤー", "rank":"着順",
            "final_points":"最終点(点)", "total_pt":"Pt", "cash_yen":"収支(円)"
        })
        disp["収支(円)"] = disp["収支(円)"].map(lambda x: f"{x:,.0f}")
        st.dataframe(disp[["シーズン","ミート","プレイヤー","最終点(点)","Pt","着順","収支(円)"]],
                     use_container_width=True, height=420)

# ============ メンバー/設定タブ ============
with tab_manage:
    st.subheader("メンバー")
    existing = players_df["display_name"].tolist()
    pool = sorted(set(existing) | set(DEFAULT_MEMBERS))
    selected = st.multiselect("候補メンバー（未登録はボタンで一括追加）", pool, default=existing or DEFAULT_MEMBERS[:4])
    cA,cB = st.columns([2,1])
    with cA:
        new_nm = st.text_input("新メンバー名（1人ずつ）")
    with cB:
        if st.button("追加"):
            if new_nm.strip():
                ensure_players(con, room_id, [new_nm.strip()])
                st.success(f"追加: {new_nm.strip()}")
                safe_rerun()
    if st.button("未登録の候補をまとめて登録"):
        ensure_players(con, room_id, selected)
        st.success("未登録メンバーを登録しました。")
        safe_rerun()

    st.divider()
    st.subheader("シーズン")
    seasons_df = df_seasons(con, room_id)
    c1,c2 = st.columns([2,1])
    with c1:
        st.dataframe(seasons_df.rename(columns={"name":"シーズン名","start_date":"開始日","end_date":"終了日"}),
                     use_container_width=True, height=240)
    with c2:
        with st.form("season_form"):
            s_name = st.text_input("シーズン名", value=f"{date.today().year} 前期")
            s_start = st.date_input("開始日", value=date(date.today().year,1,1))
            s_end   = st.date_input("終了日", value=date(date.today().year,6,30))
            if st.form_submit_button("シーズン作成"):
                sid = str(uuid.uuid4())
                con.execute(
                    "INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at) VALUES(?,?,?,?,?,?)",
                    (sid, room_id, s_name, s_start.isoformat(), s_end.isoformat(), datetime.utcnow().isoformat())
                )
                con.commit()
                st.success("シーズン作成OK")
                safe_rerun()

    st.divider()
    st.subheader("ミート（開催）")
    if seasons_df.empty:
        st.info("先にシーズンを作成してください。")
    else:
        sel_s2 = st.selectbox("対象シーズン", seasons_df["name"].tolist(), key="season_manage")
        sel_sid2 = seasons_df[seasons_df["name"]==sel_s2]["id"].values[0]
        meets_df2 = df_meets(con, sel_sid2)
        m1,m2 = st.columns([2,1])
        with m1:
            st.dataframe(meets_df2.rename(columns={"name":"ミート名","meet_date":"開催日"}),
                         use_container_width=True, height=240)
        with m2:
            with st.form("meet_form"):
                m_name = st.text_input("ミート名", value="第1回")
                m_date = st.date_input("開催日", value=date.today())
                if st.form_submit_button("ミート作成"):
                    mid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO meets(id,season_id,name,meet_date,created_at) VALUES(?,?,?,?,?)",
                        (mid, sel_sid2, m_name, m_date.isoformat(), datetime.utcnow().isoformat())
                    )
                    con.commit()
                    st.success("ミート作成OK")
                    safe_rerun()

            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_nm = st.selectbox("編集対象", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_id = meets_df2[meets_df2["name"]==edit_nm]["id"].values[0]
                edit_dt = meets_df2[meets_df2["name"]==edit_nm]["meet_date"].values[0]
                with st.form("meet_edit_form"):
                    new_nm = st.text_input("新しいミート名", value=edit_nm)
                    new_dt = st.date_input("新しい開催日", value=date.fromisoformat(edit_dt))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?",
                                    (new_nm, new_dt.isoformat(), edit_id))
                        con.commit()
                        st.success("更新しました")
                        safe_rerun()

                with st.expander("⚠️ ミート削除（関連半荘・結果も削除）"):
                    ok = st.checkbox("本当に削除する")
                    if st.button("このミートを削除", disabled=not ok):
                        cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?", (edit_id,))
                        hids = [r[0] for r in cur.fetchall()]
                        if hids:
                            con.executemany("DELETE FROM results WHERE hanchan_id=?", [(hid,) for hid in hids])
                            con.executemany("DELETE FROM hanchan WHERE id=?", [(hid,) for hid in hids])
                        con.execute("DELETE FROM meets WHERE id=?", (edit_id,))
                        con.commit()
                        st.success("削除しました")
                        safe_rerun()

con.close()
