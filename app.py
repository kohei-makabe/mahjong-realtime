# app.py
# 麻雀リーグ 精算ツール（ルーム/シーズン/ミート、OKA pt/円、役満/焼き鳥、スマホ最適化）
# - 素点(＝ポイント)= (丸め後最終点 - 返し)/1000 + UMApt (+ OKApt[トップ])
# - 収支(円) = 素点 × レート (+ OKA円[トップ])
# - OKAはポイント用/円用/無効を切替。レポートはミート/シーズン/全リーグの切替表示。
# - 東南西北の選択、役満回数/焼き鳥(半荘単位)入力に対応。

import streamlit as st
import sqlite3, uuid
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional

# ------------------------- 基本UI -------------------------
st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)
st.markdown("""
<style>
/* モバイル寄りの余白・フォント */
button, .stButton>button { padding: .55rem .9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.03rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")
DEFAULT_MEMBERS = ["眞壁","内藤","森","浜野","傅田","須崎","中間","高田","内藤士"]

# ------------------------- DBユーティリティ -------------------------
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
      rounding TEXT NOT NULL,         -- none/round/floor/ceil
      oka_mode TEXT NOT NULL DEFAULT 'none',  -- none/pt/yen
      oka_pt REAL NOT NULL DEFAULT 0.0,
      oka_yen REAL NOT NULL DEFAULT 0.0
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
      FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE,
      FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE SET NULL
    );
    CREATE TABLE IF NOT EXISTS results(
      id TEXT PRIMARY KEY,
      hanchan_id TEXT NOT NULL,
      player_id TEXT NOT NULL,
      final_points INTEGER NOT NULL,
      rank INTEGER NOT NULL,
      points_pt REAL NOT NULL,          -- ポイント(=素点)：(最終点-返し)/1000 + UMA(+OKApt)
      net_cash REAL NOT NULL,           -- 収支(円)       ：points_pt * レート (+OKA円)
      yakuman_count INTEGER NOT NULL DEFAULT 0,
      yakitori INTEGER NOT NULL DEFAULT 0,  -- 0/1
      FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
      FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
      UNIQUE(hanchan_id, player_id)
    );
    """)
    # 既存DB進化（後方互換）
    for col in ["oka_mode","oka_pt","oka_yen"]:
        if not table_has_column(con, "rooms", col):
            con.execute(f"ALTER TABLE rooms ADD COLUMN {col} " +
                        ("TEXT NOT NULL DEFAULT 'none'" if col=="oka_mode" else "REAL NOT NULL DEFAULT 0.0"))
    for col in ["points_pt","yakuman_count","yakitori"]:
        if not table_has_column(con, "results", col):
            if col=="points_pt":
                con.execute("ALTER TABLE results ADD COLUMN points_pt REAL NOT NULL DEFAULT 0.0;")
            elif col=="yakuman_count":
                con.execute("ALTER TABLE results ADD COLUMN yakuman_count INTEGER NOT NULL DEFAULT 0;")
            else:
                con.execute("ALTER TABLE results ADD COLUMN yakitori INTEGER NOT NULL DEFAULT 0;")
    if not table_has_column(con, "hanchan", "meet_id"):
        con.execute("ALTER TABLE hanchan ADD COLUMN meet_id TEXT;")
    con.commit(); con.close()

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id,name,created_at FROM rooms ORDER BY datetime(created_at) DESC;", con
    )

def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?", (room_id,))
    row = cur.fetchone()
    if not row: return None
    cols = [d[0] for d in cur.description]
    d = {cols[i]: row[i] for i in range(len(cols))}
    # 数値キャスト
    for k in ["start_points","target_points"]: d[k] = int(d[k])
    for k in ["rate_per_1000","uma1","uma2","uma3","uma4","oka_pt","oka_yen"]:
        d[k] = float(d[k])
    d["rounding"] = str(d["rounding"])
    d["oka_mode"] = str(d["oka_mode"])
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
    SELECT h.id, h.room_id, h.meet_id, h.started_at, h.finished_at, h.memo,
           p.display_name, r.final_points, r.rank, r.points_pt, r.net_cash,
           r.yakuman_count, r.yakitori, r.player_id,
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
        q += " AND s.id=?"; params.append(season_id)
    if meet_id:
        q += " AND h.meet_id=?"; params.append(meet_id)
    q += " ORDER BY datetime(h.started_at) DESC, r.rank ASC;"
    return pd.read_sql_query(q, con, params=tuple(params))

# ------------------------- 計算ロジック -------------------------
def apply_rounding(points: int, mode: str) -> int:
    if mode == "none": return int(points)
    if mode == "floor": return (int(points)//100)*100
    if mode == "ceil":  return ((int(points)+99)//100)*100
    return int(round(int(points)/100.0)*100)  # 'round'

def settle_points_and_cash(room: dict, finals: Dict[str, int]):
    """
    finals = {player_id: raw_final_points}
      戻り値:
        points_pt: {pid: ポイント(千点)} = ((丸め後最終点-返し)/1000) + UMApt (+OKApt[トップ])
        cash_yen : {pid: 収支(円)}       = points_pt * レート (+OKA円[トップ])
        ranks    : {pid: 着順}
        rounded  : {pid: 丸め後最終点}
    """
    target = room["target_points"]
    rate   = room["rate_per_1000"]
    uma    = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    rd     = room["rounding"]

    oka_mode = room.get("oka_mode","none")
    oka_pt   = float(room.get("oka_pt",0.0) or 0.0)
    oka_yen  = float(room.get("oka_yen",0.0) or 0.0)

    rounded = {pid: apply_rounding(int(pts), rd) for pid, pts in finals.items()}
    order   = sorted(rounded.items(), key=lambda x: x[1], reverse=True)  # 高い順
    ranks   = {pid: i+1 for i,(pid,_) in enumerate(order)}

    base_pt = {pid: (rounded[pid]-target)/1000.0 for pid in rounded}
    points_pt = {pid: base_pt[pid] + uma[ranks[pid]-1] for pid in rounded}

    if oka_mode == "pt" and order:
        points_pt[order[0][0]] += oka_pt

    cash_yen = {pid: points_pt[pid]*rate for pid in rounded}
    if oka_mode == "yen" and order:
        cash_yen[order[0][0]] += oka_yen

    return points_pt, cash_yen, ranks, rounded

# ------------------------- 補助 -------------------------
def ensure_players(con, room_id: str, names: list[str]):
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed = False
    for nm in names:
        if nm and nm not in have:
            con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                        (str(uuid.uuid4()), room_id, nm, datetime.utcnow().isoformat()))
            changed = True
    if changed: con.commit()

def points_input(label: str, key: str, default: int=25000) -> int:
    return int(st.number_input(label, value=default, step=100, key=f"{key}_num"))

# ========================================================
#                     アプリ本体
# ========================================================
st.title("🀄 麻雀リーグ 精算ツール")
init_db()

# ------------------------- サイドバー：ルーム -------------------------
with st.sidebar:
    st.header("ルーム")
    action = st.radio("操作を選択", ["ルーム作成","ルーム参加"], horizontal=True)

    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")

        colL, colR = st.columns(2)
        with colL:
            start_points = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate_per_1000 = st.number_input("レート(円/千点)", value=100.0, step=10.0)
            rounding = st.selectbox("点数丸め", ["none","round","floor","ceil"], index=0)
        with colR:
            uma1 = st.number_input("ウマ1位(+千点)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ2位(+千点)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ3位(−千点)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ4位(−千点)", value=-10.0, step=1.0)

        st.markdown("### OKA（ポイント用・収支に直接は使いません）")
        c1,c2,c3 = st.columns([1.2,1,1])
        with c1:
            oka_mode = st.selectbox("OKAモード", ["none","pt","yen"], help="none:未使用 / pt:トップにOKA pt加点 / yen:トップにOKA円加算")
        with c2:
            oka_pt = st.number_input("OKA pt(トップ加算)", value=0.0, step=0.5, help="OKAモードがptのときだけ使用")
        with c3:
            oka_yen = st.number_input("OKA 円(参考)", value=0.0, step=100.0, help="OKAモードがyenのときだけ使用")

        creator = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            con = connect()
            rid = str(uuid.uuid4())
            con.execute("""
              INSERT INTO rooms(id,name,created_at,start_points,target_points,rate_per_1000,
                                uma1,uma2,uma3,uma4,rounding,oka_mode,oka_pt,oka_yen)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (rid, name, datetime.utcnow().isoformat(), start_points, target_points, rate_per_1000,
                  uma1, uma2, uma3, uma4, rounding, oka_mode, oka_pt, oka_yen))
            pid = str(uuid.uuid4())
            con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                        (pid, rid, creator, datetime.utcnow().isoformat()))
            con.commit(); con.close()
            st.session_state["room_id"] = rid
            st.session_state["player_id"] = pid
            st.success("作成OK！")

    else:
        con = connect()
        rooms_df = df_rooms(con)
        if rooms_df.empty:
            st.info("まだルームがありません。『ルーム作成』から作成してください。")
        else:
            def lab(r): 
                ts = r["created_at"][:10] + " " + r["created_at"][11:16]
                return f'{r["name"]}（{ts}）'
            idx = st.selectbox("参加するルームを選択", options=list(range(len(rooms_df))),
                               format_func=lambda i: lab(rooms_df.iloc[i]))
            sel_room_id = rooms_df.iloc[idx]["id"]
            st.caption(f"Room ID: `{sel_room_id}`")
            disp = st.text_input("あなたの表示名", value="あなた")
            if st.button("参加"):
                cur = con.execute("SELECT id FROM players WHERE room_id=? AND display_name=?",(sel_room_id, disp))
                row = cur.fetchone()
                if row: pid = row[0]
                else:
                    pid = str(uuid.uuid4())
                    con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                                (pid, sel_room_id, disp, datetime.utcnow().isoformat()))
                    con.commit()
                st.session_state["room_id"] = sel_room_id
                st.session_state["player_id"] = pid
                st.success("参加しました。"); st.rerun()
        con.close()

    st.divider()
    st.markdown("### 🗑️ ルーム削除")
    con = connect(); rooms_df2 = df_rooms(con)
    if rooms_df2.empty:
        st.caption("まだルームは存在しません。")
    else:
        idxd = st.selectbox("削除するルームを選択", options=list(range(len(rooms_df2))),
                            format_func=lambda i: rooms_df2.iloc[i]["name"]+"（"+rooms_df2.iloc[i]["created_at"][:16]+"）",
                            key="del_room")
        rid_del = rooms_df2.iloc[idxd]["id"]
        ok = st.checkbox("⚠️ 本当に削除する（すべてのシーズン・成績が失われます）")
        if st.button("ルーム削除実行", disabled=not ok):
            con.execute("DELETE FROM rooms WHERE id=?", (rid_del,))
            con.commit(); con.close()
            if st.session_state.get("room_id")==rid_del:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("削除しました。"); st.rerun()
    con.close()

st.caption("誰でも入力OK。シーズン→ミート→半荘で管理します。")

# ------------------------- ルーム未選択なら停止 -------------------------
if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。"); st.stop()

players_df = df_players(con, room_id)
st.write(f"**ルーム: {room['name']}**")
st.dataframe(players_df[["display_name","joined_at"]].rename(columns={"display_name":"プレイヤー","joined_at":"参加"}),
             use_container_width=True, height=220)

# 共通セレクタ
seasons_df = df_seasons(con, room_id)
sel_season_id = None; sel_meet_id = None
if not seasons_df.empty:
    sn = st.selectbox("集計対象シーズン", seasons_df["name"].tolist(), key="season_top")
    sel_season_id = seasons_df[seasons_df["name"]==sn]["id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        mn = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist(), key="meet_top")
        sel_meet_id = meets_df[meets_df["name"]==mn]["id"].values[0]

# ------------------------- タブ -------------------------
tab_input, tab_results, tab_manage = st.tabs(["📝 入力","📊 成績","👤 メンバー/設定"])

# =============== 入力 ===============
with tab_input:
    st.subheader("半荘入力（誰でも）")
    if not seasons_df.empty and sel_season_id and sel_meet_id:
        names = players_df["display_name"].tolist()
        idmap = dict(zip(players_df["display_name"], players_df["id"]))

        cE,cS = st.columns(2); cW,cN = st.columns(2)
        east  = cE.selectbox("東", names, index=min(0, len(names)-1))
        south = cS.selectbox("南", names, index=min(1, len(names)-1))
        west  = cW.selectbox("西", names, index=min(2, len(names)-1))
        north = cN.selectbox("北", names, index=min(3, len(names)-1))
        picked = [east,south,west,north]

        if len(set(picked))<4:
            st.warning("同じ人が重複しています。4人とも別のメンバーを選んでください。")
        else:
            with st.form("hc_form"):
                st.write("**最終点（100点単位推奨）**")
                pE = points_input(east,  f"pt_{east}",  default=room["start_points"])
                pS = points_input(south, f"pt_{south}", default=room["start_points"])
                pW = points_input(west,  f"pt_{west}",  default=room["start_points"])
                pN = points_input(north, f"pt_{north}", default=room["start_points"])

                st.write("**役満/焼き鳥**（役満は回数、焼き鳥はチェック）")
                ykm = {}
                ykr = {}
                cols = st.columns(4)
                for i,name in enumerate(picked):
                    with cols[i]:
                        ykm[name] = st.number_input(f"{name} 役満回", min_value=0, step=1, value=0, key=f"yakm_{name}")
                        ykr[name] = st.checkbox(f"{name} 焼き鳥", value=False, key=f"yaki_{name}")

                memo = st.text_input("メモ（任意）", value="")
                sub = st.form_submit_button("精算を記録")
                if sub:
                    finals = { idmap[east]:pE, idmap[south]:pS, idmap[west]:pW, idmap[north]:pN }
                    points_pt, cash_yen, ranks, rounded = settle_points_and_cash(room, finals)

                    hid = str(uuid.uuid4())
                    con.execute("INSERT INTO hanchan(id,room_id,meet_id,started_at,finished_at,memo) VALUES(?,?,?,?,?,?)",
                                (hid, room_id, sel_meet_id, datetime.utcnow().isoformat(),
                                 datetime.utcnow().isoformat(), memo))
                    for nm in picked:
                        pid = idmap[nm]
                        rid = str(uuid.uuid4())
                        con.execute("""
                          INSERT INTO results(id,hanchan_id,player_id,final_points,rank,points_pt,net_cash,yakuman_count,yakitori)
                          VALUES(?,?,?,?,?,?,?,?,?)
                        """,(rid, hid, pid, int(rounded[pid]), int(ranks[pid]),
                             float(points_pt[pid]), float(cash_yen[pid]),
                             int(ykm[nm]), int(1 if ykr[nm] else 0)))
                    con.commit()
                    st.success("半荘を登録しました！")
    else:
        st.info("まず『👤 メンバー/設定』でシーズンとミートを作成・選択してください。")

# =============== 成績 ===============
with tab_results:
    st.subheader("成績 / 履歴")
    scope = "ミート（選択ミートのみ）"
    r = st.radio("集計範囲", ["ミート（選択ミートのみ）","シーズン（全ミート）","全リーグ（すべて）"],
                 horizontal=True, index=0 if sel_meet_id else (1 if sel_season_id else 2))
    if r=="シーズン（全ミート）" or not sel_meet_id:
        # シーズン集計
        hdf = df_hanchan_join(con, room_id, season_id=sel_season_id, meet_id=None)
    elif r=="全リーグ（すべて）":
        hdf = df_hanchan_join(con, room_id, season_id=None, meet_id=None)
    else:
        hdf = df_hanchan_join(con, room_id, season_id=None, meet_id=sel_meet_id)

    if hdf.empty:
        st.info("まだ成績がありません。")
    else:
        # 個人集計
        g = hdf.groupby("display_name")
        summary = pd.DataFrame({
            "回数": g["rank"].count(),
            "1位": g["rank"].apply(lambda s: (s==1).sum()),
            "2位": g["rank"].apply(lambda s: (s==2).sum()),
            "3位": g["rank"].apply(lambda s: (s==3).sum()),
            "4位": g["rank"].apply(lambda s: (s==4).sum()),
            "収支合計(円)": g["net_cash"].sum().round(0),
            "素点合計(千点)": g["points_pt"].sum().round(2),
            "平均素点(千点)": g["points_pt"].mean().round(2),
            "平均順位": g["rank"].mean().round(2),
            "役満(回)": g["yakuman_count"].sum(),
            "焼き鳥(回)": g["yakitori"].sum()
        }).reset_index()

        # 並び替え（収支合計降順）＆左端に順位列
        summary = summary.sort_values(["収支合計(円)","素点合計(千点)"], ascending=[False,False]).reset_index(drop=True)
        summary.insert(0, "順位", summary.index+1)

        st.write("### 個人成績（累積）")
        st.dataframe(summary, use_container_width=True, height=380)

        st.write("### 半荘履歴（主要列）")
        disp = hdf.copy()
        disp["精算(円)"] = disp["net_cash"].map(lambda x: f"{x:,.0f}")
        disp["点棒(最終点)"] = disp["final_points"].map(lambda x: f"{x:,}")
        disp = disp.rename(columns={
            "season_name":"シーズン","meet_name":"ミート","display_name":"プレイヤー",
            "rank":"着順","points_pt":"素点(千点)","yakuman_count":"役満(回)","yakitori":"焼き鳥"
        })
        st.dataframe(
            disp[["シーズン","ミート","プレイヤー","点棒(最終点)","素点(千点)","着順","役満(回)","焼き鳥","精算(円)"]],
            use_container_width=True, height=430
        )

# =============== メンバー/設定 ===============
with tab_manage:
    st.subheader("メンバー管理")
    existing = players_df["display_name"].tolist()
    cand = sorted(set(existing) | set(DEFAULT_MEMBERS))
    selected = st.multiselect("候補に入れておくメンバー（未登録はボタンで一括追加できます）",
                              options=cand, default=existing or DEFAULT_MEMBERS[:4])
    cc1,cc2 = st.columns([2,1])
    with cc1:
        newname = st.text_input("新メンバー名（1人ずつ）", placeholder="例）Ami")
    with cc2:
        if st.button("追加"):
            if newname.strip():
                ensure_players(con, room_id, [newname.strip()])
                st.success(f"追加：{newname.strip()}"); st.rerun()
    if st.button("未登録の候補をまとめて登録"):
        ensure_players(con, room_id, selected); st.success("登録しました。"); st.rerun()

    st.divider()
    st.subheader("シーズン")
    seasons_df2 = df_seasons(con, room_id)
    csa, csb = st.columns([2,1])
    with csa:
        st.dataframe(seasons_df2.rename(columns={"name":"シーズン名","start_date":"開始日","end_date":"終了日"}),
                     use_container_width=True, height=250)
    with csb:
        with st.form("season_form"):
            sname = st.text_input("シーズン名", value=f"{date.today().year} 前期")
            sstart = st.date_input("開始日", value=date(date.today().year,1,1))
            send   = st.date_input("終了日", value=date(date.today().year,6,30))
            if st.form_submit_button("シーズン作成"):
                sid = str(uuid.uuid4())
                con.execute("""INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at)
                               VALUES(?,?,?,?,?,?)""",
                            (sid, room_id, sname, sstart.isoformat(), send.isoformat(), datetime.utcnow().isoformat()))
                con.commit(); st.rerun()

    st.divider()
    st.subheader("ミート（開催）")
    if seasons_df2.empty:
        st.info("先にシーズンを作成してください。")
    else:
        sname2 = st.selectbox("対象シーズン", seasons_df2["name"].tolist(), key="season_manage")
        sid2 = seasons_df2[seasons_df2["name"]==sname2]["id"].values[0]
        meets_df2 = df_meets(con, sid2)
        cm1, cm2 = st.columns([2,1])
        with cm1:
            st.dataframe(meets_df2.rename(columns={"name":"ミート名","meet_date":"開催日"}),
                         use_container_width=True, height=250)
        with cm2:
            with st.form("meet_add"):
                mname = st.text_input("ミート名", value="第1回")
                mdate = st.date_input("開催日", value=date.today())
                if st.form_submit_button("ミート作成"):
                    mid = str(uuid.uuid4())
                    con.execute("""INSERT INTO meets(id,season_id,name,meet_date,created_at)
                                   VALUES(?,?,?,?,?)""",
                                (mid, sid2, mname, mdate.isoformat(), datetime.utcnow().isoformat()))
                    con.commit(); st.rerun()

            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_id   = meets_df2[meets_df2["name"]==edit_name]["id"].values[0]
                edit_date = meets_df2[meets_df2["name"]==edit_name]["meet_date"].values[0]
                with st.form("meet_edit_form"):
                    new_n = st.text_input("新しいミート名", value=edit_name)
                    new_d = st.date_input("新しい開催日", value=date.fromisoformat(edit_date))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?;",
                                    (new_n, new_d.isoformat(), edit_id))
                        con.commit(); st.success("更新しました。"); st.rerun()
                with st.expander("⚠️ ミート削除（関連半荘・結果も削除）", expanded=False):
                    sure = st.checkbox("本当に削除する")
                    if st.button("このミートを削除", disabled=not sure):
                        cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?",(edit_id,))
                        hids = [r[0] for r in cur.fetchall()]
                        if hids:
                            con.executemany("DELETE FROM results WHERE hanchan_id=?", [(h,) for h in hids])
                            con.executemany("DELETE FROM hanchan WHERE id=?", [(h,) for h in hids])
                        con.execute("DELETE FROM meets WHERE id=?", (edit_id,))
                        con.commit(); st.success("削除しました。"); st.rerun()

st.caption("式: ポイント(千点)=((最終点-返し)/1000)+UMA(+OKApt)。収支(円)=ポイント×レート(+OKA円)。丸めは最終点に適用。")

con.close()
