# app.py
# 麻雀リーグ 精算ツール（スマホ最適・シーズン/ミート・UMA常時適用・Pt集計・役満/焼き鳥）
import streamlit as st
import sqlite3, uuid
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Optional

# ---------------- UI基本 ----------------
st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)
st.markdown("""
<style>
/* モバイル操作しやすく */
button, .stButton>button { padding: .6rem .9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.02rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")
DEFAULT_MEMBERS = ["眞壁", "内藤", "森", "浜野", "傅田", "須崎", "中間", "高田", "内藤士"]

# ---------------- DBユーティリティ ----------------
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
    # ベーススキーマ
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS rooms(
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      created_at TEXT NOT NULL,
      start_points INTEGER NOT NULL,
      target_points INTEGER NOT NULL,
      rate_per_1000 REAL NOT NULL,
      uma1 REAL NOT NULL, uma2 REAL NOT NULL, uma3 REAL NOT NULL, uma4 REAL NOT NULL,
      rounding TEXT NOT NULL
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
      started_at TEXT NOT NULL,
      finished_at TEXT,
      memo TEXT,
      meet_id TEXT,
      FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE,
      FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS results(
      id TEXT PRIMARY KEY,
      hanchan_id TEXT NOT NULL,
      player_id TEXT NOT NULL,
      final_points INTEGER NOT NULL,
      rank INTEGER NOT NULL,
      net_cash REAL NOT NULL,
      league_pt REAL DEFAULT 0,
      yakuman_cnt INTEGER DEFAULT 0,
      yakitori INTEGER DEFAULT 0,
      FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
      FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
      UNIQUE(hanchan_id, player_id)
    );
    """)
    # 追加列の後方互換
    if not table_has_column(con, "rooms", "oka_pt"):
        con.execute("ALTER TABLE rooms ADD COLUMN oka_pt REAL DEFAULT 0;")
    for col in ("league_pt","yakuman_cnt","yakitori"):
        if not table_has_column(con, "results", col):
            default = "0" if col != "league_pt" else "0"
            con.execute(f"ALTER TABLE results ADD COLUMN {col} REAL DEFAULT {default};")
    if not table_has_column(con, "hanchan", "meet_id"):
        con.execute("ALTER TABLE hanchan ADD COLUMN meet_id TEXT;")
    con.commit(); con.close()

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id,name,created_at FROM rooms ORDER BY datetime(created_at) DESC;", con
    )

def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?;", (room_id,))
    row = cur.fetchone()
    if not row: return None
    cols = [d[0] for d in cur.description]
    d = {cols[i]: row[i] for i in range(len(cols))}
    # 型整備
    for k in ["start_points","target_points"]: d[k] = int(d[k])
    for k in ["rate_per_1000","uma1","uma2","uma3","uma4","oka_pt"]: d[k] = float(d[k])
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
             p.display_name, r.final_points, r.rank, r.net_cash, r.league_pt, r.player_id,
             r.yakuman_cnt, r.yakitori,
             m.name AS meet_name, m.meet_date, s.name AS season_name
      FROM hanchan h
      JOIN results r ON r.hanchan_id=h.id
      JOIN players p ON p.id=r.player_id
      LEFT JOIN meets m ON m.id=h.meet_id
      LEFT JOIN seasons s ON s.id=m.season_id
      WHERE h.room_id=?
    """
    params=[room_id]
    if season_id:
        q += " AND s.id=?"; params.append(season_id)
    if meet_id:
        q += " AND h.meet_id=?"; params.append(meet_id)
    q += " ORDER BY h.started_at DESC, r.rank ASC;"
    return pd.read_sql_query(q, con, params=tuple(params))

def ensure_players(con, room_id: str, names: list[str]) -> None:
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed=False
    for nm in names:
        if nm and nm not in have:
            con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                        (str(uuid.uuid4()), room_id, nm, datetime.utcnow().isoformat()))
            changed=True
    if changed: con.commit()

def apply_rounding(points: int, mode: str) -> int:
    if mode == "none": return int(points)
    if mode == "floor": return (points//100)*100
    if mode == "ceil":  return ((points+99)//100)*100
    return int(round(points/100.0)*100)  # round

# ---------------- 精算（UMA常時・Pt中心） ----------------
def settlement_for_room(room: dict, finals: Dict[str,int]):
    """
    finals: {player_id: 最終点}
    league_pt = (final - target)/1000 + UMA(rank) + (topのみ OKApt)
    cash(円)   = league_pt * rate
    """
    target = int(room["target_points"])
    rate   = float(room["rate_per_1000"])
    uma    = [float(room["uma1"]), float(room["uma2"]), float(room["uma3"]), float(room["uma4"])]
    rounding = room["rounding"]
    oka_pt = float(room.get("oka_pt", 0.0))

    items = [(pid, apply_rounding(pts, rounding)) for pid,pts in finals.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks = {pid: i+1 for i,(pid,_) in enumerate(items)}

    league = {}
    for pid, pts in items:
        soten = (pts - target)/1000.0
        league[pid] = soten + uma[ranks[pid]-1]
    if oka_pt:
        top_pid = items[0][0]
        league[top_pid] += oka_pt

    cash = {pid: league[pid]*rate for pid,_ in items}
    rounded = dict(items)
    return league, ranks, rounded, cash

# ---------------- 画面：サイドバー（ルーム作成/参加/削除） ----------------
st.title("🀄 麻雀リーグ 精算ツール")
init_db()

with st.sidebar:
    st.header("ルーム")
    mode = st.radio("操作を選択", ["ルーム作成","ルーム参加"], horizontal=True)

    if mode=="ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1,col2 = st.columns(2)
        with col1:
            start_points = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate = st.number_input("レート(円/千点)", value=100.0, step=10.0)
        with col2:
            uma1 = st.number_input("ウマ 1位(+千点)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ 2位(+千点)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ 3位(−千点)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ 4位(−千点)", value=-10.0, step=1.0)
        rounding = st.selectbox("点数丸め", ["none","round","floor","ceil"], index=0)
        oka_pt = st.number_input("OKA pt（トップ加点/pt）", value=0.0, step=0.5, help="Ptに加点。収支はPt×レートで計算。")
        creator = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            rid = str(uuid.uuid4())
            con = connect()
            con.execute("""INSERT INTO rooms(id,name,created_at,start_points,target_points,rate_per_1000,
                           uma1,uma2,uma3,uma4,rounding,oka_pt)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (rid, name, datetime.utcnow().isoformat(),
                         start_points, target_points, rate,
                         uma1, uma2, uma3, uma4, rounding, oka_pt))
            pid = str(uuid.uuid4())
            con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                        (pid, rid, creator, datetime.utcnow().isoformat()))
            con.commit(); con.close()
            st.session_state["room_id"] = rid
            st.session_state["player_id"] = pid
            st.success("作成OK！")
    else:
        con = connect()
        df = df_rooms(con)
        if df.empty:
            st.info("まだルームがありません。『ルーム作成』からどうぞ。")
        else:
            def fmt(r):
                ts = r["created_at"].replace("T"," ")[:16]
                return f'{r["name"]}（{ts}）'
            idx = st.selectbox("参加するルームを選択", options=list(range(len(df))),
                               format_func=lambda i: fmt(df.iloc[i]))
            join_id = df.iloc[idx]["id"]
            st.caption(f"Room ID: `{join_id}`")
            disp = st.text_input("あなたの表示名", value="あなた")
            if st.button("参加"):
                cur = con.execute("SELECT id FROM players WHERE room_id=? AND display_name=?",
                                  (join_id, disp))
                r = cur.fetchone()
                if r: pid = r[0]
                else:
                    pid=str(uuid.uuid4())
                    con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES(?,?,?,?)",
                                (pid, join_id, disp, datetime.utcnow().isoformat()))
                    con.commit()
                st.session_state["room_id"]=join_id
                st.session_state["player_id"]=pid
                st.success("参加しました！"); st.rerun()
        con.close()

    # ルーム削除
    st.divider(); st.markdown("### 🗑️ ルーム削除")
    con = connect(); df2 = df_rooms(con)
    if df2.empty:
        st.caption("削除対象なし。")
    else:
        def fmt2(r):
            ts = r["created_at"].replace("T"," ")[:16]
            return f'{r["name"]}（{ts}）'
        idx2 = st.selectbox("削除するルーム", options=list(range(len(df2))),
                            format_func=lambda i: fmt2(df2.iloc[i]), key="delroom")
        del_id = df2.iloc[idx2]["id"]
        ok = st.checkbox("⚠️ 本当に削除する（シーズン/成績すべて消去）")
        if st.button("ルーム削除実行", disabled=not ok):
            con.execute("DELETE FROM rooms WHERE id=?",(del_id,))
            con.commit(); con.close()
            if st.session_state.get("room_id")==del_id:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("削除しました。"); st.rerun()
    con.close()

st.caption("Pt = (最終点 − 返し)/1000 + UMA + (トップのみOKApt). 収支(円) = Pt × レート。")

# ---------------- メイン（ルーム選択後） ----------------
if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。"); st.stop()

players_df = df_players(con, room_id)
st.write(f"**ルーム：{room['name']}**")
st.dataframe(players_df[["display_name","joined_at"]].rename(
    columns={"display_name":"プレイヤー","joined_at":"参加"}), use_container_width=True, height=200)

# 共通セレクタ
seasons_df = df_seasons(con, room_id)
sel_season_id = None; sel_meet_id = None
if not seasons_df.empty:
    season_name = st.selectbox("集計対象シーズン", seasons_df["name"].tolist(), key="season_sel_top")
    sel_season_id = seasons_df.loc[seasons_df["name"]==season_name, "id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        meet_name = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist(), key="meet_sel_top")
        sel_meet_id = meets_df.loc[meets_df["name"]==meet_name, "id"].values[0]

tab_input, tab_results, tab_manage = st.tabs(["📝 入力","📊 成績","👤 メンバー/設定"])

# ---------------- 入力タブ ----------------
with tab_input:
    st.subheader("半荘入力")
    if seasons_df.empty or not sel_season_id or not sel_meet_id:
        st.info("『👤 メンバー/設定』でシーズンとミートを作成/選択してください。")
    else:
        names = players_df["display_name"].tolist()
        name_to_id = dict(zip(players_df["display_name"], players_df["id"]))

        c1,c2 = st.columns(2); c3,c4 = st.columns(2)
        east  = c1.selectbox("東", names, index=min(0,len(names)-1))
        south = c2.selectbox("南", names, index=min(1,len(names)-1))
        west  = c3.selectbox("西", names, index=min(2,len(names)-1))
        north = c4.selectbox("北", names, index=min(3,len(names)-1))
        picked=[east,south,west,north]
        if len(set(picked))<4:
            st.warning("同一人物が重複しています。4人別々を選択してください。")
        else:
            with st.form("hanchan_form"):
                finals={}
                st.write("**最終点（100点単位推奨）**")
                p_e = int(st.number_input(east,  value=25000, step=100, key="ptE"))
                p_s = int(st.number_input(south, value=25000, step=100, key="ptS"))
                p_w = int(st.number_input(west,  value=25000, step=100, key="ptW"))
                p_n = int(st.number_input(north, value=25000, step=100, key="ptN"))
                finals[name_to_id[east]]  = p_e
                finals[name_to_id[south]] = p_s
                finals[name_to_id[west]]  = p_w
                finals[name_to_id[north]] = p_n

                st.write("**役満/焼き鳥（任意）**")
                yaku_cols = st.columns(4)
                yakuman = {
                    name_to_id[east]:  int(yaku_cols[0].number_input(f"{east} 役満回", 0, 99, 0)),
                    name_to_id[south]: int(yaku_cols[1].number_input(f"{south} 役満回",0,99,0)),
                    name_to_id[west]:  int(yaku_cols[2].number_input(f"{west} 役満回", 0, 99, 0)),
                    name_to_id[north]: int(yaku_cols[3].number_input(f"{north} 役満回",0,99,0)),
                }
                yaki_cols = st.columns(4)
                yakitori = {
                    name_to_id[east]:  int(yaki_cols[0].checkbox(f"{east} 焼き鳥", False)),
                    name_to_id[south]: int(yaki_cols[1].checkbox(f"{south} 焼き鳥", False)),
                    name_to_id[west]:  int(yaki_cols[2].checkbox(f"{west} 焼き鳥", False)),
                    name_to_id[north]: int(yaki_cols[3].checkbox(f"{north} 焼き鳥", False)),
                }

                memo = st.text_input("メモ（任意）","")
                submitted = st.form_submit_button("精算を記録")
                if submitted:
                    league_pt, ranks, rounded, cash = settlement_for_room(room, finals)
                    hid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO hanchan(id,room_id,started_at,finished_at,memo,meet_id) VALUES(?,?,?,?,?,?)",
                        (hid, room_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), memo, sel_meet_id)
                    )
                    for nm in picked:
                        pid = name_to_id[nm]
                        con.execute(
                            "INSERT INTO results(id,hanchan_id,player_id,final_points,rank,net_cash,league_pt,yakuman_cnt,yakitori) "
                            "VALUES(?,?,?,?,?,?,?,?,?)",
                            (str(uuid.uuid4()), hid, pid, int(rounded[pid]), int(ranks[pid]),
                             float(cash[pid]), float(league_pt[pid]), int(yakuman[pid]), int(yakitori[pid]))
                        )
                    con.commit()
                    st.success("半荘を登録しました！")

# ---------------- 成績タブ ----------------
with tab_results:
    st.subheader("成績 / 履歴")
    scope = "ミート（選択ミートのみ）"
    if sel_season_id:
        scope = st.radio("集計範囲", ["ミート（選択ミートのみ）","シーズン（全ミート）","全リーグ（すべて）"], horizontal=True,
                         index=0 if sel_meet_id else 1)
    use_all = (scope=="全リーグ（すべて）")
    use_season = (scope=="シーズン（全ミート）") or (sel_meet_id is None)
    hdf = df_hanchan_join(con, room_id, None if use_all else (sel_season_id if use_season else None),
                          None if (use_all or use_season) else sel_meet_id)
    if hdf.empty:
        st.info("まだ成績がありません。")
    else:
        g = hdf.groupby("display_name")
        summary = pd.DataFrame({
            "回数": g["rank"].count(),
            "1位": g["rank"].apply(lambda s:(s==1).sum()),
            "2位": g["rank"].apply(lambda s:(s==2).sum()),
            "3位": g["rank"].apply(lambda s:(s==3).sum()),
            "4位": g["rank"].apply(lambda s:(s==4).sum()),
            "総Pt": g["league_pt"].sum().round(2),
            "平均Pt": g["league_pt"].mean().round(2),
            "役満(回)": g["yakuman_cnt"].sum().astype(int),
            "焼き鳥(回)": g["yakitori"].sum().astype(int),
            "収支合計(円)": g["net_cash"].sum().round(0),
            "平均順位": g["rank"].mean().round(2),
        }).reset_index().sort_values(["総Pt","収支合計(円)"], ascending=[False,False])

        # 左端を順位表示に
        summary = summary.reset_index(drop=True)
        summary.index = summary.index + 1
        summary.insert(0, "順位", summary.index)

        st.write("### 個人成績（累積 / Pt基準）")
        st.dataframe(summary, use_container_width=True, height=420)

        st.write("### 半荘履歴（主要列）")
        disp = hdf.copy()
        disp["精算(円)"] = disp["net_cash"].map(lambda x:f"{x:,.0f}")
        disp["Pt"] = disp["league_pt"].map(lambda x:f"{x:.2f}")
        disp["点棒(最終点)"] = disp["final_points"].map(lambda x:f"{x:,}")
        disp = disp.rename(columns={
            "season_name":"シーズン","meet_name":"ミート","display_name":"プレイヤー",
            "rank":"着順","yakuman_cnt":"役満(回)","yakitori":"焼き鳥"
        })
        st.dataframe(
            disp[["シーズン","ミート","プレイヤー","点棒(最終点)","Pt","着順","役満(回)","焼き鳥","精算(円)"]],
            use_container_width=True, height=420
        )

        st.download_button("成績CSVをダウンロード",
                           summary.to_csv(index=False).encode("utf-8-sig"),
                           file_name="summary_pt.csv", mime="text/csv")

# ---------------- メンバー/設定タブ ----------------
with tab_manage:
    st.subheader("メンバー管理")
    exist = players_df["display_name"].tolist()
    pool = sorted(set(exist)|set(DEFAULT_MEMBERS))
    _ = st.multiselect("候補（未登録は一括追加可）", options=pool, default=exist or DEFAULT_MEMBERS[:4])
    c1,c2 = st.columns([2,1])
    with c1:
        new_name = st.text_input("新メンバー名（1人ずつ）", placeholder="例）Ami")
    with c2:
        if st.button("追加"):
            if new_name.strip():
                ensure_players(con, room_id, [new_name.strip()])
                st.success(f"追加：{new_name.strip()}"); st.rerun()
    if st.button("未登録候補をまとめて登録"):
        ensure_players(con, room_id, pool)
        st.success("未登録メンバーを登録しました。"); st.rerun()

    st.divider(); st.subheader("シーズン")
    seasons_df = df_seasons(con, room_id)
    a,b = st.columns([2,1])
    with a:
        st.dataframe(seasons_df.rename(columns={"name":"シーズン名","start_date":"開始日","end_date":"終了日"}),
                     use_container_width=True, height=220)
    with b:
        with st.form("season_form"):
            s_name = st.text_input("シーズン名", value=f"{date.today().year} 前期")
            s_start = st.date_input("開始日", value=date(date.today().year,1,1))
            s_end   = st.date_input("終了日", value=date(date.today().year,6,30))
            if st.form_submit_button("シーズン作成"):
                con.execute("INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at) VALUES(?,?,?,?,?,?)",
                            (str(uuid.uuid4()), room_id, s_name, s_start.isoformat(), s_end.isoformat(),
                             datetime.utcnow().isoformat()))
                con.commit(); st.rerun()

    st.divider(); st.subheader("ミート（開催）")
    if seasons_df.empty:
        st.info("先にシーズンを作成してください。")
    else:
        pick_s = st.selectbox("対象シーズン", seasons_df["name"].tolist(), key="season_manage_sel")
        pick_sid = seasons_df.loc[seasons_df["name"]==pick_s,"id"].values[0]
        meets_df2 = df_meets(con, pick_sid)
        m1,m2 = st.columns([2,1])
        with m1:
            st.dataframe(meets_df2.rename(columns={"name":"ミート名","meet_date":"開催日"}),
                         use_container_width=True, height=220)
        with m2:
            with st.form("meet_make"):
                mn = st.text_input("ミート名", value="第1回")
                md = st.date_input("開催日", value=date.today())
                if st.form_submit_button("ミート作成"):
                    con.execute("INSERT INTO meets(id,season_id,name,meet_date,created_at) VALUES(?,?,?,?,?)",
                                (str(uuid.uuid4()), pick_sid, mn, md.isoformat(), datetime.utcnow().isoformat()))
                    con.commit(); st.rerun()

            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="edit_meet_pick")
                edit_id = meets_df2.loc[meets_df2["name"]==edit_name,"id"].values[0]
                edit_date = meets_df2.loc[meets_df2["name"]==edit_name,"meet_date"].values[0]
                with st.form("meet_edit"):
                    new_n = st.text_input("新しいミート名", value=edit_name)
                    new_d = st.date_input("新しい開催日", value=date.fromisoformat(edit_date))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?",
                                    (new_n, new_d.isoformat(), edit_id))
                        con.commit(); st.success("更新しました。"); st.rerun()
                with st.expander("⚠️ ミート削除（関連半荘・結果も削除）", expanded=False):
                    sure = st.checkbox("本当に削除する", key="meet_del_ok")
                    if st.button("このミートを削除", disabled=not sure):
                        cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?", (edit_id,))
                        hids = [r[0] for r in cur.fetchall()]
                        if hids:
                            con.executemany("DELETE FROM results WHERE hanchan_id=?", [(hid,) for hid in hids])
                            con.executemany("DELETE FROM hanchan WHERE id=?", [(hid,) for hid in hids])
                        con.execute("DELETE FROM meets WHERE id=?", (edit_id,))
                        con.commit(); st.success("削除しました。"); st.rerun()

con.close()
