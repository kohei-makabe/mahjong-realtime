# app.py
import streamlit as st
import sqlite3
import uuid
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List

st.set_page_config(page_title="麻雀リーグ 精算ツール", page_icon="🀄", layout="centered")

DB_PATH = Path("mahjong.db")
DEFAULT_MEMBERS = ["眞壁","内藤","森","浜野","傅田","須崎","中間","高田","内藤士"]

# ============ DB ============

def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def table_has_column(con, table, col):
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
      rate_per_pt REAL NOT NULL,
      uma1 REAL NOT NULL, uma2 REAL NOT NULL, uma3 REAL NOT NULL, uma4 REAL NOT NULL,
      rounding TEXT NOT NULL,
      oka_pt REAL NOT NULL DEFAULT 0.0,
      yakuman_pt REAL NOT NULL DEFAULT 0.0,
      yakitori_pt REAL NOT NULL DEFAULT 0.0
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
      FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS results(
      id TEXT PRIMARY KEY,
      hanchan_id TEXT NOT NULL,
      player_id TEXT NOT NULL,
      final_points INTEGER NOT NULL,
      rank INTEGER NOT NULL,
      total_pt REAL NOT NULL,
      cash_yen REAL NOT NULL,
      yakuman_cnt INTEGER NOT NULL DEFAULT 0,
      yakitori INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
      FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
      UNIQUE(hanchan_id, player_id)
    );
    """)
    # 既存DBの不足列を補完
    if not table_has_column(con, "rooms", "rate_per_pt"):
        cur.execute("ALTER TABLE rooms ADD COLUMN rate_per_pt REAL NOT NULL DEFAULT 100.0")
    for col in ["oka_pt","yakuman_pt","yakitori_pt"]:
        if not table_has_column(con, "rooms", col):
            cur.execute(f"ALTER TABLE rooms ADD COLUMN {col} REAL NOT NULL DEFAULT 0.0")
    for col, typ, dflt in [("total_pt","REAL","0.0"),("cash_yen","REAL","0.0"),
                           ("yakuman_cnt","INTEGER","0"),("yakitori","INTEGER","0")]:
        if not table_has_column(con, "results", col):
            cur.execute(f"ALTER TABLE results ADD COLUMN {col} {typ} NOT NULL DEFAULT {dflt}")
    if not table_has_column(con, "hanchan", "meet_id"):
        cur.execute("ALTER TABLE hanchan ADD COLUMN meet_id TEXT")
    con.commit(); con.close()

def df_rooms(con):
    return pd.read_sql_query("SELECT id,name,created_at FROM rooms ORDER BY datetime(created_at) DESC", con)

def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?", (room_id,))
    row = cur.fetchone()
    if not row: return None
    cols = [d[0] for d in cur.description]
    d = {cols[i]: row[i] for i in range(len(cols))}
    d["start_points"] = int(d["start_points"]); d["target_points"] = int(d["target_points"])
    for k in ["rate_per_pt","uma1","uma2","uma3","uma4","oka_pt","yakuman_pt","yakitori_pt"]:
        d[k] = float(d[k])
    return d

def df_players(con, room_id):
    return pd.read_sql_query("SELECT * FROM players WHERE room_id=? ORDER BY joined_at", con, params=(room_id,))

def df_seasons(con, room_id):
    return pd.read_sql_query("SELECT * FROM seasons WHERE room_id=? ORDER BY start_date", con, params=(room_id,))

def df_meets(con, season_id):
    return pd.read_sql_query("SELECT * FROM meets WHERE season_id=? ORDER BY meet_date", con, params=(season_id,))

# 安全な結合（パラメータ不一致を防ぐ）
def df_hanchan_join(con, room_id, season_id: Optional[str]=None, meet_id: Optional[str]=None):
    q = """
    SELECT  h.id, h.room_id, h.meet_id, h.started_at, h.memo,
            p.display_name, r.final_points, r.rank, r.total_pt, r.cash_yen,
            r.yakuman_cnt, r.yakitori,
            m.name AS meet_name, m.meet_date, s.name AS season_name
    FROM hanchan h
      JOIN results r ON r.hanchan_id = h.id
      JOIN players p ON p.id = r.player_id
      LEFT JOIN meets m   ON m.id = h.meet_id
      LEFT JOIN seasons s ON s.id = m.season_id
    WHERE h.room_id=?
    """
    params: List = [room_id]
    if season_id is not None:
        q += " AND s.id=?"; params.append(season_id)
    if meet_id is not None:
        q += " AND h.meet_id=?"; params.append(meet_id)
    q += " ORDER BY h.started_at DESC, r.rank ASC"
    cur = con.execute(q, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)

def ensure_players(con, room_id, names: List[str]):
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    add = [n for n in names if n and n not in have]
    for nm in add:
        con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES (?,?,?,?)",
                    (str(uuid.uuid4()), room_id, nm, datetime.utcnow().isoformat()))
    if add: con.commit()

# ============ ロジック ============
def apply_rounding(points: int, mode: str) -> int:
    if mode == "none": return int(points)
    if mode == "floor": return (points // 100) * 100
    if mode == "ceil":  return ((points + 99) // 100) * 100
    return int(round(points / 100.0) * 100)  # round

def settle_points(room: dict, finals_raw: dict, ranks: dict):
    target = room["target_points"]
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    oka_pt = room["oka_pt"]; rate = room["rate_per_pt"]; rounding = room["rounding"]

    rounded = {pid: apply_rounding(pts, rounding) for pid, pts in finals_raw.items()}
    top_pid = sorted(rounded.items(), key=lambda x: x[1], reverse=True)[0][0]

    total_pt = {}; cash = {}
    for pid, pts in rounded.items():
        base_pt = (pts - target) / 1000.0
        uma_pt = uma[ranks[pid]-1]
        add_oka = oka_pt if pid == top_pid else 0.0
        t = base_pt + uma_pt + add_oka
        total_pt[pid] = t
        cash[pid] = t * rate
    return rounded, total_pt, cash

# ============ UI ============

st.title("🀄 麻雀リーグ 精算ツール")
init_db()

with st.sidebar:
    st.header("ルーム")
    action = st.radio("操作", ["ルーム作成","ルーム参加"], horizontal=True)

    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        c1,c2 = st.columns(2)
        with c1:
            start_points  = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate_per_pt   = st.number_input("レート(円/pt)", value=100.0, step=10.0)
        with c2:
            uma1 = st.number_input("ウマ1位(+pt)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ2位(+pt)", value=5.0,  step=1.0)
            uma3 = st.number_input("ウマ3位(-pt)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ4位(-pt)", value=-10.0,step=1.0)
        rounding = st.selectbox("丸め(100点)", ["none","round","floor","ceil"], index=0)

        st.markdown("— 任意設定 —")
        oka_pt      = st.number_input("OKA pt（トップ加点/pt）", value=0.0, step=0.5)
        yakuman_pt  = st.number_input("役満 pt（1回あたり/pt）", value=0.0, step=0.5)
        yakitori_pt = st.number_input("焼き鳥 pt（1で加点。マイナス推奨）", value=0.0, step=0.5)
        yourname    = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            rid = str(uuid.uuid4()); pid = str(uuid.uuid4())
            con = connect()
            con.execute("""INSERT INTO rooms
               (id,name,created_at,start_points,target_points,rate_per_pt,
                uma1,uma2,uma3,uma4,rounding,oka_pt,yakuman_pt,yakitori_pt)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
               (rid,name,datetime.utcnow().isoformat(),int(start_points),int(target_points),
                float(rate_per_pt),float(uma1),float(uma2),float(uma3),float(uma4),
                rounding,float(oka_pt),float(yakuman_pt),float(yakitori_pt)))
            con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES (?,?,?,?)",
                        (pid,rid,yourname,datetime.utcnow().isoformat()))
            con.commit(); con.close()
            st.session_state["room_id"]=rid; st.session_state["player_id"]=pid
            st.success("作成しました。"); st.rerun()

    else:
        con = connect(); rdf = df_rooms(con)
        if rdf.empty:
            st.info("ルームがありません。『ルーム作成』へ。")
        else:
            idx = st.selectbox("参加ルーム", options=list(range(len(rdf))),
                               format_func=lambda i: f'{rdf.iloc[i]["name"]}（{rdf.iloc[i]["created_at"].replace("T"," ")[:16]}）')
            sel_room_id = rdf.iloc[idx]["id"]; st.caption(f"Room ID: `{sel_room_id}`")
            nm = st.text_input("あなたの表示名", value="あなた")
            if st.button("参加"):
                cur = con.execute("SELECT id FROM players WHERE room_id=? AND display_name=?",(sel_room_id,nm))
                row = cur.fetchone()
                pid = row[0] if row else str(uuid.uuid4())
                if not row:
                    con.execute("INSERT INTO players(id,room_id,display_name,joined_at) VALUES (?,?,?,?)",
                                (pid,sel_room_id,nm,datetime.utcnow().isoformat()))
                    con.commit()
                con.close()
                st.session_state["room_id"]=sel_room_id; st.session_state["player_id"]=pid
                st.success("参加しました。"); st.rerun()
        con.close()

    st.divider()
    st.subheader("🗑️ ルーム削除（全消去）")
    con = connect(); rdf2 = df_rooms(con)
    if not rdf2.empty:
        idx_d = st.selectbox("削除対象", options=list(range(len(rdf2))),
                             format_func=lambda i: f'{rdf2.iloc[i]["name"]}（{rdf2.iloc[i]["created_at"].replace("T"," ")[:16]}）')
        del_id = rdf2.iloc[idx_d]["id"]
        sure = st.checkbox("⚠️ 本当に削除する（成績・シーズン・ミートも全消去）")
        if st.button("削除実行", disabled=not sure):
            con.execute("DELETE FROM rooms WHERE id=?", (del_id,))
            con.commit(); con.close()
            if st.session_state.get("room_id")==del_id:
                st.session_state.pop("room_id",None); st.session_state.pop("player_id",None)
            st.success("削除しました。"); st.rerun()
    con.close()

st.caption("合計Pt=(最終点-返し)/1000 + ウマ + [トップOKA] + 役満pt×回数 + 焼き鳥pt。収支=合計Pt×レート。")
if room is None:
    st.warning("以前のルームは削除されたか無効になっています。サイドバーから作成/参加し直してください。")
    st.session_state.pop("room_id", None)
    st.session_state.pop("player_id", None)
    con.close()
    st.stop()

if not room: st.error("ルームが見つかりません。"); st.stop()
players_df = df_players(con, room_id)

st.write(f"**ルーム: {room['name']}**")
st.dataframe(players_df[["display_name","joined_at"]].rename(columns={"display_name":"プレイヤー","joined_at":"参加"}),
             use_container_width=True, height=200)

seasons_df = df_seasons(con, room_id)
sel_season_id = None; sel_meet_id = None
if not seasons_df.empty:
    sel_season_name = st.selectbox("集計シーズン", seasons_df["name"].tolist())
    sel_season_id = seasons_df.loc[seasons_df["name"]==sel_season_name,"id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        sel_meet_name = st.selectbox("入力・表示ミート", meets_df["name"].tolist())
        sel_meet_id = meets_df.loc[meets_df["name"]==sel_meet_name,"id"].values[0]

tab_input, tab_results, tab_manage = st.tabs(["📝 入力","📊 成績","👤 メンバー/設定"])

# ----- 入力 -----
with tab_input:
    st.subheader("半荘入力")
    if seasons_df.empty:
        st.info("先に『👤 メンバー/設定』でシーズン/ミートを作成してください。")
    elif sel_meet_id is None:
        st.info("ミートを選択してください。")
    else:
        names = players_df["display_name"].tolist()
        name2id = dict(players_df[["display_name","id"]].values)

        c1,c2 = st.columns(2); c3,c4 = st.columns(2)
        e = c1.selectbox("東", names, index=min(0,len(names)-1))
        s = c2.selectbox("南", names, index=min(1,len(names)-1))
        w = c3.selectbox("西", names, index=min(2,len(names)-1))
        n = c4.selectbox("北", names, index=min(3,len(names)-1))
        picked = [e,s,w,n]

        if len(set(picked))<4:
            st.warning("同一プレイヤーが選択されています。")
        else:
            with st.form("hanchan_form"):
                st.write("**最終点（100点単位推奨）**")
                finals = {}
                def pin(label, key):
                    return int(st.number_input(label, value=room["start_points"], step=100, key=key))
                finals[name2id[e]] = pin(e, f"pt_{e}")
                finals[name2id[s]] = pin(s, f"pt_{s}")
                finals[name2id[w]] = pin(w, f"pt_{w}")
                finals[name2id[n]] = pin(n, f"pt_{n}")

                st.write("**役満回数 / 焼き鳥**")
                cols1 = st.columns(4); ykm = {}
                for i,nm in enumerate(picked):
                    ykm[name2id[nm]] = int(cols1[i].number_input(f"{nm} 役満回数", value=0, step=1, min_value=0))
                cols2 = st.columns(4); ytr = {}
                for i,nm in enumerate(picked):
                    ytr[name2id[nm]] = int(cols2[i].checkbox(f"{nm} 焼き鳥", value=False))

                memo = st.text_input("メモ", value="")
                if st.form_submit_button("精算を記録"):
                    rounded_tmp = {pid: apply_rounding(v, room["rounding"]) for pid,v in finals.items()}
                    order = sorted(rounded_tmp.items(), key=lambda x:x[1], reverse=True)
                    ranks = {pid:i+1 for i,(pid,_) in enumerate(order)}
                    rounded, total_pt, cash = settle_points(room, finals, ranks)
                    # 役満/焼き鳥 pt 加算後に収支再算出
                    for pid in total_pt:
                        total_pt[pid] += room["yakuman_pt"]*ykm.get(pid,0)
                        if ytr.get(pid,0): total_pt[pid] += room["yakitori_pt"]
                        cash[pid] = total_pt[pid]*room["rate_per_pt"]
                    hid = str(uuid.uuid4())
                    con.execute("INSERT INTO hanchan(id,room_id,meet_id,started_at,finished_at,memo) VALUES (?,?,?,?,?,?)",
                                (hid,room_id,sel_meet_id,datetime.utcnow().isoformat(),
                                 datetime.utcnow().isoformat(), memo))
                    for nm in picked:
                        pid = name2id[nm]
                        con.execute("""INSERT INTO results
                          (id,hanchan_id,player_id,final_points,rank,total_pt,cash_yen,yakuman_cnt,yakitori)
                          VALUES (?,?,?,?,?,?,?,?,?)""",
                          (str(uuid.uuid4()),hid,pid,int(rounded[pid]),int(ranks[pid]),
                           float(total_pt[pid]), float(cash[pid]), int(ykm.get(pid,0)), int(ytr.get(pid,0))))
                    con.commit()
                    st.success("登録しました。"); st.rerun()

# ----- 成績 -----
with tab_results:
    st.subheader("成績 / 履歴")
    scope = st.radio("集計範囲", ["ミート（選択）","シーズン（全ミート）","全リーグ"], horizontal=True,
                     index=0 if sel_meet_id else (1 if sel_season_id else 2))
    season_arg = None; meet_arg = None
    if scope=="シーズン（全ミート）" and sel_season_id: season_arg=sel_season_id
    if scope=="ミート（選択）" and sel_meet_id: meet_arg=sel_meet_id

    hdf = df_hanchan_join(con, room_id, season_arg, meet_arg)
    if hdf.empty:
        st.info("成績データがありません。")
    else:
        g = hdf.groupby("display_name")
        summary = pd.DataFrame({
            "回数": g["rank"].count(),
            "1位": g["rank"].apply(lambda s:(s==1).sum()),
            "2位": g["rank"].apply(lambda s:(s==2).sum()),
            "3位": g["rank"].apply(lambda s:(s==3).sum()),
            "4位": g["rank"].apply(lambda s:(s==4).sum()),
            "合計Pt": g["total_pt"].sum().round(2),
            "収支合計(円)": g["cash_yen"].sum().round(0),
            "平均Pt": g["total_pt"].mean().round(2),
            "平均順位": g["rank"].mean().round(2),
            "役満(回)": g["yakuman_cnt"].sum(),
            "焼き鳥(回)": g["yakitori"].sum()
        }).reset_index()
        summary = summary.sort_values(["合計Pt","収支合計(円)"], ascending=[False,False]).reset_index(drop=True)
        summary.insert(0,"順位", summary.index+1)

        st.write("### 個人成績（Pt主義）")
        st.dataframe(summary, use_container_width=True, height=380)

        st.write("### 半荘履歴")
        disp = hdf.copy()
        disp["点棒(最終点)"] = disp["final_points"].astype(int)
        disp["合計Pt"] = disp["total_pt"].round(2)
        disp["収支(円)"] = disp["cash_yen"].round(0).astype(int)
        disp = disp.rename(columns={
            "season_name":"シーズン","meet_name":"ミート","display_name":"プレイヤー",
            "rank":"着順","yakuman_cnt":"役満","yakitori":"焼き鳥"})
        st.dataframe(disp[["シーズン","ミート","プレイヤー","点棒(最終点)","着順","合計Pt","収支(円)","役満","焼き鳥","started_at"]],
                     use_container_width=True, height=420)

        st.download_button("成績CSVダウンロード",
                           summary.to_csv(index=False).encode("utf-8-sig"),
                           file_name="summary_pt.csv", mime="text/csv")

# ----- メンバー / 設定 -----
with tab_manage:
    st.subheader("メンバー管理")
    existing = players_df["display_name"].tolist()
    pool = sorted(set(existing) | set(DEFAULT_MEMBERS))
    want = st.multiselect("候補（未登録は一括追加可）", pool, default=existing or DEFAULT_MEMBERS[:4])
    nm_new = st.text_input("新メンバー名（1人ずつ）")
    cA,cB = st.columns([1,1])
    if cA.button("新メンバー追加") and nm_new.strip():
        ensure_players(con, room_id, [nm_new.strip()]); st.success("追加しました。"); st.rerun()
    if cB.button("候補をまとめて登録"):
        ensure_players(con, room_id, want); st.success("登録しました。"); st.rerun()

    st.divider()
    st.subheader("シーズン")
    s_df = df_seasons(con, room_id)
    if not s_df.empty:
        st.dataframe(s_df.rename(columns={"name":"シーズン名","start_date":"開始日","end_date":"終了日"}),
                     use_container_width=True, height=220)
    with st.form("season_create"):
        s_name  = st.text_input("シーズン名", value=f"{date.today().year} 前期")
        s_start = st.date_input("開始日", value=date(date.today().year,1,1))
        s_end   = st.date_input("終了日", value=date(date.today().year,6,30))
        if st.form_submit_button("シーズン作成"):
            sid = str(uuid.uuid4())
            con.execute("INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at) VALUES (?,?,?,?,?,?)",
                        (sid,room_id,s_name,s_start.isoformat(),s_end.isoformat(),datetime.utcnow().isoformat()))
            con.commit(); st.success("作成しました。"); st.rerun()

    st.subheader("ミート（開催）")
    if s_df.empty:
        st.info("先にシーズンを作成してください。")
    else:
        sel_s = st.selectbox("対象シーズン", s_df["name"].tolist(), key="season_for_meet")
        sel_sid = s_df.loc[s_df["name"]==sel_s,"id"].values[0]
        m_df = df_meets(con, sel_sid)
        if not m_df.empty:
            st.dataframe(m_df.rename(columns={"name":"ミート名","meet_date":"開催日"}),
                         use_container_width=True, height=220)
        with st.form("meet_create"):
            m_name = st.text_input("ミート名", value="第1回")
            m_date = st.date_input("開催日", value=date.today())
            if st.form_submit_button("ミート作成"):
                mid = str(uuid.uuid4())
                con.execute("INSERT INTO meets(id,season_id,name,meet_date,created_at) VALUES (?,?,?,?,?)",
                            (mid,sel_sid,m_name,m_date.isoformat(),datetime.utcnow().isoformat()))
                con.commit(); st.success("作成しました。"); st.rerun()

        st.markdown("#### ミート修正 / 削除")
        if not m_df.empty:
            pick = st.selectbox("編集対象ミート", m_df["name"].tolist())
            pick_id = m_df.loc[m_df["name"]==pick,"id"].values[0]
            pick_date = m_df.loc[m_df["name"]==pick,"meet_date"].values[0]
            with st.form("meet_edit"):
                new_name = st.text_input("新ミート名", value=pick)
                new_date = st.date_input("新開催日", value=date.fromisoformat(pick_date))
                if st.form_submit_button("更新保存"):
                    con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?",
                                (new_name,new_date.isoformat(),pick_id))
                    con.commit(); st.success("更新しました。"); st.rerun()
            with st.expander("⚠️ ミート削除（半荘・結果も削除）"):
                sure = st.checkbox("本当に削除する", key="del_meet_confirm")
                if st.button("このミートを削除", disabled=not sure):
                    cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?", (pick_id,))
                    hids = [r[0] for r in cur.fetchall()]
                    if hids:
                        con.executemany("DELETE FROM results WHERE hanchan_id=?", [(x,) for x in hids])
                        con.executemany("DELETE FROM hanchan WHERE id=?", [(x,) for x in hids])
                    con.execute("DELETE FROM meets WHERE id=?", (pick_id,))
                    con.commit(); st.success("削除しました。"); st.rerun()

con.close()
