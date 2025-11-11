# app.py
# 麻雀リーグ 精算ツール（シーズン/ミート、ポイント主義）
# - 合計Pt = (丸め後点棒-返し)/1000 + ウマ(順位) + [トップならOKA pt] + 役満pt*回数 + 焼き鳥pt
# - 収支(円) = 合計Pt × レート(円/pt)
# - 25000返しでもウマは常に有効
# - 役満/焼き鳥のptはルーム単位で設定可能
# - 成績は合計Ptを主にランキング表示（収支も集計）
# - シーズン/ミート作成・編集・削除 + ルーム削除
# - pandas.read_sql の params 不一致を避けるため df_hanchan_join を安全実装

import streamlit as st
import sqlite3
import uuid
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, List

st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
button, .stButton>button { padding: 0.6rem 0.9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.05rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")

DEFAULT_MEMBERS = ["眞壁", "内藤", "森", "浜野", "傅田", "須崎", "中間", "高田", "内藤士"]

# ---------------- DB utils ----------------
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
      start_points INTEGER NOT NULL,     -- 持ち点
      target_points INTEGER NOT NULL,    -- 返し
      rate_per_pt REAL NOT NULL,         -- 円/pt
      uma1 REAL NOT NULL, uma2 REAL NOT NULL, uma3 REAL NOT NULL, uma4 REAL NOT NULL,
      rounding TEXT NOT NULL,            -- none/round/floor/ceil（100点単位）
      oka_pt REAL NOT NULL DEFAULT 0.0,  -- トップに加算する pt（任意）
      yakuman_pt REAL NOT NULL DEFAULT 0.0,  -- 役満1回あたりの pt（任意）
      yakitori_pt REAL NOT NULL DEFAULT 0.0  -- 焼き鳥の pt（任意、マイナス推奨）
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
      final_points INTEGER NOT NULL,  -- 丸め後の最終点
      rank INTEGER NOT NULL,
      total_pt REAL NOT NULL,         -- 合計Pt（主指標）
      cash_yen REAL NOT NULL,         -- 収支(円) = total_pt × rate
      yakuman_cnt INTEGER NOT NULL DEFAULT 0,
      yakitori INTEGER NOT NULL DEFAULT 0,  -- 0/1
      FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
      FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
      UNIQUE(hanchan_id, player_id)
    );
    """)
    # 既存DBへの足りない列追加（後方互換）
    for col, typ, dflt in [
        ("rate_per_pt", "REAL", "100.0"),
        ("oka_pt", "REAL", "0.0"),
        ("yakuman_pt", "REAL", "0.0"),
        ("yakitori_pt", "REAL", "0.0")
    ]:
        if not table_has_column(con, "rooms", col):
            cur.execute(f"ALTER TABLE rooms ADD COLUMN {col} {typ} NOT NULL DEFAULT {dflt};")
    for col, typ, dflt in [
        ("total_pt", "REAL", "0.0"),
        ("cash_yen", "REAL", "0.0"),
        ("yakuman_cnt", "INTEGER", "0"),
        ("yakitori", "INTEGER", "0")
    ]:
        if not table_has_column(con, "results", col):
            cur.execute(f"ALTER TABLE results ADD COLUMN {col} {typ} NOT NULL DEFAULT {dflt};")
    if not table_has_column(con, "hanchan", "meet_id"):
        cur.execute("ALTER TABLE hanchan ADD COLUMN meet_id TEXT;")
    con.commit(); con.close()

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id, name, created_at FROM rooms ORDER BY datetime(created_at) DESC", con
    )

def apply_rounding(points: int, mode: str) -> int:
    if mode == "none": return int(points)
    if mode == "floor": return (points // 100) * 100
    if mode == "ceil":  return ((points + 99) // 100) * 100
    return int(round(points / 100.0) * 100)  # round

def row_to_dict(row, cols): return {cols[i]: row[i] for i in range(len(cols))}

def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?", (room_id,))
    row = cur.fetchone()
    if not row: return None
    cols = [d[0] for d in cur.description]
    d = row_to_dict(row, cols)
    for k in ["start_points", "target_points"]: d[k] = int(d[k])
    for k in ["rate_per_pt", "uma1", "uma2", "uma3", "uma4", "oka_pt", "yakuman_pt", "yakitori_pt"]:
        d[k] = float(d[k])
    return d

def df_players(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM players WHERE room_id=? ORDER BY joined_at", con, params=(room_id,)
    )

def df_seasons(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM seasons WHERE room_id=? ORDER BY start_date", con, params=(room_id,)
    )

def df_meets(con, season_id):
    return pd.read_sql_query(
        "SELECT * FROM meets WHERE season_id=? ORDER BY meet_date", con, params=(season_id,)
    )

# 🔧 安全版：選択の有無で SQL を組み立て（params 不一致を防止）
def df_hanchan_join(con, room_id, season_id: Optional[str] = None, meet_id: Optional[str] = None):
    q = """
    SELECT  h.id, h.room_id, h.meet_id, h.started_at, h.memo,
            p.display_name, r.final_points, r.rank, r.total_pt, r.cash_yen, r.player_id,
            r.yakuman_cnt, r.yakitori,
            m.name as meet_name, m.meet_date, s.name as season_name
    FROM hanchan h
      JOIN results r ON r.hanchan_id = h.id
      JOIN players p ON p.id = r.player_id
      LEFT JOIN meets m   ON m.id = h.meet_id
      LEFT JOIN seasons s ON s.id = m.season_id
    WHERE h.room_id=?
    """
    params: List = [room_id]
    if season_id is not None:
        q += " AND s.id=?"
        params.append(season_id)
    if meet_id is not None:
        q += " AND h.meet_id=?"
        params.append(meet_id)
    q += " ORDER BY h.started_at DESC, r.rank ASC"
    cur = con.execute(q, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)

def ensure_players(con, room_id: str, names: List[str]) -> None:
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed = False
    for nm in names:
        if nm and nm not in have:
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (str(uuid.uuid4()), room_id, nm, datetime.utcnow().isoformat())
            )
            changed = True
    if changed: con.commit()

# ------------- 精算ロジック（Pt主義） -------------
def settle_points(room: dict, finals_raw: Dict[str, int], ranks: Dict[str, int]):
    """
    returns:
      rounded_points: Dict[player_id]->int
      total_pt:       Dict[player_id]->float
      cash_yen:       Dict[player_id]->float
    """
    target = room["target_points"]
    uma_list = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    oka_pt = room["oka_pt"]
    rate = room["rate_per_pt"]
    rounding = room["rounding"]

    # 丸め→素点(千点)→UMA→OKA(pt)
    rounded = {pid: apply_rounding(pts, rounding) for pid, pts in finals_raw.items()}
    total_pt = {}
    cash_yen  = {}
    # トップの判定
    top_pid = sorted(rounded.items(), key=lambda x: x[1], reverse=True)[0][0]

    for pid, pts in rounded.items():
        base_pt = (pts - target) / 1000.0
        uma_pt  = uma_list[ranks[pid] - 1]
        add_oka = oka_pt if pid == top_pid else 0.0
        total = base_pt + uma_pt + add_oka
        total_pt[pid] = total
        cash_yen[pid] = total * rate
    return rounded, total_pt, cash_yen

# ---------------- UI helpers ----------------
def points_input(label: str, key: str, default: int = 25000) -> int:
    return int(st.number_input(label, value=default, step=100, key=f"{key}_num"))

# ================== アプリ本体 ==================
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
            rate_per_pt = st.number_input("レート(円/pt)", value=100.0, step=10.0)
        with col2:
            uma1 = st.number_input("ウマ 1位(+pt)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ 2位(+pt)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ 3位(−pt)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ 4位(−pt)", value=-10.0, step=1.0)
        rounding = st.selectbox("点数丸め（100点単位）", ["none", "round", "floor", "ceil"], index=0)
        st.markdown("— 追加設定（任意） —")
        oka_pt = st.number_input("OKA pt（トップ加点：pt）", value=0.0, step=0.5)
        yakuman_pt = st.number_input("役満 pt（1回あたり）", value=0.0, step=0.5)
        yakitori_pt = st.number_input("焼き鳥 pt（1で加算。マイナス推奨）", value=0.0, step=0.5)
        creator = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            rid = str(uuid.uuid4())
            con = connect()
            con.execute(
                """INSERT INTO rooms(id,name,created_at,start_points,target_points,rate_per_pt,
                                      uma1,uma2,uma3,uma4,rounding,oka_pt,yakuman_pt,yakitori_pt)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (rid, name, datetime.utcnow().isoformat(), int(start_points), int(target_points),
                 float(rate_per_pt), float(uma1), float(uma2), float(uma3), float(uma4),
                 rounding, float(oka_pt), float(yakuman_pt), float(yakitori_pt))
            )
            pid = str(uuid.uuid4())
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (pid, rid, creator, datetime.utcnow().isoformat())
            )
            con.commit(); con.close()
            st.session_state["room_id"] = rid
            st.session_state["player_id"] = pid
            st.success("ルームを作成しました。")
            st.rerun()

    else:
        con = connect()
        rooms_df = df_rooms(con)
        if rooms_df.empty:
            st.info("まだルームがありません。『ルーム作成』から作成してください。")
        else:
            def fmt(r):
                ts = r["created_at"].replace("T", " ")[:16]
                return f'{r["name"]}（{ts}）'
            idx = st.selectbox("参加するルームを選択", options=list(range(len(rooms_df))),
                               format_func=lambda i: fmt(rooms_df.iloc[i]))
            sel_room_id = rooms_df.iloc[idx]["id"]
            st.caption(f"Room ID: `{sel_room_id}`")
            nm = st.text_input("あなたの表示名", value="あなた")
            if st.button("参加"):
                cur = con.execute(
                    "SELECT id FROM players WHERE room_id=? AND display_name=?",
                    (sel_room_id, nm)
                )
                got = cur.fetchone()
                if got: pid = got[0]
                else:
                    pid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                        (pid, sel_room_id, nm, datetime.utcnow().isoformat())
                    )
                    con.commit()
                st.session_state["room_id"] = sel_room_id
                st.session_state["player_id"] = pid
                st.success("参加しました。")
                st.rerun()
        con.close()

    st.divider()
    st.markdown("### 🗑️ ルーム削除")
    con = connect()
    rooms_df2 = df_rooms(con)
    if rooms_df2.empty:
        st.caption("まだルームは存在しません。")
    else:
        idx_del = st.selectbox("削除するルームを選択", options=list(range(len(rooms_df2))),
                               format_func=lambda i: f'{rooms_df2.iloc[i]["name"]}（{rooms_df2.iloc[i]["created_at"].replace("T"," ")[:16]}）',
                               key="del_room")
        del_room_id = rooms_df2.iloc[idx_del]["id"]
        confirm = st.checkbox("⚠️ 本当に削除する（すべてのシーズン・成績が失われます）")
        if st.button("ルーム削除実行", disabled=not confirm):
            con.execute("DELETE FROM rooms WHERE id=?", (del_room_id,))
            con.commit(); con.close()
            if st.session_state.get("room_id") == del_room_id:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("ルームを削除しました。")
            st.rerun()
    con.close()

st.caption("合計Pt=(最終点-返し)/1000 + ウマ + [トップOKA pt] + 役満pt×回数 + 焼き鳥pt。収支=合計Pt×レート。")

# ルーム未選択なら終了
if "room_id" not in st.session_state:
    st.info("左のサイドバーでルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。")
    st.stop()

players_df = df_players(con, room_id)
st.write(f"**ルーム: {room['name']}**")
st.dataframe(
    players_df[["display_name", "joined_at"]].rename(columns={"display_name":"プレイヤー","joined_at":"参加"}),
    use_container_width=True, height=220
)

# 共通セレクタ
seasons_df = df_seasons(con, room_id)
sel_season_id = None
sel_meet_id = None
if not seasons_df.empty:
    sel_season_name = st.selectbox("集計対象シーズン", seasons_df["name"].tolist(), key="season_sel_top")
    sel_season_id = seasons_df.loc[seasons_df["name"]==sel_season_name, "id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        sel_meet_name = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist(), key="meet_sel_top")
        sel_meet_id = meets_df.loc[meets_df["name"]==sel_meet_name, "id"].values[0]

tab_input, tab_results, tab_manage = st.tabs(["📝 入力", "📊 成績", "👤 メンバー/設定"])

# -------- 入力タブ --------
with tab_input:
    st.subheader("半荘入力（誰でも）")
    if seasons_df.empty:
        st.info("先に『👤 メンバー/設定』でシーズン/ミートを作成してください。")
    elif sel_meet_id is None:
        st.info("ミートを選択してください。")
    else:
        names = players_df["display_name"].tolist()
        name_to_id = dict(players_df[["display_name","id"]].values)

        colE,colS = st.columns(2); colW,colN = st.columns(2)
        east  = colE.selectbox("東", names, index=min(0,len(names)-1))
        south = colS.selectbox("南", names, index=min(1,len(names)-1))
        west  = colW.selectbox("西", names, index=min(2,len(names)-1))
        north = colN.selectbox("北", names, index=min(3,len(names)-1))
        picked = [east, south, west, north]

        if len(set(picked)) < 4:
            st.warning("同じ人が選択されています。4人とも別のメンバーを選んでください。")
        else:
            with st.form("hanchan_form"):
                st.write("**最終点（100点単位推奨）**")
                finals_raw = {}
                p_e = points_input(east,  f"pt_{east}",  room["start_points"])
                p_s = points_input(south, f"pt_{south}", room["start_points"])
                p_w = points_input(west,  f"pt_{west}",  room["start_points"])
                p_n = points_input(north, f"pt_{north}", room["start_points"])
                finals_raw[name_to_id[east]]  = p_e
                finals_raw[name_to_id[south]] = p_s
                finals_raw[name_to_id[west]]  = p_w
                finals_raw[name_to_id[north]] = p_n

                # 役満・焼き鳥
                st.write("**役満回数 / 焼き鳥（任意）**")
                cols = st.columns(4)
                yakumans = {}
                yakitoris = {}
                for i, nm in enumerate(picked):
                    yakumans[name_to_id[nm]] = int(cols[i].number_input(f"{nm} 役満回数", value=0, step=1, min_value=0))
                cols2 = st.columns(4)
                for i, nm in enumerate(picked):
                    yakitoris[name_to_id[nm]] = int(cols2[i].checkbox(f"{nm} 焼き鳥", value=False))

                memo = st.text_input("メモ（任意）", value="")
                submitted = st.form_submit_button("精算を記録")

                if submitted:
                    # 並び替え→順位付け
                    rounded_temp = {pid: apply_rounding(pts, room["rounding"]) for pid, pts in finals_raw.items()}
                    order = sorted(rounded_temp.items(), key=lambda x: x[1], reverse=True)
                    ranks = {pid: i+1 for i, (pid, _) in enumerate(order)}

                    rounded, total_pt_map, cash_map = settle_points(room, finals_raw, ranks)

                    # 役満/焼き鳥 pt を加算して再計算（合計Ptが主指標）
                    for pid in total_pt_map:
                        total_pt_map[pid] += room["yakuman_pt"] * yakumans.get(pid, 0)
                        if yakitoris.get(pid, 0):
                            total_pt_map[pid] += room["yakitori_pt"]
                        cash_map[pid] = total_pt_map[pid] * room["rate_per_pt"]

                    hid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO hanchan(id, room_id, meet_id, started_at, finished_at, memo) VALUES (?,?,?,?,?,?)",
                        (hid, room_id, sel_meet_id, datetime.utcnow().isoformat(),
                         datetime.utcnow().isoformat(), memo)
                    )
                    for nm in picked:
                        pid = name_to_id[nm]
                        con.execute(
                            """INSERT INTO results(id,hanchan_id,player_id,final_points,rank,total_pt,cash_yen,yakuman_cnt,yakitori)
                               VALUES (?,?,?,?,?,?,?,?,?)""",
                            (str(uuid.uuid4()), hid, pid, int(rounded[pid]), int(ranks[pid]),
                             float(total_pt_map[pid]), float(cash_map[pid]),
                             int(yakumans.get(pid,0)), int(yakitoris.get(pid,0)))
                        )
                    con.commit()
                    st.success("半荘を登録しました。")
                    st.rerun()

# -------- 成績タブ --------
with tab_results:
    st.subheader("成績 / 履歴")
    scope = st.radio("集計範囲", ["ミート（選択ミートのみ）", "シーズン（全ミート）", "全リーグ（すべて）"],
                     horizontal=True,
                     index=0 if sel_meet_id else (1 if sel_season_id else 2))
    if scope == "ミート（選択ミートのみ）" and not sel_meet_id:
        st.info("ミートを選択してください。")
    else:
        season_arg = None; meet_arg = None
        if scope == "全リーグ（すべて）":
            pass
        elif scope == "シーズン（全ミート）":
            season_arg = sel_season_id if sel_season_id else None
        else:
            meet_arg = sel_meet_id if sel_meet_id else None

        hdf = df_hanchan_join(con, room_id, season_arg, meet_arg)
        if hdf.empty:
            st.info("まだ成績がありません。")
        else:
            g = hdf.groupby("display_name")
            summary = pd.DataFrame({
                "回数": g["rank"].count(),
                "1位": g["rank"].apply(lambda s: (s==1).sum()),
                "2位": g["rank"].apply(lambda s: (s==2).sum()),
                "3位": g["rank"].apply(lambda s: (s==3).sum()),
                "4位": g["rank"].apply(lambda s: (s==4).sum()),
                "合計Pt": g["total_pt"].sum().round(2),
                "収支合計(円)": g["cash_yen"].sum().round(0),
                "平均Pt": g["total_pt"].mean().round(2),
                "平均順位": g["rank"].mean().round(2),
                "役満(回)": g["yakuman_cnt"].sum(),
                "焼き鳥(回)": g["yakitori"].sum()
            }).reset_index()

            # ランキング列を先頭に
            summary = summary.sort_values(["合計Pt","収支合計(円)"], ascending=[False, False]).reset_index(drop=True)
            summary.insert(0, "順位", summary.index + 1)

            st.write("### 個人成績（累積・Pt主義）")
            st.dataframe(summary, use_container_width=True, height=380)

            st.write("### 半荘履歴（主要列）")
            disp = hdf.copy()
            disp["点棒(最終点)"] = disp["final_points"].map(lambda x: f"{int(x):,}")
            disp["合計Pt"] = disp["total_pt"].round(2)
            disp["収支(円)"] = disp["cash_yen"].round(0).astype(int)
            disp = disp.rename(columns={
                "season_name":"シーズン", "meet_name":"ミート",
                "display_name":"プレイヤー", "rank":"着順",
                "yakuman_cnt":"役満", "yakitori":"焼き鳥"
            })
            st.dataframe(
                disp[["シーズン","ミート","プレイヤー","点棒(最終点)","着順","合計Pt","収支(円)","役満","焼き鳥","started_at"]],
                use_container_width=True, height=420
            )

            st.download_button(
                "成績CSVをダウンロード",
                summary.to_csv(index=False).encode("utf-8-sig"),
                file_name="summary_pt.csv",
                mime="text/csv"
            )

# -------- メンバー/設定タブ --------
with tab_manage:
    st.subheader("メンバー管理")
    existing = players_df["display_name"].tolist()
    candidate_pool = sorted(set(existing) | set(DEFAULT_MEMBERS))
    selected_candidates = st.multiselect("候補（未登録は一括追加可）", options=candidate_pool,
                                         default=existing or DEFAULT_MEMBERS[:4])
    col_a, col_b = st.columns([2,1])
    with col_a:
        new_name = st.text_input("新メンバー名（1人ずつ）")
    with col_b:
        if st.button("追加"):
            if new_name.strip():
                ensure_players(con, room_id, [new_name.strip()])
                st.success(f"追加しました：{new_name.strip()}")
                st.rerun()
    if st.button("未登録の候補をまとめて登録"):
        ensure_players(con, room_id, selected_candidates)
        st.success("未登録メンバーを登録しました。")
        st.rerun()

    st.divider()
    st.subheader("シーズン")
    seasons_df2 = df_seasons(con, room_id)
    colS1, colS2 = st.columns([2,1])
    with colS1:
        if not seasons_df2.empty:
            st.dataframe(
                seasons_df2.rename(columns={"name":"シーズン名","start_date":"開始日","end_date":"終了日"}),
                use_container_width=True, height=220
            )
    with colS2:
        with st.form("season_form"):
            s_name = st.text_input("シーズン名", value=f"{date.today().year} 前期")
            s_start = st.date_input("開始日", value=date(date.today().year,1,1))
            s_end   = st.date_input("終了日", value=date(date.today().year,6,30))
            if st.form_submit_button("シーズン作成"):
                sid = str(uuid.uuid4())
                con.execute(
                    "INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at) VALUES (?,?,?,?,?,?)",
                    (sid, room_id, s_name, s_start.isoformat(), s_end.isoformat(), datetime.utcnow().isoformat())
                )
                con.commit()
                st.success("シーズンを作成しました。")
                st.rerun()

    st.subheader("ミート（開催）")
    if seasons_df2.empty:
        st.info("先にシーズンを作成してください。")
    else:
        sel_season_name2 = st.selectbox("対象シーズン", seasons_df2["name"].tolist(), key="season_sel_manage")
        sel_season_id2 = seasons_df2.loc[seasons_df2["name"]==sel_season_name2, "id"].values[0]
        meets_df2 = df_meets(con, sel_season_id2)
        colM1, colM2 = st.columns([2,1])
        with colM1:
            if not meets_df2.empty:
                st.dataframe(meets_df2.rename(columns={"name":"ミート名","meet_date":"開催日"}),
                             use_container_width=True, height=220)
        with colM2:
            with st.form("meet_create"):
                m_name = st.text_input("ミート名", value="第1回")
                m_date = st.date_input("開催日", value=date.today())
                if st.form_submit_button("ミート作成"):
                    mid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO meets(id,season_id,name,meet_date,created_at) VALUES (?,?,?,?,?)",
                        (mid, sel_season_id2, m_name, m_date.isoformat(), datetime.utcnow().isoformat())
                    )
                    con.commit()
                    st.success("ミートを作成しました。")
                    st.rerun()

            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_meet_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_meet_id = meets_df2.loc[meets_df2["name"]==edit_meet_name, "id"].values[0]
                edit_meet_date = meets_df2.loc[meets_df2["name"]==edit_meet_name, "meet_date"].values[0]

                with st.form("meet_edit_form"):
                    new_name = st.text_input("新しいミート名", value=edit_meet_name)
                    new_date = st.date_input("新しい開催日", value=date.fromisoformat(edit_meet_date))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?",
                                    (new_name, new_date.isoformat(), edit_meet_id))
                        con.commit()
                        st.success("ミート情報を更新しました。")
                        st.rerun()

                with st.expander("⚠️ ミート削除（関連半荘・結果も削除）", expanded=False):
                    sure = st.checkbox("本当に削除する", key="meet_del_confirm")
                    if st.button("このミートを削除", disabled=not sure):
                        cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?", (edit_meet_id,))
                        hids = [r[0] for r in cur.fetchall()]
                        if hids:
                            con.executemany("DELETE FROM results WHERE hanchan_id=?", [(hid,) for hid in hids])
                            con.executemany("DELETE FROM hanchan WHERE id=?", [(hid,) for hid in hids])
                        con.execute("DELETE FROM meets WHERE id=?", (edit_meet_id,))
                        con.commit()
                        st.success("ミートを削除しました。")
                        st.rerun()

con.close()
