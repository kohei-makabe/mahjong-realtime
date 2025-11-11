# app.py
# 麻雀リーグ 精算ツール（シーズン/ミート管理・スマホ最適化）
# 仕様（2025-11 取り決め版）
# - ポイント = 素点(千点) + UMA(順位別pt) + OKAトップ加点(任意) + 役満pt×回数 + 焼き鳥pt×有無
# - 収支(円) = ポイント × レート(円/pt)
# - 返しが25000でも UMA は常時有効（カットしない）
# - 成績の基準は「ポイント」（素点の単独表示は不要）
# - 役満pt・焼き鳥ptは据え置き（1半荘入力で加点/減点できる）
# - 期(Season)→開催(Meet)→半荘の階層で集計
# - ルーム削除（確認付き）

import streamlit as st
import uuid
import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional, List, Tuple

st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
button, .stButton>button { padding: 0.55rem 0.9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.02rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")

DEFAULT_MEMBERS = ["眞壁", "内藤", "森", "浜野", "傅田", "須崎", "中間", "高田"]


# ---------------- DB utils ----------------
def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def table_has_column(con, table: str, col: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def add_column_if_missing(con, table: str, col: str, decl: str):
    if not table_has_column(con, table, col):
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl};")
        except Exception:
            pass

def init_db():
    con = connect()
    cur = con.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS rooms (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            start_points INTEGER NOT NULL,   -- 持ち点
            target_points INTEGER NOT NULL,  -- 返し
            rate_per_pt REAL NOT NULL,       -- レート(円/pt)
            uma1 REAL NOT NULL,              -- 1位UMA(+)
            uma2 REAL NOT NULL,              -- 2位UMA(+)
            uma3 REAL NOT NULL,              -- 3位UMA(−)
            uma4 REAL NOT NULL,              -- 4位UMA(−)
            rounding TEXT NOT NULL,          -- none/round/floor/ceil（100点単位）
            oka_pt REAL NOT NULL DEFAULT 0,  -- トップ加点pt（任意）
            yakuman_pt REAL NOT NULL DEFAULT 0,  -- 役満pt
            yakitori_pt REAL NOT NULL DEFAULT 0  -- 焼き鳥pt（1で加算、負値推奨）
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
            final_points INTEGER NOT NULL,    -- 最終点棒
            rank INTEGER NOT NULL,            -- 着順
            base_pt REAL NOT NULL,            -- 素点(千点)
            uma_pt REAL NOT NULL,             -- UMA
            oka_bonus_pt REAL NOT NULL,       -- OKAトップ加点pt
            addon_pt REAL NOT NULL,           -- 役満/焼き鳥などの加算合計
            total_pt REAL NOT NULL,           -- 合計ポイント
            net_cash REAL NOT NULL,           -- 収支(円)
            yakuman_count INTEGER NOT NULL DEFAULT 0,
            yakitori INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
            FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
            UNIQUE(hanchan_id, player_id)
        );
        """
    )
    # 念のため不足カラムを追加（既存DBの移行想定）
    add_column_if_missing(con, "rooms", "oka_pt", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing(con, "rooms", "yakuman_pt", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing(con, "rooms", "yakitori_pt", "REAL NOT NULL DEFAULT 0")
    add_column_if_missing(con, "results", "yakuman_count", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(con, "results", "yakitori", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(con, "hanchan", "meet_id", "TEXT")

    con.commit()
    con.close()

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id, name, created_at FROM rooms ORDER BY datetime(created_at) DESC;",
        con
    )

def get_room(con, room_id: str) -> Optional[dict]:
    cur = con.execute("SELECT * FROM rooms WHERE id=?", (room_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    d = {cols[i]: row[i] for i in range(len(cols))}
    # 型補正
    for k in ["start_points", "target_points"]:
        d[k] = int(d[k])
    for k in ["rate_per_pt", "uma1", "uma2", "uma3", "uma4", "oka_pt", "yakuman_pt", "yakitori_pt"]:
        d[k] = float(d[k])
    return d

def df_players(con, room_id: str):
    return pd.read_sql_query(
        "SELECT * FROM players WHERE room_id=? ORDER BY joined_at;",
        con, params=(room_id,)
    )

def df_seasons(con, room_id: str):
    return pd.read_sql_query(
        "SELECT * FROM seasons WHERE room_id=? ORDER BY start_date;",
        con, params=(room_id,)
    )

def df_meets(con, season_id: str):
    return pd.read_sql_query(
        "SELECT * FROM meets WHERE season_id=? ORDER BY meet_date;",
        con, params=(season_id,)
    )

def df_hanchan_join(con, room_id: str,
                    season_id: Optional[str] = None,
                    meet_id: Optional[str] = None):
    q = """
        SELECT h.id, h.room_id, h.meet_id, h.started_at, p.display_name,
               r.final_points, r.rank, r.base_pt, r.uma_pt, r.oka_bonus_pt,
               r.addon_pt, r.total_pt, r.net_cash, r.player_id,
               r.yakuman_count, r.yakitori,
               m.name as meet_name, m.meet_date,
               s.name as season_name
        FROM hanchan h
        JOIN results r ON r.hanchan_id = h.id
        JOIN players p ON p.id = r.player_id
        LEFT JOIN meets m ON m.id = h.meet_id
        LEFT JOIN seasons s ON s.id = m.season_id
        WHERE h.room_id=?
    """
    params: List = [room_id]
    if season_id:
        q += " AND s.id=?"
        params.append(season_id)
    if meet_id:
        q += " AND h.meet_id=?"
        params.append(meet_id)
    q += " ORDER BY datetime(h.started_at) DESC, r.rank ASC"
    return pd.read_sql_query(q, con, params=tuple(params))

def ensure_players(con, room_id: str, names: List[str]):
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed = False
    for nm in names:
        nm = nm.strip()
        if nm and nm not in have:
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (str(uuid.uuid4()), room_id, nm, datetime.utcnow().isoformat())
            )
            changed = True
    if changed:
        con.commit()

def apply_rounding(points: int, mode: str) -> int:
    """100点単位の丸め"""
    if mode == "none":
        return int(points)
    if mode == "floor":
        return (points // 100) * 100
    if mode == "ceil":
        return ((points + 99) // 100) * 100
    return int(round(points / 100.0) * 100)  # round


# -------------- Settlement core --------------
def settle_room(room: dict,
                finals: Dict[str, int],
                yakumans: Dict[str, int],
                yakitori_flags: Dict[str, int]) -> Tuple[dict, dict, dict, dict]:
    """
    入力:
      finals: {player_id: final_points}
      yakumans: {player_id: 回数}
      yakitori_flags: {player_id: 0/1}
    出力:
      totals_pt, ranks, rounded_points, nets_yen
    """
    target = room["target_points"]
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    oka_pt = room.get("oka_pt", 0.0)
    ykm_pt = room.get("yakuman_pt", 0.0)
    ykt_pt = room.get("yakitori_pt", 0.0)
    rate = room["rate_per_pt"]
    rounding = room["rounding"]

    # 丸め → 着順
    items = [(pid, apply_rounding(pts, rounding)) for pid, pts in finals.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks = {pid: i + 1 for i, (pid, _) in enumerate(items)}
    rounded = dict(items)

    totals_pt: Dict[str, float] = {}
    nets: Dict[str, float] = {}

    # トップ判定（OKA加点）
    top_pid = items[0][0]

    for pid, fpts in items:
        base_pt = (fpts - target) / 1000.0                        # 素点(千点)
        uma_pt = uma[ranks[pid] - 1]                              # UMA（常時有効）
        oka_bonus = oka_pt if pid == top_pid else 0.0             # OKAトップ加点pt（任意）
        addon_pt = (yakumans.get(pid, 0) * ykm_pt) + (yakitori_flags.get(pid, 0) * ykt_pt)
        total_pt = base_pt + uma_pt + oka_bonus + addon_pt
        totals_pt[pid] = total_pt
        nets[pid] = total_pt * rate

    return totals_pt, ranks, rounded, nets


# ----------------- UI helpers -----------------
def points_input(label: str, key: str, default: int = 25000) -> int:
    return int(st.number_input(label, value=default, step=100, key=f"{key}_num"))

def int_input(label: str, key: str, default: int = 0, minv: int = 0, step: int = 1) -> int:
    return int(st.number_input(label, value=default, step=step, min_value=minv, key=f"{key}_int"))

def checkbox01(label: str, key: str) -> int:
    return 1 if st.checkbox(label, key=key, value=False) else 0


# ================== App main ==================
st.title("🀄 麻雀リーグ 精算ツール")
init_db()

with st.sidebar:
    st.header("ルーム")
    mode = st.radio("操作を選択", ["ルーム作成", "ルーム参加"], horizontal=True)

    if mode == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1, col2 = st.columns(2)
        with col1:
            start_points = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate_per_pt = st.number_input("レート(円/pt)", value=100.0, step=10.0, min_value=0.0)
        with col2:
            uma1 = st.number_input("ウマ1位(+pt)", value=10.0, step=0.5)
            uma2 = st.number_input("ウマ2位(+pt)", value=5.0, step=0.5)
            uma3 = st.number_input("ウマ3位(−pt)", value=-5.0, step=0.5)
            uma4 = st.number_input("ウマ4位(−pt)", value=-10.0, step=0.5)

        rounding = st.selectbox("点数丸め（100点単位）", ["none", "round", "floor", "ceil"], index=0)

        st.markdown("### ー 任意設定 ー")
        oka_pt = st.number_input("OKA pt（トップ加点：pt）", value=0.0, step=0.5)
        yakuman_pt = st.number_input("役満 pt（1回あたり/pt）", value=0.0, step=0.5)
        yakitori_pt = st.number_input("焼き鳥 pt（1で加算。マイナス推奨）", value=0.0, step=0.5)

        creator = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            rid = str(uuid.uuid4())
            con = connect()
            con.execute(
                """INSERT INTO rooms
                   (id,name,created_at,start_points,target_points,rate_per_pt,
                    uma1,uma2,uma3,uma4,rounding,oka_pt,yakuman_pt,yakitori_pt)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (rid, name, datetime.utcnow().isoformat(), int(start_points), int(target_points),
                 float(rate_per_pt), float(uma1), float(uma2), float(uma3), float(uma4),
                 rounding, float(oka_pt), float(yakuman_pt), float(yakitori_pt))
            )
            # ルーム作成者を登録
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

    st.divider()
    st.markdown("### 🗑️ ルーム削除（全データ消失）")
    con = connect()
    rooms_df2 = df_rooms(con)
    if rooms_df2.empty:
        st.caption("まだルームは存在しません。")
    else:
        def fmt2(r):
            ts = r["created_at"].split("T")[0] + " " + r["created_at"][11:16]
            return f'{r["name"]}（{ts}）'
        idx_del = st.selectbox("削除するルームを選択", options=list(range(len(rooms_df2))),
                               format_func=lambda i: fmt2(rooms_df2.iloc[i]), key="del_room")
        rid_del = rooms_df2.iloc[idx_del]["id"]
        confirm = st.checkbox("⚠️ 本当に削除する（すべてのシーズン・成績が失われます）")
        if st.button("ルーム削除実行", disabled=not confirm):
            con.execute("DELETE FROM rooms WHERE id=?", (rid_del,))
            con.commit(); con.close()
            if st.session_state.get("room_id") == rid_del:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("ルームを削除しました。")
            st.rerun()
    con.close()

# ---------------- guard ----------------
if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。")
    st.stop()

st.write(f"**ルーム: {room['name']}**")

players_df = df_players(con, room_id)
st.dataframe(
    players_df[["display_name", "joined_at"]].rename(columns={"display_name": "プレイヤー", "joined_at": "参加"}),
    use_container_width=True, height=240
)

# ---- selectors ----
seasons_df = df_seasons(con, room_id)
sel_season_id = None
sel_meet_id = None
if not seasons_df.empty:
    sel_season_name = st.selectbox("集計対象シーズン", seasons_df["name"].tolist(), key="season_sel_top")
    sel_season_id = seasons_df[seasons_df["name"] == sel_season_name]["id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        sel_meet_name = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist(), key="meet_sel_top")
        sel_meet_id = meets_df[meets_df["name"] == sel_meet_name]["id"].values[0]

tab_input, tab_results, tab_manage = st.tabs(["📝 入力", "📊 成績", "👤 メンバー/設定"])

# ================= 入力 =================
with tab_input:
    st.subheader("半荘入力")

    if (sel_season_id is None) or (sel_meet_id is None):
        st.info("まず『👤 メンバー/設定』でシーズン＆ミートを作成・選択してください。")
    else:
        names = players_df["display_name"].tolist()
        name_to_id = dict(zip(players_df["display_name"], players_df["id"]))
        c1, c2 = st.columns(2); c3, c4 = st.columns(2)
        east  = c1.selectbox("東", names, index=min(0, len(names)-1))
        south = c2.selectbox("南", names, index=min(1, len(names)-1))
        west  = c3.selectbox("西", names, index=min(2, len(names)-1))
        north = c4.selectbox("北", names, index=min(3, len(names)-1))
        picked = [east, south, west, north]
        if len(set(picked)) < 4:
            st.warning("同じ人が重複しています。4人とも別のメンバーを選んでください。")
        else:
            with st.form("hanchan_form"):
                st.write("**最終点（100点単位推奨）**")
                p_e = points_input(east,  key=f"pt_{east}")
                p_s = points_input(south, key=f"pt_{south}")
                p_w = points_input(west,  key=f"pt_{west}")
                p_n = points_input(north, key=f"pt_{north}")

                st.write("**役満回数 / 焼き鳥（チェック=1）**")
                yk_e = int_input(f"役満回数：{east}",  key=f"yk_{east}",  default=0, minv=0)
                yk_s = int_input(f"役満回数：{south}", key=f"yk_{south}", default=0, minv=0)
                yk_w = int_input(f"役満回数：{west}",  key=f"yk_{west}",  default=0, minv=0)
                yk_n = int_input(f"役満回数：{north}", key=f"yk_{north}", default=0, minv=0)

                yt_e = checkbox01(f"焼き鳥：{east}",  key=f"yt_{east}")
                yt_s = checkbox01(f"焼き鳥：{south}", key=f"yt_{south}")
                yt_w = checkbox01(f"焼き鳥：{west}",  key=f"yt_{west}")
                yt_n = checkbox01(f"焼き鳥：{north}", key=f"yt_{north}")

                memo = st.text_input("メモ（任意）", value="")
                submitted = st.form_submit_button("精算を記録")

                if submitted:
                    finals = {
                        name_to_id[east]:  p_e,
                        name_to_id[south]: p_s,
                        name_to_id[west]:  p_w,
                        name_to_id[north]: p_n
                    }
                    yakumans = {
                        name_to_id[east]:  yk_e,
                        name_to_id[south]: yk_s,
                        name_to_id[west]:  yk_w,
                        name_to_id[north]: yk_n
                    }
                    yakitori = {
                        name_to_id[east]:  yt_e,
                        name_to_id[south]: yt_s,
                        name_to_id[west]:  yt_w,
                        name_to_id[north]: yt_n
                    }
                    totals_pt, ranks, rounded, nets = settle_room(room, finals, yakumans, yakitori)

                    hid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO hanchan(id, room_id, meet_id, started_at, finished_at, memo) VALUES (?,?,?,?,?,?)",
                        (hid, room_id, sel_meet_id, datetime.utcnow().isoformat(),
                         datetime.utcnow().isoformat(), memo)
                    )
                    for nm in picked:
                        pid = name_to_id[nm]
                        rid = str(uuid.uuid4())
                        base_pt = (rounded[pid] - room["target_points"]) / 1000.0
                        uma_pt = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]][ranks[pid]-1]
                        oka_bonus = room.get("oka_pt", 0.0) if pid == max(rounded, key=rounded.get) else 0.0
                        addon_pt = yakumans[pid]*room.get("yakuman_pt", 0.0) + yakitori[pid]*room.get("yakitori_pt", 0.0)
                        total_pt = totals_pt[pid]
                        net = nets[pid]
                        con.execute(
                            """INSERT INTO results
                               (id,hanchan_id,player_id,final_points,rank,
                                base_pt,uma_pt,oka_bonus_pt,addon_pt,total_pt,net_cash,
                                yakuman_count,yakitori)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (rid, hid, pid, int(rounded[pid]), int(ranks[pid]),
                             float(base_pt), float(uma_pt), float(oka_bonus), float(addon_pt),
                             float(total_pt), float(net),
                             int(yakumans[pid]), int(yakitori[pid]))
                        )
                    con.commit()
                    st.success("半荘を登録しました。")
                    st.rerun()

# ================= 成績 =================
with tab_results:
    st.subheader("成績 / 履歴")

    scope = "ミート（選択ミートのみ）"
    if sel_season_id:
        scope = st.radio("集計範囲", ["ミート（選択ミートのみ）", "シーズン（全ミート）", "全リーグ（すべて）"],
                         horizontal=True, index=0)
    use_season = (scope == "シーズン（全ミート）")
    use_all = (scope == "全リーグ（すべて）")

    hdf = df_hanchan_join(
        con, room_id,
        season_id=(sel_season_id if (use_season and not use_all) else None),
        meet_id=(None if (use_season or use_all) else sel_meet_id)
    )

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
            "ポイント合計(pt)": g["total_pt"].sum().round(2),
            "平均pt": g["total_pt"].mean().round(2),
            "収支合計(円)": g["net_cash"].sum().round(0),
            "役満回数": g["yakuman_count"].sum().astype(int),
            "焼き鳥回数": g["yakitori"].sum().astype(int),
            "平均順位": g["rank"].mean().round(2),
        }).reset_index()

        # ランキング（左端は順位）
        summary = summary.sort_values(["ポイント合計(pt)", "収支合計(円)"], ascending=[False, False]).reset_index(drop=True)
        summary.insert(0, "順位", summary.index + 1)

        st.write("### 個人成績（累積・基準=ポイント）")
        st.dataframe(summary, use_container_width=True, height=420)

        st.write("### 半荘履歴（主要列）")
        disp = hdf.copy()
        disp = disp.rename(columns={
            "season_name": "シーズン",
            "meet_name": "ミート",
            "display_name": "プレイヤー",
            "rank": "着順",
            "final_points": "点棒(最終点)",
            "base_pt": "素点pt",
            "uma_pt": "UMA",
            "oka_bonus_pt": "OKApt",
            "addon_pt": "加算pt",
            "total_pt": "合計pt",
            "net_cash": "精算(円)",
        })
        st.dataframe(
            disp[["シーズン","ミート","プレイヤー","点棒(最終点)","着順","素点pt","UMA","OKApt","加算pt","合計pt","精算(円)"]],
            use_container_width=True, height=420
        )

        st.download_button(
            "集計CSVをダウンロード",
            summary.to_csv(index=False).encode("utf-8-sig"),
            file_name="mahjong_summary.csv",
            mime="text/csv"
        )

# ================= メンバー/設定 =================
with tab_manage:
    st.subheader("メンバー管理")
    existing_names = players_df["display_name"].tolist()
    candidate_pool = sorted(set(existing_names) | set(DEFAULT_MEMBERS))
    selected_candidates = st.multiselect(
        "候補に入れておくメンバー（未登録はボタンで一括追加できます）",
        options=candidate_pool,
        default=existing_names or DEFAULT_MEMBERS[:4]
    )
    col_add1, col_add2 = st.columns([2,1])
    with col_add1:
        new_name = st.text_input("新メンバー名（1人ずつ）", placeholder="例）Ami")
    with col_add2:
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
            s_end = st.date_input("終了日", value=date(date.today().year,6,30))
            if st.form_submit_button("シーズン作成"):
                sid = str(uuid.uuid4())
                con.execute(
                    "INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at) VALUES (?,?,?,?,?,?)",
                    (sid, room_id, s_name, s_start.isoformat(), s_end.isoformat(), datetime.utcnow().isoformat())
                )
                con.commit()
                st.success("シーズンを作成しました。")
                st.rerun()

    st.divider()
    st.subheader("ミート（開催）")
    if seasons_df.empty:
        st.info("先にシーズンを作成してください。")
    else:
        sel_season_name2 = st.selectbox("対象シーズン", seasons_df["name"].tolist(), key="season_sel_manage")
        sel_season_id2 = seasons_df[seasons_df["name"] == sel_season_name2]["id"].values[0]
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
                        "INSERT INTO meets(id,season_id,name,meet_date,created_at) VALUES (?,?,?,?,?)",
                        (mid, sel_season_id2, m_name, m_date.isoformat(), datetime.utcnow().isoformat())
                    )
                    con.commit()
                    st.success("ミートを作成しました。")
                    st.rerun()

            # 編集/削除
            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_id = meets_df2[meets_df2["name"] == edit_name]["id"].values[0]
                edit_date = meets_df2[meets_df2["name"] == edit_name]["meet_date"].values[0]

                with st.form("meet_edit_form"):
                    new_name = st.text_input("新しいミート名", value=edit_name)
                    new_date = st.date_input("新しい開催日", value=date.fromisoformat(edit_date))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?",
                                    (new_name, new_date.isoformat(), edit_id))
                        con.commit()
                        st.success("ミートを更新しました。")
                        st.rerun()

                with st.expander("⚠️ ミート削除（関連半荘・結果も削除）", expanded=False):
                    sure = st.checkbox("本当に削除する", key="meet_del_confirm")
                    if st.button("このミートを削除", disabled=not sure):
                        cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?", (edit_id,))
                        hids = [r[0] for r in cur.fetchall()]
                        if hids:
                            con.executemany("DELETE FROM results WHERE hanchan_id=?", [(hid,) for hid in hids])
                            con.executemany("DELETE FROM hanchan WHERE id=?", [(hid,) for hid in hids])
                        con.execute("DELETE FROM meets WHERE id=?", (edit_id,))
                        con.commit()
                        st.success("ミートを削除しました。")
                        st.rerun()

st.caption("ポイント= 素点(千点) + UMA + OKA(任意) + 役満pt×回数 + 焼き鳥pt×有無 / 収支=ポイント×レート。UMAは返しが25000でも常時有効。")
con.close()
