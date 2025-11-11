# app.py
# 麻雀リーグ（フル機能統合版）
# - 期(Season)→開催(Meet)→半荘 の階層管理
# - 代表固定なし：誰でも入力OK（ルームからプルダウン）
# - 既定メンバー候補＋その場で追加、未登録の一括追加
# - ルーム参加は「既存ルーム一覧から選択」
# - ルーム削除（確認付き）
# - ミートの名称/日付 修正・削除（関連半荘/結果も整理）
# - 成績：素点(千点)/ポイント(pt)/収支(円) を表示、ミート/シーズン/全リーグの切替
# - ランキング表は左端「順位」列表示（インデックス非表示）
# - スマホ配慮（centered、初期サイドバー折りたたみ、軽量CSS）
# - UMAとOKAはルームごとに設定可能（OKAは「なし/トップにpt加算/トップに円加算」を選択）
# - 丸め設定：none/round/floor/ceil を最終点に適用して順位確定

import streamlit as st
import uuid
import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional

st.set_page_config(
    page_title="麻雀リーグ精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# 軽いモバイル向けCSS
st.markdown("""
<style>
button, .stButton>button { padding: 0.6rem 0.9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.05rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")

# 既定メンバー（初期候補）
DEFAULT_MEMBERS = ["眞壁", "内藤", "森", "浜野", "傅田", "須崎", "中間", "高田", "内藤士"]


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
            meet_id TEXT,
            FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE,
            FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE SET NULL
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
    # --- 後方互換用：OKA設定（モード/pt/yen）をroomsに追加 ---
    if not table_has_column(con, "rooms", "oka_mode"):
        con.execute("ALTER TABLE rooms ADD COLUMN oka_mode TEXT DEFAULT 'none';")
    if not table_has_column(con, "rooms", "oka_pt"):
        con.execute("ALTER TABLE rooms ADD COLUMN oka_pt REAL DEFAULT 0;")
    if not table_has_column(con, "rooms", "oka_yen"):
        con.execute("ALTER TABLE rooms ADD COLUMN oka_yen REAL DEFAULT 0;")
    con.commit()
    con.close()


def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id, name, created_at FROM rooms ORDER BY datetime(created_at) DESC;",
        con
    )


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
    """
    最終点(丸め適用)で着順→
    素点pt = (最終点 - 返し) / 1000
    total_pt = 素点pt + UMA(順位) + (OKA_pt if トップかつモードpt)
    収支(円) = total_pt × レート + (OKA_yen if トップかつモードyen)
    """
    target = room["target_points"]
    rate = room["rate_per_1000"]
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    rounding = room["rounding"]
    oka_mode = room.get("oka_mode", "none")  # 'none' | 'pt' | 'yen'
    oka_pt = float(room.get("oka_pt", 0) or 0)
    oka_yen = float(room.get("oka_yen", 0) or 0)

    # 100点丸めなどを適用してから着順確定
    items = [(pid, apply_rounding(pts, rounding)) for pid, pts in finals.items()]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks = {pid: i + 1 for i, (pid, _) in enumerate(items)}

    nets_yen = {}
    rounded_finals = {}
    for pid, pts in items:
        rounded_finals[pid] = pts
        base_pt = (pts - target) / 1000.0     # 素点pt
        total_pt = base_pt + uma[ranks[pid] - 1]
        if ranks[pid] == 1 and oka_mode == "pt":
            total_pt += oka_pt
        net = total_pt * rate
        if ranks[pid] == 1 and oka_mode == "yen":
            net += oka_yen
        nets_yen[pid] = net

    return nets_yen, ranks, rounded_finals


def row_to_dict(row, columns):
    return {columns[i]: row[i] for i in range(len(columns))}


def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?;", (room_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    d = row_to_dict(row, cols)
    # 型補正
    for k in ["start_points", "target_points"]:
        d[k] = int(d[k])
    for k in ["rate_per_1000", "uma1", "uma2", "uma3", "uma4", "oka_pt", "oka_yen"]:
        d[k] = float(d.get(k, 0) or 0)
    d["oka_mode"] = d.get("oka_mode", "none")
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


def df_hanchan_join(con, room_id, season_id: Optional[str] = None, meet_id: Optional[str] = None):
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


def ensure_players(con, room_id: str, names: list[str]) -> None:
    """roomに未登録のdisplay_nameがあれば追加する"""
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed = False
    for name in names:
        if name and name not in have:
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (str(uuid.uuid4()), room_id, name, datetime.utcnow().isoformat())
            )
            changed = True
    if changed:
        con.commit()


# 点数入力（フォーム内で安全：number_inputのみ）
def points_input(label: str, key: str, default: int = 25000) -> int:
    return int(st.number_input(label, value=default, step=100, key=f"{key}_num"))


# --------------- Sidebar：Room ---------------
st.title("🀄 麻雀リーグ精算ツール（フル版）")
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
            rate_per_1000 = st.number_input("レート(円/千点)", value=10.0, step=1.0)
        with col2:
            uma1 = st.number_input("ウマ 1位(+千点)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ 2位(+千点)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ 3位(−千点)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ 4位(−千点)", value=-10.0, step=1.0)
        rounding = st.selectbox("点数丸め", ["none", "round", "floor", "ceil"], index=0)

        st.markdown("#### OKA（トップボーナス）の扱い")
        oka_mode = st.selectbox(
            "OKAモード",
            ["none（なし）", "pt（トップにpt加算）", "yen（トップに円加算）"],
            index=0
        )
        col_ok1, col_ok2 = st.columns(2)
        with col_ok1:
            oka_pt = st.number_input("OKA pt（千点換算）", value=0.0, step=1.0, help="例：Mリーグ等の+20ptなら20")
        with col_ok2:
            oka_yen = st.number_input("OKA 円（直接加算）", value=0.0, step=100.0, help="トップに現金加算したい場合のみ使用")

        creator = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            room_id = str(uuid.uuid4())
            con = connect()
            con.execute(
                """INSERT INTO rooms(
                    id,name,created_at,start_points,target_points,rate_per_1000,
                    uma1,uma2,uma3,uma4,rounding,oka_mode,oka_pt,oka_yen
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
                (room_id, name, datetime.utcnow().isoformat(),
                 start_points, target_points, rate_per_1000,
                 uma1, uma2, uma3, uma4, rounding,
                 "none" if oka_mode.startswith("none") else ("pt" if oka_mode.startswith("pt") else "yen"),
                 oka_pt, oka_yen)
            )
            # ルーム作成者をとりあえず登録
            pid = str(uuid.uuid4())
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (pid, room_id, creator, datetime.utcnow().isoformat())
            )
            con.commit(); con.close()
            st.session_state["room_id"] = room_id
            st.session_state["player_id"] = pid
            st.success(f"作成OK！ Room ID: {room_id}")

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
                # 既に同名がいれば既存ID、なければ作成
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
                st.success("参加しました！")
                st.rerun()
        con.close()

    # --- ルーム削除機能（確認付き） ---
    st.divider()
    st.markdown("### 🗑️ ルーム削除")
    con = connect()
    rooms_df2 = df_rooms(con)
    if rooms_df2.empty:
        st.caption("まだルームは存在しません。")
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
            st.success("ルームを削除しました。")
            # もし削除したルームが現在選択中ならセッションを初期化
            if st.session_state.get("room_id") == selected_room_id_del:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.rerun()
    con.close()

st.caption("誰でも入力OK。シーズン→ミート→半荘で管理します。")

if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。")
    st.stop()

# 参加者一覧（簡易）
players_df = df_players(con, room_id)
st.write(f"**ルーム: {room['name']}**")
st.dataframe(
    players_df[["display_name", "joined_at"]].rename(columns={"display_name": "プレイヤー", "joined_at": "参加"}),
    use_container_width=True, height=260
)

# ---- 共通セレクタ（シーズン/ミート） ----
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

# ---------------- Tabs ----------------
tab_input, tab_results, tab_manage = st.tabs(["📝 入力", "📊 成績", "👤 メンバー/設定"])

# ========== 入力タブ ==========
with tab_input:
    st.subheader("半荘入力（誰でも）")

    if not seasons_df.empty and sel_season_id and sel_meet_id:
        names = players_df["display_name"].tolist()
        name_to_id = dict(zip(players_df["display_name"], players_df["id"]))
        # 東南西北の選択（重複防止）
        colE, colS = st.columns(2)
        colW, colN = st.columns(2)
        east  = colE.selectbox("東", names, index=min(0, len(names)-1))
        south = colS.selectbox("南", names, index=min(1, len(names)-1))
        west  = colW.selectbox("西", names, index=min(2, len(names)-1))
        north = colN.selectbox("北", names, index=min(3, len(names)-1))
        picked = [east, south, west, north]
        if len(set(picked)) < 4:
            st.warning("同じ人が重複しています。4人とも別のメンバーを選んでください。")
        else:
            with st.form("hanchan_form"):
                finals = {}
                st.write("**最終点（100点単位推奨）**")
                p_e = points_input(east,  key=f"pt_{east}")
                p_s = points_input(south, key=f"pt_{south}")
                p_w = points_input(west,  key=f"pt_{west}")
                p_n = points_input(north, key=f"pt_{north}")
                finals[name_to_id[east]]  = p_e
                finals[name_to_id[south]] = p_s
                finals[name_to_id[west]]  = p_w
                finals[name_to_id[north]] = p_n

                memo = st.text_input("メモ（任意）", value="")
                submitted = st.form_submit_button("精算を記録")

                if submitted:
                    nets, ranks, rounded_finals = settlement_for_room(room, finals)
                    hid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO hanchan(id, room_id, started_at, finished_at, memo, meet_id) VALUES (?,?,?,?,?,?);",
                        (hid, room_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), memo, sel_meet_id)
                    )
                    for name in picked:
                        pid = name_to_id[name]
                        rid = str(uuid.uuid4())
                        con.execute(
                            "INSERT INTO results(id, hanchan_id, player_id, final_points, rank, net_cash) VALUES (?,?,?,?,?,?);",
                            (rid, hid, pid, int(rounded_finals[pid]), int(ranks[pid]), float(nets[pid]))
                        )
                    con.commit()
                    st.success("半荘を登録しました！")
    else:
        st.info("まず『👤 メンバー/設定』でシーズンとミートを作成・選択してください。")

# ========== 成績タブ ==========
with tab_results:
    st.subheader("成績 / 履歴")

    # 集計単位の切り替え：ミート／シーズン／全リーグ
    scope = "ミート"
    if sel_season_id:
        scope = st.radio(
            "集計範囲",
            ["ミート（選択ミートのみ）", "シーズン（全ミート）", "全リーグ（すべて）"],
            horizontal=True,
            index=0 if sel_meet_id else 1
        )
    use_season = (scope == "シーズン（全ミート）") or (sel_meet_id is None and scope != "全リーグ（すべて）")
    hdf = df_hanchan_join(
        con,
        room_id,
        None if scope == "全リーグ（すべて）" else (sel_season_id if use_season else None),
        None if (use_season or scope == "全リーグ（すべて）") else sel_meet_id
    )

    if hdf.empty:
        st.info("まだ成績がありません。")
    else:
        # 数値化と素点
        hdf["final_points"] = pd.to_numeric(hdf["final_points"], errors="coerce").fillna(0).astype(int)
        target = int(room["target_points"])
        rate = float(room["rate_per_1000"])
        hdf["素点(千点)"] = ((hdf["final_points"] - target) / 1000.0).round(2)

        # 参考：ポイント(pt)を逆算（ウマとOKAモードに基づく） ※履歴表示用
        # rank→uma値のマップ
        rank_to_uma = {1: room["uma1"], 2: room["uma2"], 3: room["uma3"], 4: room["uma4"]}
        oka_mode = room.get("oka_mode", "none")
        oka_pt = float(room.get("oka_pt", 0) or 0)
        # ポイント(pt)（= 素点 + ウマ + (トップならOKA_pt)）
        hdf["pt(千点)"] = hdf.apply(
            lambda r: round(
                ((r["final_points"] - target) / 1000.0) + rank_to_uma.get(int(r["rank"]), 0) + (oka_pt if (oka_mode == "pt" and int(r["rank"]) == 1) else 0)
            , 2),
            axis=1
        )

        g = hdf.groupby("display_name")
        summary = pd.DataFrame({
            "回数": g["rank"].count(),
            "1位": g["rank"].apply(lambda s: (s == 1).sum()),
            "2位": g["rank"].apply(lambda s: (s == 2).sum()),
            "3位": g["rank"].apply(lambda s: (s == 3).sum()),
            "4位": g["rank"].apply(lambda s: (s == 4).sum()),
            "素点合計(千点)": g["素点(千点)"].sum().round(2),
            "平均素点(千点)": g["素点(千点)"].mean().round(2),
            "pt合計(千点)": g["pt(千点)"].sum().round(2),
            "収支合計(円)": g["net_cash"].sum().round(0),
            "平均順位": g["rank"].mean().round(2),
        }).reset_index()

        # 並べ替え（収支→1位数→平均順位）後に連番の順位列を付与、インデックスは非表示
        summary = summary.sort_values(
            ["収支合計(円)", "1位", "平均順位"], ascending=[False, False, True]
        ).reset_index(drop=True)
        summary.insert(0, "順位", summary.index + 1)

        st.write("### 個人成績（累積）")
        st.dataframe(summary, use_container_width=True, height=380, hide_index=True)

        st.write("### 半荘履歴（主要列）")
        disp = hdf.copy()
        disp["精算(円)"] = disp["net_cash"].map(lambda x: f"{x:,.0f}")
        disp["点棒(最終点)"] = disp["final_points"].map(lambda x: f"{x:,}")
        disp = disp.rename(columns={
            "season_name": "シーズン",
            "meet_name": "ミート",
            "display_name": "プレイヤー",
            "rank": "着順",
            "素点(千点)": "素点(千点)",
            "pt(千点)": "ポイント(千点)"
        })
        st.dataframe(
            disp[["シーズン", "ミート", "プレイヤー", "点棒(最終点)", "素点(千点)", "ポイント(千点)", "着順", "精算(円)"]],
            use_container_width=True, height=440
        )

        st.write("### 対人（ヘッドトゥヘッド）")
        rows = []
        for hid, gg in hdf.groupby("id"):
            net = gg.set_index("player_id")["net_cash"]
            pids = list(net.index)
            names_map = gg.set_index("player_id")["display_name"].to_dict()
            for i in range(len(pids)):
                for j in range(i + 1, len(pids)):
                    a, b = pids[i], pids[j]
                    rows.append({"A": names_map[a], "B": names_map[b],
                                 "同卓回数": 1, "A基準ネット(円)": (net[a] - net[b]) / 2.0})
        if rows:
            h2h = pd.DataFrame(rows).groupby(["A", "B"]).agg({"同卓回数": "sum", "A基準ネット(円)": "sum"}).reset_index()
            st.dataframe(h2h, use_container_width=True)

        st.download_button(
            "成績CSVをダウンロード",
            summary.to_csv(index=False).encode("utf-8-sig"),
            file_name="summary.csv",
            mime="text/csv"
        )

# ========== メンバー/設定タブ ==========
with tab_manage:
    st.subheader("メンバー管理")
    existing_names = players_df["display_name"].tolist()
    candidate_pool = sorted(set(existing_names) | set(DEFAULT_MEMBERS))
    selected_candidates = st.multiselect(
        "候補に入れておくメンバー（未登録はボタンで一括追加できます）",
        options=candidate_pool,
        default=existing_names or DEFAULT_MEMBERS[:4]
    )
    col_add1, col_add2 = st.columns([2, 1])
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
    colA, colB = st.columns([2, 1])
    with colA:
        st.dataframe(
            seasons_df.rename(columns={"name": "シーズン名", "start_date": "開始日", "end_date": "終了日"}),
            use_container_width=True, height=260
        )
    with colB:
        with st.form("season_form"):
            s_name = st.text_input("シーズン名", value=f"{date.today().year} 前期")
            s_start = st.date_input("開始日", value=date(date.today().year, 1, 1))
            s_end = st.date_input("終了日", value=date(date.today().year, 6, 30))
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
        sel_season_id2 = seasons_df[seasons_df["name"] == sel_season_name2]["id"].values[0]
        meets_df2 = df_meets(con, sel_season_id2)
        colM1, colM2 = st.columns([2, 1])
        with colM1:
            st.dataframe(
                meets_df2.rename(columns={"name": "ミート名", "meet_date": "開催日"}),
                use_container_width=True, height=260
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

            # --- ミートの修正／削除 ---
            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                # 編集対象のミートを選択
                edit_meet_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_meet_id = meets_df2[meets_df2["name"] == edit_meet_name]["id"].values[0]
                edit_meet_date = meets_df2[meets_df2["name"] == edit_meet_name]["meet_date"].values[0]

                with st.form("meet_edit_form"):
                    new_name = st.text_input("新しいミート名", value=edit_meet_name)
                    new_date = st.date_input("新しい開催日", value=date.fromisoformat(edit_meet_date))
                    do_update = st.form_submit_button("更新を保存")
                    if do_update:
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?;",
                                    (new_name, new_date.isoformat(), edit_meet_id))
                        con.commit()
                        st.success("ミート情報を更新しました。")
                        st.rerun()

                # 危険操作：削除
                with st.expander("⚠️ ミート削除（関連半荘・結果も削除）", expanded=False):
                    sure = st.checkbox("本当に削除する", key="meet_del_confirm")
                    if st.button("このミートを削除", disabled=not sure):
                        # 関連する半荘→結果も削除（resultsはCASCADEだが、meet紐づきhanchanを明示削除）
                        cur = con.execute("SELECT id FROM hanchan WHERE meet_id=?;", (edit_meet_id,))
                        hids = [r[0] for r in cur.fetchall()]
                        if hids:
                            con.executemany("DELETE FROM results WHERE hanchan_id=?;", [(hid,) for hid in hids])
                            con.executemany("DELETE FROM hanchan WHERE id=?;", [(hid,) for hid in hids])
                        con.execute("DELETE FROM meets WHERE id=?;", (edit_meet_id,))
                        con.commit()
                        st.success("ミートを削除しました。")
                        st.rerun()

st.caption("式: 素点 = (最終点 - 返し)/1000,  pt = 素点 + UMA(+OKA pt),  収支 = pt×レート (+OKA円)。丸めは最終点に適用。")
con.close()
