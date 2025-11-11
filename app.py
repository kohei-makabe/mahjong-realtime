# app.py
# 麻雀リーグ（シーズン/ミート）成績ボード：スマホ最適化 / 代表不要 / ルーム削除 / ミート編集削除
# 仕様:
# - 階層: ルーム → シーズン(前期/後期など) → ミート(第1回…) → 半荘
# - 誰でも入力OK（代表固定なし）
# - 参加は「既存ルーム一覧から選択」
# - ルーム作成: 持ち点/返し/レート/点数丸め/UMA/OKA を設定（UMA/OKAは保存のみ。精算には使わない）
# - 入力: 東南西北のプレイヤー選択、最終点、メモ、役満回数、焼き鳥（半荘でカウント）
# - 精算: 収支(円) = 素点(千点) × レート、素点 = (最終点 - 返し)/1000。順位は最終点で決定
# - 成績表示: ミート / シーズン（全ミート） / 全リーグ（すべて）で集計切替
# - 個人成績: 回数・着順数・収支合計・素点合計/平均・平均順位・役満回数合計・焼き鳥回数
# - 半荘履歴: シーズン/ミート/プレイヤー/点棒(最終点)/素点/着順/精算を表示
# - ミート編集＆削除（関連半荘/結果も整理）
# - ルーム削除（確認付き、全データ削除）
# - 既存DBにも安全対応（不足列を自動追加 & 存在列だけでINSERT）

import streamlit as st
import uuid
import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional, List

# ---------------- Page & Style ----------------
st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)
st.markdown("""
<style>
/* モバイル配慮：ボタン/入力のタップ領域を広く */
button, .stButton>button { padding: 0.6rem 0.9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.05rem; }
.block-container { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path("mahjong.db")

# 初期候補メンバー
DEFAULT_MEMBERS = ["眞壁", "内藤", "森", "浜野", "傅田", "須崎", "中間", "高田", "内藤士"]

# ---------------- DB helpers ----------------
def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def table_has_column(con, table: str, col: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def get_table_columns(con, table: str) -> List[str]:
    cur = con.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

def insert_dynamic(con, table: str, data: dict):
    cols_exist = get_table_columns(con, table)
    cols = [c for c in data.keys() if c in cols_exist]
    vals = [data[c] for c in cols]
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders});"
    con.execute(sql, vals)

def init_db():
    con = connect()
    cur = con.cursor()

    # rooms
    cur.execute("""
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
    """)
    # 追加列: OKA 設定（保存のみ）
    try:
        if not table_has_column(con, "rooms", "oka_mode"):
            con.execute("ALTER TABLE rooms ADD COLUMN oka_mode TEXT DEFAULT 'none';")
        if not table_has_column(con, "rooms", "oka_pt"):
            con.execute("ALTER TABLE rooms ADD COLUMN oka_pt REAL DEFAULT 0;")
        if not table_has_column(con, "rooms", "oka_yen"):
            con.execute("ALTER TABLE rooms ADD COLUMN oka_yen REAL DEFAULT 0;")
    except Exception:
        pass

    # players
    cur.execute("""
    CREATE TABLE IF NOT EXISTS players (
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        display_name TEXT NOT NULL,
        joined_at TEXT NOT NULL,
        UNIQUE(room_id, display_name),
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
    );
    """)

    # seasons
    cur.execute("""
    CREATE TABLE IF NOT EXISTS seasons (
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        name TEXT NOT NULL,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE
    );
    """)

    # meets
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meets (
        id TEXT PRIMARY KEY,
        season_id TEXT NOT NULL,
        name TEXT NOT NULL,
        meet_date TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(season_id) REFERENCES seasons(id) ON DELETE CASCADE
    );
    """)

    # hanchan（meet 紐付け）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hanchan (
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        memo TEXT,
        meet_id TEXT,
        FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE,
        FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE
    );
    """)

    # results（役満回数・焼き鳥も保持）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS results (
        id TEXT PRIMARY KEY,
        hanchan_id TEXT NOT NULL,
        player_id TEXT NOT NULL,
        final_points INTEGER NOT NULL,
        rank INTEGER NOT NULL,
        net_cash REAL NOT NULL,
        yakuman_count INTEGER NOT NULL DEFAULT 0,
        yakitori INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
        FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
        UNIQUE(hanchan_id, player_id)
    );
    """)
    # 既存テーブルへの追加列（安全に）
    try:
        if not table_has_column(con, "results", "yakuman_count"):
            con.execute("ALTER TABLE results ADD COLUMN yakuman_count INTEGER NOT NULL DEFAULT 0;")
        if not table_has_column(con, "results", "yakitori"):
            con.execute("ALTER TABLE results ADD COLUMN yakitori INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass

    con.commit()
    con.close()

def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id, name, created_at FROM rooms ORDER BY datetime(created_at) DESC;",
        con
    )

def row_to_dict(row, columns):
    return {columns[i]: row[i] for i in range(len(columns))}

def get_room(con, room_id):
    cur = con.execute("SELECT * FROM rooms WHERE id=?;", (room_id,))
    row = cur.fetchone()
    if not row: return None
    cols = [d[0] for d in cur.description]
    d = row_to_dict(row, cols)
    # 型整形
    for k in ["start_points", "target_points"]:
        d[k] = int(d[k])
    for k in ["rate_per_1000", "uma1","uma2","uma3","uma4","oka_pt","oka_yen"]:
        if k in d and d[k] is not None:
            d[k] = float(d[k])
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
           r.yakuman_count, r.yakitori,
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
    q += " ORDER BY datetime(h.started_at) DESC, r.rank ASC;"
    return pd.read_sql_query(q, con, params=tuple(params))

def apply_rounding(points: int, mode: str) -> int:
    if mode == "none":
        return int(points)
    if mode == "floor":
        return (points // 100) * 100
    if mode == "ceil":
        return ((points + 99) // 100) * 100
    return int(round(points / 100.0) * 100)  # round

def ensure_players(con, room_id: str, names: List[str]) -> None:
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    changed = False
    for name in names:
        n = name.strip()
        if n and n not in have:
            insert_dynamic(con, "players", {
                "id": str(uuid.uuid4()),
                "room_id": room_id,
                "display_name": n,
                "joined_at": datetime.utcnow().isoformat(),
            })
            changed = True
    if changed:
        con.commit()

def points_input(label: str, key: str, default: int = 25000) -> int:
    return int(st.number_input(label, value=default, step=100, key=f"{key}_num"))

# ---------------- App Body ----------------
st.title("🀄 麻雀リーグ 精算ツール")
init_db()

# --------- Sidebar: Room create / join / delete ----------
with st.sidebar:
    st.header("ルーム")
    action = st.radio("操作を選択", ["ルーム作成", "ルーム参加"], horizontal=True)

    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1, col2 = st.columns(2)
        with col1:
            start_points = st.number_input("持ち点(開始)", value=25000, step=100)
            target_points = st.number_input("返し(ターゲット)", value=25000, step=100)
            rate_per_1000 = st.number_input("レート(円/千点)", value=100.0, step=10.0, help="収支=素点×このレート")
        with col2:
            uma1 = st.number_input("ウマ 1位(+千点)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ 2位(+千点)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ 3位(−千点)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ 4位(−千点)", value=-10.0, step=1.0)
        rounding = st.selectbox("点数丸め", ["none", "round", "floor", "ceil"], index=0)

        st.markdown("#### OKA（ポイント用・収支には使いません）")
        oka_mode = st.selectbox("OKAモード", ["none", "top_fixed_pt", "top_fixed_yen"], index=0,
                                help="保存のみ。精算(円)には未使用です。")
        col3, col4 = st.columns(2)
        with col3:
            oka_pt = st.number_input("OKA pt（トップ加算）", value=0.0, step=1.0)
        with col4:
            oka_yen = st.number_input("OKA 円（参考）", value=0.0, step=100.0)

        creator = st.text_input("あなたの表示名", value="あなた")
        if st.button("ルーム作成"):
            rid = str(uuid.uuid4())
            con = connect()
            # 不足列は init_db で付与済みだが、念のため
            try:
                if not table_has_column(con, "rooms", "oka_mode"):
                    con.execute("ALTER TABLE rooms ADD COLUMN oka_mode TEXT DEFAULT 'none';")
                if not table_has_column(con, "rooms", "oka_pt"):
                    con.execute("ALTER TABLE rooms ADD COLUMN oka_pt REAL DEFAULT 0;")
                if not table_has_column(con, "rooms", "oka_yen"):
                    con.execute("ALTER TABLE rooms ADD COLUMN oka_yen REAL DEFAULT 0;")
            except Exception:
                pass

            insert_dynamic(con, "rooms", {
                "id": rid, "name": name, "created_at": datetime.utcnow().isoformat(),
                "start_points": start_points, "target_points": target_points,
                "rate_per_1000": rate_per_1000, "uma1": uma1, "uma2": uma2,
                "uma3": uma3, "uma4": uma4, "rounding": rounding,
                "oka_mode": oka_mode, "oka_pt": oka_pt, "oka_yen": oka_yen,
            })
            pid = str(uuid.uuid4())
            insert_dynamic(con, "players", {
                "id": pid, "room_id": rid, "display_name": creator,
                "joined_at": datetime.utcnow().isoformat()
            })
            con.commit(); con.close()
            st.session_state["room_id"] = rid
            st.session_state["player_id"] = pid
            st.success(f"作成OK！ Room ID: {rid}")

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
                if row: pid = row[0]
                else:
                    pid = str(uuid.uuid4())
                    insert_dynamic(con, "players", {
                        "id": pid, "room_id": selected_room_id,
                        "display_name": name_in, "joined_at": datetime.utcnow().isoformat()
                    })
                    con.commit()
                st.session_state["room_id"] = selected_room_id
                st.session_state["player_id"] = pid
                st.success("参加しました！")
                st.rerun()
        con.close()

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
            if st.session_state.get("room_id") == selected_room_id_del:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("ルームを削除しました。")
            st.rerun()
    con.close()

st.caption("収支=素点(千点)×レート、素点=(最終点-返し)/1000。順位は最終点で決定。")

if "room_id" not in st.session_state:
    st.info("左のサイドバーからルームを作成/参加してください。")
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
room = get_room(con, room_id)
if not room:
    st.error("ルームが見つかりません。")
    st.stop()

# 参加者リスト
players_df = df_players(con, room_id)
st.write(f"**ルーム: {room['name']}**")
st.dataframe(
    players_df[["display_name", "joined_at"]].rename(columns={"display_name":"プレイヤー","joined_at":"参加"}),
    use_container_width=True, height=240
)

# 共通セレクタ
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

# ---------------- 入力タブ ----------------
with tab_input:
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

        picked = [east, south, west, north]
        if len(set(picked)) < 4:
            st.warning("同じ人が重複しています。4人とも別のメンバーを選んでください。")
        else:
            with st.form("hanchan_form"):
                st.write("**最終点（100点単位推奨）**")
                p_e = points_input(east,  key=f"pt_{east}",  default=room["target_points"])
                p_s = points_input(south, key=f"pt_{south}", default=room["target_points"])
                p_w = points_input(west,  key=f"pt_{west}",  default=room["target_points"])
                p_n = points_input(north, key=f"pt_{north}", default=room["target_points"])

                st.markdown("**役満回数 / 焼き鳥（半荘でカウント）**")
                c1,c2,c3,c4 = st.columns(4)
                ykm = {}
                ytr = {}
                for i,(n,c) in enumerate(zip(picked,[c1,c2,c3,c4])):
                    ykm[n] = c.number_input(f"{n} 役満回数", min_value=0, max_value=9, value=0, step=1, key=f"ykm_{n}")
                    ytr[n] = c.checkbox(f"{n} 焼き鳥", value=False, key=f"ytr_{n}")

                memo = st.text_input("メモ（任意）", value="")
                submitted = st.form_submit_button("精算を記録")

                if submitted:
                    # 丸め → 順位 → 素点/収支
                    finals_raw = {
                        name_to_id[east]: p_e, name_to_id[south]: p_s,
                        name_to_id[west]: p_w, name_to_id[north]: p_n
                    }
                    finals_rounded = {pid: apply_rounding(v, room["rounding"]) for pid,v in finals_raw.items()}
                    # 順位（最終点降順）
                    order = sorted(finals_rounded.items(), key=lambda x: x[1], reverse=True)
                    ranks = {pid: i+1 for i,(pid,_) in enumerate(order)}

                    target = room["target_points"]
                    rate   = room["rate_per_1000"]
                    # 収支=素点×レート
                    nets = {}
                    for pid, pts in finals_rounded.items():
                        soten = (pts - target)/1000.0
                        nets[pid] = soten * rate

                    hid = str(uuid.uuid4())
                    insert_dynamic(con, "hanchan", {
                        "id": hid, "room_id": room_id,
                        "started_at": datetime.utcnow().isoformat(),
                        "finished_at": datetime.utcnow().isoformat(),
                        "memo": memo, "meet_id": sel_meet_id
                    })
                    # 役満/焼き鳥の保存
                    for n in picked:
                        pid = name_to_id[n]
                        insert_dynamic(con, "results", {
                            "id": str(uuid.uuid4()),
                            "hanchan_id": hid, "player_id": pid,
                            "final_points": int(finals_rounded[pid]),
                            "rank": int(ranks[pid]),
                            "net_cash": float(nets[pid]),
                            "yakuman_count": int(ykm[n]),
                            "yakitori": 1 if ytr[n] else 0
                        })
                    con.commit()
                    st.success("半荘を登録しました！")
    else:
        st.info("まず『👤 メンバー/設定』でシーズンとミートを作成・選択してください。")

# ---------------- 成績タブ ----------------
with tab_results:
    st.subheader("成績 / 履歴")

    scope = "ミート（選択ミートのみ）"
    opt = ["ミート（選択ミートのみ）", "シーズン（全ミート）", "全リーグ（すべて）"]
    idx_default = 0 if sel_meet_id else (1 if sel_season_id else 2)
    scope = st.radio("集計範囲", opt, horizontal=True, index=idx_default)
    if scope == "全リーグ（すべて）":
        hdf = df_hanchan_join(con, room_id, None, None)
    elif scope == "シーズン（全ミート）":
        hdf = df_hanchan_join(con, room_id, sel_season_id, None)
    else:
        hdf = df_hanchan_join(con, room_id, None, sel_meet_id)

    if hdf.empty:
        st.info("まだ成績がありません。")
    else:
        target = room["target_points"]
        hdf["素点(千点)"] = (hdf["final_points"] - target)/1000.0

        g = hdf.groupby("display_name")
        summary = pd.DataFrame({
            "回数": g["rank"].count(),
            "1位": g["rank"].apply(lambda s: (s==1).sum()),
            "2位": g["rank"].apply(lambda s: (s==2).sum()),
            "3位": g["rank"].apply(lambda s: (s==3).sum()),
            "4位": g["rank"].apply(lambda s: (s==4).sum()),
            "収支合計(円)": g["net_cash"].sum().round(0),
            "素点合計(千点)": g["素点(千点)"].sum().round(2),
            "平均素点(千点)": g["素点(千点)"].mean().round(2),
            "平均順位": g["rank"].mean().round(2),
            "役満(回)": g["yakuman_count"].sum(),
            "焼き鳥(回)": g["yakitori"].sum(),
        }).reset_index()

        # 並び替え（収支降順）＆順位列
        summary = summary.sort_values(["収支合計(円)", "素点合計(千点)"], ascending=[False, False]).reset_index(drop=True)
        summary.insert(0, "順位", summary.index + 1)

        st.write("### 個人成績（累積）")
        st.dataframe(summary, use_container_width=True, height=420)

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
            "yakuman_count": "役満(回)",
            "yakitori": "焼き鳥(有=1)",
        })
        st.dataframe(
            disp[["シーズン","ミート","プレイヤー","点棒(最終点)","素点(千点)","着順","役満(回)","焼き鳥(有=1)","精算(円)"]],
            use_container_width=True, height=420
        )

        st.download_button(
            "個人成績CSVをダウンロード",
            summary.to_csv(index=False).encode("utf-8-sig"),
            file_name="summary.csv",
            mime="text/csv"
        )

# ---------------- メンバー/設定タブ ----------------
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
                insert_dynamic(con, "seasons", {
                    "id": sid, "room_id": room_id, "name": s_name,
                    "start_date": s_start.isoformat(), "end_date": s_end.isoformat(),
                    "created_at": datetime.utcnow().isoformat()
                })
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
                    insert_dynamic(con, "meets", {
                        "id": mid, "season_id": sel_season_id2, "name": m_name,
                        "meet_date": m_date.isoformat(), "created_at": datetime.utcnow().isoformat()
                    })
                    con.commit()
                    st.rerun()

            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_meet_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_meet_id = meets_df2[meets_df2["name"] == edit_meet_name]["id"].values[0]
                edit_meet_date = meets_df2[meets_df2["name"] == edit_meet_name]["meet_date"].values[0]

                with st.form("meet_edit_form"):
                    new_name = st.text_input("新しいミート名", value=edit_meet_name)
                    new_date = st.date_input("新しい開催日", value=date.fromisoformat(edit_meet_date))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?;",
                                    (new_name, new_date.isoformat(), edit_meet_id))
                        con.commit()
                        st.success("ミート情報を更新しました。")
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
                        st.success("ミートを削除しました。")
                        st.rerun()

st.caption("※ UMA/OKA は保存のみ（将来のポイント運用向け）。収支は素点×レートで計算。")
con.close()
