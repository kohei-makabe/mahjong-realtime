# app.py
# 麻雀リーグ 精算ツール（スマホ最適化 / ルーム・シーズン・ミート / 役満・焼き鳥 / 通算集計）
# 仕様（重要）:
# - 点棒(最終点) → 素点pt = (最終点 - 返し) / 1000   ※返し=持ち点のときUMAは無効（一般的な運用に合わせる）
# - UMAは 返し != 持ち点 のときのみ加算（例: 5-10、10-20 など）。返し=持ち点(25,000→25,000)なら UMA なし。
# - 収支(円) = ( 素点pt + UMApt ) × レート(円/千点)
# - OKA（トップ加点）は DB に保存のみ。ポイント/収支には使用しない（必要なら後から有効化可）。
# - 役満回数・焼き鳥(半荘単位)を入力して通算表示。
# - ルーム作成/参加（一覧選択）・ルーム削除・ミート編集/削除対応。
# - スマホ向けUI（centered / サイドバー初期折りたたみ）。

import uuid
import sqlite3
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Optional

import pandas as pd
import streamlit as st

# ---------------------- ページ設定 / スタイル ----------------------
st.set_page_config(
    page_title="麻雀リーグ 精算ツール",
    page_icon="🀄",
    layout="centered",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
/* モバイルの押しやすさ向上 */
button, .stButton>button { padding: .55rem .9rem; }
div[data-testid="stNumberInput"] input { font-size: 1.05rem; }
.dataframe td, .dataframe th { font-size: .95rem; }
</style>
""",
    unsafe_allow_html=True,
)

# ---------------------- 定数 / 共通 ----------------------
DB_PATH = Path("mahjong.db")

DEFAULT_MEMBERS = ["眞壁", "内藤", "森", "浜野", "傅田", "須崎", "中間", "高田", "内藤士"]

ROUNDING_OPTIONS = ["none", "round", "floor", "ceil"]  # 点棒の端数丸め（100点単位）


def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def table_has_column(con: sqlite3.Connection, table: str, col: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols


def init_db():
    """スキーマ作成 + マイグレーション（不足カラムをALTER）"""
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
            rounding TEXT NOT NULL,
            oka_mode TEXT NOT NULL,   -- 'none' 固定（今は保存のみ）
            oka_pt REAL NOT NULL,     -- 保存のみ
            oka_yen REAL NOT NULL     -- 保存のみ
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
            final_points INTEGER NOT NULL,  -- 最終点(点棒)
            rank INTEGER NOT NULL,
            point_pt REAL NOT NULL,         -- 素点+UMA の合計(pt)
            net_cash REAL NOT NULL,         -- 円換算（= point_pt * rate）
            yakuman INTEGER NOT NULL DEFAULT 0,   -- 役満回数
            yakitori INTEGER NOT NULL DEFAULT 0,  -- 焼き鳥(0/1)
            FOREIGN KEY(hanchan_id) REFERENCES hanchan(id) ON DELETE CASCADE,
            FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
            UNIQUE(hanchan_id, player_id)
        );
        """
    )
    # 既存DBの不足カラムを補完（念のため）
    for table, cols in [
        ("rooms", ["oka_mode", "oka_pt", "oka_yen"]),
        ("results", ["point_pt", "yakuman", "yakitori"]),
        ("hanchan", ["meet_id"]),
    ]:
        for c in cols:
            if not table_has_column(con, table, c):
                default = "TEXT" if c in ["oka_mode"] else "REAL"
                if c in ["yakuman", "yakitori"]:
                    default = "INTEGER"
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {c} {default};")
    con.commit()
    con.close()


def reset_db_file():
    try:
        if DB_PATH.exists():
            DB_PATH.unlink()
        st.toast("DB を初期化しました。")
    except Exception as e:
        st.error(f"DB 初期化に失敗: {e}")
    finally:
        st.rerun()


# ---------------------- データ取得ユーティリティ ----------------------
def df_rooms(con):
    return pd.read_sql_query(
        "SELECT id, name, created_at FROM rooms ORDER BY datetime(created_at) DESC;",
        con,
    )


def get_room(con, room_id: str) -> Optional[dict]:
    cur = con.execute("SELECT * FROM rooms WHERE id=?;", (room_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = [c[0] for c in cur.description]
    d = {cols[i]: row[i] for i in range(len(cols))}
    # 型整形
    for k in ["start_points", "target_points"]:
        d[k] = int(d[k])
    for k in ["rate_per_1000", "uma1", "uma2", "uma3", "uma4", "oka_pt", "oka_yen"]:
        d[k] = float(d[k])
    d["rounding"] = str(d["rounding"])
    d["oka_mode"] = str(d["oka_mode"])
    return d


def df_players(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM players WHERE room_id=? ORDER BY joined_at;",
        con,
        params=(room_id,),
    )


def df_seasons(con, room_id):
    return pd.read_sql_query(
        "SELECT * FROM seasons WHERE room_id=? ORDER BY start_date;",
        con,
        params=(room_id,),
    )


def df_meets(con, season_id):
    return pd.read_sql_query(
        "SELECT * FROM meets WHERE season_id=? ORDER BY meet_date;",
        con,
        params=(season_id,),
    )


def df_hanchan_join(
    con, room_id: str, season_id: Optional[str] = None, meet_id: Optional[str] = None
):
    q = """
    SELECT h.id, h.room_id, h.meet_id, h.started_at, h.finished_at, h.memo,
           p.display_name, r.final_points, r.rank, r.point_pt, r.net_cash,
           r.yakuman, r.yakitori, r.player_id,
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
    cur = con.execute("SELECT display_name FROM players WHERE room_id=?", (room_id,))
    have = {r[0] for r in cur.fetchall()}
    inserted = False
    for name in names:
        name = name.strip()
        if name and name not in have:
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (str(uuid.uuid4()), room_id, name, datetime.utcnow().isoformat()),
            )
            inserted = True
    if inserted:
        con.commit()


# ---------------------- 計算ロジック ----------------------
def apply_rounding(points: int, mode: str) -> int:
    """点棒の丸め（100点単位）"""
    if mode == "none":
        return int(points)
    if mode == "floor":
        return (points // 100) * 100
    if mode == "ceil":
        return ((points + 99) // 100) * 100
    # round
    return int(round(points / 100.0) * 100)


def settlement(room: dict, finals: Dict[str, int]) -> tuple[dict, dict, dict, dict]:
    """
    入力: finals[player_id] = 最終点(点棒)
    出力:
      point_pts[player]  : pt(素点+UMA)
      ranks[player]      : 着順(1-4)
      rounded_finals     : 丸め後の最終点(点棒)
      cash[player]       : 円換算（= pt * rate）
    仕様:
      - base_pt = (final - target) / 1000
      - UMAは target != start のときのみ rank別に付与
      - 収支(円) = (base_pt + UMApt) × rate
      - OKAは保存のみで未使用
    """
    start = room["start_points"]
    target = room["target_points"]
    rate = room["rate_per_1000"]
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    rounding = room["rounding"]

    # 丸め
    rounded = {pid: apply_rounding(pts, rounding) for pid, pts in finals.items()}
    # ランク付け（点棒の降順）
    order = sorted(rounded.items(), key=lambda x: x[1], reverse=True)
    ranks = {pid: i + 1 for i, (pid, _) in enumerate(order)}

    uma_applies = target != start  # 返し != 持ち点 のとき UMA 有効（一般的運用）

    point_pts: dict[str, float] = {}
    cash: dict[str, float] = {}

    for pid, pts in rounded.items():
        base_pt = (pts - target) / 1000.0
        uma_pt = uma[ranks[pid] - 1] if uma_applies else 0.0
        pt = base_pt + uma_pt
        point_pts[pid] = pt
        cash[pid] = pt * rate

    return point_pts, ranks, rounded, cash


# ---------------------- アプリ本体 ----------------------
st.title("🀄 麻雀リーグ 精算ツール")
init_db()

# ===== サイドバー =====
with st.sidebar:
    st.header("ルーム")
    action = st.radio("操作を選択", ["ルーム作成", "ルーム参加"], horizontal=True)

    if action == "ルーム作成":
        name = st.text_input("ルーム名", value="今夜の卓")
        col1, col2 = st.columns(2)
        with col1:
            start_points = st.number_input("持ち点", value=25000, step=100)
            target_points = st.number_input("返し", value=25000, step=100)
            rate_per_1000 = st.number_input("レート(円/千点)", value=100.0, step=10.0)
        with col2:
            uma1 = st.number_input("ウマ1位(+千点)", value=10.0, step=1.0)
            uma2 = st.number_input("ウマ2位(+千点)", value=5.0, step=1.0)
            uma3 = st.number_input("ウマ3位(−千点)", value=-5.0, step=1.0)
            uma4 = st.number_input("ウマ4位(−千点)", value=-10.0, step=1.0)

        rounding = st.selectbox("点数丸め(100点)", ROUNDING_OPTIONS, index=0)

        st.caption("※OKAは保存のみ。ポイント/収支計算には未使用。")
        oka_mode = st.selectbox("OKAモード（保存）", ["none"], index=0)
        oka_pt = st.number_input("OKA pt(トップ加算/保存)", value=0.0, step=0.5)
        oka_yen = st.number_input("OKA 円(参考/保存)", value=0.0, step=100.0)

        creator = st.text_input("あなたの表示名", value="あなた")

        if st.button("ルーム作成"):
            con = connect()
            rid = str(uuid.uuid4())
            con.execute(
                """
                INSERT INTO rooms(
                    id, name, created_at,
                    start_points, target_points, rate_per_1000,
                    uma1, uma2, uma3, uma4,
                    rounding, oka_mode, oka_pt, oka_yen
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    rid,
                    name,
                    datetime.utcnow().isoformat(),
                    int(start_points),
                    int(target_points),
                    float(rate_per_1000),
                    float(uma1),
                    float(uma2),
                    float(uma3),
                    float(uma4),
                    rounding,
                    oka_mode,
                    float(oka_pt),
                    float(oka_yen),
                ),
            )
            pid = str(uuid.uuid4())
            con.execute(
                "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                (pid, rid, creator, datetime.utcnow().isoformat()),
            )
            con.commit()
            con.close()
            st.session_state["room_id"] = rid
            st.session_state["player_id"] = pid
            st.success("作成しました。")
            st.rerun()

    else:
        con = connect()
        rooms_df = df_rooms(con)
        if rooms_df.empty:
            st.info("まだルームがありません。『ルーム作成』から作成してください。")
        else:
            def fmt_room(r):
                ts = r["created_at"].split("T")[0] + " " + r["created_at"][11:16]
                return f'{r["name"]}（{ts}）'

            labels = [fmt_room(r) for _, r in rooms_df.iterrows()]
            idx = st.selectbox(
                "参加するルームを選択", options=list(range(len(labels))), format_func=lambda i: labels[i]
            )
            selected_room_id = rooms_df.iloc[idx]["id"]
            st.caption(f"Room ID: `{selected_room_id}`")

            name_in = st.text_input("あなたの表示名", value="あなた")
            if st.button("参加"):
                # 既存なら再利用、無ければ追加
                cur = con.execute(
                    "SELECT id FROM players WHERE room_id=? AND display_name=?",
                    (selected_room_id, name_in),
                )
                row = cur.fetchone()
                if row:
                    pid = row[0]
                else:
                    pid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO players(id, room_id, display_name, joined_at) VALUES (?,?,?,?)",
                        (pid, selected_room_id, name_in, datetime.utcnow().isoformat()),
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
        st.caption("まだルームは存在しません。")
    else:
        def fmt_room2(r):
            ts = r["created_at"].split("T")[0] + " " + r["created_at"][11:16]
            return f'{r["name"]}（{ts}）'

        idx_del = st.selectbox(
            "削除するルームを選択",
            options=list(range(len(rooms_df2))),
            format_func=lambda i: fmt_room2(rooms_df2.iloc[i]),
            key="del_room",
        )
        selected_room_id_del = rooms_df2.iloc[idx_del]["id"]
        confirm = st.checkbox("⚠️ 本当に削除する（全シーズン・成績が失われます）")
        if st.button("ルーム削除実行", disabled=not confirm):
            con.execute("DELETE FROM rooms WHERE id=?;", (selected_room_id_del,))
            con.commit()
            if st.session_state.get("room_id") == selected_room_id_del:
                st.session_state.pop("room_id", None)
                st.session_state.pop("player_id", None)
            st.success("ルームを削除しました。")
            st.rerun()
    con.close()

    st.divider()
    if st.button("🧹 DB初期化（全削除）", type="secondary"):
        reset_db_file()

st.caption("式: 素点pt=(最終点-返し)/1000, UMAは返し≠持ち点の時のみ。収支=pt×レート。")

# ルーム未選択なら終了
if "room_id" not in st.session_state:
    st.info("サイドバーからルームを作成/参加してください。")
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
    players_df[["display_name", "joined_at"]]
    .rename(columns={"display_name": "プレイヤー", "joined_at": "参加"}),
    use_container_width=True,
    height=220,
)

# 共通セレクタ：シーズン/ミート
seasons_df = df_seasons(con, room_id)
sel_season_id = None
sel_meet_id = None
if not seasons_df.empty:
    sel_season = st.selectbox("集計対象シーズン", seasons_df["name"].tolist())
    sel_season_id = seasons_df.loc[seasons_df["name"] == sel_season, "id"].values[0]
    meets_df = df_meets(con, sel_season_id)
    if not meets_df.empty:
        sel_meet = st.selectbox("入力・表示対象ミート", meets_df["name"].tolist())
        sel_meet_id = meets_df.loc[meets_df["name"] == sel_meet, "id"].values[0]

# タブ
tab_input, tab_results, tab_manage = st.tabs(["📝 入力", "📊 成績", "👤 メンバー/設定"])

# ===== 入力 =====
with tab_input:
    st.subheader("半荘入力（誰でも）")
    if not seasons_df.empty and sel_season_id and sel_meet_id:
        names = players_df["display_name"].tolist()
        id_map = dict(zip(players_df["display_name"], players_df["id"]))

        # 東南西北（重複防止は手動で注意喚起）
        c1, c2 = st.columns(2)
        c3, c4 = st.columns(2)
        east = c1.selectbox("東", names, index=min(0, len(names) - 1))
        south = c2.selectbox("南", names, index=min(1, len(names) - 1))
        west = c3.selectbox("西", names, index=min(2, len(names) - 1))
        north = c4.selectbox("北", names, index=min(3, len(names) - 1))

        picked = [east, south, west, north]
        if len(set(picked)) < 4:
            st.warning("同じ人が重複しています。4人とも別のメンバーを選択してください。")
        else:
            with st.form("hanchan_form"):
                st.write("**最終点（点棒、100点単位推奨）**")
                p_e = int(st.number_input(east, value=25000, step=100))
                p_s = int(st.number_input(south, value=25000, step=100))
                p_w = int(st.number_input(west, value=25000, step=100))
                p_n = int(st.number_input(north, value=25000, step=100))

                st.write("**役満回数 / 焼き鳥（任意）**")
                cye, cys = st.columns(2)
                cyw, cyn = st.columns(2)
                yk_e = int(cye.number_input(f"{east} 役満回数", value=0, step=1))
                yt_e = cys.checkbox(f"{east} 焼き鳥", value=False)
                yk_s = int(cyw.number_input(f"{south} 役満回数", value=0, step=1))
                yt_s = cyn.checkbox(f"{south} 焼き鳥", value=False)
                cyw2, cyn2 = st.columns(2)
                yk_w = int(cyw2.number_input(f"{west} 役満回数", value=0, step=1))
                yt_w = cyn2.checkbox(f"{west} 焼き鳥", value=False)
                cyw3, cyn3 = st.columns(2)
                yk_n = int(cyw3.number_input(f"{north} 役満回数", value=0, step=1))
                yt_n = cyn3.checkbox(f"{north} 焼き鳥", value=False)

                memo = st.text_input("メモ（任意）", value="")
                submitted = st.form_submit_button("精算を記録")

                if submitted:
                    finals = {
                        id_map[east]: p_e,
                        id_map[south]: p_s,
                        id_map[west]: p_w,
                        id_map[north]: p_n,
                    }
                    ykm = {
                        id_map[east]: yk_e,
                        id_map[south]: yk_s,
                        id_map[west]: yk_w,
                        id_map[north]: yk_n,
                    }
                    ytr = {
                        id_map[east]: int(yt_e),
                        id_map[south]: int(yt_s),
                        id_map[west]: int(yt_w),
                        id_map[north]: int(yt_n),
                    }
                    pts, ranks, rounded, cash = settlement(room, finals)

                    hid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO hanchan(id, room_id, meet_id, started_at, finished_at, memo) VALUES (?,?,?,?,?,?)",
                        (
                            hid,
                            room_id,
                            sel_meet_id,
                            datetime.utcnow().isoformat(),
                            datetime.utcnow().isoformat(),
                            memo,
                        ),
                    )
                    for pid in finals.keys():
                        rid = str(uuid.uuid4())
                        con.execute(
                            """
                            INSERT INTO results(id, hanchan_id, player_id, final_points, rank, point_pt, net_cash, yakuman, yakitori)
                            VALUES (?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                rid,
                                hid,
                                pid,
                                int(rounded[pid]),
                                int(ranks[pid]),
                                float(pts[pid]),
                                float(cash[pid]),
                                int(ykm[pid]),
                                int(ytr[pid]),
                            ),
                        )
                    con.commit()
                    st.success("半荘を登録しました！")
    else:
        st.info("『👤 メンバー/設定』でシーズンとミートを作成・選択してください。")

# ===== 成績 =====
with tab_results:
    st.subheader("成績 / 履歴")
    scope = "ミート（選択のみ）"
    if sel_season_id:
        scope = st.radio(
            "集計範囲",
            ["ミート（選択ミートのみ）", "シーズン（全ミート）", "全リーグ（すべて）"],
            horizontal=True,
            index=0 if sel_meet_id else 1,
        )
    use_season = scope == "シーズン（全ミート）"
    use_all = scope == "全リーグ（すべて）"

    hdf = df_hanchan_join(
        con,
        room_id,
        None if use_all else (sel_season_id if use_season or not sel_meet_id else None),
        None if (use_season or use_all) else sel_meet_id,
    )

    if hdf.empty:
        st.info("まだ成績がありません。")
    else:
        # 素点(千点)= (final - 返し)/1000
        target = room["target_points"]
        hdf["素点(千点)"] = (hdf["final_points"] - target) / 1000.0
        hdf["pt(素点+UMA)"] = hdf["point_pt"]
        hdf["精算(円)"] = hdf["net_cash"]

        g = hdf.groupby("display_name")
        summary = pd.DataFrame(
            {
                "回数": g["rank"].count(),
                "1位": g["rank"].apply(lambda s: (s == 1).sum()),
                "2位": g["rank"].apply(lambda s: (s == 2).sum()),
                "3位": g["rank"].apply(lambda s: (s == 3).sum()),
                "4位": g["rank"].apply(lambda s: (s == 4).sum()),
                "収支合計(円)": g["精算(円)"].sum().round(0),
                "素点合計(千点)": g["素点(千点)"].sum().round(2),
                "平均素点(千点)": g["素点(千点)"].mean().round(2),
                "平均順位": g["rank"].mean().round(2),
                "pt合計": g["pt(素点+UMA)"].sum().round(2),
                "役満(回)": g["yakuman"].sum(),
                "焼き鳥(回)": g["yakitori"].sum(),
            }
        ).reset_index()

        # ランキング表示（左端を順位）
        summary = summary.sort_values(
            ["pt合計", "収支合計(円)"], ascending=[False, False]
        ).reset_index(drop=True)
        summary.insert(0, "順位", range(1, len(summary) + 1))

        st.write("### 個人成績（累積）")
        st.dataframe(summary, use_container_width=True, height=420)

        st.write("### 半荘履歴（主要列）")
        disp = hdf.copy()
        disp["点棒(最終点)"] = disp["final_points"].map(lambda x: f"{x:,}")
        disp["精算(円)"] = disp["精算(円)"].map(lambda x: f"{x:,.0f}")
        disp = disp.rename(
            columns={
                "season_name": "シーズン",
                "meet_name": "ミート",
                "display_name": "プレイヤー",
                "rank": "着順",
                "yakuman": "役満(回)",
                "yakitori": "焼き鳥",
            }
        )
        st.dataframe(
            disp[
                [
                    "シーズン",
                    "ミート",
                    "プレイヤー",
                    "点棒(最終点)",
                    "素点(千点)",
                    "pt(素点+UMA)",
                    "着順",
                    "精算(円)",
                    "役満(回)",
                    "焼き鳥",
                ]
            ],
            use_container_width=True,
            height=440,
        )

        # ヘッドトゥヘッド（pt基準）
        st.write("### 対人（ヘッドトゥヘッド, pt差）")
        rows = []
        for hid, gg in hdf.groupby("id"):
            pts = gg.set_index("player_id")["pt(素点+UMA)"]
            names_map = gg.set_index("player_id")["display_name"].to_dict()
            pids = list(pts.index)
            for i in range(len(pids)):
                for j in range(i + 1, len(pids)):
                    a, b = pids[i], pids[j]
                    rows.append(
                        {
                            "A": names_map[a],
                            "B": names_map[b],
                            "同卓回数": 1,
                            "A基準pt差": (pts[a] - pts[b]) / 2.0,
                        }
                    )
        if rows:
            h2h = (
                pd.DataFrame(rows)
                .groupby(["A", "B"])
                .agg({"同卓回数": "sum", "A基準pt差": "sum"})
                .reset_index()
            )
            st.dataframe(h2h, use_container_width=True)

        st.download_button(
            "成績CSVをダウンロード（個人成績）",
            summary.to_csv(index=False).encode("utf-8-sig"),
            file_name="summary.csv",
            mime="text/csv",
        )

# ===== メンバー/設定 =====
with tab_manage:
    st.subheader("メンバー管理")
    existing_names = players_df["display_name"].tolist()
    candidate_pool = sorted(set(existing_names) | set(DEFAULT_MEMBERS))
    selected_candidates = st.multiselect(
        "候補に入れておくメンバー（未登録はボタンで一括追加）",
        options=candidate_pool,
        default=existing_names or DEFAULT_MEMBERS[:4],
    )
    c1, c2 = st.columns([2, 1])
    with c1:
        new_name = st.text_input("新メンバー名（1人ずつ）")
    with c2:
        if st.button("追加"):
            if new_name.strip():
                ensure_players(con, room_id, [new_name.strip()])
                st.success(f"追加：{new_name.strip()}")
                st.rerun()

    if st.button("未登録候補をまとめて登録"):
        ensure_players(con, room_id, selected_candidates)
        st.success("未登録メンバーを登録しました。")
        st.rerun()

    st.divider()
    st.subheader("シーズン")
    seasons_df = df_seasons(con, room_id)
    colA, colB = st.columns([2, 1])
    with colA:
        st.dataframe(
            seasons_df.rename(
                columns={"name": "シーズン名", "start_date": "開始日", "end_date": "終了日"}
            ),
            use_container_width=True,
            height=240,
        )
    with colB:
        with st.form("season_form"):
            s_name = st.text_input("シーズン名", value=f"{date.today().year} 前期")
            s_start = st.date_input("開始日", value=date(date.today().year, 1, 1))
            s_end = st.date_input("終了日", value=date(date.today().year, 6, 30))
            if st.form_submit_button("シーズン作成"):
                sid = str(uuid.uuid4())
                con.execute(
                    "INSERT INTO seasons(id,room_id,name,start_date,end_date,created_at) VALUES (?,?,?,?,?,?)",
                    (
                        sid,
                        room_id,
                        s_name,
                        s_start.isoformat(),
                        s_end.isoformat(),
                        datetime.utcnow().isoformat(),
                    ),
                )
                con.commit()
                st.rerun()

    st.divider()
    st.subheader("ミート（開催）")
    if seasons_df.empty:
        st.info("先にシーズンを作成してください。")
    else:
        sel_season2 = st.selectbox("対象シーズン", seasons_df["name"].tolist(), key="season_sel_manage")
        sel_season_id2 = seasons_df.loc[seasons_df["name"] == sel_season2, "id"].values[0]
        meets_df2 = df_meets(con, sel_season_id2)

        colM1, colM2 = st.columns([2, 1])
        with colM1:
            st.dataframe(
                meets_df2.rename(columns={"name": "ミート名", "meet_date": "開催日"}),
                use_container_width=True,
                height=240,
            )
        with colM2:
            with st.form("meet_form"):
                m_name = st.text_input("ミート名", value="第1回")
                m_date = st.date_input("開催日", value=date.today())
                if st.form_submit_button("ミート作成"):
                    mid = str(uuid.uuid4())
                    con.execute(
                        "INSERT INTO meets(id,season_id,name,meet_date,created_at) VALUES (?,?,?,?,?)",
                        (mid, sel_season_id2, m_name, m_date.isoformat(), datetime.utcnow().isoformat()),
                    )
                    con.commit()
                    st.rerun()

            st.markdown("#### ミート修正 / 削除")
            if not meets_df2.empty:
                edit_meet_name = st.selectbox("編集対象ミート", meets_df2["name"].tolist(), key="meet_edit_pick")
                edit_meet_id = meets_df2.loc[meets_df2["name"] == edit_meet_name, "id"].values[0]
                edit_meet_date = meets_df2.loc[meets_df2["name"] == edit_meet_name, "meet_date"].values[0]

                with st.form("meet_edit_form"):
                    new_name = st.text_input("新しいミート名", value=edit_meet_name)
                    new_date = st.date_input("新しい開催日", value=date.fromisoformat(edit_meet_date))
                    if st.form_submit_button("更新を保存"):
                        con.execute("UPDATE meets SET name=?, meet_date=? WHERE id=?;", (new_name, new_date.isoformat(), edit_meet_id))
                        con.commit()
                        st.success("ミートを更新しました。")
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

con.close()
