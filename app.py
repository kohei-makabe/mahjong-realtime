# 麻雀リーグ精算ツール 完全版
# オカ・ウマ・レート設定対応／役満・焼き鳥入力付き

import streamlit as st
import sqlite3, uuid
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="麻雀リーグ精算ツール", page_icon="🀄", layout="centered")

DB_PATH = "mahjong.db"

def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    return con

def init_db():
    con = connect()
    con.executescript(
        '''
        CREATE TABLE IF NOT EXISTS rooms (
            id TEXT PRIMARY KEY,
            name TEXT,
            created_at TEXT,
            start_points INTEGER,
            target_points INTEGER,
            rate_per_1000 REAL,
            uma1 REAL, uma2 REAL, uma3 REAL, uma4 REAL,
            rounding TEXT,
            oka_mode TEXT,
            oka_pt REAL
        );
        '''
    )
    con.commit(); con.close()

def calc_settlement(room, points_dict):
    start = room["start_points"]
    target = room["target_points"]
    rate = room["rate_per_1000"]
    uma = [room["uma1"], room["uma2"], room["uma3"], room["uma4"]]
    oka_pt = room["oka_pt"]
    items = sorted(points_dict.items(), key=lambda x: x[1], reverse=True)
    ranks = {pid: i+1 for i,(pid,_) in enumerate(items)}
    base_points = {pid: (pts - target) / 1000.0 for pid, pts in items}
    nets = {}
    for pid, pts in items:
        net = base_points[pid] * rate + uma[ranks[pid]-1] * rate
        if ranks[pid] == 1:
            net += oka_pt * rate
        nets[pid] = net
    return nets, ranks, base_points

init_db()
st.title("🀄 麻雀リーグ精算ツール")

with st.sidebar:
    st.header("ルーム作成")
    name = st.text_input("ルーム名", "今夜の卓")
    start_points = st.number_input("持ち点", value=25000, step=1000)
    target_points = st.number_input("返し", value=25000, step=1000)
    rate = st.number_input("レート(円/千点)", value=100.0, step=10.0)
    uma1 = st.number_input("ウマ1位(+千点)", value=10.0)
    uma2 = st.number_input("ウマ2位(+千点)", value=5.0)
    uma3 = st.number_input("ウマ3位(-千点)", value=-5.0)
    uma4 = st.number_input("ウマ4位(-千点)", value=-10.0)
    oka_pt = st.number_input("オカ(トップpt)", value=0.0)
    if st.button("ルーム作成"):
        con = connect()
        rid = str(uuid.uuid4())
        con.execute("INSERT INTO rooms VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (rid,name,datetime.utcnow().isoformat(),start_points,target_points,rate,uma1,uma2,uma3,uma4,"none","none",oka_pt))
        con.commit(); con.close()
        st.session_state["room_id"] = rid
        st.success("ルーム作成完了")

if "room_id" not in st.session_state:
    st.stop()

room_id = st.session_state["room_id"]
con = connect()
row = con.execute("SELECT * FROM rooms WHERE id=?",(room_id,)).fetchone()
cols = [r[1] for r in con.execute("PRAGMA table_info(rooms)")]
room = dict(zip(cols,row))

st.subheader("試合入力")
players = ["東","南","西","北"]
cols = st.columns(4)
points={}
yakuman={}
yakitori={}
for p in players:
    with cols[players.index(p)]:
        pts = st.number_input(f"{p}の最終点",value=25000,step=100,key=f"pt_{p}")
        yakuman[p]=st.number_input(f"{p}の役満回数",value=0,step=1,key=f"ykm_{p}")
        yakitori[p]=st.checkbox(f"{p}焼き鳥",key=f"yt_{p}")
        points[p]=pts

if st.button("精算計算"):
    pid_map={p:str(uuid.uuid4()) for p in players}
    nets,ranks,base=calc_settlement(room,{pid_map[p]:v for p,v in points.items()})
    df=pd.DataFrame([{"席":p,"順位":ranks[pid_map[p]],"素点(千点)":base[pid_map[p]],"収支(円)":nets[pid_map[p]],"役満":yakuman[p],"焼き鳥":1 if yakitori[p] else 0} for p in players])
    df=df.sort_values("順位")
    st.dataframe(df,use_container_width=True)
    st.success("計算完了！")

con.close()
