"""Team 1 · Citi Bike 15-minute stockout map.

Databricks: deploy this folder as a Databricks App (see ../README.md).
Locally:    python ../make_demo_data.py && streamlit run app.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

import data

st.set_page_config(page_title="Citi Bike 15-minute map", layout="wide")
st.markdown("""
<style>
  .stApp { background: #f6f1e7; }
  .block-container { padding: 0.8rem 1.2rem 0 1.2rem; max-width: 100%; }
  header[data-testid="stHeader"] { display: none; }
</style>""", unsafe_allow_html=True)

# One character per status keeps the payload small (about 2,300 stations x 96 frames).
CODE = {"empty_risk": "E", "full_risk": "F", "watch": "W", "ok": "O", "offline": "X",
        "empty_now": "e", "full_now": "f"}


@st.cache_data(ttl=600, show_spinner=False)
def load_static():
    return data.stations(), data.model_info(), data.playback_dates()


@st.cache_data(ttl=60, show_spinner="Loading predictions…")
def load_predictions(mode, date):
    return data.predictions(mode, date)


def column(g, name, n, pos, scale=1.0):
    """Array aligned with the frame times; None where missing."""
    arr = np.full(n, np.nan)
    if name in g:
        arr[pos] = pd.to_numeric(g[name], errors="coerce").to_numpy(dtype=float) * scale
    return [None if np.isnan(x) else int(round(x)) for x in arr]


def build_payload(stations, preds, model_info, mode):
    times = np.sort(preds["prediction_time"].unique())
    idx = {t: i for i, t in enumerate(times)}
    n = len(times)
    groups = dict(tuple(preds.groupby("station_id")))
    out = []
    for s in stations.itertuples():
        g = groups.get(s.station_id)
        if g is None:
            continue
        pos = g["prediction_time"].map(idx).to_numpy()
        status = np.array(["."] * n, dtype=object)
        status[pos] = g["status"].map(CODE).fillna(".").to_numpy()
        out.append({
            "id": s.station_id, "name": s.name, "lat": round(float(s.lat), 5), "lon": round(float(s.lon), 5),
            "cap": None if pd.isna(s.capacity) else int(s.capacity),
            "s": "".join(status),
            "pb": column(g, "p_bike", n, pos, 100), "pd": column(g, "p_dock", n, pos, 100),
            "b": column(g, "num_bikes_available", n, pos), "d": column(g, "num_docks_available", n, pos),
            "tb": column(g, "bike_target", n, pos), "td": column(g, "dock_target", n, pos),
        })
    mi = model_info.set_index("target")
    model = {}
    for t in ("bike", "dock"):
        if t in mi.index:
            r = mi.loc[t]
            model[t] = {"type": str(r["model_type"]),
                        "version": "" if pd.isna(r["version"]) else str(int(r["version"])),
                        "threshold": round(float(r["threshold"]) * 100, 1),
                        "pr_auc": round(float(r["test_pr_auc"]), 3),
                        "history_pr_auc": round(float(r["test_history_pr_auc"]), 3),
                        "base_rate": round(float(r["test_base_rate"]) * 100, 1)}
    return {"mode": mode, "demo": not data.ON_DATABRICKS,
            "times": [pd.Timestamp(t).isoformat() for t in times], "stations": out, "model": model}


# =============================================================================
# Controls
# =============================================================================
stations, model_info, dates = load_static()
c1, c2, c3 = st.columns([1.3, 1.4, 5])
with c1:
    mode = st.segmented_control("Mode", ["Playback", "Live"], default="Playback",
                                label_visibility="collapsed") or "Playback"
date = None
with c2:
    if mode == "Playback":
        if not dates:
            st.warning("No playback yet — run notebook 3 in production.")
            st.stop()
        date = st.selectbox("Day", dates, index=len(dates) - 1, label_visibility="collapsed",
                            format_func=lambda d: pd.Timestamp(d).strftime("%a %b %d, %Y"))
with c3:
    if not data.ON_DATABRICKS:
        st.caption("Demo data (synthetic). Deployed on Databricks it reads the team1 tables.")

preds = load_predictions(mode.lower(), date)
if preds.empty:
    st.info("No live predictions yet — schedule the poller + notebook 4 (see README)."
            if mode == "Live" else "No predictions for this day.")
    st.stop()

payload = build_payload(stations, preds, model_info, mode.lower())
html = (Path(__file__).parent / "ui.html").read_text(encoding="utf-8")
html = html.replace("__PAYLOAD__", json.dumps(payload, separators=(",", ":")).replace("</", "<\\/"))
if hasattr(st, "iframe"):
    st.iframe(html, height=880)
else:
    import streamlit.components.v1 as components
    components.html(html, height=880, scrolling=False)
