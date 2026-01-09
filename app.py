import streamlit as st
import os
from scanner_core import run_scan

st.set_page_config(page_title="Nifty 500 Swing Scanner", layout="wide")

st.title("📈 Nifty 500 Swing Scanner")
st.caption("EMA + RSI + Relative Strength Scanner")

DEFAULTS = {
    "REL_RET_MIN": 0.5,
    "REL_RET_MAX": 10.0,
    "RSI_MIN": 30,
    "RSI_MAX": 75,
    "MIN_VOLUME": 300000
}

def reset_settings():
    for k, v in DEFAULTS.items():
        st.session_state[k] = v

st.sidebar.header("🔧 Scanner Settings")
st.sidebar.button("♻ Reset Settings", on_click=reset_settings)

REL_RET_MIN = st.sidebar.slider("Relative Strength Min (%)", 0.0, 10.0,
                               DEFAULTS["REL_RET_MIN"], key="REL_RET_MIN")
REL_RET_MAX = st.sidebar.slider("Relative Strength Max (%)", 0.0, 15.0,
                               DEFAULTS["REL_RET_MAX"], key="REL_RET_MAX")
RSI_MIN = st.sidebar.slider("RSI Min", 10, 50,
                            DEFAULTS["RSI_MIN"], key="RSI_MIN")
RSI_MAX = st.sidebar.slider("RSI Max", 50, 90,
                            DEFAULTS["RSI_MAX"], key="RSI_MAX")
MIN_VOLUME = st.sidebar.number_input("Min Volume", 100000, 2000000,
                                     DEFAULTS["MIN_VOLUME"], key="MIN_VOLUME")

st.markdown("---")
run = st.button("👁️ RUN SCAN", use_container_width=True, type="primary")

if run:
    status = st.empty()
    bar = st.progress(0)

    def progress_cb(stock, i, total):
        status.markdown(f"🔍 Scanning **{stock}** ({i}/{total})")
        bar.progress(i / total)

    dfs, excel_path = run_scan(
        REL_RET_MIN, REL_RET_MAX, RSI_MIN, RSI_MAX, MIN_VOLUME,
        progress_cb=progress_cb
    )

    status.success("✅ Scan Completed")
    bar.empty()

    def add_tv_icon(df):
        if df is None or df.empty:
            return None
        df = df.copy()
        df["TV"] = df["Stock"].apply(
            lambda x: f'<a href="https://www.tradingview.com/chart/?symbol=NSE:{x}" '
                      f'target="_blank">🔗</a>'
        )
        return df


    tab1, tab2, tab3, tab4 = st.tabs([
        "WITH C0",
        "WITHOUT C0",
        "WITH C0 + Pullback",
        "ALL STOCKS"
    ])

    # ===== TAB 1 : WITH C0 =====
    with tab1:
        df = add_tv_icon(dfs["WITH_C0"])
        if df is None or df.empty:
            st.info("No stocks found.")
        else:
            st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)

    # ===== TAB 2 : WITHOUT C0 =====
    with tab2:
        df = add_tv_icon(dfs["WITHOUT_C0"])
        if df is None or df.empty:
            st.info("No stocks found.")
        else:
            st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)

    # ===== TAB 3 : WITH C0 + PULLBACK =====
    with tab3:
        df = add_tv_icon(dfs["WITH_C0_PULLBACK"])
        if df is None or df.empty:
            st.info("No stocks found.")
        else:
            st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)

    # ===== TAB 4 : ALL STOCKS =====
    with tab4:
        df = add_tv_icon(dfs["ALL_STOCKS"])
        if df is None or df.empty:
            st.info("No stocks found.")
        else:
            st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)


    with open(excel_path, "rb") as f:
        st.download_button(
            "📥 Download Excel",
            f,
            file_name=os.path.basename(excel_path)
        )
