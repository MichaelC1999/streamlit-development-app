from substreams import Substream
import streamlit as st
from tempfile import NamedTemporaryFile
import pandas as pd
import substreams as sub

st.set_page_config(layout='wide')
sb = None
sb_keys = []

with NamedTemporaryFile(dir='.', suffix='.spkg') as f:
    if "spkg" in st.session_state:
        if st.session_state["spkg"] is not None:
            f.write(st.session_state["spkg"].getbuffer())
            sb = Substream(f.name)
            sb_keys = list(dict.keys(sb.output_modules))

if 'rank_col' not in st.session_state:
    st.session_state['rank_col'] = None

if 'selected_key' not in st.session_state:
    if len(sb_keys) == 0:
        st.session_state["selected_key"] = None
    else: 
        st.session_state["selected_key"] = sb_keys[0]

spkgFile = None
def inputSubstream():
    print(st.session_state["spkg"].getbuffer(), 1)

min_block_number = 0
if st.session_state["selected_key"] is not None and sb is not None:
    if sb.output_modules[st.session_state["selected_key"]]["initial_block"] > 0:
        min_block_number = sb.output_modules[st.session_state["selected_key"]]["initial_block"]
        st.write(str(min_block_number) + " is the start block defined for this module")
min_block = st.number_input('Start Block', min_value=min_block_number, max_value=20000000)

max_block = st.number_input('End Block (for now, leave as 20000000 to stream to chain head)', min_value=min_block_number)
            
st.selectbox("Select Store Key", options=sb_keys, key="selected_key") 

if st.session_state["selected_key"] is not None:
    if 'store' not in st.session_state["selected_key"]:
        st.write('Not a store module. May have unexpected behavior.')

spkgFile = st.file_uploader("Input substream .spkg file", type='spkg', key="spkg")


if st.session_state["selected_key"] is not None and sb is not None:
    def run_substream(max_block, min_block):
        print(dir(sb))
        if max_block < min_block:
            max_block = 20000000
        result = sb.poll([st.session_state["selected_key"]], start_block=min_block, end_block=max_block)
        result_dfs = result[0]
        print(result_dfs.snapshots, result_dfs.data, list(result_dfs.snapshots.columns), list(result_dfs.data.columns), 'dataframes')
        if list(result_dfs.data.columns):
            print( type(result_dfs.data))
            copy_df = result_dfs.data.copy()
            st.session_state['df'] = copy_df
    st.button("Run Substream", on_click=run_substream, args=(max_block, min_block))

if 'df' in st.session_state:
    if st.session_state['df'].empty is False:
        if list(st.session_state['df'].columns):
            st.selectbox("Select Substream Table Sort Column", options=list(st.session_state['df'].columns), key="rank_col") 

        copy_df = st.session_state['df'].copy()
        if st.session_state['rank_col'] is not None:
            copy_df = copy_df.sort_values(by=st.session_state['rank_col'],ascending=False)
            copy_df.index = range(1, len(copy_df) + 1)
        html_table = '<div class="table-container">' + copy_df[:500].to_html() + '</div>'
        style_css = """
                <style>
                    div.table-container {
                        width: 100%;
                        overflow: scroll;
                    }

                    table.dataframe {
                    width: 100%;
                    background-color: rgb(35,58,79);
                    border-collapse: collapse;
                    border-width: 2px;
                    border-color: rgb(17,29,40);
                    border-style: solid;
                    color: white;
                    font-size: 14px;
                    }

                    table.dataframe td, table.dataframe th {
                    text-align: left;
                    border-top: 2px rgb(17,29,40) solid;
                    border-bottom: 2px rgb(17,29,40) solid;
                    padding: 3px;
                    white-space:nowrap;
                    }

                    table.dataframe thead {
                        color: rgb(215,215,215);
                    background-color: rgb(17,29,40);
                    }
                </style>"""
        st.markdown(style_css + html_table, unsafe_allow_html=True)