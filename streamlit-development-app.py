from substreams import Substream
import streamlit as st
from tempfile import NamedTemporaryFile
import pandas as pd
import substreams as sub

st.set_page_config(layout='wide')
sb = None
sb_keys = []

def reset_state():
    st.session_state['streamed_data'] = []
    st.session_state['attempt_failures'] = 0
    st.session_state['block_to_start'] = 0
    st.session_state['run_substream'] = False
    st.session_state['rank_col'] = 'block'

if bool(st.session_state) is False:
    reset_state()
    st.experimental_rerun()

with NamedTemporaryFile(dir='.', suffix='.spkg') as f:
    if "spkg" in st.session_state:
        if st.session_state["spkg"] is not None:
            f.write(st.session_state["spkg"].getbuffer())
            sb = Substream(f.name)
            sb_keys = list(dict.keys(sb.output_modules))

if 'selected_key' not in st.session_state:
    if len(sb_keys) == 0:
        st.session_state["selected_key"] = None
    else: 
        st.session_state["selected_key"] = sb_keys[0]

spkgFile = None

min_block_number = 0
if st.session_state["selected_key"] is not None and sb is not None:
    if sb.output_modules[st.session_state["selected_key"]]["initial_block"] > 0:
        min_block_number = sb.output_modules[st.session_state["selected_key"]]["initial_block"]
        st.write(str(min_block_number) + " is the start block defined for this module")
min_block = st.number_input('Start Block', min_value=min_block_number, max_value=20000000)

max_block = st.number_input('End Block (for now, leave as 20000000 to stream to chain head)', min_value=min_block_number)
            
st.selectbox("Select Module Key", options=sb_keys, key="selected_key") 

if st.session_state["selected_key"] is not None:
    if 'store' in st.session_state["selected_key"]:
        st.write('Store modules are not supported. May have unexpected behavior.')

spkgFile = st.file_uploader("Input substream .spkg file", type='spkg', key="spkg", on_change=reset_state)

if st.session_state["selected_key"] is not None and sb is not None:
    def run_substream(max_block, min_block):
        st.session_state['error_message'] = ""
        st.session_state['attempt_failures'] = 0
        st.session_state['rank_col'] = 'block'
        st.session_state['run_substream'] = True
        st.session_state['min_block'] = min_block
        st.session_state['streamed_data'] = []
    st.button("Run Substream", on_click=run_substream, args=(max_block, min_block))

placeholder = st.empty()
error_message = st.empty()

if 'error_message' in st.session_state:
    if st.session_state['error_message'] != "": 
        error_message.text(st.session_state['error_message'])

if 'streamed_data' in st.session_state:
    if len(st.session_state['streamed_data']) > 0:
        copy_df = pd.DataFrame(st.session_state['streamed_data'])
        if list(copy_df.columns):
            st.selectbox("Select Substream Table Sort Column", options=list(copy_df.columns), key="rank_col") 

        if st.session_state['rank_col'] is not None:
            copy_df = copy_df.sort_values(by=st.session_state['rank_col'],ascending=False)
        copy_df.index = range(1, len(copy_df) + 1)
        copy_df = copy_df.fillna("")

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

if st.session_state['run_substream'] is True:
    if 'min_block' in st.session_state:
        # If min_block is saved in state, override the min_block from the UI input
        min_block = st.session_state['min_block']
    if min_block > 0:
        if max_block < min_block:
            raise TypeError('`min_block` is greater than `max_block`. This cannot be validly polled.')
        if max_block == min_block:
            st.session_state["min_block"] = 0
            st.session_state['run_substream'] = False
            st.experimental_rerun()
        if max_block > min_block and sb is not None:
            module_name = st.session_state['selected_key']
            poll_return = {}
            try:
                placeholder.text("Loading Substream Results...")
                poll_return = sb.poll(module_name, start_block=min_block, end_block=max_block, return_first_result=True, return_type="dict")
                placeholder.empty()
                if 'error' in poll_return:
                    if poll_return["error"] is not None:
                        if "debug_error_string" in dir(poll_return["error"]):
                            raise TypeError(poll_return["error"].debug_error_string() + ' BLOCK: ' + poll_return["data_block"])
                        else:
                            raise TypeError(poll_return["error"] + ' BLOCK: ' + poll_return["data_block"])
                if 'data_block' in poll_return and int(poll_return["data_block"]) + 1 == max_block:
                    st.session_state['min_block'] = int(poll_return["data_block"]) + 1
                elif "data" in poll_return:
                    if (len(poll_return["data"]) > 0):
                        st.session_state['streamed_data'].extend(poll_return["data"])
                    st.session_state['min_block'] = int(poll_return["data_block"]) + 1
            except Exception as err:
                print("ERROR --- ", err)
                attempt_failures = st.session_state['attempt_failures']
                attempt_failures += 1
                if attempt_failures % 10 == 0:
                    st.session_state['error_message'] = "ERROR --- " + str(err)
                    st.session_state['run_substream'] = False
                    st.session_state["min_block"] = max_block
                st.session_state['attempt_failures'] = attempt_failures
            st.experimental_rerun()
elif 'streamed_data' in st.session_state:
    if len(st.session_state['streamed_data']) > 0:
        st.write('Substream Polling Completed') 