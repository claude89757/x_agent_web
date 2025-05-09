#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024年10月14日
@Author  : claude
@File    : 1_X智能获客.py
@Software: cursor
"""
import os
import json
import time
import streamlit as st

from dotenv import load_dotenv
from sidebar import sidebar_for_x
from common.log_config import setup_logger
from common.mysql import MySQLDatabase

# 导入各个标签页的函数
from pages.xhs_tab.data_collect import data_collect
from pages.xhs_tab.data_filter import data_filter
from pages.xhs_tab.data_analyze import data_analyze
from pages.xhs_tab.generate_msg import generate_msg
from pages.xhs_tab.send_msg import send_msg

# 加载环境变量
load_dotenv()

# 设置页面配置
st.set_page_config(page_title="X智能获客", page_icon="🤖", layout="wide")

# Configure logger
logger = setup_logger(__name__)


# 定义缓存文件路径
KEYWORD_CACHE_FILE = 'x_keyword_cache.json'

def load_keyword_from_cache():
    """从缓存文件加载关键字"""
    if os.path.exists(KEYWORD_CACHE_FILE):
        with open(KEYWORD_CACHE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('keyword', '')
    return ''


# 从URL读取缓存数据
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if 'language' not in st.session_state:
    st.session_state.language = st.query_params.get('language')
if 'cached_keyword' not in st.session_state:
    st.session_state.cached_keyword = load_keyword_from_cache()

# check access
if st.session_state.access_code and st.session_state.access_code in os.environ['ACCESS_CODE_LIST'].split(','):
    st.query_params.access_code = st.session_state.access_code
    st.query_params.language = st.session_state.language
    sidebar_for_x()
else:
    st.warning("Access not Granted!")
    time.sleep(3)
    st.switch_page("主页.py")

# Force responsive layout for columns also on mobile
st.write(
    """<style>
    [data-testid="column"] {
        width: calc(50% - 1rem);
        flex: 1 1 calc(50% - 1rem);
        min-width: calc(50% - 1rem);
    }
    </style>""",
    unsafe_allow_html=True,
)

# Hide Streamlit elements
hide_streamlit_style = """
            <style>
            .stDeployButton {visibility: hidden;}
            [data-testid="stToolbar"] {visibility: hidden !important;}
            footer {visibility: hidden !important;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# 添加大标题
st.title("小红书_智能助手")

# 创建数据库连接
db = MySQLDatabase()
db.connect()

# 创建标签页
tab1, tab2, tab3, tab4, tab5 = st.tabs(["数据采集", "数据预处理", "数据分析_AI", "文案生成_AI", "触达用户"])

try:
    # 在每个标签页中运行相应的py文件内容
    with tab1:
        data_collect(db)

    with tab2:
        data_filter(db)

    with tab3:
        data_analyze(db)

    with tab4:
        generate_msg(db)

    with tab5:
        send_msg(db)
except Exception as e:
    logger.error(f"发生错误: {e}")
    st.error(f"发生错误: {e}")
finally:
    # 关闭数据库连接
    db.disconnect()
