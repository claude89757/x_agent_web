#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging

from common.mysql import MySQLDatabase
from common.log_config import setup_logger

# 设置日志
logger = setup_logger(__name__)

def data_collect(db: MySQLDatabase):
    """
    本页面用于从X收集数据并创建数据采集任务。
    """
    # 全局面板
    st.info(f"本页面用于从X收集数据并创建数据采集任务")

    # 获取所有关键字
    keywords = db.get_all_x_keywords()

    # 创建下拉框让用户选择关键字，使用session_state中的cached_keyword作为默认值
    if 'cached_keyword' not in st.session_state:
        st.session_state.cached_keyword = keywords[0] if keywords else ""
    
    selected_keyword = st.selectbox("选择关键字", keywords, 
                                    index=keywords.index(st.session_state.cached_keyword) if st.session_state.cached_keyword in keywords else 0,
                                    key="collect_keyword_select")
    
    # 显示当前关键字
    st.subheader(f"当前关键字: {selected_keyword}")

    # 获取选定关键字的笔记
    notes = db.get_xhs_notes_by_keyword(selected_keyword)

    if notes:
        df = pd.DataFrame(notes)
        st.write(f"原始笔记数量: {len(notes)}")
        st.dataframe(df)
    else:
        st.warning("⚠️ 没有找到相关笔记数据")

    st.divider()

    # 获取选定关键字的评论
    comments = db.get_xhs_comments_by_keyword(selected_keyword)

    if comments:
        df = pd.DataFrame(comments)
        st.write(f"原始评论数量: {len(comments)}")
        st.dataframe(df)
    else:
        st.warning("⚠️ 没有找到相关评论数据")
    
    
    
    
    
    