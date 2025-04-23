#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging

from common.mysql import MySQLDatabase
from common.log_config import setup_logger

# 设置日志
logger = setup_logger(__name__)


def preprocess_comment(comment):
    """预处理评论内容，删除逗号、单引号和双引号"""
    return comment.replace(',', '').replace("'", "").replace('"', '')


def data_filter(db: MySQLDatabase):
    """
    本页面用于过滤和处理X评论数据。
    """
    # 全局面板
    st.info("本页面用于过滤和处理X评论数据。")

    # 获取所有关键字
    keywords = db.get_all_x_keywords()

    # 创建下拉框让用户选择关键字，使用session_state中的cached_keyword作为默认值
    if 'cached_keyword' not in st.session_state:
        st.session_state.cached_keyword = keywords[0] if keywords else ""
    
    selected_keyword = st.selectbox("选择关键字", keywords, 
                                    index=keywords.index(st.session_state.cached_keyword) if st.session_state.cached_keyword in keywords else 0,
                                    key="filter_keyword_select")

    if selected_keyword:
        # 获取选定关键字的评论数据
        comments = db.get_xhs_comments_by_keyword(selected_keyword)

        if comments:
            st.write(f"原始评论数量: {len(comments)}")

            # 2. 内容质量过滤
            # 添加过滤条件的控制面板
            st.subheader("过滤条件设置")
            
            # 使用列布局来组织过滤条件
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # 点赞数过滤
                min_likes = st.number_input("最小点赞数", min_value=0, value=0)
            
            with col2:
                # 评论长度过滤
                min_length = st.number_input("最小评论长度", min_value=2, value=2)
            
            with col3:
                # 关键词过滤
                filter_keywords = st.text_input("过滤关键词（用逗号分隔）").split(',')

            # 过滤和处理数据
            df = pd.DataFrame(comments)
            
            # 1. 基础数据清洗
            # 去除空评论
            df = df.dropna(subset=['content'])
            
            # 去除纯表情符号评论（使用正则表达式匹配表情符号）
            df = df[df['content'].str.replace(r'[\U0001F300-\U0001F9FF]', '', regex=True).str.strip().str.len() > 0]
            
            # 去除重复评论
            df = df.drop_duplicates(subset=['content'])
            
            # 去除过短评论（少于2个字符）
            df = df[df['content'].str.len() > 2]
            
            # 应用用户设置的过滤条件
            if min_likes > 0:
                df = df[df['likes'] >= min_likes]
            
            df = df[df['content'].str.len() >= min_length]
            
            if filter_keywords and filter_keywords[0]:  # 确保输入不为空
                filter_pattern = '|'.join(filter_keywords)
                df = df[df['content'].str.contains(filter_pattern, case=False, na=False)]
            
            # 显示过滤统计
            st.write(f"过滤后评论数量: {len(df)}")
            
            # 显示过滤后的数据
            st.subheader("过滤后的评论数据")
            st.dataframe(df)
            
            # 将过滤后的数据保存到session_state中
            st.session_state.filtered_comments = df
            
            # 添加一个按钮来传递数据到分析页面
            if st.button("传递到分析页面"):
                st.success("数据已传递到分析页面，请切换到分析标签页查看")

        else:
            st.warning("⚠️ 没有找到相关评论数据")


    # TODO(ivy): 添加数据过滤规则 和 保存过滤后的数据
