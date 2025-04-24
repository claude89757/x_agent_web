#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging
import datetime

from common.mysql import MySQLDatabase
from common.log_config import setup_logger
from common.airflow import AirflowClient

# 设置日志
logger = setup_logger(__name__)


def create_notes_collection_task():
    """
    本函数用于创建笔记采集任务
    """
    st.subheader("创建笔记采集任务")
    with st.form(key="create_notes_collection_task"):
        col1, col2 = st.columns(2)
        with col1:
            # 允许用户输入关键字
            keyword = st.text_input("关键字")
        
        with col2:
            note_count = st.number_input("采集笔记数量", min_value=1, max_value=1000, value=100, step=10, 
                                      help="指定要采集的笔记数量上限")
        
        submit_button = st.form_submit_button(label="创建笔记采集任务")
        
        if submit_button:
            try:
                # 验证关键字是否输入
                if not keyword.strip():
                    st.error("请输入关键字")
                    return
                
                # 创建AirflowClient实例
                airflow = AirflowClient()
                
                # 自动生成任务ID和备注
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                # 将关键词转换为 ASCII 安全的字符串
                safe_keyword = ''.join(c if c.isalnum() or c == '_' else '_' for c in keyword)
                # 确保 dag_run_id 只包含字母、数字和下划线
                dag_run_id = f"xhs_{safe_keyword}_{timestamp}"
                
                # 准备配置参数
                conf = {
                    "keyword": keyword,  # 保留原始关键词用于实际查询
                    "note_count": int(note_count)
                }
                
                # 触发DAG运行
                result = airflow.trigger_dag_run(
                    dag_id="xhs_notes_collector",
                    dag_run_id=dag_run_id,
                    conf=conf,
                )

                st.success(f"成功创建笔记采集任务，任务ID: {result.get('dag_run_id')}")
                logger.info(f"成功创建笔记采集任务，关键词: {keyword}, 笔记数量: {note_count}, 任务ID: {result.get('dag_run_id')}")
            except Exception as e:
                st.error(f"创建笔记采集任务失败: {str(e)}")
                logger.error(f"创建笔记采集任务失败: {str(e)}")


def get_recent_notes_collection_tasks():
    """
    本函数用于获取最近几个笔记采集任务
    """
     # （表格形式）显示最近几个dagrun的运行状态
    st.subheader("最近的笔记采集任务")
    try:
        # 创建AirflowClient实例
        airflow = AirflowClient()
        
        # 获取最近的DAG运行，使用order_by参数按开始时间降序排序
        dag_runs = airflow.get_dag_runs(dag_id="xhs_notes_collector", limit=10, order_by="-start_date")
        
        if dag_runs and 'dag_runs' in dag_runs:
            # 提取有用信息
            runs_data = []
            for run in dag_runs['dag_runs']:
                runs_data.append({
                    "任务ID": run.get('dag_run_id'),
                    "状态": run.get('state'),
                    "开始时间": run.get('start_date'),
                    "结束时间": run.get('end_date'),
                    "备注": run.get('note'),
                    "配置": str(run.get('conf'))
                })
            
            # 显示表格
            if runs_data:
                st.dataframe(pd.DataFrame(runs_data))
            else:
                st.info("没有找到笔记采集任务记录")
        else:
            st.info("没有找到笔记采集任务记录")
    except Exception as e:
        st.error(f"获取笔记采集任务记录失败: {str(e)}")
        logger.error(f"获取笔记采集任务记录失败: {str(e)}")


def show_collected_notes(db: MySQLDatabase, selected_keyword: str):
    """
    本函数用于展示已采集的笔记
    """        # 获取选定关键字的笔记
    notes = db.get_xhs_notes_by_keyword(selected_keyword)

    if notes:
        df = pd.DataFrame(notes)
        st.write(f"原始笔记数量: {len(notes)}")
        st.dataframe(df)
    else:
        st.warning("⚠️ 没有找到相关笔记数据")
    
    
def show_collected_comments(db: MySQLDatabase, selected_keyword: str):
    """
    本函数用于展示已采集的评论
    """    
    # 获取选定关键字的评论
    comments = db.get_xhs_comments_by_keyword(selected_keyword)

    if comments:
        df = pd.DataFrame(comments)
        st.write(f"原始评论数量: {len(comments)}")
        st.dataframe(df)


def get_show_keyword(db: MySQLDatabase):
    """
    本函数用于获取并展示所有关键字
    """    
    # 获取所有关键字
    keywords = db.get_all_x_keywords()

    # 创建下拉框让用户选择关键字，使用session_state中的cached_keyword作为默认值
    if 'cached_keyword' not in st.session_state:
        st.session_state.cached_keyword = keywords[0] if keywords else ""
    
    selected_keyword = st.selectbox("选择关键字", keywords, 
                                    index=keywords.index(st.session_state.cached_keyword) if st.session_state.cached_keyword in keywords else 0,
                                    key="collect_keyword_select")
    
    return selected_keyword

def data_collect(db: MySQLDatabase):
    """
    本页面用于从X收集数据并创建数据采集任务。
    """
    # 全局面板
    st.info(f"本页面用于从X收集数据并创建数据采集任务")

    # 创建笔记采集任务
    create_notes_collection_task()

    # 获取最近笔记采集任务
    get_recent_notes_collection_tasks()
   
    st.divider()

    # 展示所有关键字
    selected_keyword = get_show_keyword(db)
    
    st.divider()

    # 展示已采集的笔记
    show_collected_notes(db, selected_keyword)

    st.divider()

    # 展示已采集的评论
    show_collected_comments(db, selected_keyword)
