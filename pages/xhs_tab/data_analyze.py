#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging
import os
import json
import datetime
from dotenv import load_dotenv

from common.mysql import MySQLDatabase
from common.log_config import setup_logger
from common.airflow import AirflowClient

# 加载环境变量
load_dotenv()

# 设置日志
logger = setup_logger(__name__)

def data_analyze(db: MySQLDatabase):
    """
    本页面用于分析和分类X评论数据。
    """
    # 全局面板
    st.info("本页面用于分析和分类X评论数据。")
    
    # 检查是否有缓存的过滤后数据
    if 'filtered_comments' in st.session_state:
        df = st.session_state.filtered_comments
        st.write(f"已加载过滤后的评论数据，共 {len(df)} 条")
        st.dataframe(df)
        
        # 添加用户画像描述输入框
        profile_sentence = st.text_area("请输入用户画像描述（可选）", 
                                        placeholder="例如：这是一个对美妆产品感兴趣的年轻女性用户群体...",
                                        help="添加用户画像描述可以帮助AI更好地理解评论背景")
        
        # 初始化session_state变量
        if 'dag_run_id' not in st.session_state:
            st.session_state.dag_run_id = None
        if 'dag_id' not in st.session_state:
            st.session_state.dag_id = "xhs_comments_openrouter"
        if 'analysis_status' not in st.session_state:
            st.session_state.analysis_status = None
        
        # 添加分析内容按钮
        if st.button("分析内容", type="primary"):
            # 获取评论ID列表 - 修正为使用comment.id而不是表格的排序id
            if 'id' in df.columns:
                comment_ids = df['id'].tolist()
            else:
                st.error("无法找到评论ID列，请确保数据中包含'id'或'comment_id'列")
                return
                
            # 限制分析的评论数量，避免处理过多
            max_comments = min(len(comment_ids), 20)  # 最多分析20条评论
            comment_ids = comment_ids[:max_comments]
            
            # 显示进度信息
            status_text = st.empty()
            status_text.text(f"正在准备分析评论，共 {max_comments} 条...")
            
            # 准备DAG运行配置
            conf = {
                "profile_sentence": profile_sentence,
                "comment_ids": comment_ids
            }
            
            # 生成唯一的 DAG 运行 ID
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            dag_run_id = f"xhs_comments_openrouter_{timestamp}"
            
            # 触发DAG运行
            try:
                # 创建AirflowClient实例
                airflow = AirflowClient()
                
                dag_id = "xhs_comments_openrouter"
                result = airflow.trigger_dag_run(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    conf=conf
                )
                
                if result and 'dag_run_id' in result:
                    # 保存DAG运行ID到session_state
                    st.session_state.dag_run_id = result['dag_run_id']
                    st.session_state.dag_id = dag_id
                    st.session_state.analysis_status = "running"
                    
                    # 显示DAG运行状态
                    status_text.text(f"已提交分析任务，任务ID: {result['dag_run_id']}，正在处理中...")
                else:
                    st.error("提交分析任务失败，请检查Airflow服务是否正常运行")
            except Exception as e:
                logger.error(f"触发DAG运行时出错: {str(e)}")
                st.error(f"分析过程中出现错误: {str(e)}")
        
        # 检查分析状态按钮 - 始终显示，只要有DAG运行ID
        if st.session_state.dag_run_id:
            status_container = st.container()
            
            if st.button("检查分析状态"):
                try:
                    # 创建AirflowClient实例
                    airflow = AirflowClient()
                    
                    # 获取DAG运行状态
                    dag_runs = airflow.get_dag_runs(st.session_state.dag_id)
                    
                    # 查找对应的DAG运行
                    status = "未知"
                    for run in dag_runs.get('dag_runs', []):
                        if run.get('dag_run_id') == st.session_state.dag_run_id:
                            status = run.get('state', '未知')
                            break
                    
                    # 更新分析状态
                    st.session_state.analysis_status = status
                    
                    # 根据状态显示不同信息
                    if status == "success":
                        status_container.success("分析任务已完成！")
                        st.session_state.analysis_complete = True
                    elif status == "failed":
                        status_container.error("分析任务失败，请检查Airflow日志或重试")
                    elif status == "running":
                        status_container.info("分析任务正在运行中，请稍后再次检查")
                    else:
                        status_container.info(f"分析任务状态: {status}，请稍后再次检查")
                except Exception as e:
                    logger.error(f"获取DAG运行状态时出错: {str(e)}")
                    status_container.error(f"获取分析状态时出现错误: {str(e)}")
        
        # 显示当前分析状态
        if st.session_state.analysis_status:
            st.write(f"当前分析任务状态: {st.session_state.analysis_status}")
            
            # 如果分析已完成，显示"生成文案"按钮
            if st.session_state.analysis_status == "success":
                if st.button("生成文案", type="primary", key="generate_content"):
                    st.info("正在生成文案，请稍候...")
                    # 这里添加生成文案的代码
                    # ...
    else:
        st.warning("没有找到过滤后的评论数据，请先在数据预处理页面进行过滤")
    
    # 添加分隔线
    st.markdown("---")
    
    # 展示意向客户表数据
    st.subheader("意向客户数据")
    st.info("本部分展示已分析的意向客户数据，可通过关键词和意向类型进行筛选")
    
    # 添加查询意向客户数据的方法到MySQLDatabase类
    def get_customer_intent(db, keyword=None, intent=None):
        """获取意向客户数据，可按关键词和意向类型筛选"""
        query = "SELECT * FROM customer_intent"
        params = []
        
        # 构建WHERE子句
        where_clauses = []
        if keyword:
            where_clauses.append("keyword = %s")
            params.append(keyword)
        if intent:
            where_clauses.append("intent = %s")
            params.append(intent)
        
        # 添加WHERE子句到查询
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # 执行查询
        result = db.execute_query(query, tuple(params) if params else None)
        return result
    
    try:
        # 获取所有关键词和意向类型，用于筛选
        all_keywords_query = "SELECT DISTINCT keyword FROM customer_intent"
        all_intents_query = "SELECT DISTINCT intent FROM customer_intent"
        
        keywords_result = db.execute_query(all_keywords_query)
        intents_result = db.execute_query(all_intents_query)
        
        # 提取关键词和意向类型列表
        keywords = ["全部"] + [row['keyword'] for row in keywords_result] if keywords_result else ["全部"]
        intents = ["全部"] + [row['intent'] for row in intents_result] if intents_result else ["全部"]
        
        # 创建筛选控件
        col1, col2 = st.columns(2)
        
        with col1:
            selected_keyword = st.selectbox("按关键词筛选", keywords, key="intent_keyword_filter")
        
        with col2:
            selected_intent = st.selectbox("按意向类型筛选", intents, key="intent_type_filter")
        
        # 处理筛选条件
        filter_keyword = None if selected_keyword == "全部" else selected_keyword
        filter_intent = None if selected_intent == "全部" else selected_intent
        
        # 获取筛选后的数据
        customer_data = get_customer_intent(db, filter_keyword, filter_intent)
        
        if customer_data:
            # 转换为DataFrame并显示
            customer_df = pd.DataFrame(customer_data)
            
            # 显示数据统计
            st.write(f"找到 {len(customer_df)} 条意向客户数据")
            
            # 显示数据表格
            st.dataframe(customer_df, use_container_width=True)
            
            # 添加导出功能
            if st.button("导出筛选后的意向客户数据", key="export_intent_data"):
                # 将DataFrame转换为CSV
                csv = customer_df.to_csv(index=False)
                
                # 创建下载按钮
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"意向客户数据_{timestamp}.csv"
                
                st.download_button(
                    label="下载CSV文件",
                    data=csv,
                    file_name=filename,
                    mime="text/csv",
                    key="download_intent_csv"
                )
                st.success(f"数据已准备好，点击上方按钮下载 {filename}")
        else:
            st.warning("未找到符合条件的意向客户数据")
    
    except Exception as e:
        logger.error(f"获取意向客户数据时出错: {str(e)}")
        st.error(f"获取意向客户数据时出现错误: {str(e)}")
