#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging
import datetime
import os
from dotenv import load_dotenv

from common.mysql import MySQLDatabase
from common.log_config import setup_logger
from common.airflow import AirflowClient

# 加载环境变量
load_dotenv()

# 设置日志
logger = setup_logger(__name__)


def send_msg(db: MySQLDatabase):
    """自动批量关注、留言、发送推广信息给高意向客户"""
    st.title("发送小红书评论回复")
    st.info("在此页面可以批量发送已生成的评论回复")
    
    # 初始化表，确保必要的表存在
    db.initialize_tables()
    
    # 检查是否有从generate_msg页面传递过来的选中评论
    if 'comments_to_send' in st.session_state and st.session_state.comments_to_send:
        comments = st.session_state.comments_to_send
        st.success(f"已接收 {len(comments)} 条待发送的评论回复")
        
        # 将数据转换为DataFrame并显示为表格
        st.subheader("待发送的评论列表")
        
        # 创建DataFrame
        comments_df = pd.DataFrame(comments)
        
        # 如果有注意用户体验，可以对大型字段进行处理
        if 'content' in comments_df.columns:
            comments_df['content'] = comments_df['content'].apply(lambda x: x[:100] + '...' if len(x) > 100 else x)
        if 'reply' in comments_df.columns:
            comments_df['reply'] = comments_df['reply'].apply(lambda x: x[:100] + '...' if len(x) > 100 else x)
            
        # 使用st.dataframe显示表格
        st.dataframe(comments_df, use_container_width=True)
        
        # 添加自动发送功能
        st.markdown("---")
        st.subheader("自动发送功能")
        
        # 初始化session_state变量
        if 'dag_run_id' not in st.session_state:
            st.session_state.dag_run_id = None
        if 'dag_id' not in st.session_state:
            st.session_state.dag_id = "xhs_comments_replier"
        if 'sending_status' not in st.session_state:
            st.session_state.sending_status = None
        
        # 添加发送按钮
        if st.button("开始自动发送评论回复", type="primary"):
            # 获取评论ID列表
            comment_ids = [comment['comment_id'] for comment in comments]
            
            # 显示进度信息
            status_text = st.empty()
            status_text.text(f"正在准备发送评论回复，共 {len(comment_ids)} 条...")
            
            # 准备DAG运行配置
            conf = {
                "comment_ids": comment_ids
            }
            
            # 生成唯一的 DAG 运行 ID
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            dag_run_id = f"xhs_comments_replier_{timestamp}"
            
            # 触发DAG运行
            try:
                # 创建AirflowClient实例
                airflow = AirflowClient(base_url="https://marketing.lucyai.sale/airflow")
                
                dag_id = "xhs_comments_replier"
                result = airflow.trigger_dag_run(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    conf=conf
                )
                
                if result and 'dag_run_id' in result:
                    # 保存DAG运行ID到session_state
                    st.session_state.dag_run_id = result['dag_run_id']
                    st.session_state.dag_id = dag_id
                    st.session_state.sending_status = "running"
                    
                    # 显示DAG运行状态
                    status_text.text(f"已提交发送任务，任务ID: {result['dag_run_id']}，正在处理中...")
                    st.success(f"成功启动自动发送任务，任务ID: {result['dag_run_id']}")
                    
                    # 更新数据库中的发送状态
                    for comment in comments:
                        db.execute_query(
                            "UPDATE comment_reply SET is_sent = 1 WHERE id = %s",
                            (comment['id'],)
                        )
                    
                    # 清空选择，避免重复发送
                    st.session_state.comments_to_send = []
                else:
                    st.error("提交发送任务失败，请检查Airflow服务是否正常运行")
            except Exception as e:
                logger.error(f"触发DAG运行时出错: {str(e)}")
                st.error(f"发送过程中出现错误: {str(e)}")
        
        # 检查任务状态
        if st.session_state.dag_run_id and st.session_state.sending_status == "running":
            status_container = st.container()
            if st.button("刷新发送状态"):
                try:
                    # 创建AirflowClient实例检查状态
                    airflow = AirflowClient(base_url="https://marketing.lucyai.sale/airflow")
                    
                    # 获取DAG运行状态
                    status = airflow.get_dag_run_status(
                        dag_id=st.session_state.dag_id,
                        dag_run_id=st.session_state.dag_run_id
                    )
                    
                    # 根据状态显示不同信息
                    if status == "success":
                        status_container.success("评论回复发送任务已完成！")
                        st.session_state.sending_status = "completed"
                    elif status == "failed":
                        status_container.error("评论回复发送任务失败，请检查Airflow日志或重试")
                        st.session_state.sending_status = "failed"
                    elif status == "running":
                        status_container.info("评论回复发送任务仍在执行中，请稍后刷新查看状态")
                    else:
                        status_container.warning(f"评论回复发送任务状态: {status}")
                except Exception as e:
                    status_container.error(f"获取发送状态时出错: {str(e)}")
                    logger.error(f"获取发送状态时出错: {str(e)}")
        
        # 添加清除按钮
        if st.button("清除当前选择"):
            # 清除当前选择
            st.session_state.comments_to_send = []
            st.success("已清除当前选择的评论")
            st.rerun()
    else:
        st.warning("未找到需要发送的评论，请先在生成页面选择要发送的评论")

