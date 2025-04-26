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

def generate_msg(db: MySQLDatabase):
    """
    生成推广信息给高意向客户
    """
    # 初始化表，确保必要的表存在
    db.initialize_tables()
    
    st.info("AI生成推广文案")
    
    # 展示意向客户数据
    st.subheader("意向客户数据")
    st.info("本部分展示已分析的意向客户数据，可通过关键词和意向类型进行筛选")
    
    # 添加查询意向客户数据的方法
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
            selected_keyword = st.selectbox("按关键词筛选", keywords, key="msg_keyword_filter")
        
        with col2:
            selected_intent = st.selectbox("按意向类型筛选", intents, key="msg_intent_filter")
        
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
            if st.button("导出筛选后的意向客户数据", key="msg_export_intent_data"):
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
                    key="msg_download_intent_csv"
                )
                st.success(f"数据已准备好，点击上方按钮下载 {filename}")
            
            # 文案生成部分
        #     st.markdown("---")
        #     st.subheader("ai批量生成回复文案")
            
        #     # 初始化session_state变量
        #     if 'dag_run_id' not in st.session_state:
        #         st.session_state.dag_run_id = None
        #     if 'dag_id' not in st.session_state:
        #         st.session_state.dag_id = "xhs_reply_generator"
        #     if 'generation_status' not in st.session_state:
        #         st.session_state.generation_status = None
            
        #     # 检查是否有可用的评论ID
        #     if 'comment_id' in customer_df.columns or 'id' in customer_df.columns:
        #         # 显示评论数量
        #         comment_count = len(customer_df)
        #         st.write(f"检测到 {comment_count} 条评论可用于生成回复文案")
                
        #         # 提示文本输入
        #         reply_prompt = st.text_area(
        #             "请输入回复文案生成提示", 
        #             placeholder="例如：请生成一段友好的回复，告知用户我们的产品优势和特点...",
        #             help="输入提示内容以指导AI生成适合的回复文案"
        #         )
                
        #         # 文案生成按钮
        #         if st.button("生成回复文案", type="primary"):
        #             if not reply_prompt.strip():
        #                 st.warning("请输入回复文案生成提示")
        #             else:
        #                 # 显示进度信息
        #                 status_text = st.empty()
        #                 status_text.text(f"正在准备生成回复文案，共 {comment_count} 条...")
                        
        #                 # 获取所有评论ID - 修改为使用comment_id字段
        #                 if 'comment_id' in customer_df.columns:
        #                     comment_ids = customer_df['comment_id'].tolist()
        #                 elif 'id' in customer_df.columns:
        #                     comment_ids = customer_df['id'].tolist()
        #                 else:
        #                     st.error("无法找到评论ID列，请确保数据中包含'comment_id'或'id'列")
        #                     return
                        
        #                 # 准备DAG运行配置
        #                 conf = {
        #                     "reply_prompt": reply_prompt,
        #                     "comment_ids": comment_ids
        #                 }
                        
        #                 # 生成唯一的 DAG 运行 ID
        #                 timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        #                 dag_run_id = f"xhs_reply_generator_{timestamp}"
                        
        #                 # 触发DAG运行
        #                 try:
        #                     # 创建AirflowClient实例
        #                     airflow = AirflowClient()
                            
        #                     dag_id = "xhs_reply_generator"
        #                     result = airflow.trigger_dag_run(
        #                         dag_id=dag_id,
        #                         dag_run_id=dag_run_id,
        #                         conf=conf
        #                     )
                            
        #                     if result and 'dag_run_id' in result:
        #                         # 保存DAG运行ID到session_state
        #                         st.session_state.dag_run_id = result['dag_run_id']
        #                         st.session_state.dag_id = dag_id
        #                         st.session_state.generation_status = "running"
                                
        #                         # 显示DAG运行状态
        #                         status_text.text(f"已提交生成任务，任务ID: {result['dag_run_id']}，正在处理中...")
        #                     else:
        #                         st.error("提交生成任务失败，请检查Airflow服务是否正常运行")
        #                 except Exception as e:
        #                     logger.error(f"触发DAG运行时出错: {str(e)}")
        #                     st.error(f"生成过程中出现错误: {str(e)}")
                
        #         # 检查生成状态按钮 - 只要有DAG运行ID就显示
        #         if st.session_state.dag_run_id:
        #             status_container = st.container()
                    
        #             # 添加检查状态按钮
        #             if st.button("检查回复文案生成状态", key="check_generation_status"):
        #                 try:
        #                     # 创建AirflowClient实例
        #                     airflow = AirflowClient()
                            
        #                     # 获取DAG运行状态 - 修改为使用get_dag_runs方法
        #                     dag_runs = airflow.get_dag_runs(
        #                         dag_id=st.session_state.dag_id,
        #                         limit=10,  # 限制返回结果数量
        #                         order_by="-start_date"  # 按开始时间降序排序
        #                     )
                            
        #                     # 从结果中查找特定的DAG运行
        #                     dag_run = None
        #                     if dag_runs and 'dag_runs' in dag_runs:
        #                         for run in dag_runs['dag_runs']:
        #                             if run.get('dag_run_id') == st.session_state.dag_run_id:
        #                                 dag_run = run
        #                                 break
                            
        #                     if dag_run:
        #                         state = dag_run.get('state', 'UNKNOWN')
        #                         st.session_state.generation_status = state
                                
        #                         # 根据状态显示不同的信息
        #                         if state == 'success':
        #                             status_container.success(f"回复文案生成任务已完成。任务ID: {st.session_state.dag_run_id}")
                                    
        #                             # 添加查看结果的按钮或链接
        #                             # 这里可以添加查询和显示生成结果的代码
        #                             status_container.info("请到结果查询页面查看生成的回复文案。")
                                    
        #                         elif state == 'running' or state == 'queued':
        #                             status_container.info(f"回复文案生成任务正在进行中... 状态: {state}")
        #                         else:
        #                             status_container.warning(f"回复文案生成任务状态: {state}")
        #                     else:
        #                         status_container.error("无法获取回复文案生成任务状态，请稍后再试")
        #                 except Exception as e:
        #                     logger.error(f"获取DAG运行状态时出错: {str(e)}")
        #                     status_container.error(f"获取任务状态时出现错误: {str(e)}")
        #     else:
        #         st.warning("客户数据中缺少ID字段，无法生成回复文案")
        # else:
        #     st.warning("未找到符合条件的意向客户数据")
        
        # 添加设置语料库部分
        st.markdown("---")
        # 设置跟评语料库部分
        st.subheader("人工设置跟评语料库")
        st.info("本部分用于设置和管理跟评语料库，可用于生成回复文案的参考")
        
        # 默认用户ID
        user_id = "zacks"
        
        # 选择语料数量
        corpus_count = st.slider("选择要编辑的语料数量", min_value=5, max_value=20, value=5, step=1)
        
        # 查询当前语料库
        reply_templates = db.get_reply_templates(user_id)
        template_count = len(reply_templates) if reply_templates else 0
        
        st.write(f"当前语料库共有 {template_count} 条语料。")

        # 准备表单提交的数据
        template_inputs = []
        template_ids = []
        
        # 判断是修改模式还是添加模式
        if template_count >= corpus_count:
            st.subheader("编辑现有语料")
            # 如果已有足够模板，以编辑模式显示
            for i in range(corpus_count):
                template = reply_templates[i] if i < template_count else None
                template_content = template['content'] if template else ""
                template_id = template['id'] if template else None
                
                content = st.text_area(
                    f"语料 {i+1}" + (f" (ID: {template_id})" if template_id else ""),
                    value=template_content,
                    placeholder="请输入跟评语料...",
                    key=f"edit_corpus_{i}"
                )
                
                template_inputs.append(content)
                template_ids.append(template_id)
            
            # 更新按钮和清空按钮并排
            col1, col2 = st.columns([1, 1])
            with col1:
                update_button = st.button("更新语料", type="primary", key="update_templates")
            with col2:
                clear_button = st.button("清空语料库", type="secondary", key="clear_templates")
                
            # 处理清空语料库
            if clear_button:
                if db.delete_all_reply_templates(user_id):
                    st.success("语料库已清空")
                    st.rerun()  # 清空后刷新页面
                else:
                    st.error("清空语料库时出错")
            
            # 处理更新语料
            if update_button:
                updated = False
                added = False
                
                for i, content in enumerate(template_inputs):
                    if not content.strip():
                        continue
                        
                    if template_ids[i]:
                        # 如果有ID，使用更新函数直接更新
                        update_result = db.update_reply_template(template_ids[i], content, user_id)
                        updated = update_result > 0
                    else:
                        # 没有ID则添加新语料
                        db.add_reply_template(content, user_id)
                        added = True
                
                if updated and added:
                    st.success("成功更新和添加语料")
                elif updated:
                    st.success("成功更新语料")
                elif added:
                    st.success("成功添加新语料")
                else:
                    st.warning("没有有效的语料更新")
                    
                st.rerun()  # 更新后刷新页面
        else:
            # 如果模板数量不足，显示现有模板为可编辑模式，并添加新的输入框
            st.subheader("添加和编辑语料")
            
            # 先显示已存在的模板
            for i, template in enumerate(reply_templates):
                content = st.text_area(
                    f"语料 {i+1} (ID: {template['id']})",
                    value=template['content'],
                    placeholder="请输入跟评语料...",
                    key=f"exist_corpus_{i}"
                )
                
                template_inputs.append(content)
                template_ids.append(template['id'])
            
            # 然后添加新的输入框
            for i in range(template_count, corpus_count):
                content = st.text_area(
                    f"语料 {i+1} (新)",
                    placeholder="请输入跟评语料...",
                    key=f"new_corpus_{i}"
                )
                
                template_inputs.append(content)
                template_ids.append(None)  # 新的输入框没有ID
            
            # 保存按钮和清空按钮并排
            col1, col2 = st.columns([1, 1])
            with col1:
                save_button = st.button("保存语料", type="primary", key="save_templates")
            with col2:
                clear_button = st.button("清空语料库", type="secondary", key="clear_templates2")
                
            # 处理清空语料库
            if clear_button:
                if db.delete_all_reply_templates(user_id):
                    st.success("语料库已清空")
                    st.rerun()  # 清空后刷新页面
                else:
                    st.error("清空语料库时出错")
            
            # 处理保存语料
            if save_button:
                updated = False
                added = False
                
                # 处理现有模板的更新
                for i in range(template_count):
                    if not template_inputs[i].strip():
                        continue
                        
                    # 使用更新函数直接更新
                    update_result = db.update_reply_template(template_ids[i], template_inputs[i], user_id)
                    updated = update_result > 0
                
                # 收集新的语料
                new_templates = []
                for i in range(template_count, corpus_count):
                    if template_inputs[i].strip():
                        new_templates.append(template_inputs[i])
                
                # 批量添加新语料
                if new_templates:
                    added_count = db.add_reply_templates(new_templates, user_id)
                    added = added_count > 0
                
                if updated and added:
                    st.success("成功更新现有语料并添加新语料")
                elif updated:
                    st.success("成功更新语料")
                elif added:
                    st.success(f"成功添加 {added_count} 条新语料")
                else:
                    st.warning("没有有效的语料更新或添加")
                    
                st.rerun()  # 保存后刷新页面
        
        # 展示全部语料列表
        if reply_templates and template_count > corpus_count:
            # 显示所有语料列表，只展示不能编辑
            st.write(f"以下显示全部 {template_count} 条语料，只有前 {corpus_count} 条可编辑。")
            
            for i, template in enumerate(reply_templates, 1):
                col1, col2 = st.columns([5, 1])
                with col1:
                    # 显示模板内容，不可编辑
                    st.text_area(f"语料 {i} (ID: {template['id']})", template['content'], disabled=True, key=f"view_corpus_{i}")
                with col2:
                    # 为每个语料添加删除按钮
                    if st.button(f"删除", key=f"delete_{template['id']}"):
                            if db.delete_reply_template(template['id'], user_id):
                                st.rerun()  # 删除后刷新页面
        
        # 添加分隔线
        st.markdown("---")
        
        # 展示已生成的回复文案
        st.subheader("ai已生成的回复文案")
        st.info("本部分展示已生成的回复文案，可查看和导出")
    
    except Exception as e:
        logger.error(f"获取意向客户数据时出错: {str(e)}")
        st.error(f"获取意向客户数据时出现错误: {str(e)}")
    
    try:
        # 查询已生成的回复文案
        query = """
        SELECT cr.id, cr.comment_id, cr.author, cr.content, cr.reply, 
               cr.note_url, cr.generated_at, cr.is_sent
        FROM comment_reply cr
        ORDER BY cr.generated_at DESC
        LIMIT 100
        """
        
        reply_data = db.execute_query(query)
        
        if reply_data:
            # 转换为DataFrame并显示
            reply_df = pd.DataFrame(reply_data)
            
            # 显示数据统计
            st.write(f"找到 {len(reply_df)} 条已生成的回复文案")
            
            # 添加筛选功能
            is_sent_options = ["全部", "已发送", "未发送"]
            selected_is_sent = st.selectbox("按发送状态筛选", is_sent_options, key="reply_is_sent_filter")
            
            # 根据发送状态筛选
            if selected_is_sent == "已发送":
                filtered_reply_df = reply_df[reply_df['is_sent'] == 1]
            elif selected_is_sent == "未发送":
                filtered_reply_df = reply_df[reply_df['is_sent'] == 0]
            else:
                filtered_reply_df = reply_df
            
            # 显示筛选后的数据统计
            st.write(f"筛选后共 {len(filtered_reply_df)} 条回复文案")
            
            # 显示数据表格
            st.dataframe(filtered_reply_df, use_container_width=True)
            
            # 添加导出功能
            if st.button("导出回复文案", key="export_reply_data"):
                # 将DataFrame转换为CSV
                csv = filtered_reply_df.to_csv(index=False)
                
                # 创建下载按钮
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"回复文案_{timestamp}.csv"
                
                st.download_button(
                    label="下载CSV文件",
                    data=csv,
                    file_name=filename,
                    mime="text/csv",
                    key="download_reply_csv"
                )
                st.success(f"数据已准备好，点击上方按钮下载 {filename}")
        else:
            st.warning("未找到已生成的回复文案数据")
    
    except Exception as e:
        logger.error(f"获取回复文案数据时出错: {str(e)}")
        st.error(f"获取回复文案数据时出现错误: {str(e)}")
