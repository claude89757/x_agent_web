#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging
import os
import json
import requests
from dotenv import load_dotenv

from common.mysql import MySQLDatabase
from common.log_config import setup_logger

# 加载环境变量
load_dotenv()

# 设置日志
logger = setup_logger(__name__)

# OpenAI API配置
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

def analyze_comment_with_openai(author, content):
    """
    使用OpenAI API分析评论
    """
    if not OPENAI_API_KEY:
        st.error("未设置OpenAI API密钥，请在.env文件中添加OPENAI_API_KEY")
        return None
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    
    # 构建提示词
    prompt = f"""
    请分析以下小红书评论，并提供以下信息：
    1. 用户意向度（高/中/低）
    2. 用户兴趣点
    3. 潜在需求
    4. 建议回复策略
    
    用户名: {author}
    评论内容: {content}
    
    请直接返回JSON格式结果，不要添加任何其他标记，格式如下:
    {{
        "意向度": "高/中/低",
        "兴趣点": "简短描述",
        "潜在需求": "简短描述",
        "回复策略": "简短建议"
    }}
    """
    
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {"role": "system", "content": "你是一个专业的社交媒体营销分析师，擅长分析用户评论并提供营销建议。请直接返回JSON格式数据，不要添加markdown代码块标记。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7
    }
    
    try:
        response = requests.post(OPENAI_API_URL, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        
        # 提取AI回复内容
        ai_response = result["choices"][0]["message"]["content"]
        
        # 清理可能的markdown代码块标记
        cleaned_response = ai_response
        # 移除可能的```json和```标记
        if "```json" in cleaned_response:
            cleaned_response = cleaned_response.replace("```json", "")
        if "```" in cleaned_response:
            cleaned_response = cleaned_response.replace("```", "")
        
        # 尝试解析JSON
        try:
            # 去除前后空白字符
            cleaned_response = cleaned_response.strip()
            analysis_result = json.loads(cleaned_response)
            return analysis_result
        except json.JSONDecodeError as e:
            # 如果无法解析为JSON，返回原始文本
            logger.error(f"无法解析AI回复为JSON: {ai_response}, 错误: {str(e)}")
            return {"错误": "无法解析AI回复", "原始回复": ai_response}
            
    except Exception as e:
        logger.error(f"调用OpenAI API时出错: {str(e)}")
        return {"错误": str(e)}

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
        
        # 添加分析内容按钮
        if st.button("分析内容", type="primary"):
            # 显示进度条
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 创建结果DataFrame
            results = []
            
            # 限制分析的评论数量，避免API调用过多
            max_comments = min(len(df), 20)  # 最多分析20条评论
            
            status_text.text(f"正在分析评论，共 {max_comments} 条...")
            
            # 分析每条评论
            for i, row in df.head(max_comments).iterrows():
                author = row.get('author', '未知用户')
                content = row.get('content', '')
                
                # 更新进度
                progress = min((i + 1) / max_comments, 1.0)  # 确保不超过1.0
                progress_bar.progress(progress)
                status_text.text(f"正在分析第 {i+1}/{max_comments} 条评论...")
                
                # 调用OpenAI API分析评论
                analysis = analyze_comment_with_openai(author, content)
                
                if analysis:
                    # 将原始评论和分析结果合并
                    result = {
                        "用户名": author,
                        "评论内容": content,
                        "点赞数": row.get('likes', 0)
                    }
                    
                    # 添加分析结果
                    if isinstance(analysis, dict):
                        result.update(analysis)
                    
                    results.append(result)
            
            # 完成分析
            progress_bar.progress(1.0)
            status_text.text("分析完成！")
            
            # 显示分析结果
            if results:
                st.subheader("评论分析结果")
                result_df = pd.DataFrame(results)
                st.dataframe(result_df)
                
                # 提供下载分析结果的选项
                csv = result_df.to_csv(index=False)
                st.download_button(
                    label="下载分析结果 (CSV)",
                    data=csv,
                    file_name="评论分析结果.csv",
                    mime="text/csv",
                )
            else:
                st.warning("分析过程中出现错误，未能生成分析结果")
    else:
        st.warning("没有找到过滤后的评论数据，请先在数据预处理页面进行过滤")
