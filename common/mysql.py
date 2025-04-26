#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024年10月13日
@Author  : claude by cursor
@File    : mysql.py
@Software: PyCharm
@Description: MySQL数据库操作的通用模块，使用pymysql库
"""

import os
import pymysql
import logging
from pymysql.converters import escape_string


# 配置日志
logger = logging.getLogger(__name__)


class MySQLDatabase:
    def __init__(self):
        self.host = os.environ['MYSQL_HOST']
        self.port = int(os.environ.get('MYSQL_PORT', 29838))  # 添加端口配置，默认为29838
        self.user = os.environ['MYSQL_USER']
        self.password = os.environ['MYSQL_PASSWORD']
        self.database = os.environ['MYSQL_DATABASE']
        if not all([self.host, self.user, self.password, self.database]):
            raise ValueError("缺少必要的MySQL连接环境变量配置")
        self.connection = None

    def log_sql(self, query, params=None):
        """记录 SQL 查询"""
        if isinstance(params, str):  # 处理批量插入情况
            formatted_query = f"{query} {params}"
        elif params:
            # 使用 pymysql.converters.escape_string 来正确转义参数值
            escaped_params = tuple(escape_string(str(p)) for p in params)
            # 使用 SQL 的格式化方法来插入参数
            formatted_query = query % escaped_params
        else:
            formatted_query = query
        
        logger.info(f"执行 SQL: {formatted_query}")

    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,  # 添加端口参数
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info(f"成功连接到MySQL数据库，地址：{self.host}:{self.port}")
        except pymysql.Error as e:
            logger.error(f"连接数据库时出错: {e}")
    
    def is_connected(self):
        """检查数据库连接是否仍然有效"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1 as is_alive")
                result = cursor.fetchone()
                return result is not None and result['is_alive'] == 1
        except (pymysql.Error, AttributeError):
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")

    def execute_query(self, query, params=None):
        """执行查询操作"""
        self.log_sql(query, params)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                result = cursor.fetchall()
            return result
        except pymysql.Error as e:
            logger.error(f"执行查询时出错: {e}")
            return None

    def execute_update(self, query, params=None):
        """执行更新操作（插入、更新、删除）"""
        self.log_sql(query, params)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
            self.connection.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            logger.error(f"执行更新时出错: {e}")
            self.connection.rollback()
            return -1

    def insert_many(self, query, data):
        """批量插入数据"""
        self.log_sql(query, f"(批量插入 {len(data)} 条记录)")
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, data)
            self.connection.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            logger.error(f"批量插入数据时出错: {e}")
            self.connection.rollback()
            return -1

    def initialize_tables(self):
        """初始化并创建所需的表"""
        create_tables_queries = [
            # self._create_tiktok_tasks_table(),
            # self._create_tiktok_videos_table(),
            # self._create_tiktok_comments_table(),
            # self._create_tiktok_task_logs_table(),
            # self._create_worker_infos_table(),
            # self._create_tiktok_accounts_table(),
            # self._create_tiktok_messages_table(),
            # self._create_tiktok_filtered_comments_table(),
            # self._create_tiktok_analyzed_comments_table(),
            # self._create_tiktok_second_round_analyzed_comments_table(),
            self._create_reply_template_table(),
        ]

        for query in create_tables_queries:
            self.execute_update(query)
        
        logger.info("所有必要的表和索引已创建或已存在")
        
    def _create_reply_template_table(self):
        """创建回复模板表"""
        return """
        CREATE TABLE IF NOT EXISTS reply_template (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(50) NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_user_id (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    
    def get_all_x_keywords(self):
        """获取所有关键字"""
        query = "SELECT DISTINCT keyword FROM xhs_notes"
        result = self.execute_query(query)
        if result:
            return [row['keyword'] for row in result]
        return []
    
    def get_xhs_comments_by_keyword(self, keyword):
        """获取指定关键字的评论"""
        query = "SELECT * FROM xhs_comments WHERE keyword = %s"
        params = (keyword,)
        result = self.execute_query(query, params)
        return result
    
    def get_xhs_comments_by_urls(self, urls):
        """获取指定URL的评论"""
        query = "SELECT * FROM xhs_comments WHERE note_url IN (%s)"
        params = tuple(urls)
        result = self.execute_query(query, params)
        return result
    
    def get_xhs_comments(self, limit=100):
        """获取指定关键字的评论"""
        query = "SELECT * FROM xhs_comments LIMIT %s"
        params = (limit,)
        result = self.execute_query(query, params)
        return result


    def get_xhs_notes_by_keyword(self, keyword):
        """获取指定关键字的笔记"""
        query = "SELECT * FROM xhs_notes WHERE keyword = %s"
        params = (keyword,)
        result = self.execute_query(query, params)
        return result
        
    def get_reply_templates(self, user_id="zacks"):
        """获取用户的回复模板"""
        query = "SELECT id, user_id, content, created_at FROM reply_template WHERE user_id = %s ORDER BY created_at DESC"
        params = (user_id,)
        result = self.execute_query(query, params)
        return result
    
    def add_reply_template(self, content, user_id="zacks"):
        """添加回复模板"""
        query = "INSERT INTO reply_template (user_id, content) VALUES (%s, %s)"
        params = (user_id, content)
        return self.execute_update(query, params)
    
    def add_reply_templates(self, templates, user_id="zacks"):
        """批量添加回复模板"""
        if not templates:
            return 0
        query = "INSERT INTO reply_template (user_id, content) VALUES (%s, %s)"
        data = [(user_id, template) for template in templates]
        return self.insert_many(query, data)
    
    def delete_reply_template(self, template_id, user_id="zacks"):
        """删除指定ID的回复模板"""
        query = "DELETE FROM reply_template WHERE id = %s AND user_id = %s"
        params = (template_id, user_id)
        return self.execute_update(query, params)
    
    def delete_all_reply_templates(self, user_id="zacks"):
        """删除用户的所有回复模板"""
        query = "DELETE FROM reply_template WHERE user_id = %s"
        params = (user_id,)
        return self.execute_update(query, params)
        
    def update_reply_template(self, template_id, content, user_id="zacks"):
        """更新指定ID的回复模板内容"""
        query = "UPDATE reply_template SET content = %s WHERE id = %s AND user_id = %s"
        params = (content, template_id, user_id)
        return self.execute_update(query, params)


# 使用示例
if __name__ == "__main__":
    pass