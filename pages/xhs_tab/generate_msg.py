#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging

from common.mysql import MySQLDatabase
from common.log_config import setup_logger

# 设置日志
logger = setup_logger(__name__)

def generate_msg(db: MySQLDatabase):
    """
    生成推广信息给高意向客户
    """
    st.info("AI生成推广文案")
