#!/usr/bin/env python
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import logging

from common.mysql import MySQLDatabase
from common.log_config import setup_logger

# 设置日志
logger = setup_logger(__name__)


def send_msg(db: MySQLDatabase):
    st.info("自动批量关注、留言、发送推广信息给高意向客户")

