# 智能助手项目

## 简介
本项目是一个基于 Streamlit 的智能助手应用，旨在通过数据收集、分析和生成个性化消息来帮助用户进行市场营销。该应用集成了多种功能模块，支持从平台收集信息，并利用大模型进行数据分析和消息生成。

## 功能
- **数据收集**：从平台自动化收集用户评论，支持多任务并行处理。
- **数据过滤**：对收集到的评论进行智能过滤，去除无关信息，保留有价值的用户反馈。
- **数据分析**：使用 GPT 模型分析评论，识别潜在客户和市场趋势。
- **消息生成**：为高意向客户生成个性化的推广信息，提升营销效果。
- **消息发送**：通过平台 API 自动发送生成的消息，支持批量操作。

## 环境设置

## 安装
1. 克隆本仓库：
   ```bash
   git clone https://github.com/yourusername/x_agent_web.git
   ```
2. 进入项目目录：
   ```bash
   cd x_agent_web
   ```
3. 安装依赖和环境变量配置：
   ```bash
   pip install -r requirements.txt

   cp .env.example .env

   # 填入环境变量信息
   vim .env 
   ```


## 使用
1. 启动 Streamlit 应用：
   ```bash
   streamlit run 主页.py --server.port=80
   ```
   
2. 后台启动 Streamlit 应用：
   ```bash
   nohup streamlit run 主页.py --server.port=80 > streamlit.log 2>&1 &
   ```
2. 在浏览器中访问 `http://localhost:8501`，根据界面提示进行操作。


## 目录结构
- `pages/`：包含不同功能模块的实现，如数据收集、分析和消息生成。
- `common/`：包含通用配置、日志和工具模块。
- `sidebar.py`：定义侧边栏的布局和功能。

## 贡献
欢迎贡献代码！请 fork 本仓库并提交 pull request。我们欢迎任何形式的贡献，包括但不限于代码、文档和测试。

## 许可证
本项目采用 MIT 许可证。详情请参阅 `LICENSE` 文件。

## 联系
如有任何问题或建议，请通过 [claude89757@gmail.com](mailto:claude89757@gmail.com) 联系我们。
