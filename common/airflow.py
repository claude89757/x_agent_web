import requests
import os
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv


# 加载.env文件中的环境变量
load_dotenv()


class AirflowClient:
    """Airflow API客户端，用于查询和触发DAG运行"""
    
    def __init__(self, base_url: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None):
        """
        初始化Airflow客户端
        
        Args:
            base_url: Airflow REST API的基础URL，如不提供则从环境变量AIRFLOW_URL读取
            username: Airflow API的用户名，如不提供则从环境变量AIRFLOW_USERNAME读取
            password: Airflow API的密码，如不提供则从环境变量AIRFLOW_PASSWORD读取
        """
        self.base_url = base_url or os.environ.get('AIRFLOW_URL', '')
        self.username = username or os.environ.get('AIRFLOW_USERNAME', '')
        self.password = password or os.environ.get('AIRFLOW_PASSWORD', '')
        
        # 确保base_url格式正确
        self.base_url = self.base_url.rstrip('/')
        if not self.base_url.endswith('/api/v1'):
            self.base_url += '/api/v1'
    
    def get_dag_runs(self, dag_id: str, limit: int = 100, order_by: str = '-start_date') -> Dict[str, Any]:
        """
        获取指定DAG的运行记录
        
        Args:
            dag_id: DAG的ID
            limit: 返回结果的最大数量
            order_by: 排序字段，默认按开始时间降序排序，前缀'-'表示降序
            
        Returns:
            包含DAG运行记录的字典
        """
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        params = {"limit": limit, "order_by": order_by}
        
        response = requests.get(
            url, 
            params=params, 
            auth=(self.username, self.password) if self.username and self.password else None
        )
        response.raise_for_status()
        
        return response.json()
    
    def trigger_dag_run(
        self, 
        dag_id: str, 
        dag_run_id: Optional[str] = None,
        conf: Optional[Dict[str, Any]] = None,
        logical_date: Optional[str] = None,
        note: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        触发一个DAG运行
        
        Args:
            dag_id: DAG的ID
            dag_run_id: 可选的DAG运行ID，如果不提供，Airflow会自动生成
            conf: 可选的配置参数
            logical_date: 可选的逻辑执行日期，格式为ISO8601
            note: 可选的备注
            
        Returns:
            包含新创建的DAG运行信息的字典
        """
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        
        # 构建请求体
        payload = {}
        if dag_run_id:
            payload["dag_run_id"] = dag_run_id
        if conf:
            payload["conf"] = conf
        if logical_date:
            payload["logical_date"] = logical_date
        if note:
            payload["note"] = note
        
        response = requests.post(
            url, 
            json=payload, 
            auth=(self.username, self.password) if self.username and self.password else None
        )
        response.raise_for_status()
        
        return response.json()


# 示例用法
if __name__ == "__main__":
    # 创建Airflow客户端 - 会自动从环境变量读取配置
    airflow = AirflowClient()
    
    # 获取DAG运行记录
    dag_runs = airflow.get_dag_runs(dag_id="xhs_notes_collector")
    print(f"找到 {dag_runs['total_entries']} 个DAG运行记录")